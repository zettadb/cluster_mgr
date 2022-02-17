/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "sys_config.h"
#include "global.h"
#include "sys.h"
#include "log.h"
#include "config.h"
#include "thread_manager.h"
#include <signal.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <limits.h>
#include <sys/types.h>

extern "C" void *signal_hander(void *arg);
extern "C" void *thread_func(void*thrdarg);
extern "C" void *thread_func_storage_sync(void*thrdarg);

int64_t num_worker_threads = 3;
int Thread_manager::do_exit = 0;
int64_t thread_work_interval = 3;
int64_t storage_sync_interval = 60;
int64_t commit_log_retention_hours = 24;

Thread_manager* Thread_manager::m_inst = NULL;

Thread_manager::Thread_manager()
{
	pthread_mutexattr_init(&mtx_attr);
	pthread_mutexattr_settype(&mtx_attr, PTHREAD_MUTEX_RECURSIVE);
	pthread_mutex_init(&mtx, &mtx_attr);
	pthread_cond_init(&cond, NULL);

	(void)pthread_attr_init(&thr_attr);
	(void)pthread_attr_setscope(&thr_attr, PTHREAD_SCOPE_SYSTEM);
	(void)pthread_attr_setdetachstate(&thr_attr, PTHREAD_CREATE_JOINABLE);
	
	size_t guardize = 0;
	(void)pthread_attr_getguardsize(&thr_attr, &guardize);
#if defined(__ia64__) || defined(__ia64)
	/*
	Peculiar things with ia64 platforms - it seems we only have half the
	stack size in reality, so we have to double it here
	*/
	guardize = pthread_stack_size;
#endif
	if (0 !=
		pthread_attr_setstacksize(&thr_attr, pthread_stack_size + guardize)) {
		Assert(false);
	}

	m_inst = this;

	start_signal_handler();
	start_worker_thread();
}

Thread_manager::~Thread_manager()
{
	pthread_mutex_destroy(&mtx);
	pthread_cond_destroy(&cond);
	pthread_mutexattr_destroy(&mtx_attr);
	(void)pthread_attr_destroy(&thr_attr);
}

void Thread_manager::sleep_wait(Thread *thrd, int ms)
{
	timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	ts.tv_sec += (ms/1000);

	// ts.tv_nsec may overflow if we simply add ns to it.
	long ns = (ms%1000)*1000000; // nanosecs

	if (LONG_MAX - ts.tv_nsec < ns)
	{
		ts.tv_sec++;
		ts.tv_nsec = ns - (LONG_MAX - ts.tv_nsec);
	}
	else
		ts.tv_nsec += ns;

	Scopped_mutex sm(mtx);
	if (thrd->decr_kicks())
		return;
	pthread_cond_timedwait(&cond, &mtx, &ts);
}


void Thread_manager::wakeup_all()
{
	Scopped_mutex sm(mtx);
	/*
	  When user kicks this process for work, it may be working so accumulate
	  such kicks for them to be effective always. --- When about to sleep if
	  there are more kicks, keep working.
	*/
	for (auto&thd:thrds)
		thd->incr_kicks();

	pthread_cond_broadcast(&cond);
}


void Thread_manager::start_signal_handler() {
  int error;

  /*
    Set main_thread_id so that SIGTERM/SIGQUIT/SIGKILL/SIGUSR2 can interrupt
    the socket listener successfully.
  */
  main_thread_id = pthread_self();

  if ((error = pthread_create(&signal_hdlr_thrd,
                                   &thr_attr, signal_hander, 0))) {
	char errmsg_buf[256];
    syslog(Logger::ERROR,
		"Can not create interrupt handler thread, error: %d, %s",
		error, errno, strerror_r(errno, errmsg_buf, sizeof(errmsg_buf)));
    do_exit = 1;
  }

}

void Thread_manager::start_worker_thread()
{
	int error = 0;

	for (int i = 0; i < num_worker_threads; i++)
	{
		pthread_t hdl;
		Thread *thd = new Thread;
		if ((error = pthread_create(&hdl,
			 &Thread_manager::get_instance()->thr_attr, thread_func, thd)))
		{
			char errmsg_buf[256];
			syslog(Logger::ERROR, "Can not create worker thread, error: %d, %s",
			error, errno, strerror_r(errno, errmsg_buf, sizeof(errmsg_buf)));
			delete thd;
			do_exit = 1;
			break;
		}

		thd->set_pthread_hdl(hdl);
		thrds.push_back(thd);
	}

	//start storage sync thread
	{
		pthread_t hdl;
		Thread *thd = new Thread;
		if ((error = pthread_create(&hdl,
			 &Thread_manager::get_instance()->thr_attr, thread_func_storage_sync, thd)))
		{
			char errmsg_buf[256];
			syslog(Logger::ERROR, "Can not create storage sync thread, error: %d, %s",
			error, errno, strerror_r(errno, errmsg_buf, sizeof(errmsg_buf)));
			delete thd;
			do_exit = 1;
			return;
		}

		thd->set_pthread_hdl(hdl);
		thrds.push_back(thd);
	}
}


void Thread_manager::join_all()
{
	pthread_join(signal_hdlr_thrd, NULL);
	for (auto &i:thrds)
	{
		pthread_join(i->m_hdl, NULL);
		delete i;
	}
}


int g_exit_signal = 0;

/*
Do not set signal handlers for those to be handled in a dedicated thread,
otherwise the handlers will be effective even if it's SIG_IGN or SIG_DFL,
before the signal is queued for delivery to sigtimedwait() call.

Every thread of a process must mask the signals so that we can use a dedicated
thread to handle signals.
*/
void mask_signals()
{
  sigset_t set;
  (void)sigemptyset(&set);
  (void)sigaddset(&set, SIGTERM);
  (void)sigaddset(&set, SIGQUIT);
  (void)sigaddset(&set, SIGINT);
  (void)sigaddset(&set, SIGHUP);
  (void)sigaddset(&set, SIGUSR1);
  sigprocmask(SIG_BLOCK, &set, NULL);
}

/** This thread handles SIGTERM, SIGQUIT and SIGHUP signals. */
extern "C" void *signal_hander(void *arg) {

  sigset_t set;
  (void)sigemptyset(&set);
  (void)sigaddset(&set, SIGTERM);
  (void)sigaddset(&set, SIGQUIT);
  (void)sigaddset(&set, SIGINT);
  (void)sigaddset(&set, SIGHUP);
  (void)sigaddset(&set, SIGUSR1);
  timespec wait_len;
  wait_len.tv_sec = 2;
  wait_len.tv_nsec = 0;

  for (;;) {
    int sig = 0;
    int rc;
    bool error;
    siginfo_t sig_info;

    while ((rc = sigtimedwait(&set, &sig_info, &wait_len)) == -1 && errno == EINTR)
    	;
	if (rc == -1 && errno == EAGAIN) // timedout
	{
		if (Thread_manager::do_exit == 1)
			break;
		continue;
	}

    error = (rc == -1 && errno != EAGAIN);
    if (!error) sig = sig_info.si_signo;
    if (error)
	{
		char errmsg_buf[256];
		syslog(Logger::ERROR,
				"Fatal error in signal handler thread. sigwait/sigwaitinfo returned "
				"error  (%d, %s)\n. Exiting signal handler thread",
				errno, strerror_r(errno, errmsg_buf, sizeof(errmsg_buf)));
		Thread_manager::do_exit = 1;
	}

    if (error || Thread_manager::do_exit) {
      pthread_exit(0);  // Safety
      return NULL;        // Avoid compiler warnings
    }
    switch (sig) {
      case SIGUSR1:
	  	// wake up all
		Thread_manager::get_instance()->wakeup_all();
		break;
      case SIGUSR2:
	    // avoid doing so in another thread since it may be blocked by the main thread
		// and that would cause delay in signal handling. user can send SIGUSR1 so main
		// thread will immediately do the MetadataShard::refresh_shards() work.
	  	//System::get_instance()->refresh_shards_from_metadata_server();
		break;
      case SIGTERM:
      case SIGINT:
      case SIGQUIT:
	    g_exit_signal = sig;
	    Thread_manager::do_exit = 1;
	  	Thread_manager::get_instance()->wakeup_all();
        pthread_exit(nullptr);
        return NULL;  // Avoid compiler warnings
        break;
      case SIGHUP:
	    /*
		  Can't do this to refresh configs, because our process has multiple
		  threads, some of which may be reading the config var when this thread
		  modifies it. We can't make accesses to the string vars atomic.

		  To refresh vars, modify config file and then restart the cluster_manager
		  process.
		*/

	    //Configs::get_instance()->process_config_file(
		//	System::get_instance()->get_config_path());
        break;
      default:
        break; /* purecov: tested */
    }
  }
  return NULL; /* purecov: deadcode */
}


void Thread::run()
{
	pid_t tid = gettid();
	while (!Thread_manager::do_exit)
	{
		if (System::get_instance()->get_cluster_mgr_working() &&
			System::get_instance()->acquire_shard(this, decr_kicks()) && cur_shard)
		{
			syslog(Logger::LOG, "Thread (%p, %d) starts working on shard (%s.%s, %u)",
				this, tid, cur_shard->get_cluster_name().c_str(),
				cur_shard->get_name().c_str(), cur_shard->get_id());
			cur_shard->maintenance();
			syslog(Logger::LOG, "Thread (%p, %d) finishes working on shard (%s.%s, %u)",
				this, tid, cur_shard->get_cluster_name().c_str(),
				cur_shard->get_name().c_str(), cur_shard->get_id());
		}
		else
			Thread_manager::get_instance()->sleep_wait(this, thread_work_interval * 1000);
	}
}

extern "C" void *thread_func(void*thrdarg)
{
	Thread*thd = (Thread*)thrdarg;
	Assert(thd);
	mask_signals();
	thd->run();
	return NULL;
}

extern "C" void *thread_func_storage_sync(void*thrdarg)
{
	Thread*thd = (Thread*)thrdarg;
	Assert(thd);
	mask_signals();

	int commit_log_count = 0;
	int commit_log_count_max = commit_log_retention_hours*60*60/storage_sync_interval;
	
	Thread_manager::get_instance()->sleep_wait(thd, 1000);

	while (!Thread_manager::do_exit)
	{
		if(System::get_instance()->get_cluster_mgr_working())
		{
			System::get_instance()->refresh_storages_info_to_computers();
			System::get_instance()->refresh_storages_info_to_computers_metashard();

			if(commit_log_count++ >= commit_log_count_max)
			{
				commit_log_count = 0;
				System::get_instance()->truncate_commit_log_from_metadata_server();
			}
		}

		Thread_manager::get_instance()->sleep_wait(thd, storage_sync_interval * 1000);
	}
	
	return NULL;
}

