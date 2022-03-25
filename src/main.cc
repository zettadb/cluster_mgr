/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "sys_config.h"
#include "global.h"
#include "sys.h"
#include "os.h"
#include "log.h"
#include "job.h"
#include "config.h"
#include "thread_manager.h"
#include <unistd.h>
#include <signal.h>

extern int g_exit_signal;
extern int64_t thread_work_interval;

int main(int argc, char **argv)
{
	Thread main_thd;

	if (argc != 2)
	{
		printf("\nUsage: cluster_mgr /config/file/path/my_cluster_cfg.conf\n");
		return 1;
	}

	mask_signals();

	/*
	  Do not set signal handlers for those to be handled in a dedicated thread,
	  otherwise the handlers will be effective even it's SIG_IGN or SIG_DFL,
	  before the signal is queued for delivery to sigtimedwait() call.
	*/
	handle_signal(SIGALRM, SIG_IGN);
	handle_signal(SIGPIPE, SIG_IGN);
	handle_signal(SIGUSR2, SIG_IGN);
	handle_signal(SIGCHLD, SIG_DFL);
	handle_signal(SIGTTIN, SIG_DFL);
	handle_signal(SIGTTOU, SIG_DFL);
	handle_signal(SIGCONT, SIG_DFL);
	handle_signal(SIGWINCH, SIG_DFL);
	
	if (System::create_instance(argv[1]))
		return 1;

	// waiting for create meta shard or network connected
	while(!Thread_manager::do_exit)
	{
		if (System::get_instance()->get_cluster_mgr_working() && 
			System::get_instance()->setup_metadata_shard() == 0)
		{
			// roll back job because cluster_mgr crash
			System::get_instance()->refresh_shards_from_metadata_server();
			System::get_instance()->refresh_computers_from_metadata_server();
			Job::get_instance()->job_roll_back_check();
			break;
		}
		
		syslog(Logger::ERROR, "setup_metadata_shard fail, waiting ...");
		Thread_manager::get_instance()->sleep_wait(&main_thd, thread_work_interval * 3000);
	}

	// cluster refresh and working
	while (!Thread_manager::do_exit)
	{
		if (System::get_instance()->get_cluster_mgr_working() && 
			System::get_instance()->setup_metadata_shard() == 0)
		{
			System::get_instance()->refresh_shards_from_metadata_server();
			System::get_instance()->refresh_computers_from_metadata_server();
			System::get_instance()->meta_shard_maintenance();
			System::get_instance()->process_recovered_prepared();
		}

		Thread_manager::get_instance()->sleep_wait(&main_thd, thread_work_interval * 1000);
	}

	if (g_exit_signal)
		syslog(Logger::INFO, "Instructed to exit by signal %d.", g_exit_signal);
	else
		syslog(Logger::INFO, "Exiting because of internal error.");

	delete System::get_instance();
}

