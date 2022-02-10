/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include <pthread.h>
#include <vector>

class Shard;
class Thread;

void mask_signals();

class Thread_manager
{
public:
	static int do_exit;
	const static unsigned long pthread_stack_size = 65535;
private:
	friend class Thread;
	std::vector<Thread*> thrds;
	pthread_mutex_t mtx;
	pthread_cond_t cond;
	pthread_mutexattr_t mtx_attr;
    pthread_attr_t thr_attr;
	pthread_t main_thread_id;
	pthread_t signal_hdlr_thrd;
	static Thread_manager *m_inst;
	Thread_manager();
public:
	~Thread_manager();
	static Thread_manager *get_instance()
	{
		if (!m_inst) m_inst = new Thread_manager();
		return m_inst;
	}

	void join_all();
	void start_worker_thread();
	void start_signal_handler();
	void sleep_wait(Thread*thrd, int milli_seconds);
	void wakeup_all();
};


class Thread
{
	friend class Thread_manager;
	pthread_t m_hdl;
	Shard *cur_shard;
	int kicks;
public:
	Thread() : cur_shard(NULL), kicks(0)
	{}

	~Thread()
	{

	}

	/*
	  Return true if there are more kicks.
	*/
	bool decr_kicks()
	{
		bool ret = (kicks-- > 0);

		if (kicks < 0) kicks = 0;
		return ret;
	}

	int incr_kicks()
	{
		return ++kicks;
	}

	void set_pthread_hdl(pthread_t hdl)
	{
		Scopped_mutex(Thread_manager::get_instance()->mtx);
		m_hdl = hdl;
	}

	void set_shard(Shard*s)
	{
		Scopped_mutex(Thread_manager::get_instance()->mtx);
		cur_shard = s;
	}

	void run();
};
