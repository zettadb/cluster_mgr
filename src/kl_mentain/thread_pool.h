/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef _CLUSTER_MGR_THREADPOOL_H_
#define _CLUSTER_MGR_THREADPOOL_H_

#include <vector>
#include <queue>
#include <atomic>
#include <future>
#include <functional>
#include <condition_variable>
#include <thread>
#include <stdexcept>

using namespace std;

namespace kunlun
{

#define  THREADPOOL_MAX_NUM 10

/*
* threadpool 
*/
class CThreadPool {
    using Task = function<void()>;	
	vector<thread> pool_;     
	queue<Task> tasks_;            
	mutex lock_;                   
	condition_variable task_cv_;   
	atomic<bool> run_{ true };     
	std::atomic<int>  idlThrNum_{ 0 };  

public:
	inline CThreadPool(unsigned short size = 4) { addThread(size); }
	virtual ~CThreadPool() {
	    run_=false;
		task_cv_.notify_all(); // wake all threads in pool
		for (thread& thread : pool_) {
			if(thread.joinable())
				thread.join(); 
		}
	}

public:
	template<class F, class... Args>
	auto commit(F&& f, Args&&... args) ->future<decltype(f(args...))>
	{
		if (!run_) {   // check pool status
			future<void> future;
			return future;
		}

		using RetType = decltype(f(args...)); // typename std::result_of<F(Args...)>::type
		auto task = make_shared<packaged_task<RetType()>>(
			std::bind(forward<F>(f), forward<Args>(args)...)
		); 
		future<RetType> future = task->get_future();
        {    
			lock_guard<mutex> lock{ lock_ };
			tasks_.emplace([task](){ 
				(*task)();
			});
		}

		task_cv_.notify_one(); // wake one thread for task
		return future;
	}

	int idlCount() { return idlThrNum_; }
	int thrCount() { return pool_.size(); }

private:
	void addThread(unsigned short size) {
		for (; pool_.size() < THREADPOOL_MAX_NUM && size > 0; --size) {   
			pool_.emplace_back( [this]{ 
				while (run_) {
					Task task; 
					{
						unique_lock<mutex> lock{ lock_ };
						task_cv_.wait(lock, [this]{
								return !run_ || !tasks_.empty();
						}); 
						if (!run_ && tasks_.empty())
							return;
						task = move(tasks_.front()); 
						tasks_.pop();
					}
					idlThrNum_--;
					task();  //execute task;
					idlThrNum_++;
				}
			});
			idlThrNum_++;
		}
	}
};

}
#endif /*_CLUSTER_MGR_THREADPOOL_H_*/
