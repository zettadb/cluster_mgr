/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "func_timer.h"

namespace kunlun
{

uint64_t KLTimer::AddTimerUnit(CallCB cb, void* arg, int ss) {
    KlWrapGuard<KlWrapMutex> guard(tu_mux_);

    return (InnerTimerUnit(cb, arg, ss, false));  
}

uint64_t KLTimer::AddLoopTimerUnit(CallCB cb, void* arg, int ss) {
    KlWrapGuard<KlWrapMutex> guard(tu_mux_);
    return (InnerTimerUnit(cb, arg, ss, true));    
}

uint64_t KLTimer::InnerTimerUnit(CallCB cb, void* arg, int ss, bool isLoop) {
    TimerUnitPtr s(new STimeUnit);
    s->ss_ = ss;
    s->dt_.reset(new boost::asio::deadline_timer(ioService_, boost::posix_time::seconds(ss)));
    s->cb_ = cb;
    s->arg_ = arg;
    ++tid_;

    TimerUnit_map_[tid_.load()] = s;
    s->dt_->async_wait(boost::bind(&KLTimer::TimerProcess, this, tid_.load(), arg, isLoop,
                boost::asio::placeholders::error));
    return tid_.load();
}

bool KLTimer::DelTimerUnit(uint64_t id) {
    KlWrapGuard<KlWrapMutex> guard(tu_mux_);
    auto it = TimerUnit_map_.find(id);
    if( it != TimerUnit_map_.end()) {
        it->second->dt_->cancel();
        TimerUnit_map_.erase(id);
    }
    return true;
}

void *KLTimer::GetTimerUnitArg(uint64_t id) {
    KlWrapGuard<KlWrapMutex> guard(tu_mux_);
    auto it = TimerUnit_map_.find(id);
    if(it != TimerUnit_map_.end()) 
        return it->second->arg_;

    return nullptr;
}

int KLTimer::run() {
    ioService_.run();
    return 0;
}

void KLTimer::TimerProcess(uint64_t id, void* arg, bool isLoopTimer, const boost::system::error_code& e) {
    if(e == boost::asio::error::operation_aborted)
        return;

    TimerUnitPtr TU_Ptr;
    {
        KlWrapGuard<KlWrapMutex> guard(tu_mux_);
        auto it = TimerUnit_map_.find(id);
        if(it != TimerUnit_map_.end()) {
            TU_Ptr = it->second;
            if(!isLoopTimer)
                TimerUnit_map_.erase(id);
        }
    }

    if(TU_Ptr) {
        if(TU_Ptr->cb_)
            TU_Ptr->cb_(arg);

        if(isLoopTimer) {
            TU_Ptr->dt_->expires_at(TU_Ptr->dt_->expires_at() + boost::posix_time::seconds(TU_Ptr->ss_));
            TU_Ptr->dt_->async_wait(boost::bind(&KLTimer::TimerProcess, this, id, 
                    arg, true, boost::asio::placeholders::error));
        }
    }
}

}