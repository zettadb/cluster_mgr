/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef _CLUSTER_MGR_FUNC_TIMER_H_
#define _CLUSTER_MGR_FUNC_TIMER_H_
#include <atomic>
#include <unordered_map>
#include "zettalib/errorcup.h"
#include "zettalib/zthread.h"
#include "util_func/kl_mutex.h"
#include "boost/asio/io_service.hpp"
#include "boost/asio/deadline_timer.hpp"
#include "boost/date_time/posix_time/posix_time.hpp"
#include "boost/bind.hpp"
#include "boost/function.hpp"
#include "boost/asio/placeholders.hpp"

namespace kunlun {

typedef void (*CallCB)(void*);

typedef boost::shared_ptr<boost::asio::deadline_timer> DeadlineTimer;

typedef struct  {
    DeadlineTimer dt_;
    CallCB cb_;
    int ss_;
    void* arg_;
} STimeUnit;

typedef boost::shared_ptr<STimeUnit> TimerUnitPtr;

class KLTimer : public ZThread, public ErrorCup {
public:
    KLTimer() : ioWork_(ioService_), tid_(0) {}
    virtual ~KLTimer() {
        ioService_.stop();
    }

    uint64_t AddTimerUnit(CallCB cb, void* arg, int ss);
    uint64_t AddLoopTimerUnit(CallCB cb, void *arg, int ss);
    bool DelTimerUnit(uint64_t id);
    void *GetTimerUnitArg(uint64_t id);
    int run();

private:
    uint64_t InnerTimerUnit(CallCB cb, void* arg, int ss, bool isLoop);

    void TimerProcess(uint64_t id, void* arg, bool isLoopTimer, const boost::system::error_code& e);

private:
    boost::asio::io_service ioService_;
    boost::asio::io_service::work ioWork_;
    KlWrapMutex tu_mux_;
    std::atomic<uint64_t> tid_;
    std::unordered_map<uint64_t, TimerUnitPtr> TimerUnit_map_;
};

}

#endif