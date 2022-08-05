/*
   Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#pragma once
#include <cassert>
#include "bthread/bthread.h"

namespace kunlun
{

class KlWrapMutex {
public:
    KlWrapMutex(const bthread_mutexattr_t* attr = NULL) {
        bthread_mutex_init(&mutex_, attr);
    }

    virtual ~KlWrapMutex() {
        bthread_mutex_destroy(&mutex_);
    }

    int Lock() {
        return bthread_mutex_lock(&mutex_);
    }
    int UnLock() {
        return bthread_mutex_unlock(&mutex_);
    }
    int TryLock() {
        return bthread_mutex_trylock(&mutex_);
    }

    KlWrapMutex(const KlWrapMutex&) = delete;
    KlWrapMutex &operator= (const KlWrapMutex&) = delete;
private:
    bthread_mutex_t mutex_;
};

template <class Lock>
class KlWrapGuard {
public:
    KlWrapGuard(Lock& lock) : lock_(&lock), isLock_(-1) {
        AcquireLock();
    }

    KlWrapGuard(Lock* lock) : lock_(lock), isLock_(-1) {
        AcquireLock();
    }

    virtual ~KlWrapGuard() {
        ReleaseLock();
    }

    int AcquireLock() {
        if(lock_)
            return isLock_ = lock_->Lock();
        else
            return 0;
    }

    int ReleaseLock() {
        if(isLock_ != 0)
            return 0;
        else {
            isLock_ = -1;
            if(lock_)
                return lock_->UnLock();
            else
                return 0;
        }
    }

    bool IsLocked() const {
        return (isLock_ == 0);
    }

    KlWrapGuard(const KlWrapGuard &) = delete;
    KlWrapGuard& operator= (const KlWrapGuard &) = delete;
private:
    Lock *lock_;
    int isLock_;
};

class KlWrapCond {
public:
    KlWrapCond() {
        bthread_mutex_init(&mutex_, NULL);
        bthread_cond_init(&cond_, NULL);
    }
    virtual ~KlWrapCond() {
        bthread_mutex_destroy(&mutex_);
        bthread_cond_destroy(&cond_);
    }

    int Wait() {
        bthread_mutex_lock(&mutex_);
        int ret = bthread_cond_wait(&cond_, &mutex_);
        bthread_mutex_unlock(&mutex_);
        return ret;
    }

    int WaitWithTime(const struct timespec* abstime) {
        bthread_mutex_lock(&mutex_);
        int ret = bthread_cond_timedwait(&cond_, &mutex_, abstime);
        bthread_mutex_unlock(&mutex_);
        return ret;
    }

    void SignalOne() {
        bthread_mutex_lock(&mutex_);
        bthread_cond_signal(&cond_);
        bthread_mutex_unlock(&mutex_); 
    }

    void SignalAll() {
        bthread_mutex_lock(&mutex_);
        bthread_cond_broadcast(&cond_);
        bthread_mutex_unlock(&mutex_);
    }

    KlWrapCond(const KlWrapCond&) = delete;
    KlWrapCond &operator= (const KlWrapCond&) = delete;

private:
    bthread_cond_t cond_;
    bthread_mutex_t mutex_;
};

} // namespace kunlun
