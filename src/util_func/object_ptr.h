/*
   Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#pragma once
#include <atomic>
//#include <mutex>
#include <cstdio>
#include "kl_mutex.h"

namespace kunlun
{

#define KL_LIKELY(x) __builtin_expect(!!(x), 1) 
#define KL_UNLIKELY(x) __builtin_expect(!!(x), 0)

class ObjectRef {
public:
    ObjectRef() : ref_(1) {}
    virtual ~ObjectRef() {}

    void IncRef() {
        ref_.fetch_add(1);
    }

    bool ZeroAndDecRef() {
        return (ref_.fetch_sub(1) == 1);
    }

    //just for test.
    int GetRef() const {
        return ref_.load();
    }

    ObjectRef(const ObjectRef&) = delete;
    ObjectRef operator=(const ObjectRef&) = delete;
private:
    std::atomic<int> ref_;
};

template<class T>
class ObjectPtr  {
public:
    ObjectPtr() : ptr_(nullptr) {}
    ObjectPtr(T* ptr) : ptr_(ptr) {}
    
    virtual ~ObjectPtr() {
        if(ptr_ && ptr_->ZeroAndDecRef()) {
            delete ptr_;
            ptr_ = nullptr;
        }
    }

    bool Invalid() const {
        return (!!ptr_);
    }
    ObjectPtr(const ObjectPtr &optr) {
        ptr_ = optr.ptr_;
        if(ptr_)
            ptr_->IncRef();
    }

    ObjectPtr &operator=(const ObjectPtr &optr) {
        ptr_ = optr.ptr_;
        if(ptr_)
            ptr_->IncRef();
        return *this;
    }
    
    T &operator * () {
        return *ptr_;
    }

    T *operator -> () const {
        return ptr_;
    }

    T *GetTRaw() const {
        if(ptr_)
            ptr_->IncRef();
        return ptr_;
    }

    void SetTRaw(T *ptr) {
        ptr_ = ptr;
    }

    //just for test.
    int GetTRef() const {
        return ptr_->GetRef();
    }
private:
    T *ptr_;
};

}