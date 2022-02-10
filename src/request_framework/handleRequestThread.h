/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#ifndef _CLUSTER_MGR_HANDLEREQUEST_THREAD_H_
#define _CLUSTER_MGR_HANDLEREQUEST_THREAD_H_

#include "boost/lockfree/spsc_queue.hpp"
#include "requestBase.h"
#include "zettalib/errorcup.h"
#include "zettalib/zthread.h"
#define CLUSTER_MANAGER_MAX_WAIT_REQUEST 10240
using namespace kunlun;
class HandleRequestThread : public ZThread, public ErrorCup {
  static void* AsyncDealRequest(void *arg);
public:
  HandleRequestThread(){};
  ~HandleRequestThread(){};
  void DispatchRequest(ClusterRequest *);
  int run();

private:
  boost::lockfree::spsc_queue<
      ClusterRequest *,
      boost::lockfree::capacity<CLUSTER_MANAGER_MAX_WAIT_REQUEST> >
      request_queue_;
};

#endif /*_CLUSTER_MGR_HANDLEREQUEST_THREAD_H_*/
