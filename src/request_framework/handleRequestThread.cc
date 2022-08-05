/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "handleRequestThread.h"
#include "unistd.h"
#include "string.h"
#include "zettalib/op_log.h"

using namespace kunlun;

void *HandleRequestThread::AsyncDealRequest(void *arg)
{
  ObjectPtr<ClusterRequest> request(static_cast<ClusterRequest *>(arg));
  request->DealRequest();
  request->TearDown();
  KLOG_INFO("request: {} already finished", request->get_request_unique_id());
  //if(request->get_default_del_request()) {
  //  KLOG_INFO("request: {} delete by HandleRequest", request->get_request_unique_id());
  //  delete request;
  //}
  return nullptr;
}

void HandleRequestThread::DispatchRequest(ObjectPtr<ClusterRequest> request_base)
{
  // always successfully
  while (!request_queue_.push(request_base.GetTRaw()))
  {
    KLOG_ERROR(
           "can not dispatch the request, maybe the request_queue_ is full");
    sleep(1);
  }
  return;
}

int HandleRequestThread::run()
{
  while (m_state)
  {
    ClusterRequest *request = nullptr;
    int ret = request_queue_.pop(request);
    if (!ret)
    {
      usleep(20 * 100);
      continue;
    }
    
    if(!request)
      continue;

    ObjectPtr<ClusterRequest> request_base(request);
    // deal the request
    request_base->SetUp();

    // next two phase will execute in bthread
    ret = 0;
    do
    {
      bthread_t bid;
      ret = bthread_start_background(
          &bid, nullptr, &(HandleRequestThread::AsyncDealRequest),
          (void *)(request_base.GetTRaw()));
      if (ret != 0)
      {
        KLOG_ERROR( "start bthread to async deal Request failed: {}",
               strerror(ret));
      }
    } while (ret != 0);
  }
  return 0;
}
