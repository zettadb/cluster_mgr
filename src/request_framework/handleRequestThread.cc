/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "handleRequestThread.h"
#include "unistd.h"
#include "string.h"

void *HandleRequestThread::AsyncDealRequest(void *arg)
{
  ClusterRequest *request = static_cast<ClusterRequest *>(arg);
  request->DealRequest();
  request->TearDown();
  syslog(Logger::DEBUG1, "request: %d already finished", request->get_request_unique_id());
  // delete arg;
  return nullptr;
}

void HandleRequestThread::DispatchRequest(ClusterRequest *request_base)
{
  // always successfully
  while (!request_queue_.push(request_base))
  {
    syslog(Logger::ERROR,
           "can not dispatch the request, maybe the request_queue_ is full");
    sleep(1);
  }
  return;
}

int HandleRequestThread::run()
{
  while (m_state)
  {
    ClusterRequest *request_base = nullptr;
    int ret = request_queue_.pop(request_base);
    if (!ret)
    {
      usleep(20 * 100);
      continue;
    }
    // request_base should not be null
    assert(request_base);

    // deal the request
    request_base->SetUp();

    // next two phase will execute in bthread
    ret = 0;
    do
    {
      bthread_t bid;
      ret = bthread_start_background(
          &bid, nullptr, &(HandleRequestThread::AsyncDealRequest),
          (void *)request_base);
      if (ret != 0)
      {
        syslog(Logger::ERROR, "start bthread to async deal Request failed: %s",
               strerror(ret));
      }
    } while (ret != 0);
  }
  return 0;
}
