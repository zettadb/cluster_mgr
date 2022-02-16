/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "missionRequest.h"

MissionRequest::~MissionRequest() {}

void MissionRequest::SetUpImpl()
{
  task_manager_ = new TaskManager();
  SetUpMisson();
  ArrangeRemoteTask();
}

void MissionRequest::DealRequest()
{
  // do the request iterator vec
  auto &task_vec = task_manager_->get_remote_task_vec();
  auto iter = task_vec.begin();
  for (; iter != task_vec.end(); iter++)
  {
    bool ret = (*iter)->RunTaskImpl();
    if (!ret)
    {
      setErr("%s", (*iter)->getErr());
      syslog(Logger::ERROR,"%s",getErr());
      return;
    }
  }
  return;
}
