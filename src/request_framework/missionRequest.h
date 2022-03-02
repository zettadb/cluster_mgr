/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#ifndef _CLUSTER_MNG_MISSION_REQUEST_H_
#define _CLUSTER_MNG_MISSION_REQUEST_H_

#include "requestBase.h"
#include "remoteTask.h"

class MissionRequest : public ClusterRequest
{
  typedef ClusterRequest super;

public:
  explicit MissionRequest(Json::Value *doc)
      : super(doc)
  {
    task_manager_ = nullptr;
  }
  virtual ~MissionRequest();
  virtual void SetUpImpl() override final;
  // user should add arrange remote task logic
  virtual bool ArrangeRemoteTask() = 0;
  // user shold add setup logic here
  virtual bool SetUpMisson() = 0;
  void DealRequestImpl() override final;
  virtual void TearDownImpl() override final;
  virtual bool TearDownMission() = 0;

  TaskManager *get_task_manager() { return task_manager_; }

private:
  TaskManager *task_manager_;
};

#endif /*_CLUSTER_MNG_MISSION_REQUEST_H_*/
