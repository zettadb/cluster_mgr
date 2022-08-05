/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#ifndef _CLUSTER_MNG_MISSION_REQUEST_H_
#define _CLUSTER_MNG_MISSION_REQUEST_H_

#include "remoteTask.h"
#include "requestBase.h"

class MissionRequest : public ClusterRequest {
  typedef ClusterRequest super;

public:
  MissionRequest():call_back_(nullptr),cb_context_(nullptr){};
  explicit MissionRequest(Json::Value *doc) : super(doc),call_back_(nullptr),cb_context_(nullptr) {
    task_manager_ = nullptr;
  }
  virtual ~MissionRequest();

  // sync task impl
  virtual bool SetUpSyncTaskImpl() { return SyncTaskImpl(); };
  virtual bool SyncTaskImpl() { return true; };
  virtual int GetErrorCode() { return 0; }
  virtual void SetUpImpl() override final;
  // user should add arrange remote task logic
  virtual bool ArrangeRemoteTask() = 0;
  // user shold add setup logic here
  virtual bool SetUpMisson() = 0;
  void DealRequestImpl() override final;
  virtual void DealLocal();
  virtual void TearDownImpl() override final;
  virtual bool TearDownMission() = 0;

  TaskManager *get_task_manager() { return task_manager_; }

  void SetSerializeResult(const std::string& result);
  void set_cb(void (*call_back)(Json::Value& root, void *)) {
    call_back_ = call_back;
  }
  void set_cb_context(void *ctx) {
    cb_context_ = ctx;
  }
protected:
  void (*call_back_)(Json::Value& root, void *);
  void *cb_context_;

private:
  TaskManager *task_manager_;
};

#endif /*_CLUSTER_MNG_MISSION_REQUEST_H_*/
