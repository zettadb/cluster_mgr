/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "missionRequest.h"
#include "zettalib/op_log.h"

MissionRequest::~MissionRequest() {
  if(task_manager_){
    delete task_manager_;
  }
}

void MissionRequest::SetUpImpl() {
  set_status(kunlun::ON_GOING);
  task_manager_ = new TaskManager();
  bool ret = SetUpMisson();
  if(!ret){
    KLOG_ERROR("SetUpMisson Failed: {}, will skip ArrangeRemoteTask, go to TearDown directly",getErr());
  }
  ret = ArrangeRemoteTask();
  if(!ret){
    KLOG_ERROR("ArrangeRemoteTask Failed: {}",getErr());
  }
  return;
}

void MissionRequest::DealLocal(){
  return ;
}

void MissionRequest::DealRequestImpl() {
  // do the request iterator vec
  auto &task_vec = task_manager_->get_remote_task_vec();
  auto iter = task_vec.begin();
  for (; iter != task_vec.end(); iter++) {
    bool ret = (*iter)->RunTask();
    if (!ret) {
      setErr("%s", (*iter)->getErr());
      KLOG_ERROR("DealRequestImpl Failed: {}", getErr());
      return;
    }
  }
  DealLocal();
  return;
}

void MissionRequest::SetSerializeResult(const std::string& result) {
  auto task_pt = get_task_manager();
  task_pt->SetSerializeResult(result);
}

void MissionRequest::TearDownImpl() {
  // confirm wether the task error occur or not
  auto task_pt = get_task_manager();
  task_pt->SerializeAllResponse();
  if(task_pt->ok()){
    set_status(kunlun::DONE);
    KLOG_INFO("Mission DONE reported during TearDown phase, request_id : {}",get_request_unique_id());
  }
  else{
    set_status(kunlun::FAILED);
    KLOG_ERROR("Mission faied detected during TearDown phase, request_id : {}",get_request_unique_id());
  }

  bool ret = TearDownMission();
  if(!ret){
    KLOG_ERROR("Mission TearDown Execute failed, request_id: {}, info: {}",get_request_unique_id(),getErr());
  }

  if(call_back_){
    Json::Value root;
    Json::Reader reader;
    bool ret = reader.parse(task_pt->serialized_result_, root);
    if(!ret) {
        KLOG_ERROR("json parse {} failed", task_pt->serialized_result_);
        return;
    }
    
    std::string request_id = root["request_id"].asString();
    std::string id = get_request_unique_id();
    KLOG_INFO("request_id {} --- id {}", request_id, id);
    if(request_id == id) {
      (*call_back_)(root, cb_context_);
    }
  }
  return;
}
