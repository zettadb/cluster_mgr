/*
  Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#ifndef _MACHINE_MISSION_H_
#define _MACHINE_MISSION_H_
#include "http_server/node_channel.h"
#include "request_framework/missionRequest.h"
#include "machine_new.h"

void Machine_Call_Back(void *);
namespace kunlun {
class MachineMission;
class MachineRemoteTask : public ::RemoteTask {
  typedef ::RemoteTask super;

public:
  explicit MachineRemoteTask(const char *task_spec_info, const char *request_id, MachineMission *mission)
      : super(task_spec_info), unique_request_id_(request_id), mission_(mission) {
        super::Set_call_back(&Machine_Call_Back);
        super::Set_cb_context((void *)this);
      }
  ~MachineRemoteTask() {}
  void SetParaToRequestBody(brpc::Controller *cntl,
                            std::string node_hostaddr) override {
    if (prev_task_ == nullptr) {
      return super::SetParaToRequestBody(cntl, node_hostaddr);
    }
  }

  MachineMission *getMission() { return mission_; }
private:
  std::string unique_request_id_;
  MachineMission *mission_;
};

class MachineMission : public ::MissionRequest {
  typedef MissionRequest super;

private:
  ClusterRequestTypes request_type_;
  Machine_New machine_;
  std::string job_id_;

public:
  explicit MachineMission(Json::Value *doc) : super(doc){};
  ~MachineMission(){};

  Machine_New &getMachine() { return machine_; }
  std::string &getJobId() { return job_id_; }
  ClusterRequestTypes getRequestTypes() { return request_type_; }
  void CreateMachine();
  void UpdateMachine();
  void DeleteMachine();
  virtual bool ArrangeRemoteTask() override final;
  virtual bool SetUpMisson() override { return true; }
  virtual bool TearDownMission() override { return true; }
  virtual bool FillRequestBodyStImpl() override { return true; }
  virtual void ReportStatus() override {}
};
} // namespace kunlun
#endif /*_MACHINE_MISSION_H_*/
