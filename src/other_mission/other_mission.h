/*
  Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#ifndef _OTHER_MISSION_H_
#define _OTHER_MISSION_H_
#include "http_server/node_channel.h"
#include "request_framework/missionRequest.h"
#include "kl_mentain/global.h"
#include "kl_mentain/sys.h"

void OTHER_Call_Back(void *);
namespace kunlun {
class OtherMission;
class OtherRemoteTask : public ::RemoteTask {
  typedef ::RemoteTask super;

public:
  explicit OtherRemoteTask(const char *task_spec_info, const char *request_id, OtherMission *mission)
      : super(task_spec_info), unique_request_id_(request_id), mission_(mission) {
        super::Set_call_back(&OTHER_Call_Back);
        super::Set_cb_context((void *)this);
      }
  ~OtherRemoteTask() {}
  void SetParaToRequestBody(brpc::Controller *cntl,
                            std::string node_hostaddr) override {
    if (prev_task_ == nullptr) {
      return super::SetParaToRequestBody(cntl, node_hostaddr);
    }
  }

  OtherMission *getMission() { return mission_; }
private:
  std::string unique_request_id_;
  OtherMission *mission_;
};

class OtherMission : public ::MissionRequest {
  typedef MissionRequest super;

public:
  ClusterRequestTypes request_type;
  std::string job_id;

public:
  explicit OtherMission(Json::Value *doc) : super(doc){};
  ~OtherMission(){};

  void ControlInstance();
  void UpdatePrometheus();
  void PostgresExporter();
  void MysqldExporter();

	bool restart_node_exporter(std::vector<std::string> &vec_node);
	bool restart_postgres_exporter(std::string &ip, int port);
	bool restart_mysql_exporter(std::string &ip, int port);
	bool restart_prometheus();
	bool update_prometheus();
  bool save_file(std::string &path, char* buf);

  virtual bool ArrangeRemoteTask() override final;
  virtual bool SetUpMisson() override { return true; }
  virtual bool TearDownMission() override { 
    System::get_instance()->set_cluster_mgr_working(true);
    return true; 
  }
  virtual bool FillRequestBodyStImpl() override { return true; }
  virtual void ReportStatus() override {}
};
} // namespace kunlun
#endif /*_OTHER_MISSION_H_*/
