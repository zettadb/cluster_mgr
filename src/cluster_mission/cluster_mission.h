/*
  Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#ifndef _CLUSTER_MISSION_H_
#define _CLUSTER_MISSION_H_
#include "http_server/node_channel.h"
#include "request_framework/missionRequest.h"
#include "kl_mentain/machine_info.h"
#include "kl_mentain/global.h"
#include "kl_mentain/sys.h"

void CLuster_Call_Back(void *);
namespace kunlun {
class ClusterMission;
class ClusterRemoteTask : public ::RemoteTask {
  typedef ::RemoteTask super;

public:
  explicit ClusterRemoteTask(const char *task_spec_info, const char *request_id, ClusterMission *mission)
      : super(task_spec_info), unique_request_id_(request_id), mission_(mission), status_(0) {
        super::Set_call_back(&CLuster_Call_Back);
        super::Set_cb_context((void *)this);
      }
  ~ClusterRemoteTask() {}
  void SetParaToRequestBody(brpc::Controller *cntl,
                            std::string node_hostaddr) override {
    if (prev_task_ == nullptr) {
      return super::SetParaToRequestBody(cntl, node_hostaddr);
    }
  }

  ClusterMission *getMission() { return mission_; }
  void setStatus(int status) { status_ = status; }
  int getStatus() { return status_; }
private:
  std::string unique_request_id_;
  ClusterMission *mission_;
  int status_;
};

class ClusterMission : public ::MissionRequest {
  typedef MissionRequest super;

public:
  ClusterRequestTypes request_type;
  std::string job_id;
  std::string user_name;
  std::string cluster_name;
  std::string nick_name;
	Tpye_cluster_info cluster_info;
  std::vector<Machine*> vec_machine;
	std::vector <std::vector<Tpye_Ip_Port_Paths>> vec_shard_storage_ip_port_paths;
	std::vector<Tpye_Ip_Port_Paths> vec_comps_ip_port_paths;
	std::vector<std::string> vec_shard_name;
	std::vector<std::string> vec_comp_name;
  std::vector<std::string> vec_shard_json;
  std::string comps_json;
  int nodes_select;
  int task_num;
  int task_wait;
  int task_incomplete;
  int task_step;

  enum CreateClusterStep {GET_PATH_SIZE, INSTALL_STORAGE, INSTALL_COMPUTER,
                          DELETE_INSTANCE};

public:
  explicit ClusterMission(Json::Value *doc) : super(doc){};
  ~ClusterMission(){};

  void createCluster();
  void deleteCluster();

  void createStorageInfo();
  void createComputerInfo();
  void createClusterInfo();
  void create_shard(std::vector<Tpye_Ip_Port_Paths> &storages, std::string &shard_name);
  void create_storage(Tpye_Ip_Port_Paths &storage, Json::Value &para);
  void create_comps(std::vector<Tpye_Ip_Port_Paths> &comps, 
                                std::vector<std::string> &vec_comp_name, int comps_id);
  void create_computer(Tpye_Ip_Port_Paths &comp, Json::Value &para);
  void delete_cluster(std::string &cluster_name);
  void delete_storage(Tpye_Ip_Port &storage);
  void delete_computer(Tpye_Ip_Port &computer);
  void stop_cluster();

  bool system_cmd(std::string &cmd);
  void get_uuid(std::string &uuid);
  void get_timestamp(std::string &timestamp);
  bool save_file(std::string &path, const char *buf);
  void generate_cluster_name(std::string &cluster_name);
  bool create_program_path();
  void get_user_name();

  virtual bool ArrangeRemoteTask() override final;
  virtual bool SetUpMisson() override { return true; }
  virtual bool TearDownMission() override { return true; }
  virtual bool FillRequestBodyStImpl() override { return true; }
  virtual void ReportStatus() override {}
};
} // namespace kunlun
#endif /*_CLUSTER_MISSION_H_*/