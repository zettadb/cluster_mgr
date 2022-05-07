/*
  Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#ifndef _CLUSTER_MISSION_H_
#define _CLUSTER_MISSION_H_
#include "http_server/node_channel.h"
#include "request_framework/missionRequest.h"
#include "zettalib/biodirectpopen.h"
#include "zettalib/errorcup.h"
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
	std::string job_status;
	std::string job_error_code;
  std::string job_error_info;
  Json::Value job_memo_json;

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
  std::vector<std::string> vec_shard_uuid;
  std::string comps_json;

  std::string shard_name;
  std::string comp_name;
  int add_shards;
  int add_comps;
  int add_nodes;
  bool all_shard;

  std::string backup_cluster_name;
  std::string timestamp;
  std::string start_time;
  std::string shard_names;
  std::string cluster_id;
  std::string backup_storage_id;
  std::string backup_storage_str;
  std::vector<std::string> vec_backup_shard_name;
  std::vector<std::vector<Tpye_Ip_Port>> vec_vec_storage_ip_port;
  std::vector<Tpye_Ip_Port> vec_computer_ip_port;

  int nodes_select;
  int task_num;
  int task_wait;
  int task_incomplete;
  int task_step;
  bool missiom_finish;

  enum ClusterStep {GET_PATH_SIZE, INSTALL_STORAGE, INSTALL_COMPUTER, 
                    DELETE_INSTANCE, BACKUP_STORAGE, RESTORE_INSTANCE};

public:
  explicit ClusterMission(Json::Value *doc) : super(doc){};
  ~ClusterMission(){};

  void UpdateMachinePathSize(std::vector<Machine*> &vec_machine, std::string &response);
  void CheckInstanceInstall(std::string &response);
  void CheckInstanceDelete(std::string &response);
  void CheckBackupCluster(std::string &response);
  void CheckInstanceRestore(std::string &response);
  void CreateClusterCallBack(std::string &response);
  void DeleteClusterCallBack(std::string &response);
  void AddShardsCallBack(std::string &response);
  void DeleteShardCallBack(std::string &response);
  void AddCompsCallBack(std::string &response);
  void DeleteCompCallBack(std::string &response);
  void AddNodesCallBack(std::string &response);
  void DeleteNodeCallBack(std::string &response);
  void BackupClusterCallBack(std::string &response);
  void RestoreNewCallBack(std::string &response);

  void renameCluster();
  void createCluster();
  void deleteCluster();
  void addShards();
  void deleteShard();
  void addComps();
  void deleteComp();
  void addNodes();
  void deleteNode();
  void backupCluster();
  void restoreNewCluster();

  void createStorageInfo();
  void createComputerInfo();
  void startClusterInfo();
  bool updateClusterInfo();
  void addShardsInfo();
  void startShardsInfo();
  void addCompsInfo();
  void startCompsInfo();

  void create_shard(std::vector<Tpye_Ip_Port_Paths> &storages, std::string &shard_name);
  void create_shard_nodes(std::vector<Tpye_Ip_Port_Paths> &storages, std::string &shard_name);
  void create_storage(Tpye_Ip_Port_Paths &storage, Json::Value &para);
  void create_comps(std::vector<Tpye_Ip_Port_Paths> &comps, 
                    std::vector<std::string> &vec_comp_name, int comps_id);
  void create_computer(Tpye_Ip_Port_Paths &comp, Json::Value &para);

  void delete_cluster(std::string &cluster_name);
  void delete_storage(Tpye_Ip_Port &storage);
  void delete_computer(Tpye_Ip_Port &computer);
  void stop_shard_node(Tpye_Ip_Port &ip_port);
  void stop_cluster();
  void stop_shard();
  void stop_comp();
  void update_backup_cluster();
  void update_backup_nodes();
  void backup_nodes();

  void backup_cluster();
  bool backup_shard_node(std::string &cluster_id, Tpye_Shard_Id_Ip_Port_Id &shard_id_ip_port_id);
  void restoreCluster();
  bool restore_storage(std::string &shard_name, Tpye_Ip_Port &ip_port);
  bool restore_computer(std::string &shard_map, std::string &meta_str, Tpye_Ip_Port &ip_port);
  void updateRestoreInfo();
  void restoreNodes();
  void updateRestoreNodesInfo();

  bool insert_roll_back_record(Json::Value &para);
  bool delete_roll_back_record();
  bool roll_back_record();
  bool system_cmd(std::string &cmd);
  void get_uuid(std::string &uuid);
  void get_timestamp(std::string &timestamp);
  void get_datatime(std::string &datatime);
  bool save_file(std::string &path, const char *buf);
  void generate_cluster_name();
  bool get_cluster_info(std::string &cluster_name);
  bool create_program_path();
  void get_user_name();

  bool update_operation_record();
  virtual bool ArrangeRemoteTask() override final;
  virtual bool SetUpMisson() override { return true; }
  virtual bool TearDownMission() override { 
    if(!missiom_finish) {
      job_status = "failed";
      update_operation_record();
      delete_roll_back_record();
      missiom_finish = true;
    }
    System::get_instance()->set_cluster_mgr_working(true);
    return true; 
  }
  virtual bool FillRequestBodyStImpl() override { return true; }
  virtual void ReportStatus() override {}
};
} // namespace kunlun
#endif /*_CLUSTER_MISSION_H_*/
