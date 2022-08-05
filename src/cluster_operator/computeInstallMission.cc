/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "computeInstallMission.h"
#include "request_framework/handleRequestThread.h"
#include "zettalib/op_log.h"
#include "zettalib/op_mysql.h"
#include "zettalib/tool_func.h"
#include "json/json.h"

using namespace kunlun;
using namespace std;

extern HandleRequestThread *g_request_handle_thread;
extern GlobalNodeChannelManager *g_node_channel_manager;
extern std::string meta_group_seeds;
extern std::string meta_svr_user;
extern std::string meta_svr_pwd;

void KComputeInstallMission::setInstallInfo(vector<struct ComputeInstanceInfoSt> &infos) {
  auto iter = infos.begin();
  for (; iter != infos.end(); iter++) {
    std::string key =
        kunlun::string_sprintf("%s_%lu", iter->ip.c_str(), iter->pg_port);
    auto found = install_infos_.find(key);
    if (found != install_infos_.end()) {
      KLOG_WARN("install info related {} already exists, the original one ({}) "
                "will be replace by the new one ({})",
                key, found->second.to_string(), iter->to_string());
    }
    install_infos_[key] = (*iter);
  }
  return;
}

void KComputeInstallMission::setInstallInfoOneByOne(
    struct ComputeInstanceInfoSt &install_info) {
  std::string key = kunlun::string_sprintf("%s_%lu", install_info.ip.c_str(),
                                           install_info.pg_port);
  auto found = install_infos_.find(key);
  if (found != install_infos_.end()) {
    KLOG_WARN("install info related {} already exists, the original one ({}) "
              "will be replace by the new one ({})",
              key, found->second.to_string(), install_info.to_string());
  }
  install_infos_[key] = install_info;
  return;
}

void KComputeInstallMission::setGeneralRequestId(string request_id) {
  return set_request_unique_id(request_id);
}

bool KComputeInstallMission::InitFromInternal() {
  Json::Value doc;
  FillCommonJobJsonDoc(doc);
  doc["job_type"] = "compute_install";
  Json::Value para;
  if (install_infos_.size() == 0) {
    setErr("Computer install info not initialized");
    return false;
  }
  auto iter = install_infos_.begin();
  for (; iter != install_infos_.end(); iter++) {
    para.append(iter->second.to_string());
  }
  doc["paras"] = para;
  set_body_json_document(doc);
  CompleteGeneralJobInfo();
  return true;
}

void KComputeInstallMission::CompleteGeneralJobInfo() {
  // set job_type,job_info
  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    KLOG_ERROR("open shard connection for metadata cluster faild: {}",
               conn.getErr());
    return;
  }

  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  std::string job_info = writer.write(get_body_json_document());

  auto conn_ptr = conn.get_master();
  kunlun::MysqlResult result;
  char sql[8192] = {'\0'};
  snprintf(sql, 8192,
           "update kunlun_metadata_db.cluster_general_job_log set "
           "job_type='compute_install',job_info='%s' where id = %s",
           job_info.c_str(), get_request_unique_id().c_str());

  ret = conn_ptr->ExcuteQuery(sql, &result);
  if (ret <= 0) {
    KLOG_ERROR("update cluster_general_job_log faild: {}", conn_ptr->getErr());
    return;
  }
  KLOG_INFO("update cluster_general_job_log success: {}", sql);
}

// installing mysql parallelized
bool KComputeInstallMission::ArrangeRemoteTask() {
  std::string task_spec_info = kunlun::string_sprintf(
      "install_postgres_parallel_%s", get_request_unique_id().c_str());
  kunlun::PostgresInstallRemoteTask *task =
      new kunlun::PostgresInstallRemoteTask(task_spec_info.c_str(), get_request_unique_id());
  auto iter = install_infos_.begin();
  for (; iter != install_infos_.end(); iter++) {
    bool ret = task->InitInstanceInfoOneByOne(iter->second);
    if (!ret) {
      KLOG_ERROR("{}", task->getErr());
      delete task;
      return false;
    }
  }
  get_task_manager()->PushBackTask(task);
  return true;
}



bool KComputeInstallMission::TearDownMission() {
  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    KLOG_ERROR("{}", conn.getErr());
  }
  kunlun::MysqlResult result;

  bool ok = get_task_manager()->ok();
  if (!ok) {
    KLOG_ERROR(
        "compute_install mission failed in DEAL phase, TearDown Phase will skip");
    rollBackComputeInstall();
  }
  KLOG_INFO("{}", get_task_manager()->serialized_result_);

  char sql[10240] = {'\0'};
  snprintf(sql, 10240,
           "update kunlun_metadata_db.cluster_general_job_log set status='%s' "
           ",memo = '%s' where id = %s",
           ok ? "done" : "failed",
           get_task_manager()->serialized_result_.c_str(),
           get_request_unique_id().c_str());
  auto conn_ptr = conn.get_master();
  ret = conn_ptr->ExcuteQuery(sql, &result);
  if (ret <= 0) {
    KLOG_ERROR("{}", conn_ptr->getErr());
  }
  if (!ok) {
    return false;
  }
  return true;
}

bool KComputeInstallMission::SetUpMisson() {
  // mark job info is ongoing
  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    setErr("%s", conn.getErr());
    return ret;
  }
  kunlun::MysqlResult result;
  char sql[1024] = {'\0'};
  snprintf(sql, 1024,
           "update kunlun_metadata_db.cluster_general_job_log set status = "
           "'ongoing' where id = %s",
           get_request_unique_id().c_str());

  auto conn_ptr = conn.get_master();
  ret = conn_ptr->ExcuteQuery(sql, &result);
  if (ret <= 0) {
    setErr("Execute Query faild: %s", conn_ptr->getErr());
    return false;
  }
  KLOG_INFO("Execute Query successfully: {}", sql);
  return true; 
}

void KComputeInstallMission::ReportStatus() { return; }

void KComputeInstallMission::rollBackComputeInstall(){
  KLOG_INFO("Add Compute mission faild, rollback whole mission by dispatch the "
            "Delete Compute mission");
  auto delmission = new kunlun::KDelComputeMission();
  auto iter = install_infos_.begin();
  for (; iter != install_infos_.end(); iter++) {
    delmission->setInstallInfoOneByOne(iter->second);
  }
  delmission->InitFromInternal();
  // dispatch the rollback mission
  ObjectPtr<ClusterRequest> base_request(dynamic_cast<ClusterRequest*>(delmission));
  g_request_handle_thread->DispatchRequest(base_request);
  return;
}


void KDelComputeMission::setInstallInfo(vector<struct ComputeInstanceInfoSt> &infos) {
  auto iter = infos.begin();
  for (; iter != infos.end(); iter++) {
    std::string key =
        kunlun::string_sprintf("%s_%lu", iter->ip.c_str(), iter->pg_port);
    auto found = install_infos_.find(key);
    if (found != install_infos_.end()) {
      KLOG_WARN("install info related {} already exists, the original one ({}) "
                "will be replace by the new one ({})",
                key, found->second.to_string(), iter->to_string());
    }
    install_infos_[key] = (*iter);
  }
  return;
}

void KDelComputeMission::setInstallInfoOneByOne(
    struct ComputeInstanceInfoSt &install_info) {
  std::string key = kunlun::string_sprintf("%s_%lu", install_info.ip.c_str(),
                                           install_info.pg_port);
  auto found = install_infos_.find(key);
  if (found != install_infos_.end()) {
    KLOG_WARN("install info related {} already exists, the original one ({}) "
              "will be replace by the new one ({})",
              key, found->second.to_string(), install_info.to_string());
  }
  install_infos_[key] = install_info;
  return;
}

void KDelComputeMission::setGeneralRequestId(string request_id) {
  return set_request_unique_id(request_id);
}

bool KDelComputeMission::InitFromInternal() {
  Json::Value doc;
  FillCommonJobJsonDoc(doc);
  doc["job_type"] = "compute_uninstall";
  Json::Value para;
  if (install_infos_.size() == 0) {
    setErr("Computer uninstall info not initialized");
    return false;
  }
  auto iter = install_infos_.begin();
  for (; iter != install_infos_.end(); iter++) {
    para.append(iter->second.to_string());
  }
  doc["paras"] = para;
  set_body_json_document(doc);
  CompleteGeneralJobInfo();
  return true;
}

void KDelComputeMission::CompleteGeneralJobInfo() {
  // set job_type,job_info
  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    KLOG_ERROR("open shard connection for metadata cluster faild: {}",
               conn.getErr());
    return;
  }

  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  std::string job_info = writer.write(get_body_json_document());

  auto conn_ptr = conn.get_master();
  kunlun::MysqlResult result;
  char sql[8192] = {'\0'};
  snprintf(sql, 8192,
           "update kunlun_metadata_db.cluster_general_job_log set "
           "job_type='compute_uninstall',job_info='%s' where id = %s",
           job_info.c_str(), get_request_unique_id().c_str());

  ret = conn_ptr->ExcuteQuery(sql, &result);
  if (ret <= 0) {
    KLOG_ERROR("update cluster_general_job_log faild: {}", conn_ptr->getErr());
    return;
  }
  KLOG_INFO("update cluster_general_job_log success: {}", sql);
}

// uninstalling postgres parallelized
bool KDelComputeMission::ArrangeRemoteTask() {
  std::string task_spec_info = kunlun::string_sprintf(
      "uninstall_postgres_parallel_%s", get_request_unique_id().c_str());
  kunlun::PostgresUninstallRemoteTask *task =
      new kunlun::PostgresUninstallRemoteTask(task_spec_info.c_str(), get_request_unique_id());
  auto iter = install_infos_.begin();
  for (; iter != install_infos_.end(); iter++) {
    bool ret = task->InitInstanceInfoOneByOne(iter->second);
    if (!ret) {
      KLOG_ERROR("{}", task->getErr());
      delete task;
      return false;
    }
  }
  get_task_manager()->PushBackTask(task);
  return true;
}

bool KDelComputeMission::TearDownMission() {
  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    KLOG_ERROR("{}", conn.getErr());
  }
  kunlun::MysqlResult result;

  bool ok = get_task_manager()->ok();
  if (!ok) {
    KLOG_ERROR(
        "compute_uninstall mission failed in DEAL phase, TearDown Phase will skip");
    return false;
  }
  KLOG_INFO("{}", get_task_manager()->serialized_result_);
  char sql[10240] = {'\0'};
  snprintf(sql, 10240,
           "update kunlun_metadata_db.cluster_general_job_log set status='%s' "
           ",memo = '%s' where id = %s",
           ok ? "done" : "failed",
           get_task_manager()->serialized_result_.c_str(),
           get_request_unique_id().c_str());
  auto conn_ptr = conn.get_master();
  ret = conn_ptr->ExcuteQuery(sql, &result);
  if (ret <= 0) {
    KLOG_ERROR("{}", conn_ptr->getErr());
  }
  if (!ok) {
    return false;
  }
  return true;
}

bool KDelComputeMission::SetUpMisson() { 
  // mark job info is ongoing
  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    setErr("%s", conn.getErr());
    return ret;
  }
  kunlun::MysqlResult result;
  char sql[1024] = {'\0'};
  snprintf(sql, 1024,
           "update kunlun_metadata_db.cluster_general_job_log set status = "
           "'ongoing' where id = %s",
           get_request_unique_id().c_str());

  auto conn_ptr = conn.get_master();
  ret = conn_ptr->ExcuteQuery(sql, &result);
  if (ret <= 0) {
    setErr("Execute Query faild: %s", conn_ptr->getErr());
    return false;
  }
  KLOG_INFO("Execute Query successfully: {}", sql);
  return true;
}

void KDelComputeMission::ReportStatus() { return; }