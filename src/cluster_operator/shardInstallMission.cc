/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "shardInstallMission.h"
#include "request_framework/handleRequestThread.h"
#include "zettalib/op_log.h"
#include "zettalib/op_mysql.h"
#include "zettalib/tool_func.h"
#include "json/json.h"
#include <unistd.h>

using namespace kunlun;
using namespace std;

extern HandleRequestThread *g_request_handle_thread;
extern GlobalNodeChannelManager *g_node_channel_manager;
extern std::string meta_group_seeds;
extern std::string meta_svr_user;
extern std::string meta_svr_pwd;

void KAddShardMission::setInstallInfo(vector<struct InstanceInfoSt> &infos) {
  auto iter = infos.begin();
  for (; iter != infos.end(); iter++) {
    std::string key =
        kunlun::string_sprintf("%s_%lu", iter->ip.c_str(), iter->port);
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

void KAddShardMission::setInstallInfoOneByOne(
    struct InstanceInfoSt &install_info) {
  std::string key = kunlun::string_sprintf("%s_%lu", install_info.ip.c_str(),
                                           install_info.port);
  auto found = install_infos_.find(key);
  if (found != install_infos_.end()) {
    KLOG_WARN("install info related {} already exists, the original one ({}) "
              "will be replace by the new one ({})",
              key, found->second.to_string(), install_info.to_string());
  }
  install_infos_[key] = install_info;
  return;
}

void KAddShardMission::setGeneralRequestId(string request_id) {
  return set_request_unique_id(request_id);
}

bool KAddShardMission::InitFromInternal() {
  Json::Value doc;
  FillCommonJobJsonDoc(doc);
  doc["job_type"] = "shard_install";
  Json::Value para;
  if (install_infos_.size() == 0) {
    setErr("Shard install info not initialized");
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

void KAddShardMission::CompleteGeneralJobInfo() {
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
           "job_type='shard_install',job_info='%s' where id = %s",
           job_info.c_str(), get_request_unique_id().c_str());

  ret = conn_ptr->ExcuteQuery(sql, &result);
  if (ret <= 0) {
    KLOG_ERROR("update cluster_general_job_log faild: {}", conn_ptr->getErr());
    return;
  }
  KLOG_INFO("update cluster_general_job_log success: {}", sql);
}

// installing mysql parallelized
bool KAddShardMission::ArrangeRemoteTask() {
  std::string task_spec_info = kunlun::string_sprintf(
      "install_mysql_parallel_%s", get_request_unique_id().c_str());
  kunlun::MySQLInstallRemoteTask *task = new kunlun::MySQLInstallRemoteTask(
      task_spec_info.c_str(), get_request_unique_id());
  auto iter = install_infos_.begin();
  for (; iter != install_infos_.end(); iter++) {
    bool ret = task->InitInstanceInfoOneByOne(
        iter->second.ip, iter->second.port, iter->second.exporter_port,
        iter->second.mgr_port, iter->second.xport,
        iter->second.mgr_seed, iter->second.mgr_uuid,
        iter->second.innodb_buffer_size_M,
        iter->second.db_cfg, iter->second.role);
    if (!ret) {
      KLOG_ERROR("{}", task->getErr());
      delete task;
      return false;
    }
  }
  get_task_manager()->PushBackTask(task);
  return true;
}

struct InstanceInfoSt KAddShardMission::getMasterInfo() {
  auto iter = install_infos_.begin();
  for (; iter != install_infos_.end(); iter++) {
    if (iter->second.role == "master") {
      break;
    }
  }
  return iter->second;
}

bool KAddShardMission::doChangeMasterOnSlave(struct InstanceInfoSt &master,
                                             struct InstanceInfoSt &slave) {
  std::string sql = string_sprintf(
      "stop slave; change master to MASTER_AUTO_POSITION = 1, "
      "MASTER_HOST='%s' , MASTER_PORT=%u, MASTER_USER='repl' "
      ", MASTER_PASSWORD='repl_pwd',MASTER_CONNECT_RETRY=1 "
      ",MASTER_RETRY_COUNT=1000 , MASTER_HEARTBEAT_PERIOD=1 "
      "for CHANNEL 'kunlun_repl'; start slave for CHANNEL 'kunlun_repl'",
      master.ip.c_str(), master.port);
  kunlun::MysqlConnectionOption option;
  option.ip = slave.ip;
  option.port_num = slave.port;
  option.port_str = std::to_string(slave.port);
  option.user = meta_svr_user;
  option.password = meta_svr_pwd;
  kunlun::MysqlResult result;
  kunlun::MysqlConnection conn(option);
  int ret = conn.Connect();
  if (!ret) {
    setErr("%s", conn.getErr());
    return false;
  }

  int retry_count = 10;
  while ((ret = conn.ExcuteQuery(sql.c_str(), &result)) != 0) {
    sleep(1);
    setErr("%s", conn.getErr());
    retry_count--;
    if (retry_count <= 0) {
      KLOG_ERROR("After 10 times retry, change master stmt still failed: {}",
                 conn.getErr());
      return false;
    }
  }
  KLOG_INFO("Exec SQL success: {}", sql);
  return true;
}

bool KAddShardMission::enableMaster(struct InstanceInfoSt &master) {
  std::string sql =
      "set global super_read_only = false; set global read_only=false";
  kunlun::MysqlConnectionOption option;
  option.ip = master.ip;
  option.port_num = master.port;
  option.port_str = std::to_string(master.port);
  option.user = meta_svr_user;
  option.password = meta_svr_pwd;
  kunlun::MysqlResult result;
  kunlun::MysqlConnection conn(option);
  int ret = conn.Connect();
  if (!ret) {
    setErr("%s", conn.getErr());
    return false;
  }

  int retry_count = 10;
  while ((ret = conn.ExcuteQuery(sql.c_str(), &result)) != 0) {
    sleep(1);
    setErr("%s", conn.getErr());
    retry_count--;
    if (retry_count <= 0) {
      KLOG_ERROR(
          "After 10 times retry, Enable Master (%s) stmt still failed: {}", sql,
          conn.getErr());
      return false;
    }
  }
  KLOG_INFO("Exec SQL success: {}", sql);
  return true;
}

bool KAddShardMission::setUpRBR() {
  auto master = getMasterInfo();
  auto iter = install_infos_.begin();
  for (; iter != install_infos_.end(); iter++) {
    if (iter->second.role != "master") {
      if (!doChangeMasterOnSlave(master, iter->second)) {
        return false;
      }
    }
  }
  return enableMaster(master);
}

bool KAddShardMission::setUpMgr() {
  return true;
}

bool KAddShardMission::TearDownMission() {
  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    KLOG_ERROR("{}", conn.getErr());
  }
  kunlun::MysqlResult result;

  bool ok = get_task_manager()->ok();
  if (!ok) {
    KLOG_ERROR("shard_install mission failed in DEAL phase, TearDown Phase "
               "will skip, and Rollback the installation");
    rollBackShardInstall();
  }
  char sql[10240] = {'\0'};
  snprintf(sql, 10240,
           "update kunlun_metadata_db.cluster_general_job_log set status='%s' "
           ",memo = '%s' where id = %s",
           ok ? "ongoing" : "failed",
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

  bool ret1 = setUpMgr();
  // SetUp rbr
  //bool ret1 = setUpRBR();
  if (!ret1) {
    result.Clean();
    bzero((void *)sql, 10240);
    snprintf(sql, 10240,
             "update kunlun_metadata_db.cluster_general_job_log set memo = "
             "'%s' where id = %s",
             getErr(), get_request_unique_id().c_str());
    ret = conn_ptr->ExcuteQuery(sql, &result);
    if (ret <= 0) {
      KLOG_ERROR("{}", conn_ptr->getErr());
      KLOG_ERROR("Setup RBR replication faild, rollback the installation" );
      rollBackShardInstall();
    }

    Json::Value root;
    root["status"] = "failed";
    root["request_id"] = get_request_unique_id();
    root["error_code"] = ADD_SHARD_SET_RBR_SYNC_ERROR;
    root["error_info"] = std::string(getErr());
    Json::FastWriter writer;
    writer.omitEndingLineFeed();
    std::string result = writer.write(root);
    SetSerializeResult(result);

  } else {
    result.Clean();
    bzero((void *)sql, 10240);
    snprintf(sql, 10240,
             "update kunlun_metadata_db.cluster_general_job_log set status = "
             "'done' where id = %s",
             get_request_unique_id().c_str());
    ret = conn_ptr->ExcuteQuery(sql, &result);
    if (ret <= 0) {
      KLOG_ERROR("{}", conn_ptr->getErr());
    }
  }
  return ret1;
}

bool KAddShardMission::SetUpMisson() {
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

void KAddShardMission::ReportStatus() { return; }

void KAddShardMission::rollBackShardInstall() {
  KLOG_INFO("Add shard mission faild, rollback whole mission by dispatch the "
            "Delete Shard mission");
  auto delmission = new kunlun::KDelShardMission();
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

void KDelShardMission::setInstallInfo(vector<struct InstanceInfoSt> &infos) {
  auto iter = infos.begin();
  for (; iter != infos.end(); iter++) {
    std::string key =
        kunlun::string_sprintf("%s_%lu", iter->ip.c_str(), iter->port);
    auto found = install_infos_.find(key);
    if (found != install_infos_.end()) {
      KLOG_WARN(
          "uninstall info related {} already exists, the original one ({}) "
          "will be replace by the new one ({})",
          key, found->second.to_string(), iter->to_string());
    }
    install_infos_[key] = (*iter);
  }
  return;
}

void KDelShardMission::setInstallInfoOneByOne(
    struct InstanceInfoSt &install_info) {
  std::string key = kunlun::string_sprintf("%s_%lu", install_info.ip.c_str(),
                                           install_info.port);
  auto found = install_infos_.find(key);
  if (found != install_infos_.end()) {
    KLOG_WARN("uninstall info related {} already exists, the original one ({}) "
              "will be replace by the new one ({})",
              key, found->second.to_string(), install_info.to_string());
  }
  install_infos_[key] = install_info;
  return;
}

void KDelShardMission::setGeneralRequestId(string request_id) {
  return set_request_unique_id(request_id);
}

bool KDelShardMission::InitFromInternal() {
  Json::Value doc;
  FillCommonJobJsonDoc(doc);
  doc["job_type"] = "shard_uninstall";
  Json::Value para;
  if (install_infos_.size() == 0) {
    setErr("Shard install info not initialized");
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

void KDelShardMission::CompleteGeneralJobInfo() {
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
           "job_type='shard_uninstall',job_info='%s' where id = %s",
           job_info.c_str(), get_request_unique_id().c_str());

  ret = conn_ptr->ExcuteQuery(sql, &result);
  if (ret <= 0) {
    KLOG_ERROR("update cluster_general_job_log faild: {}", conn_ptr->getErr());
    return;
  }
  KLOG_INFO("update cluster_general_job_log success: {}", sql);
}

// installing mysql parallelized
bool KDelShardMission::ArrangeRemoteTask() {
  std::string task_spec_info = kunlun::string_sprintf(
      "uninstall_mysql_parallel_%s", get_request_unique_id().c_str());
  kunlun::MySQLUninstallRemoteTask *task = new kunlun::MySQLUninstallRemoteTask(
      task_spec_info.c_str(), get_request_unique_id());
  auto iter = install_infos_.begin();
  for (; iter != install_infos_.end(); iter++) {
    bool ret = task->InitInstanceInfoOneByOne(
        iter->second.ip, iter->second.port, iter->second.exporter_port, 
        iter->second.mgr_port, iter->second.xport,
        iter->second.innodb_buffer_size_M);
    if (!ret) {
      KLOG_ERROR("{}", task->getErr());
      delete task;
      return false;
    }
  }
  get_task_manager()->PushBackTask(task);
  return true;
}

bool KDelShardMission::TearDownMission() {
  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    KLOG_ERROR("{}", conn.getErr());
  }
  kunlun::MysqlResult result;

  bool ok = get_task_manager()->ok();
  if (!ok) {
    KLOG_ERROR("shard_uninstall mission failed in DEAL phase, TearDown Phase "
               "will skip");
  }
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

bool KDelShardMission::SetUpMisson() {
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

void KDelShardMission::ReportStatus() { return; }
