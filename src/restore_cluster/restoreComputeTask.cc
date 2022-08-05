/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "restoreComputeTask.h"
#include "zettalib/op_log.h"
#include "zettalib/op_mysql.h"
#include "zettalib/tool_func.h"
#include "json/json.h"

using namespace kunlun;
using namespace std;

extern GlobalNodeChannelManager *g_node_channel_manager;
extern std::string meta_group_seeds;
extern std::string meta_svr_user;
extern std::string meta_svr_pwd;

bool ComputeRestoreRemoteTask::InitComputesInfo(int srcClusterId,
                                                int dstClusterId,
                                                std::string shard_id_map,
                                                std::string restore_time) {

  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    setErr("%s", conn.getErr());
    return false;
  }
  auto conn_ptr = conn.get_master();
  kunlun::MysqlResult result;
  // fetch srcClusterName_
  std::string sql = kunlun::string_sprintf(
      "select * from kunlun_metadata_db.db_clusters where id = %d",
      srcClusterId);
  result.Clean();
  ret = conn_ptr->ExcuteQuery(sql.c_str(), &result);
  if (ret < 0) {
    KLOG_ERROR("Can't get db_cluster info failed: {}", conn_ptr->getErr());
    setErr("%s", conn_ptr->getErr());
    return false;
  }
  if (result.GetResultLinesNum() != 1) {
    setErr("There is %d db_cluster info where id is %d",
           result.GetResultLinesNum(), srcClusterId);
    return false;
  }
  srcClusterName_ = result[0]["name"];

  // fetch computes info
  sql = kunlun::string_sprintf(
      "select * from kunlun_metadata_db.comp_nodes where db_cluster_id = %d",
      dstClusterId);
  result.Clean();
  ret = conn_ptr->ExcuteQuery(sql.c_str(), &result);
  if (ret < 0) {
    KLOG_ERROR("Fetch comp_nodes info failed where db_cluster_id is {}",
               dstClusterId);
    setErr("Fetch comp_nodes info failed where db_cluster_id is %d",
           dstClusterId);
    return false;
  }
  if (result.GetResultLinesNum() == 0) {
    KLOG_ERROR("Invalid comp_nodes info nums %d where db_cluster_id is {}",
               result.GetResultLinesNum(), dstClusterId);
    setErr("Invalid comp_nodes info nums %d where db_cluster_id is %d",
           result.GetResultLinesNum(), dstClusterId);
    return false;
  }

  for (int i = 0; i < result.GetResultLinesNum(); i++) {

    kunlun::ComputeInstanceInfoSt info_st;
    info_st.ip = result[i]["hostaddr"];
    info_st.pg_port = ::atoi(result[i]["port"]);
    std::string key =
        kunlun::string_sprintf("%s_%u", info_st.ip.c_str(), info_st.pg_port);
    dstPostgresInfos_[key] = info_st;
  }

  // fetch hdfs_addr
  result.Clean();
  sql = kunlun::string_sprintf(
      "select * from kunlun_metadata_db.backup_storage limit 1");
  ret = conn_ptr->ExcuteQuery(sql.c_str(), &result);
  if (ret < 0) {
    KLOG_ERROR("Can't get backup_storage info {}", conn_ptr->getErr());
    return false;
  }
  if (result.GetResultLinesNum() == 0) {
    KLOG_ERROR("There is no recordes in backup_storage where id is");
    return false;
  }
  hdfs_addr_ = result[0]["conn_str"];

  // init shard_id_map and restore_time
  shard_id_map_ = shard_id_map;
  restore_time_ = restore_time;
  return ComposeNodeChannelsAndParas();
}

bool ComputeRestoreRemoteTask::ComposeNodeChannelsAndParas() {

  auto iter = dstPostgresInfos_.begin();
  for (; iter != dstPostgresInfos_.end(); iter++) {
    kunlun::ComputeInstanceInfoSt info_st = iter->second;
    brpc::Channel *channel =
        g_node_channel_manager->getNodeChannel(info_st.ip.c_str());
    if (channel == nullptr) {
      KLOG_ERROR("can't get the channel related ip: {}", info_st.ip);
      setErr("Can't get the channel related ip: %s", info_st.ip.c_str());
      return false;
    }
    std::string key =
        kunlun::string_sprintf("%s_%u", info_st.ip.c_str(), info_st.pg_port);
    AddNodeSubChannel(key.c_str(), channel);
    // prepare the paras
    Json::Value root;
    root["cluster_mgr_request_id"] = request_id_;
    root["task_spec_info"] = this->get_task_spec_info();
    root["job_type"] = "restore_postgres";

    Json::Value paras;
    paras["command_name"] = "restore";

    paras["port"] = std::to_string(info_st.pg_port);
    paras["restore_time"] = restore_time_;
    paras["hdfs_addr"] = hdfs_addr_;
    paras["orig_clustername"] = srcClusterName_;
    paras["shard_map_str"] = kunlun::StringReplace(shard_id_map_, "'", "");
    root["paras"] = paras;
    SetPara(key.c_str(), root);
  }
  return true;
}

bool ComputeRestoreRemoteTask::InitRestoreLogId() {

  // init cluster_coldbackups table info
  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    KLOG_ERROR("{}", conn.getErr());
  }
  auto conn_ptr = conn.get_master();
  kunlun::MysqlResult result;
  char sql[8192] = {'\0'};
  bzero((void *)sql, 8192);
  snprintf(sql, 8192, "begin");
  ret = conn_ptr->ExcuteQuery(sql, &result);

  bzero((void *)sql, 8192);
  result.Clean();
  snprintf(sql, 8192,
           "insert into kunlun_metadata_db.restore_log "
           "(general_log_id, restore_type,status,job_info,memo) values "
           "(%s,'compute','not_started','','')",
           request_id_.c_str());
  ret = conn_ptr->ExcuteQuery(sql, &result);
  if (ret < 0) {
    KLOG_ERROR(
        "fetch last_insert_id faild during the RestoreShard remote task {}",
        conn_ptr->getErr());
  }

  bzero((void *)sql, 8192);
  result.Clean();
  snprintf(sql, 8192, "select last_insert_id() as last_insert_id");
  ret = conn_ptr->ExcuteQuery(sql, &result);
  if (ret < 0 || result.GetResultLinesNum() != 1) {
    KLOG_ERROR(
        "fetch last_insert_id faild during the RestoreShard remote task {}",
        conn_ptr->getErr());
  }
  restore_log_id_ = result[0]["last_insert_id"];

  bzero((void *)sql, 8192);
  result.Clean();
  snprintf(sql, 8192, "commit");
  ret = conn_ptr->ExcuteQuery(sql, &result);
  return true;
}

bool ComputeRestoreRemoteTask::TaskReportImpl() {

  // write result into cluster
  std::string memo;
  Json::Value root = get_response()->get_all_response_json();
  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  memo = writer.write(root);
  bool ok = get_response()->ok();

  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    KLOG_ERROR("{}", conn.getErr());
  }
  auto conn_ptr = conn.get_master();
  char sql[8192] = {'\0'};
  snprintf(sql, 8192,
           "update kunlun_metadata_db.restore_log set "
           "memo='%s',status='%s' where "
           "id = %s limit 1",
           memo.c_str(), ok ? "done" : "failed", restore_log_id_.c_str());

  kunlun::MysqlResult result;
  ret = conn_ptr->ExcuteQuery(sql, &result);
  if (ret < 0) {
    KLOG_ERROR("Report Request status sql: {} ,failed: {}", sql,
               conn_ptr->getErr());
  } else {
    KLOG_INFO("Report Request status sql: {} ", sql);
  }
  return true;
}

void ComputeRestoreRemoteTask::SetUpStatus() {

  int ret = InitRestoreLogId();
  if (!ret) {
    KLOG_ERROR("{}", getErr());
  }
  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  ret = conn.init();
  if (!ret) {
    KLOG_ERROR("{}", conn.getErr());
  }
  auto conn_ptr = conn.get_master();
  std::string job_info = getTaskInfo();
  char sql[10240] = {'\0'};
  snprintf(sql, 10240,
           "update kunlun_metadata_db.restore_log set status = "
           "'ongoing',job_info='%s' where id = %s",
           job_info.c_str(), restore_log_id_.c_str());
  kunlun::MysqlResult result;
  ret = conn_ptr->ExcuteQuery(sql, &result);
  if (ret < 0) {
    KLOG_ERROR("Report Request status sql: {} ,failed: {}", sql,
               conn_ptr->getErr());
  } else {
    KLOG_INFO("Report Request status sql: {} ", sql);
  }
  return;
}
