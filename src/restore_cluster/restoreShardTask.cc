/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "restoreShardTask.h"
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

void kunlun::rebuildRbrAfterRestore(void *para) {
  ShardRestoreRemoteTask *pt = (ShardRestoreRemoteTask *)para;
  bool ret = pt->setUpRBR();
  if (!ret) {
    KLOG_ERROR("rebuild rbr after restore shard failed: {}", pt->getErr());
  }
  KLOG_ERROR("rebuild rbr after restore shard success");
}

bool ShardRestoreRemoteTask::doChangeMasterOnSlave(
    struct InstanceInfoSt &master, struct InstanceInfoSt &slave) {
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

struct InstanceInfoSt ShardRestoreRemoteTask::getMasterInfo() {
  auto iter = destMysqlInfos_.begin();
  for (; iter != destMysqlInfos_.end(); iter++) {
    if (iter->second.role == "master") {
      break;
    }
  }
  return iter->second;
}

bool ShardRestoreRemoteTask::setUpRBR() {
  auto master = getMasterInfo();
  auto iter = destMysqlInfos_.begin();
  for (; iter != destMysqlInfos_.end(); iter++) {
    if (iter->second.role != "master") {
      if (!doChangeMasterOnSlave(master, iter->second)) {
        return false;
      }
    }
  }
  return enableMaster(master);
}

bool ShardRestoreRemoteTask::enableMaster(struct InstanceInfoSt &master) {
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

bool ShardRestoreRemoteTask::InitShardsInfo(int srcShardId, int dstShardId,
                                            std::string restore_time) {

  restore_time_str_ = restore_time;

  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    setErr("%s", conn.getErr());
    return false;
  }
  auto conn_ptr = conn.get_master();
  // fetch srcShardName_
  string sql = kunlun::string_sprintf(
      "select * from kunlun_metadata_db.shards where id = %d", srcShardId);
  kunlun::MysqlResult result;
  ret = conn_ptr->ExcuteQuery(sql.c_str(), &result);
  if (ret < 0) {
    KLOG_ERROR("Can't get valid shard info refers to the shardid {}, {}",
               srcShardId, conn_ptr->getErr());
    setErr("%s", conn_ptr->getErr());
    return false;
  }
  if (result.GetResultLinesNum() != 1) {
    setErr("There is %d shard info in the metadata_cluster where id is %d",
           result.GetResultLinesNum(), srcShardId);
    return false;
  }

  srcShardName_ = result[0]["name"];
  // fetch srcClusterName_
  int db_cluster_id = ::atoi(result[0]["db_cluster_id"]);
  sql = kunlun::string_sprintf(
      "select * from kunlun_metadata_db.db_clusters where id = %d",
      db_cluster_id);
  result.Clean();
  ret = conn_ptr->ExcuteQuery(sql.c_str(), &result);
  if (ret < 0) {
    KLOG_ERROR("Can't get db_cluster info failed: {}", conn_ptr->getErr());
    setErr("%s", conn_ptr->getErr());
    return false;
  }
  if (result.GetResultLinesNum() != 1) {
    setErr("There is %d db_cluster info where id is %d",
           result.GetResultLinesNum(), db_cluster_id);
    return false;
  }
  srcClusterName_ = result[0]["name"];

  // init destMysqlInfos_
  // we need to fetch the mysql instance info from the metadata_db by
  // dstShardId.
  result.Clean();
  sql = kunlun::string_sprintf(
      "select * from kunlun_metadata_db.shard_nodes where shard_id = %d",
      dstShardId);
  ret = conn_ptr->ExcuteQuery(sql.c_str(), &result);
  if (ret < 0) {
    KLOG_ERROR("Can't get shard_nodes info where id is {}, {}", dstShardId,
               conn_ptr->getErr());
    return false;
  }
  if (result.GetResultLinesNum() == 0) {
    KLOG_ERROR("There is no recordes in shard_nodes where id is {}",
               dstShardId);
    return false;
  }
  for (int i = 0; i < result.GetResultLinesNum(); i++) {
    kunlun::InstanceInfoSt info_st;
    info_st.ip = result[i]["hostaddr"];
    info_st.port = ::atoi(result[i]["port"]);
    info_st.role = result[i]["member_state"];
    if (info_st.role == "source") {
      info_st.role = "master";
    } else {
      info_st.role = "slave";
    }
    std::string key =
        kunlun::string_sprintf("%s_%u", info_st.ip.c_str(), info_st.port);
    destMysqlInfos_[key] = info_st;
  }

  // fetch hdfs_addr
  result.Clean();
  sql = kunlun::string_sprintf(
      "select * from kunlun_metadata_db.backup_storage limit 1");
  ret = conn_ptr->ExcuteQuery(sql.c_str(), &result);
  if (ret < 0) {
    KLOG_ERROR("Can't get backup_storage info where id is {}, {}", dstShardId,
               conn_ptr->getErr());
    return false;
  }
  if (result.GetResultLinesNum() == 0) {
    KLOG_ERROR("There is no recordes in backup_storage where id is {}",
               dstShardId);
    return false;
  }
  hdfs_addr_ = result[0]["conn_str"];
  return ComposeNodeChannelsAndParas();
}

bool ShardRestoreRemoteTask::ComposeNodeChannelsAndParas() {

  auto iter = destMysqlInfos_.begin();
  for (; iter != destMysqlInfos_.end(); iter++) {
    kunlun::InstanceInfoSt info_st = iter->second;
    brpc::Channel *channel =
        g_node_channel_manager->getNodeChannel(info_st.ip.c_str());
    if (channel == nullptr) {
      KLOG_ERROR("can't get the channel related ip: {}", info_st.ip);
      setErr("Can't get the channel related ip: %s", info_st.ip.c_str());
      return false;
    }
    std::string key =
        kunlun::string_sprintf("%s_%u", info_st.ip.c_str(), info_st.port);
    AddNodeSubChannel(key.c_str(), channel);
    // prepare the paras
    Json::Value root;
    root["cluster_mgr_request_id"] = request_id_;
    root["task_spec_info"] = this->get_task_spec_info();
    root["job_type"] = "restore_mysql";

    Json::Value paras;
    paras["command_name"] = "restore";

    paras["port"] = std::to_string(info_st.port);
    paras["restore_time"] = restore_time_str_;
    paras["hdfs_addr"] = hdfs_addr_;
    paras["orig_shardname"] = srcShardName_;
    paras["orig_clustername"] = srcClusterName_;
    root["paras"] = paras;
    SetPara(key.c_str(), root);
  }
  return true;
}

bool ShardRestoreRemoteTask::InitRestoreLogId() {

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
           "(%s,'shard','not_started','','')",
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

bool ShardRestoreRemoteTask::TaskReportImpl() {

  // write result into cluster
  std::string memo;
  Json::Value root = get_response()->get_all_response_json();
  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  memo = writer.write(root);
  bool ok = get_response()->ok();
  if (ok) {
    bool ret = setUpRBR();
    if (!ret) {
      KLOG_ERROR("rebuild rbr after restore shard failed: {}", getErr());
    }
    KLOG_ERROR("rebuild rbr after restore shard success");
  }

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

void ShardRestoreRemoteTask::SetUpStatus() {

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
