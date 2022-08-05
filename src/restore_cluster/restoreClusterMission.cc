/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "restoreClusterMission.h"
#include "request_framework/handleRequestThread.h"
#include "zettalib/op_log.h"
#include "json/json.h"

using namespace kunlun;
extern HandleRequestThread *g_request_handle_thread;
extern GlobalNodeChannelManager *g_node_channel_manager;
extern std::string meta_group_seeds;
extern std::string meta_svr_user;
extern std::string meta_svr_pwd;

bool KRestoreClusterMission::SetUpMisson() {
  if (inited_) {
    return true;
  }

  Json::Value doc = get_body_json_document();
  Json::Value para = doc["paras"];
  if (!para.isMember("src_cluster_id")) {
    KLOG_ERROR("Missing src_cluster_id key in request body of restore cluster");
    return false;
  }
  src_cluster_id_ = ::atoi(para["src_cluster_id"].asString().c_str());
  if (!para.isMember("dst_cluster_id")) {
    KLOG_ERROR("Missing dst_cluster_id key in request body of restore cluster");
    return false;
  }
  dst_cluster_id_ = ::atoi(para["dst_cluster_id"].asString().c_str());
  if (!para.isMember("restore_time")) {
    KLOG_ERROR("Missing restore_time key in request body of restore cluster");
    return false;
  }
  restore_time_ = para["restore_time"].asString();
  int ret = srcAndDstClusterIsAvilable();
  // report status
  std::string sql = kunlun::string_sprintf(
      "update kunlun_metadata_db.cluster_general_job_log set status = '%s', "
      "memo = '%s' where id = %s",
      ret ? "ongoing" : "failed", this->getErr(),
      this->get_request_unique_id().c_str());
  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  ret = conn.init();
  if (!ret) {
    KLOG_ERROR("open shard connection for metadata cluster faild: {}",
               conn.getErr());
    return false;
    ;
  }
  auto conn_ptr = conn.get_master();
  kunlun::MysqlResult result;
  if (conn_ptr != nullptr) {
    ret = conn_ptr->ExcuteQuery(sql.c_str(), &result);
    if (ret < 0) {
      KLOG_ERROR("{}", conn_ptr->getErr());
    }
  }

  if (ret) {
    // set all shard to be deleted status
    auto kl_clusters = System::get_instance()->get_kl_clusters();
    for (auto iter : kl_clusters) {
      if (iter->get_id() != this->dst_cluster_id_) {
        continue;
      }
      this->SetShardStatus(iter->storage_shards, true);
    }
  }
  return ret;
}
bool KRestoreClusterMission::InitFromInternal() {
  if (inited_) {
    return true;
  }
  Json::Value doc;
  FillCommonJobJsonDoc(doc);
  doc["job_type"] = "cluster_restore";
  Json::Value para;
  para["src_cluster_id"] = src_cluster_id_;
  para["dst_cluster_id"] = dst_cluster_id_;
  para["restore_time"] = "restore_time";
  set_body_json_document(doc);
  CompleteGeneralJobInfo();
  return srcAndDstClusterIsAvilable();
}

void KRestoreClusterMission::CompleteGeneralJobInfo() {
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
           "job_type='cluster_restore',job_info='%s' where id = %s",
           job_info.c_str(), get_request_unique_id().c_str());

  ret = conn_ptr->ExcuteQuery(sql, &result);
  if (ret <= 0) {
    KLOG_ERROR("update cluster_general_job_log faild: {}", conn_ptr->getErr());
    return;
  }
  KLOG_INFO("update cluster_general_job_log success: {}", sql);
}

bool KRestoreClusterMission::srcAndDstClusterIsAvilable() {
  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    KLOG_ERROR("open shard connection for metadata cluster faild: {}",
               conn.getErr());
    setErr("%s", conn.getErr());
    return false;
  }

  kunlun::MysqlResult result;
  auto conn_ptr = conn.get_master();
  std::string sql = kunlun::string_sprintf(
      "select * from kunlun_metadata_db.db_clusters where id = %d",
      src_cluster_id_);
  ret = conn_ptr->ExcuteQuery(sql.c_str(), &result);
  if (ret < 0) {
    setErr("%s", conn_ptr->getErr());
    return false;
  }
  if (result.GetResultLinesNum() != 1) {
    setErr("There is %d rows related db_cluster_id %d which indicate the src "
           "restore cluster",
           result.GetResultLinesNum(), src_cluster_id_);
    KLOG_ERROR("There is {} rows related db_cluster_id {} which indecate the "
               "src restore cluster",
               result.GetResultLinesNum(), src_cluster_id_);
    return false;
  }

  result.Clean();

  sql = kunlun::string_sprintf(
      "select * from kunlun_metadata_db.db_clusters where id = %d",
      dst_cluster_id_);
  ret = conn_ptr->ExcuteQuery(sql.c_str(), &result);
  if (ret < 0) {
    setErr("%s", conn_ptr->getErr());
    return false;
  }
  if (result.GetResultLinesNum() != 1) {
    setErr("There is %d rows related db_cluster_id %d which indicate the dest "
           "restore cluster",
           result.GetResultLinesNum(), dst_cluster_id_);
    KLOG_ERROR("There is {} rows related db_cluster_id {} which indicate the "
               "dest restore cluster",
               result.GetResultLinesNum(), dst_cluster_id_);
    return false;
  }
  return initShardMap();
}

bool KRestoreClusterMission::initShardMap() {

  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    KLOG_ERROR("open shard connection for metadata cluster faild: {}",
               conn.getErr());
    setErr("%s", conn.getErr());
    return false;
  }
  kunlun::MysqlResult result;
  auto conn_ptr = conn.get_master();
  std::string sql =
      kunlun::string_sprintf("select * from kunlun_metadata_db.shards where "
                             "db_cluster_id = %d order by id",
                             src_cluster_id_);
  ret = conn_ptr->ExcuteQuery(sql.c_str(), &result);
  if (ret < 0) {
    KLOG_ERROR("Get src_cluster {} shards ids from metadata_cluster failed: {}",
               src_cluster_id_, conn_ptr->getErr());
    setErr("Get src_cluster %d shards ids from metadata_cluster failed: %s",
           src_cluster_id_, conn_ptr->getErr());
    return false;
  }
  kunlun::MysqlResult result_1;
  sql = kunlun::string_sprintf("select * from kunlun_metadata_db.shards where "
                               "db_cluster_id = %d order by id",
                               dst_cluster_id_);
  ret = conn_ptr->ExcuteQuery(sql.c_str(), &result_1);
  if (ret < 0) {
    KLOG_ERROR("Get dst_cluster {} shards ids from metadata_cluster failed: {}",
               dst_cluster_id_, conn_ptr->getErr());
    setErr("Get dst_cluster %d shards ids from metadata_cluster failed: %s",
           dst_cluster_id_, conn_ptr->getErr());
    return false;
  }
  if (result.GetResultLinesNum() != result_1.GetResultLinesNum()) {
    KLOG_ERROR("src cluster {} 's shards num {} is not same as dst cluster {} "
               "'s shards num {}",
               src_cluster_id_, result.GetResultLinesNum(), dst_cluster_id_,
               result_1.GetResultLinesNum());
    setErr("src cluster %d shards num %d is not same as dst cluster %d "
           "shards num %d",
           src_cluster_id_, result.GetResultLinesNum(), dst_cluster_id_,
           result_1.GetResultLinesNum());
    return false;
  }
  for (int i = 0; i < result.GetResultLinesNum(); i++) {
    auto p = std::make_pair(::atoi(result[i]["id"]), ::atoi(result_1[i]["id"]));
    shard_map_.push_back(p);
  }
  inited_ = true;
  return true;
}

std::string KRestoreClusterMission::shard_map_to_string() {
  auto iter = shard_map_.begin();
  if (iter == shard_map_.end()) {
    return "";
  }
  std::string result = "{";
  for (; iter != shard_map_.end(); iter++) {
    std::string item =
        kunlun::string_sprintf("'%d':'%d'", iter->first, iter->second);
    result.append(item);
    if (iter + 1 != shard_map_.end()) {
      result.append(",");
    }
  }
  return result.append("}");
}

bool KRestoreClusterMission::prepareRestoreOneShardTask(std::pair<int, int> p) {
  std::string task_spec_info =
      kunlun::string_sprintf("restore_shard_%d_%d", p.first, p.second);
  kunlun::ShardRestoreRemoteTask *task = new kunlun::ShardRestoreRemoteTask(
      task_spec_info.c_str(), get_request_unique_id());
  bool ret = task->InitShardsInfo(p.first, p.second, restore_time_);
  if (!ret) {
    setErr("%s", task->getErr());
    return false;
  }
  get_task_manager()->PushBackTask(task);
  return true;
}

bool KRestoreClusterMission::prepareRestoreAllCompute() {
  std::string task_spec_info = kunlun::string_sprintf("restore_computes");
  kunlun::ComputeRestoreRemoteTask *task = new kunlun::ComputeRestoreRemoteTask(
      task_spec_info.c_str(), get_request_unique_id());
  bool ret = task->InitComputesInfo(src_cluster_id_, dst_cluster_id_,
                                    shard_map_to_string(), restore_time_);
  if (!ret) {
    setErr("%s", task->getErr());
    return false;
  }
  get_task_manager()->PushBackTask(task);
  return true;
}

bool KRestoreClusterMission::ArrangeRemoteTask() {
  // first restore the shards
  auto iter = shard_map_.begin();
  if (iter == shard_map_.end()) {
    KLOG_ERROR("shard_map is null");
    setErr("shard_map is null");
    return false;
  }
  bool ret = false;
  for (; iter != shard_map_.end(); iter++) {
    ret = prepareRestoreOneShardTask(*iter);
    if (!ret) {
      KLOG_ERROR("prepare restore one shard task faild: {}", getErr());
      setErr("prepare restore one shard task faild: %s", getErr());
      return false;
    }
  }

  ret = prepareRestoreAllCompute();
  if (!ret) {
    KLOG_ERROR("prepare restore all compute task faild: {}", getErr());
    setErr("prepare restore all compute task faild: %s", getErr());
    return false;
  }
  return true;
}

void KRestoreClusterMission::ReportStatus() { return; }
bool KRestoreClusterMission::TearDownMission() {
  // set all shard to be not deleted
  auto kl_clusters = System::get_instance()->get_kl_clusters();
  for (auto iter : kl_clusters) {
    if (iter->get_id() != this->dst_cluster_id_) {
      continue;
    }
    this->SetShardStatus(iter->storage_shards, false);
  }

  bool success = get_task_manager()->ok();
  // report status
  std::string sql = kunlun::string_sprintf(
      "update kunlun_metadata_db.cluster_general_job_log set status = '%s' "
      " ,memo = '%s' "
      "where id = %s",
      success ? "done" : "failed",
      this->get_task_manager()->serialized_result_.c_str(),
      this->get_request_unique_id().c_str());
  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    KLOG_ERROR("open shard connection for metadata cluster faild: {}",
               conn.getErr());
    return false;
  }
  auto conn_ptr = conn.get_master();
  kunlun::MysqlResult result;
  if (conn_ptr != nullptr) {
    ret = conn_ptr->ExcuteQuery(sql.c_str(), &result);
    if (ret < 0) {
      KLOG_ERROR("{}", conn_ptr->getErr());
    }
  }
  KLOG_INFO("restore finish");
  return true;
}

void KRestoreClusterMission::SetShardStatus(std::vector<ObjectPtr<Shard> > &shards,
                                            bool deleted) {
  for (auto iter : shards) {
    iter->set_is_delete(deleted);
  }
}
