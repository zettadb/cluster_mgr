/*
  Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "sync_mission.h"
#include "cluster_mission/create_cluster.h"
#include "cluster_mission/delete_cluster.h"
#include "http_server/node_channel.h"
#include "kl_mentain/sys.h"
#include "rebuild_node/rebuild_node.h"
#include "strings.h"
#include "util_func/error_code.h"
#include "util_func/meta_info.h"
#include "zettalib/op_log.h"
#include "zettalib/tool_func.h"
#include "cluster_expand/table_pick.h"

using namespace kunlun;

extern std::string meta_group_seeds;
extern std::string meta_svr_user;
extern std::string meta_svr_pwd;

bool SyncMission::GetStatus() {
  Json::Value attachment;
  Json::Reader reader;
  // kunlun::MysqlConnection *meta_conn =
  // g_node_channel_manager.get_meta_conn();
  char sql_buffer[4096] = {'\0'};
  bool ret;
  kunlun::MysqlResult query_result;
  sprintf(sql_buffer,
          "select id,job_type,status,memo from %s.cluster_general_job_log "
          "where id=%s",
          KUNLUN_METADATA_DB_NAME, get_request_body().request_id.c_str());
  ret = g_node_channel_manager->send_stmt(sql_buffer, &query_result,
                                          stmt_retries);
  
  if (ret || query_result.GetResultLinesNum() != 1) {
    attachment["error_code"] = EintToStr(ERR_JOBID_MISS);
    attachment["error_info"] = "job_id no find";
    attachment["status"] = "failed";
    attachment["job_id"] = "";
    attachment["job_type"] = "";
    return true;
  }

  std::string job_type = query_result[0]["job_type"];
  attachment["job_id"] = std::string(query_result[0]["id"]);
  attachment["job_type"] = job_type;

  if (GenerateRelatedAttachment(attachment)) {
    set_body_json_attachment(attachment);
    return true;
  }

  if (!reader.parse(query_result[0]["memo"], attachment)) {
    attachment["error_code"] = EintToStr(ERR_JSON);
    attachment["error_info"] = "memo json error";
    attachment["status"] = "failed";
    attachment["job_id"] = "";
    attachment["job_type"] = "";
    return true;
  }

  // std::string job_type = query_result[0]["job_type"];
  // attachment["job_id"] = std::string(query_result[0]["id"]);
  // attachment["job_type"] = job_type;
  int change_status = 1;
  if(job_type == "rebuild_node") {
    GetRebuildNodeStatus(attachment);
  } else if(job_type == "create_cluster") {
    GetCreateClusterStatus(attachment);
  } else if(job_type == "delete_cluster") {
    GetDeleteClusterStatus(attachment);
  }

  if(change_status)
    attachment["status"] = std::string(query_result[0]["status"]);

  set_body_json_attachment(attachment);
  return true;
};

bool SyncMission::GenerateRelatedAttachment(Json::Value &doc) {
  std::string job_type = doc["job_type"].asString();
  bool match = false;

  if (job_type == "cluster_restore") {
    match = true;
    fetchClusterRestoreResult(doc);
  } else if (job_type == "expand_cluster") {
    match = true;
    fetchClusterExpandResult(doc);
  }
  return match;
}

void SyncMission::fetchClusterExpandResult(Json::Value &doc){
  std::string job_id = doc["job_id"].asString();

  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    KLOG_ERROR("init metadata connection failed:{}", conn.getErr());
    return;
  }
  auto conn_ptr = conn.get_master();
  if (conn_ptr == nullptr) {
    KLOG_ERROR("metadata master connection is null ", conn.getErr());
    return;
  }
  std::string sql = kunlun::string_sprintf(
      "select * from kunlun_metadata_db.cluster_general_job_log where id = %s",
      job_id.c_str());
  kunlun::MysqlResult result;
  ret = conn_ptr->ExcuteQuery(sql.c_str(), &result);
  if(ret < 0){
    KLOG_ERROR("can not get mission info from table use job id %s",job_id.c_str());
    doc["error_info"] = "wrong job id";
    doc["status"] = "failed";
    return ;
  }
  doc["status"] = result[0]["status"];
  std::string related_id = result[0]["related_id"];
  sql = kunlun::string_sprintf("select * from kunlun_metadata_db.table_move_jobs where id = %s ", related_id.c_str());
  result.Clean();
  ret = conn_ptr->ExcuteQuery(sql.c_str(), &result);
  if(ret < 0){
    KLOG_ERROR("can not get mission info from table_move_job use job id {},{}",related_id.c_str(),conn_ptr->getErr());
    doc["error_info"] = "wrong table_move_job id";
    doc["status"] = "failed";
    return ;
  }
  if(result.GetResultLinesNum() <1){
    KLOG_ERROR("can not get mission info from table_move_job use job id {},{}",related_id.c_str(),conn_ptr->getErr());
    doc["error_info"] = "wrong table_move_job id";
    doc["status"] = "failed";
    return ;
  }
  doc["status"] = result[0]["status"];
  Json::Reader reader;
  Json::Value memo_doc;
  ret = reader.parse(std::string(result[0]["memo"]), memo_doc);
  if(!ret){
    doc["memo_info"] = "";
  }
  else{
    doc["memo_info"] = memo_doc;
  }
  //doc["error_info"] = result[0]["memo"];
  return ;
}

void SyncMission::fetchClusterRestoreResult(Json::Value &doc) {
  std::string job_id = doc["job_id"].asString();

  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    KLOG_ERROR("init metadata connection failed:{}", conn.getErr());
    return;
  }
  auto conn_ptr = conn.get_master();
  if (conn_ptr == nullptr) {
    KLOG_ERROR("metadata master connection is null ", conn.getErr());
    return;
  }
  std::string sql = kunlun::string_sprintf(
      "select * from kunlun_metadata_db.cluster_general_job_log where id = %s",
      job_id.c_str());
  kunlun::MysqlResult result;
  ret = conn_ptr->ExcuteQuery(sql.c_str(), &result);
  if(ret < 0){
    KLOG_ERROR("can not get mission info from table use job id %s",job_id.c_str());
    doc["error_info"] = "wrong job id";
    doc["status"] = "failed";
  }
  doc["status"] = result[0]["status"];
  Json::Reader reader;
  Json::Value memo_doc;
  ret = reader.parse(std::string(result[0]["memo"]), memo_doc);
  if(!ret){
    doc["memo_info"] = "";
  }
  else{
    doc["memo_info"] = memo_doc;
  }
  //doc["error_info"] = result[0]["memo"];
  return ;
}

bool SyncMission::GetMetaMode() {
  Json::Value attachment;
  bool ret = System::get_instance()->get_meta_mode(attachment);
  set_body_json_attachment(attachment);
  return ret;
}

bool SyncMission::GetMetaSummary() {
  Json::Value attachment;
  bool ret = System::get_instance()->get_meta_summary(attachment);
  set_body_json_attachment(attachment);
  return ret;
}

bool SyncMission::GetBackupStorage() {
  Json::Value attachment;
  bool ret = System::get_instance()->get_backup_storage(attachment);
  set_body_json_attachment(attachment);
  return ret;
}

bool SyncMission::GetExpandTableList(){
  if (!super::get_body_json_document().isMember("paras")) {
    setExtraErr("missing paras key-value pair in the request body");
    KLOG_ERROR("missing paras key-value pair in the request body");
    return false;
  }

  Json::Value paras = super::get_body_json_document()["paras"];
  std::string policy = paras["policy"].asString();
  std::string shard_id = paras["shard_id"].asString(); 
  std::string cluster_id = paras["cluster_id"].asString();
  kunlun::MysqlConnection * master_conn = nullptr; 
  const std::vector<ObjectPtr<KunlunCluster> > clusters = System::get_instance()->get_kl_clusters();
  bool found = false;
  for(auto iter : clusters){
    if(iter->get_id() != ::atoi(cluster_id.c_str())){
      continue;
    }
    std::vector<ObjectPtr<Shard> > & shards= iter->storage_shards;
    for(auto iter_shard : shards){
      if(iter_shard->get_id() != ::atoi(shard_id.c_str())){
        continue;
      }
      found = true;
      ObjectPtr<Shard_node> master_node = iter_shard->get_master();
      master_conn = master_node->get_mysql_conn();
      master_conn->CheckIsConnected();
    }
  }

  if(!found){
    setExtraErr("can not find valid shard master connection based on request");
    KLOG_ERROR("can not find valid shard master connection based on request");
    return false;
  }

  Json::Value attachment;
  TableShufflePolicy * shuffle_policy_ = nullptr;
  if(policy == "top_hit"){
    shuffle_policy_ = new TopHitShufflePolicy(master_conn);
  }
  else if(policy == "top_size"){
    shuffle_policy_ = new RandomShufflePolicy(master_conn);
  }
  else {
    setExtraErr("can not find valid table shuffle policy based on request");
    KLOG_ERROR("can not find valid table shuffle policy based on request");
    return false;
  }

  std::vector<std::string> tbl_list_vec;
  int ret = shuffle_policy_->GetTableList(tbl_list_vec);
  if (!ret) {
    setExtraErr("%s", shuffle_policy_->getErr());
    return false;
  }
  std::string table_list_str = shuffle_policy_->get_table_list_str();
  attachment["table_list"] = table_list_str;
  attachment["status"] = "done";
  attachment["error_code"] = "0";
  attachment["error_info"] = "";
  set_body_json_attachment(attachment);
  return true;
}

bool SyncMission::GetClusterDetail() {
  Json::Value attachment;
  std::string cluster_name;

  if (!super::get_body_json_document().isMember("paras")) {
    setExtraErr("missing paras key-value pair in the request body");
    return false;
  }
  Json::Value paras = super::get_body_json_document()["paras"];

  bool ret = System::get_instance()->get_cluster_detail(paras, attachment);
  set_body_json_attachment(attachment);
  return ret;
}

bool SyncMission::GetVariable() {
  Json::Value attachment;
  std::string cluster_name;

  if (!super::get_body_json_document().isMember("paras")) {
    setExtraErr("missing paras key-value pair in the request body");
    return false;
  }
  Json::Value paras = super::get_body_json_document()["paras"];

  bool ret = System::get_instance()->get_variable(paras, attachment);
  set_body_json_attachment(attachment);
  return ret;
}

bool SyncMission::SetVariable() {
  Json::Value attachment;
  std::string cluster_name;

  if (!super::get_body_json_document().isMember("paras")) {
    setExtraErr("missing paras key-value pair in the request body");
    return false;
  }
  Json::Value paras = super::get_body_json_document()["paras"];

  bool ret = System::get_instance()->set_variable(paras, attachment);
  set_body_json_attachment(attachment);
  return ret;
}
