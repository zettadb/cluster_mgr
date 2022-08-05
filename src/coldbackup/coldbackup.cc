/*
  Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "coldbackup.h"
#include "http_server/node_channel.h"
#include "zettalib/op_log.h"
#include "zettalib/op_mysql.h"
#include "zettalib/tool_func.h"
#include "json/json.h"

using namespace kunlun;
extern GlobalNodeChannelManager *g_node_channel_manager;
extern std::string meta_group_seeds;
extern std::string meta_svr_user;
extern std::string meta_svr_pwd;

bool KColdBackUpMission::IsShard() { return backup_type_ == 0; }
bool KColdBackUpMission::InitFromInternal() {
  if (ip_port_.empty()) {
    setErr("ip_port_ info is empty, so mission init failed");
    return false;
  }
  auto ct = kunlun::StringTokenize(ip_port_, "_");
  if (ct.size() != 2) {
    setErr("ip_port %s is misformat");
    return false;
  }
  ip_ = ct[0];
  port_ = ct[1];

  // Based on ip_port_, initialize the Json object
  Json::Value doc;
  FillCommonJobJsonDoc(doc);
  if (IsShard()) {
    doc["job_type"] = "shard_coldbackup";
  } else {
    doc["job_type"] = "compute_coldbackup";
  }
  Json::Value para;

  para["ip"] = ip_;
  para["port"] = port_;
  doc["paras"] = para;

  set_body_json_document(doc);
  CompleteGeneralJobInfo();
  return true;
}

void KColdBackUpMission::CompleteGeneralJobInfo() {
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
  if (IsShard()) {
    snprintf(sql, 8192,
             "update kunlun_metadata_db.cluster_general_job_log set "
             "job_type='shard_coldbackup',job_info='%s' where id = %s",
             job_info.c_str(), get_request_unique_id().c_str());
  } else {
    snprintf(sql, 8192,
             "update kunlun_metadata_db.cluster_general_job_log set "
             "job_type='compute_coldbackup',job_info='%s' where id = %s",
             job_info.c_str(), get_request_unique_id().c_str());
  }

  ret = conn_ptr->ExcuteQuery(sql, &result);
  if (ret <= 0) {
    KLOG_ERROR("update cluster_general_job_log faild: {}", conn_ptr->getErr());
    return;
  }
  KLOG_INFO("update cluster_general_job_log success: {}", sql);
  // fetch backup storage info

  result.Clean();
  bzero((void *)sql, 8192);
  snprintf(sql, 8192,
           "select id,conn_str from kunlun_metadata_db.backup_storage where "
           "stype='HDFS' limit 1",
           cluster_id_.c_str());
  ret = conn_ptr->ExcuteQuery(sql, &result, 1);
  if (ret != 0) {
    KLOG_ERROR("{}", conn_ptr->getErr());
  }
  if (result.GetResultLinesNum() != 1) {
    KLOG_ERROR("can't find valid HDFS connection str from "
               "metadatadb.backup_storage based on ");
    return;
  }
  backup_storage_info_ = result[0]["conn_str"];
  backup_storage_id_ = result[0]["id"];

  // fetch shard_id

  if (IsShard()) {
    bzero((void *)sql, 8192);
    result.Clean();
    snprintf(sql, 8192,
             "select * from kunlun_metadata_db.shard_nodes where hostaddr='%s' "
             "and port=%d",
             ip_.c_str(), ::atoi(port_.c_str()));
    ret = conn_ptr->ExcuteQuery(sql, &result, 1);
    if (ret != 0) {
      KLOG_ERROR("can't find valid shard nodes from metadatadb failed: {}",
                 conn_ptr->getErr());
      return;
    }
    if (result.GetResultLinesNum() != 1) {
      KLOG_ERROR("can't find valid shard nodes from metadatadb based on {}",
                 ip_port_);
      return;
    }

    shard_id_ = result[0]["shard_id"];
    cluster_id_ = result[0]["db_cluster_id"];
  } else {
    bzero((void *)sql, 8192);
    result.Clean();
    snprintf(sql, 8192,
             "select * from kunlun_metadata_db.comp_nodes where hostaddr='%s' "
             "and port=%d",
             ip_.c_str(), ::atoi(port_.c_str()));
    ret = conn_ptr->ExcuteQuery(sql, &result, 1);
    if (ret != 0) {
      KLOG_ERROR("can't find valid shard nodes from metadatadb failed: {}",
                 conn_ptr->getErr());
      return ;
    }
    if (result.GetResultLinesNum() != 1) {
      KLOG_ERROR("can't find valid shard nodes from metadatadb based on {}",
                 ip_port_);
      return ;
    }

    cluster_id_ = result[0]["db_cluster_id"];
    comp_id_ = result[0]["id"];
    comp_name_ = result[0]["name"];
  }

  // init cluster_coldbackups table info
  bzero((void *)sql, 8192);
  result.Clean();
  snprintf(sql, 8192, "begin");
  ret = conn_ptr->ExcuteQuery(sql, &result, 1);

  bzero((void *)sql, 8192);
  result.Clean();
  if (IsShard()) {
    snprintf(sql, 8192,
             "insert into kunlun_metadata_db.cluster_coldbackups "
             "(storage_id,cluster_id,shard_id,backup_type) values "
             "(%s,%s,%s,'storage')",
             backup_storage_id_.c_str(), cluster_id_.c_str(),
             shard_id_.c_str());
  } else {
    snprintf(sql, 8192,
             "insert into kunlun_metadata_db.cluster_coldbackups "
             "(storage_id,cluster_id,comp_id,backup_type) values "
             "(%s,%s,%s,'compute')",
              backup_storage_id_.c_str(),cluster_id_.c_str(),comp_id_.c_str());
  }
  ret = conn_ptr->ExcuteQuery(sql, &result, 1);
  if (ret <= 0) {
    KLOG_ERROR("fetch last_insert_id faild during init the cluster_coldbackups "
               "records: {}",
               conn_ptr->getErr());
  }

  bzero((void *)sql, 8192);
  result.Clean();
  snprintf(sql, 8192, "select last_insert_id() as last_insert_id");
  ret = conn_ptr->ExcuteQuery(sql, &result, 1);
  if (ret != 0 || result.GetResultLinesNum() != 1) {
    KLOG_ERROR("fetch last_insert_id faild during init the cluster_coldbackups "
               "records: {}",
               conn_ptr->getErr());
  }
  cluster_coldbackups_related_id_ = result[0]["last_insert_id"];

  bzero((void *)sql, 8192);
  result.Clean();
  snprintf(sql, 8192, "commit");
  ret = conn_ptr->ExcuteQuery(sql, &result, 1);
}

bool KColdBackUpMission::SetUpMisson() {

  // fetch shardname
  kunlun::MysqlResult result;
  char buff[4096] = {'\0'};

  bool ret = false;
  if (IsShard()) {
    result.Clean();
    bzero((void *)buff, 4096);
    snprintf(buff, 4096,
             "select name from kunlun_metadata_db.shards where id = %s",
             shard_id_.c_str());
    ret = g_node_channel_manager->send_stmt(buff, &result, 1);
    if (ret) {
      setErr("%s", g_node_channel_manager->getErr());
      return false;
    }
    if (result.GetResultLinesNum() != 1) {
      setErr(
          "can't find valid shard nodes from metadatadb based on shard_id %s",
          shard_id_.c_str());
      return false;
    }
    shard_name_ = result[0]["name"];
  }

  // fetch clustername
  result.Clean();
  bzero((void *)buff, 4096);
  snprintf(buff, 4096,
           "select name from kunlun_metadata_db.db_clusters where id = %s",
           cluster_id_.c_str());
  ret = g_node_channel_manager->send_stmt(buff, &result, 1);
  if (ret) {
    setErr("%s", g_node_channel_manager->getErr());
    return false;
  }
  if (result.GetResultLinesNum() != 1) {
    setErr("can't find valid cluster from metadatadb.db_clusters based on "
           "cluster_id %s",
           cluster_id_.c_str());
    return false;
  }
  cluster_name_ = result[0]["name"];

  // update cluster_coldbackups state to ongoing
  result.Clean();
  bzero((void *)buff, 4096);
  snprintf(buff, 4096,
           "update kunlun_metadata_db.cluster_coldbackups set status = "
           "'ongoing' where id = %s",
           cluster_coldbackups_related_id_.c_str());
  ret = g_node_channel_manager->send_stmt(buff, &result, 1);
  if (ret) {
    setErr("%s", g_node_channel_manager->getErr());
    return false;
  }

  return true;
}

bool KColdBackUpMission::ArrangeRemoteTask() {
  // get node channel
  dest_node_channle_ = g_node_channel_manager->getNodeChannel(ip_.c_str());

  // init Task
  char buff[1024] = {'\0'};
  if (IsShard()) {
    snprintf(buff, 1024, "backup_mysql_%s", ip_port_.c_str());
  } else {
    snprintf(buff, 1024, "backup_postgresql_%s", ip_port_.c_str());
  }
  KColdBackUpTask *task =
      new KColdBackUpTask(buff, cluster_coldbackups_related_id_);
  task->AddNodeSubChannel(ip_.c_str(), dest_node_channle_);

  Json::Value root;
  root["cluster_mgr_request_id"] = get_request_unique_id();
  root["task_spec_info"] = task->get_task_spec_info();
  if (IsShard()) {
    root["job_type"] = "backup_shard";
  } else {
    root["job_type"] = "backup_compute";
  }

  Json::Value paras;
  paras["ip"] = ip_;
  paras["port"] = ::atoi(port_.c_str());
  paras["cluster_name"] = cluster_name_;
  if (IsShard()) {
    paras["shard_name"] = shard_name_;
    paras["shard_id"] = shard_id_;
  }
  paras["backup_storage"] = backup_storage_info_;
  root["paras"] = paras;

  task->SetPara(ip_.c_str(), root);
  get_task_manager()->PushBackTask(task);
  return true;
}

bool KColdBackUpMission::TearDownMission() { return true; }

void KColdBackUpMission::ReportStatus() {
  kunlun::RequestStatus status = get_status();
  char sql[51200] = {'\0'};
  if (status <= kunlun::ON_GOING) {
    sprintf(sql,
            "update kunlun_metadata_db.cluster_general_job_log set status = %d "
            "where id = %s",
            get_status(), get_request_unique_id().c_str());
  } else {
    std::string memo = get_task_manager()->serialized_result_;
    sprintf(
        sql,
        "update kunlun_metadata_db.cluster_general_job_log set status = %d ,"
        "when_ended = CURRENT_TIMESTAMP(6) , memo = '%s' where id = %s",
        get_status(), memo.c_str(), get_request_unique_id().c_str());
  }
  kunlun::MysqlResult result;
  int ret = g_node_channel_manager->send_stmt(sql, &result, 1);
  if (ret) {
    KLOG_ERROR("Report Request status sql: {} ,failed: {}", sql,
               g_node_channel_manager->getErr());
  }
  return;
}

bool KColdBackUpTask::TaskReportImpl() {
  // write result into cluster
  std::string memo;
  Json::Value root = get_response()->get_all_response_json();
  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  memo = writer.write(root);
  bool ok = get_response()->ok();

  char sql[8192] = {'\0'};
  snprintf(sql, 8192,
           "update kunlun_metadata_db.cluster_coldbackups set "
           "memo='%s',status='%s' where "
           "id = %s limit 1",
           memo.c_str(), ok ? "done" : "failed", related_id_.c_str());

  kunlun::MysqlResult result;
  int ret = g_node_channel_manager->send_stmt(sql, &result, 1);
  if (ret) {
    KLOG_ERROR("Report Request status sql: {} ,failed: {}", sql,
               g_node_channel_manager->getErr());
  } else {
    KLOG_INFO("Report Request status sql: {} ", sql);
  }
  return true;
}

// {"version":"1.0","job_id":"","job_type":"update_cluster_coldback_time_period","timestamp":"1435749309","user_name":"kunlun_test","paras":{"cluster_id":"1","time_period_str":"08:00:00-24:00:00"}}
bool KUpdateBackUpPeriodMission::SetUpMisson() {

  Json::Value root = this->get_body_json_document();
  Json::Value paras = root["paras"];
  if (!paras.isMember("cluster_id")) {
    KLOG_ERROR("Missing cluster_id key in the request body");
    setErr("Missing cluster_id key in the request body");
    return false;
  }
  cluster_id_ = ::atoi(paras["cluster_id"].asString().c_str());
  if (!paras.isMember("time_period_str")) {
    KLOG_ERROR("Missing time_period_str key in the request body");
    setErr("Missing time_period_str key in the request body");
    return false;
  }
  coldbackup_period_str_ = paras["time_period_str"].asString();

  // cluster_id validation
  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    KLOG_ERROR("{}", conn.getErr());
    setErr("%s", conn.getErr());
    return false;
  }

  auto conn_ptr = conn.get_master();
  if (conn_ptr == nullptr) {
    KLOG_ERROR("metadata master conn is nullptr: {}", conn.getErr());
    setErr("metadata master conn is nullptr: %s", conn.getErr());
    return false;
  }
  std::string sql = kunlun::string_sprintf(
      "select * from kunlun_metadata_db.db_clusters where id = %d",
      cluster_id_);
  kunlun::MysqlResult result;
  ret = conn_ptr->ExcuteQuery(sql.c_str(), &result);
  if (ret < 0) {
    setErr("%s", conn_ptr->getErr());
    KLOG_ERROR("%s", conn_ptr->getErr());
    return false;
  }
  if (result.GetResultLinesNum() != 1) {
    setErr("get invalid result num %d from db_clusters where id is %d",
           result.GetResultLinesNum(), cluster_id_);
    KLOG_ERROR("get invalid result num {} from db_clusters where id is {}",
               result.GetResultLinesNum(), cluster_id_);
    return false;
  }

  // time_period_str validation
  kunlun::CounterTriggerTimePeriod period;
  ret = period.InitOrRefresh(coldbackup_period_str_.c_str());
  if (!ret) {
    setErr("%s", period.getErr());
    return false;
  }

  return true;
}

bool KUpdateBackUpPeriodMission::UpdateInstanceInfo() {
  bool found = false;
  const std::vector<ObjectPtr<KunlunCluster> > &kl_clusters =
      System::get_instance()->get_kl_clusters();
  for (auto iter : kl_clusters) {
    if (iter->get_id() != this->cluster_id_) {
      continue;
    }
    found = true;
    UpdateShardInfo(iter->storage_shards);
    UpdateComputeInfo(iter->computer_nodes);
  }
  return found;
}

void KUpdateBackUpPeriodMission::UpdateComputeInfo(
    std::vector<ObjectPtr<Computer_node> > &comps) {
  for (auto iter : comps) {
    bool ret = iter->coldbackup_timer.InitOrRefresh(coldbackup_period_str_);
    if (!ret) {
      KLOG_ERROR("Init or refresh compute coldback time faild: {}",
                 iter->coldbackup_timer.getErr());
    }
    KLOG_INFO("Init or refresh compute coldback success");
  }
  return;
}

void KUpdateBackUpPeriodMission::UpdateShardInfo(std::vector<ObjectPtr<Shard> > &shards) {
  for (auto iter : shards) {
    bool ret =
        iter->coldback_time_trigger_.InitOrRefresh(coldbackup_period_str_);
    if (!ret) {
      KLOG_ERROR("Init or refresh shard coldback time faild: {}",
                 iter->coldback_time_trigger_.getErr());
    }
    KLOG_INFO("Init or refresh shard coldback success");
  }
  return;
}

bool KUpdateBackUpPeriodMission::TearDownMission() {
  if (success_) {
    UpdateInstanceInfo();
  }
  // report status;

  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  std::string job_info_str = writer.write(this->get_body_json_document());

  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    KLOG_ERROR("update backup period mission tear down phase failed: {}",
               conn.getErr());
    return false;
  }
  auto conn_ptr = conn.get_master();
  if (conn_ptr == nullptr) {
    KLOG_ERROR("update backup period mission tear down phase failed: Can't get "
               "valid metadatacluster connection",
               conn.getErr());
    return false;
  }
  kunlun::MysqlResult result;
  std::string sql = kunlun::string_sprintf(
      "update kunlun_metadata_db.cluster_general_job_log set status = '%s' "
      ",job_info = '%s',memo = '%s' where id = %s",
      success_ ? "done" : "failed", job_info_str.c_str(), this->getErr(),
      this->get_request_unique_id().c_str());
  ret = conn_ptr->ExcuteQuery(sql.c_str(), &result);
  if (ret < 0) {
    KLOG_ERROR("update backup period mission tear down phase failed: {}",
               conn_ptr->getErr());
    return false;
  }
  return true;
}

void KUpdateBackUpPeriodMission::DealLocal() {

  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    KLOG_ERROR("{}", conn.getErr());
    setErr("%s", conn.getErr());
    return;
  }
  auto conn_ptr = conn.get_master();
  if (conn_ptr == nullptr) {
    KLOG_ERROR("metadata master conn is nullptr: {}", conn.getErr());
    setErr("metadata master conn is nullptr: %s", conn.getErr());
    return;
  }
  ret = conn_ptr->doBegin();
  if (!ret) {
    KLOG_ERROR("{}", conn_ptr->getErr());
    setErr("%s", conn_ptr->getErr());
    return;
  }

  kunlun::MysqlResult result;
  // update all shards related to current cluster_id
  std::string sql = kunlun::string_sprintf(
      "update kunlun_metadata_db.shards set coldbackup_period = '%s' where "
      "db_cluster_id = %d",
      coldbackup_period_str_.c_str(), cluster_id_);
  ret = conn_ptr->ExcuteQuery(sql.c_str(), &result);
  if (ret < 0) {
    KLOG_ERROR("{}", conn_ptr->getErr());
    setErr("%s", conn_ptr->getErr());
    conn_ptr->doRollBack();
    return;
  }

  // updata all computes related to current cluster_id
  sql = kunlun::string_sprintf(
      "update kunlun_metadata_db.comp_nodes set coldbackup_period = '%s' where "
      "db_cluster_id = %d",
      coldbackup_period_str_.c_str(), cluster_id_);
  ret = conn_ptr->ExcuteQuery(sql.c_str(), &result);
  if (ret < 0) {
    KLOG_ERROR("{}", conn_ptr->getErr());
    setErr("%s", conn_ptr->getErr());
    conn_ptr->doRollBack();
    return;
  }
  ret = conn_ptr->doCommit();
  if (!ret) {
    KLOG_ERROR("{}", conn_ptr->getErr());
    setErr("%s", conn_ptr->getErr());
    return;
  }
  success_ = true;
  return;
}

bool KManualColdBackUpMission::SetUpMisson() {
  bool valid = true;
  Json::Value job_doc = this->get_body_json_document();
  Json::Value paras = job_doc["paras"];
  if (!paras.isMember("cluster_id")) {
    setErr("Missing cluster_id key in request body");
    valid = false;
  }
  cluster_id_ = ::atoi(paras["cluster_id"].asString().c_str());
  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    KLOG_ERROR("connect to metadata cluster failed: {}", conn.getErr());
    return false;
  }
  auto conn_ptr = conn.get_master();
  if (conn_ptr == nullptr) {
    KLOG_ERROR("metadata cluster master conn is nullptr failed: {}",
               conn.getErr());
    return false;
  }

  std::string memo = "";
  if (!valid) {
    memo = this->getErr();
  }

  kunlun::MysqlResult result;
  std::string sql = kunlun::string_sprintf(
      "update kunlun_metadata_db.cluster_general_job_log set status = '%s', "
      "memo = '%s' where id= %s",
      valid ? "ongoing" : "failed", memo.c_str(),this->get_request_unique_id().c_str());
  ret = conn_ptr->ExcuteQuery(sql.c_str(), &result);
  if (ret < 0) {
    KLOG_ERROR("{}", conn_ptr->getErr());
  }
  return true;
}

void KManualColdBackUpMission::DealLocal() {
  bool found = false;
  const std::vector<ObjectPtr<KunlunCluster> > &kl_clusters =
      System::get_instance()->get_kl_clusters();
  for (auto iter : kl_clusters) {
    if (iter->get_id() != this->cluster_id_) {
      continue;
    }
    found = true;
    auto shards = iter->storage_shards;
    for (auto iter1 : shards) {
      iter1->coldback_time_trigger_.set_force(true);
      KLOG_INFO("set force shard coldback success, cluster id {}", cluster_id_);
    }
    auto comp = iter->get_coldbackup_comp_node();
    comp->coldbackup_timer.set_force(true);
    KLOG_INFO("set force compute coldback success, cluster id {}", cluster_id_);
  }
  success_ = true;
  return;
}
bool KManualColdBackUpMission::TearDownMission() {
  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  int ret = conn.init();
  if (!ret) {
    KLOG_ERROR("connect to metadata cluster failed: {}", conn.getErr());
    return false;
  }
  auto conn_ptr = conn.get_master();
  if (conn_ptr == nullptr) {
    KLOG_ERROR("metadata cluster master conn is nullptr failed: {}",
               conn.getErr());
    return false;
  }

  kunlun::MysqlResult result;
  std::string sql = kunlun::string_sprintf(
      "update kunlun_metadata_db.cluster_general_job_log set status = '%s' "
      "where id = %s",
      success_ ? "done" : "failed", get_request_unique_id().c_str());
  ret = conn_ptr->ExcuteQuery(sql.c_str(), &result);
  if (ret < 0) {
    KLOG_ERROR("{}", conn_ptr->getErr());
  }
  return true;
}
