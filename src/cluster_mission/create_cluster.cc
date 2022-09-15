/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "create_cluster.h"
#include "kl_mentain/shard.h"
#include "add_shard.h"
#include "add_computer.h"
#include "delete_shard.h"
#include "delete_computer.h"
#include "request_framework/handleRequestThread.h"
#include "zettalib/tool_func.h"
#include "zettalib/op_pg.h"
#include "kl_mentain/sys.h"

extern HandleRequestThread *g_request_handle_thread;
extern std::string meta_svr_user;
extern std::string meta_svr_pwd;

namespace kunlun
{

int g_inc_cluster_id = 0;

bool CreateClusterMission::SetUpMisson() {
    job_id_ = get_request_unique_id();
    if(get_init_by_recover_flag() == true) {
       KLOG_INFO("Create cluster ignore recover state in setup phase");
      return true;
    }
    
    KLOG_INFO("create cluster setup phase");
  
    ClusterRequestTypes request_type = get_request_type();
    
    bool ret = true;
    int i = 0;
    Json::Value storage_iplists, computer_iplists, paras;
    if(init_flag_) {
      KLOG_ERROR("create cluster called by internal");
      goto end;
    }

    if (!super::get_body_json_document().isMember("paras")) {
        err_code_ = CREATE_CLUSTER_QUEST_LOST_PARAS_ERROR;
        KLOG_ERROR("missing `paras` key-value pair in the request body");
        goto end;
    }

    paras = super::get_body_json_document()["paras"];
    if (paras.isMember("nick_name")) {
      nick_name_ = paras["nick_name"].asString();
    }

    if (!paras.isMember("ha_mode")) {
      KLOG_ERROR("missing `ha_mode` key-value pair in the request body");
      err_code_ = CREATE_CLUSTER_QUEST_MISS_HAMODE_ERROR;
      goto end;
    }
    ha_mode_ = paras["ha_mode"].asString();
    if(!(ha_mode_ == "mgr" || ha_mode_ == "rbr" || ha_mode_ == "no_rep")) {
      err_code_ = CREATE_CLUSTER_QUEST_MISS_HAMODE_ERROR;
      KLOG_ERROR("input ha_mode {} not support", ha_mode_);
      goto end;
    }

    if (!paras.isMember("shards")) {
      err_code_ = CREATE_CLUSTER_QUEST_MISS_SHARDS_ERROR;
      KLOG_ERROR("missing `shards` key-value pair in the request body");
      goto end;
    }
    if(!CheckStringIsDigit(paras["shards"].asString())) {
      err_code_ = CREATE_CLUSTER_QUEST_MISS_SHARDS_ERROR;
      KLOG_ERROR("missing `shards` key-value pair in the request body");
      goto end;
    }
    shards_ = atoi(paras["shards"].asCString());
    
    if(shards_ < 1 || shards_ > 256) {
      err_code_ = CREATE_CLUSTER_QUEST_SHARDS_PARAM_ERROR;
      KLOG_ERROR("shards error(must in 1-256)");
      goto end;
    }

    if (!paras.isMember("nodes")) {
      err_code_ = CREATE_CLUSTER_QUEST_MISS_NODES_ERROR;
      KLOG_ERROR("missing `nodes` key-value pair in the request body");
      goto end;
    }
    if(!CheckStringIsDigit(paras["nodes"].asString())) {
      err_code_ = CREATE_CLUSTER_QUEST_MISS_NODES_ERROR;
      KLOG_ERROR("missing `nodes` key-value pair in the request body");
      goto end;
    }
    nodes_ = atoi(paras["nodes"].asCString());
    if(nodes_ < 1 || nodes_ > 256) {
      err_code_ = CREATE_CLUSTER_QUEST_NODES_PARAM_ERROR;
      KLOG_ERROR("nodes error(must in 1-256)");
      goto end;
    }

    if (!paras.isMember("comps")) {
      err_code_ = CREATE_CLUSTER_QUEST_MISS_COMPS_ERROR;
      KLOG_ERROR("missing `comps` key-value pair in the request body");
      goto end;
    }
    if(!CheckStringIsDigit(paras["comps"].asString())) { 
      err_code_ = CREATE_CLUSTER_QUEST_MISS_COMPS_ERROR;
      KLOG_ERROR("missing `comps` key-value pair in the request body");
      goto end;
    }
    comps_ = atoi(paras["comps"].asCString());
    if( comps_ < 1 || comps_ > 256) {
      err_code_ = CREATE_CLUSTER_QUEST_COMPS_PARAM_ERROR ;
      KLOG_ERROR("comps error(must in 1-256)");
      goto end;
    }

    if (!paras.isMember("max_storage_size")) {
      err_code_ = CREATE_CLUSTER_QUEST_MISS_MAX_STORAGE_SIZE_ERROR;
      KLOG_ERROR("missing `max_storage_size` key-value pair in the request body");
      goto end;
    }
    if(!CheckStringIsDigit(paras["max_storage_size"].asString())) {
      err_code_ = CREATE_CLUSTER_QUEST_MISS_MAX_STORAGE_SIZE_ERROR;
      KLOG_ERROR("missing `max_storage_size` key-value pair in the request body");
      goto end;
    }
    max_storage_size_ = atoi(paras["max_storage_size"].asCString());

    if (!paras.isMember("max_connections")) {
      err_code_ = CREATE_CLUSTER_QUEST_MISS_MAX_CONNECTIONS_ERROR;
      KLOG_ERROR("missing `max_connections` key-value pair in the request body");
      goto end;
    }
    if(!CheckStringIsDigit(paras["max_connections"].asString())) {
      err_code_ = CREATE_CLUSTER_QUEST_MISS_MAX_CONNECTIONS_ERROR;
      KLOG_ERROR("missing `max_connections` key-value pair in the request body");
      goto end;
    }
    max_connections_ = atoi(paras["max_connections"].asCString());

    if (!paras.isMember("cpu_cores")) {
      err_code_ = CREATE_CLUSTER_QUEST_MISS_CPU_CORES_ERROR;
      KLOG_ERROR("missing `cpu_cores` key-value pair in the request body");
      goto end;
    }
    if(!CheckStringIsDigit(paras["cpu_cores"].asString())) {
      err_code_ = CREATE_CLUSTER_QUEST_MISS_CPU_CORES_ERROR;
      KLOG_ERROR("missing `cpu_cores` key-value pair in the request body");
      goto end;
    }
    cpu_cores_ = atoi(paras["cpu_cores"].asCString());

    if (!paras.isMember("innodb_size")) {
      err_code_ = CREATE_CLUSTER_QUEST_MISS_INNODB_SIZE_ERROR;
      KLOG_ERROR("missing `innodb_size` key-value pair in the request body");
      goto end;
    }
    if(!CheckStringIsDigit(paras["innodb_size"].asString())) {
      err_code_ = CREATE_CLUSTER_QUEST_MISS_INNODB_SIZE_ERROR;
      KLOG_ERROR("missing `innodb_size` key-value pair in the request body");
      goto end;
    }
    innodb_size_ = atoi(paras["innodb_size"].asCString());
    if(innodb_size_ < 1)	{
      err_code_ = CREATE_CLUSTER_QUEST_INNODB_SIZE_PARAM_ERROR;
      KLOG_ERROR("innodb_size error(must > 0)");
      goto end;
    }

    if (paras.isMember("dbcfg")) {
      if(CheckStringIsDigit(paras["dbcfg"].asString()))
        dbcfg_ = atoi(paras["dbcfg"].asCString());
      else
        dbcfg_ = 0;
    }

    if(paras.isMember("computer_user")) {
      std::string comp_user = paras["computer_user"].asString();
      if(!comp_user.empty())
        computer_user_ = comp_user;
    }
    if(paras.isMember("computer_password")) {
      std::string comp_pwd = paras["computer_password"].asString();
      if(!comp_pwd.empty())
        computer_pwd_ = comp_pwd;
    }

    if(paras.isMember("storage_iplists")) {
      storage_iplists = paras["storage_iplists"];
      for(i=0; i<storage_iplists.size();i++) {
        storage_iplists_.push_back(storage_iplists[i].asString());
      }
      if(!CheckMachineIplists(M_STORAGE, storage_iplists_)) {
        err_code_ = CREATE_CLUSTER_ASSIGN_STORAGE_IPLISTS_ERROR;
        goto end;
      }
    }

    if(paras.isMember("computer_iplists")) {
      computer_iplists = paras["computer_iplists"];
      for(i=0; i<computer_iplists.size();i++) {
        computer_iplists_.push_back(computer_iplists[i].asString());
      }
      if(!CheckMachineIplists(M_COMPUTER, computer_iplists_)) {
        err_code_ = CREATE_CLUSTER_ASSIGN_COMPUTER_IPLISTS_ERROR;
        goto end;
      }
    }

    GenerateClusterName();
  
end:
    if(err_code_) {
      ret = false;
      job_status_ = "failed";
      UpdateOperationRecord();
    } else {
      job_status_ = "ongoing";
      UpdateMetaAndOperationStat(S_PREPARE);
      UpdateMetaAndOperationStat(C_PREPARE);
    }
    return ret;
}

void CreateClusterMission::GenerateClusterName() {
  int cluster_id = 0;
	System::get_instance()->get_max_cluster_id(cluster_id);
	cluster_id += 1;
	if(cluster_id <= g_inc_cluster_id)
		cluster_id = g_inc_cluster_id+1;
	g_inc_cluster_id = cluster_id;

	//while(true)	{
	char buf[10];
	snprintf(buf, 10, "_%06d", cluster_id);
	GetTimestampStr(cluster_name_);
	cluster_name_ = "cluster_" + cluster_name_ + buf;
		//check for no repeat

		//if(!System::get_instance()->check_cluster_name(cluster_name_))	{
    //  KLOG_INFO("cluster_name: {}", cluster_name_);
		//	break;
		//}
	//}
}

void CreateClusterMission::GetTimestampStr(std::string& str_ts) {
	struct timespec ts = {0,0};
	clock_gettime(CLOCK_REALTIME, &ts);
  str_ts = string_sprintf("%lu", ts.tv_sec);
}

bool CreateClusterMission::ArrangeRemoteTask() {
  if (get_init_by_recover_flag() == true) {
    MysqlResult result;
    std::string sql = string_sprintf("select memo from %s.cluster_general_job_log where id=%s",
              KUNLUN_METADATA_DB_NAME, job_id_.c_str());
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
      KLOG_ERROR("get create cluster job memo failed {}", g_node_channel_manager->getErr());
      return false;
    }
    
    if(result.GetResultLinesNum() != 1) {
      KLOG_ERROR("get create cluster job memo too many by job_id {}", job_id_);
      return false;
    }

    std::string memo = result[0]["memo"];
    if(memo.empty() || memo == "NULL")
      return true;

    Json::Value memo_json;
    Json::Reader reader;
    ret = reader.parse(memo, memo_json);
    if (!ret) {
      KLOG_ERROR("parse memo json failed: {}", reader.getFormattedErrorMessages());
      return false;
    }
    std::string shard_state = memo_json["shard_state"].asString();
    std::string computer_state = memo_json["computer_state"].asString(); 
    if(memo_json.isMember("cluster_id"))
      cluster_id_ = atoi(memo_json["cluster_id"].asCString());

    if(shard_state == "done" && computer_state == "done") {
      bool ret = PostCreateCluster();
      if(ret) {
        job_status_ = "done";
        UpdateOperationRecord();
        return ret;
      }  
    } 

    RollbackStorageJob();
    RollbackComputerJob();
    if((storage_dstate_ == "done") && (computer_dstate_ == "done"))
      RollbackMetaTables();

    err_code_ = CLUSTER_MGR_RESTART_ERROR;
    job_status_ = "failed";
    UpdateOperationRecord();
  }
  return true;
}

void CreateClusterMission::DealLocal() {
  KLOG_INFO("create cluster deal local phase");
  if(job_status_ == "failed" || job_status_ == "done")
    return;

  //1 insert cluster info db_clusters
  std::string ddl_log_tblname="ddl_ops_log_"+cluster_name_;
  std::string commit_log_tblname = "commit_log_"+cluster_name_;
  std::string sql = string_sprintf("insert into %s.db_clusters(name, nick_name, owner, ddl_log_tblname, business, ha_mode) values('%s', '%s', 'abc', '%s', 'kunlun', '%s')",
          KUNLUN_METADATA_DB_NAME, cluster_name_.c_str(), nick_name_.c_str(), ddl_log_tblname.c_str(), ha_mode_.c_str());
  MysqlResult result;
  bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  if(ret) {
    KLOG_ERROR("insert cluster_name {} into db_clusters failed {}", cluster_name_, g_node_channel_manager->getErr());
    err_code_ = CREATE_CLUSTER_INSERT_CLUSTER_NAME_ERROR;
    goto end1;
  }

  //2. create ddl log 
  sql = string_sprintf("create table %s.ddl_ops_log_%s like %s.ddl_ops_log_template_table", 
            KUNLUN_METADATA_DB_NAME, cluster_name_.c_str(), KUNLUN_METADATA_DB_NAME);
  ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  if(ret) {
    if(!CheckTableExists(ddl_log_tblname)) {
      KLOG_ERROR("create ddl_op_log for cluster_name {} failed {}", cluster_name_, g_node_channel_manager->getErr());
      err_code_ = CREATE_CLUSTER_DDL_OPS_LOG_TABLE_ERROR;
      goto end1;
    }
  }
  
  //3. create commit log
  sql = string_sprintf("create table %s.commit_log_%s like %s.commit_log_template_table",
            KUNLUN_METADATA_DB_NAME, cluster_name_.c_str(), KUNLUN_METADATA_DB_NAME);
  ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  if(ret) {
    if(!CheckTableExists(commit_log_tblname)) {
      KLOG_ERROR("create commit_log for cluster_name {} failed {}", cluster_name_, g_node_channel_manager->getErr());
      err_code_ = CREATE_CLUSTER_COMMIT_LOG_TABLE_ERROR;
      goto end1;
    }
  }

  sql = string_sprintf("select id from %s.db_clusters where name='%s'", 
                KUNLUN_METADATA_DB_NAME, cluster_name_.c_str());
  ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  if(ret) {
    KLOG_ERROR("get cluster_name {} id failed {}", cluster_name_, g_node_channel_manager->getErr());
    err_code_ = CREATE_CLUSTER_GET_CLUSTER_ID_ERROR;
    goto end1;
  }
  if(result.GetResultLinesNum() != 1) {
    KLOG_ERROR("get cluster_name {} id too many", cluster_name_);
    err_code_  = CREATE_CLUSTER_GET_CLUSTER_ID_RECORD_ERROR;
    goto end1;
  }
  cluster_id_ = atoi(result[0]["id"]);

  if(!AddShardJobs()) 
    goto end1;
  UpdateMetaAndOperationStat(S_DISPATCH);
  
  if(!AddComputerJobs())
    goto end1;

  UpdateMetaAndOperationStat(C_DISPATCH);
  goto end;

end1:
  sql = string_sprintf("drop table %s.commit_log_%s", KUNLUN_METADATA_DB_NAME, cluster_name_.c_str());
  g_node_channel_manager->send_stmt(sql, &result, stmt_retries);

  sql = string_sprintf("drop table %s.ddl_ops_log_%s", KUNLUN_METADATA_DB_NAME, cluster_name_.c_str());
  g_node_channel_manager->send_stmt(sql, &result, stmt_retries);

  sql = string_sprintf("delete from %s.db_clusters where name='%s'", 
            KUNLUN_METADATA_DB_NAME, cluster_name_.c_str());
  g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
end:
  if(err_code_) 
    job_status_ = "failed";
  
  UpdateOperationRecord();

  return;
}

bool CreateClusterMission::AddShardJobs() {
  ObjectPtr<AddShardMission> mission(new AddShardMission(ha_mode_, cluster_name_, cluster_id_, 
              shards_, nodes_, storage_iplists_, innodb_size_, dbcfg_,
              computer_user_, computer_pwd_));
  mission->SetNickName(nick_name_);
  mission->set_cb(CreateClusterMission::AddShardCallCB);
  mission->set_cb_context(this);

  mission->InitFromInternal();
  ObjectPtr<ClusterRequest> base_request(dynamic_cast<ClusterRequest*>(mission.GetTRaw()));
  g_request_handle_thread->DispatchRequest(base_request);
  //g_request_handle_thread->DispatchRequest(mission);
  shard_id_ = mission->get_request_unique_id();

  return true;
}

bool CreateClusterMission::AddComputerJobs() {
  ObjectPtr<AddComputerMission> mission(new AddComputerMission(comps_, cluster_name_, cluster_id_, 
                computer_iplists_, computer_user_, computer_pwd_, ha_mode_));
  mission->SetNickName(nick_name_);
  mission->set_cb(CreateClusterMission::AddComputerCallCB);
  mission->set_cb_context(this);

  mission->InitFromInternal();
  ObjectPtr<ClusterRequest> base_request(dynamic_cast<ClusterRequest*>(mission.GetTRaw()));
  g_request_handle_thread->DispatchRequest(base_request);
  //g_request_handle_thread->DispatchRequest(mission);
  computer_id_ = mission->get_request_unique_id();
  return true;
}

void CreateClusterMission::SetInstallErrorInfo(MachType mtype, const std::string& error_info) {
  if(mtype == M_STORAGE)
    shard_error_info_ = error_info;
  else 
    comp_error_info_ = error_info;
}

void CreateClusterMission::AddShardCallCB(Json::Value& root, void *arg) {
  CreateClusterMission *mission = static_cast<CreateClusterMission*>(arg);
  Json::FastWriter writer;
  std::string result = writer.write(root);
  KLOG_INFO("AddShardCallCB result {}", result);

  std::string status = root["status"].asString();
  if(status == "failed") {
    mission->SetInstallErrorInfo(M_STORAGE, root["error_info"].asString());
    mission->UpdateMetaAndOperationStat(S_FAILED);
    return;
  }
 
  mission->UpdateMetaAndOperationStat(S_DONE);
}

void CreateClusterMission::AddComputerCallCB(Json::Value& root, void *arg) {
  CreateClusterMission *mission = static_cast<CreateClusterMission*>(arg);
  Json::FastWriter writer;
  std::string result = writer.write(root);
  KLOG_INFO("AddComputerCallCB result {}", result);

  std::string status = root["status"].asString();
  if(status == "failed") {
    mission->SetInstallErrorInfo(M_COMPUTER, root["error_info"].asString());
    mission->UpdateMetaAndOperationStat(C_FAILED);
    return;
  }

  mission->UpdateMetaAndOperationStat(C_DONE);
}

bool CreateClusterMission::UpdateOperationRecord() {
    std::string str_sql,memo;
    Json::Value memo_json;
    Json::FastWriter writer;

    memo_json["error_code"] = err_code_;
    memo_json["error_info"] = GlobalErrorNum(err_code_).get_err_num_str();
    writer.omitEndingLineFeed();
    memo = writer.write(memo_json);

    str_sql = "UPDATE cluster_general_job_log set status='" + job_status_ + "',memo='" + memo;
    str_sql += "',when_ended=current_timestamp(6) where id=" + job_id_;

    if(System::get_instance()->execute_metadate_opertation(SQLCOM_UPDATE, str_sql)) {
        KLOG_ERROR( "execute_metadate_opertation error");
        return false;
    }
    KLOG_INFO("update cluster_general_job_log success sql: {}", str_sql);
    if(job_status_ == "done" || job_status_ == "failed") {
        //std::unique_lock<std::mutex> lock{ cond_mux_ };
        //cond_.notify_all();
        cond_.SignalAll();
    }
    return true;
}

void CreateClusterMission::RollbackStorageJob() {
  MysqlResult result;
  std::string sql = string_sprintf("select id from %s.shards where db_cluster_id=%d",
                    KUNLUN_METADATA_DB_NAME, cluster_id_);
  bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  if(ret) {
    KLOG_ERROR("get shard_id from shards by db_cluster_id {} failed", cluster_id_);
    return;
  }

  std::vector<int> shard_ids;
  int nrows = result.GetResultLinesNum();
  for(int i=0; i<nrows; i++) {
    int shard_id = atoi(result[i]["id"]);
    shard_ids.emplace_back(shard_id);
  }

  if(shard_ids.size() > 0) {
    storage_dstate_ = "ongoing";
    ObjectPtr<DeleteShardMission> mission(new DeleteShardMission(cluster_id_, shard_ids));
    mission->SetRollbackFlag(true);
    mission->set_cb(CreateClusterMission::DelShardCallCB);
    mission->set_cb_context(this);
    
    mission->InitFromInternal();

    ObjectPtr<ClusterRequest> base_request(dynamic_cast<ClusterRequest*>(mission.GetTRaw()));
    g_request_handle_thread->DispatchRequest(base_request);
  } else 
    storage_dstate_ = "done";
  //g_request_handle_thread->DispatchRequest(mission);
}

void CreateClusterMission::RollbackComputerJob() {
  MysqlResult result;
  std::string sql = string_sprintf("select id from %s.comp_nodes where db_cluster_id=%d",
                    KUNLUN_METADATA_DB_NAME, cluster_id_);
  bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  if(ret) {
    KLOG_ERROR("get comp_id from comp_nodes by db_cluster_id {} failed", cluster_id_);
    return;
  }

  std::vector<int> comp_ids;
  int nrows = result.GetResultLinesNum();
  for(int i=0; i<nrows; i++) {
    int comp_id = atoi(result[i]["id"]);
    comp_ids.emplace_back(comp_id);
  }

  if(comp_ids.size() > 0) {
    computer_dstate_ = "ongoing";
    ObjectPtr<DeleteComputerMission> mission(new DeleteComputerMission(cluster_id_, comp_ids));
    mission->set_cb(CreateClusterMission::DelComputerCallCB);
    mission->set_cb_context(this);
  
    mission->InitFromInternal();
    
    ObjectPtr<ClusterRequest> base_request(dynamic_cast<ClusterRequest*>(mission.GetTRaw()));
    g_request_handle_thread->DispatchRequest(base_request);
  } else
    computer_dstate_ = "done";
}

void CreateClusterMission::UpdateRollbackState(MachType mtype, JobType jtype) {
  KlWrapGuard<KlWrapMutex> guard(roll_mux_);

  if(mtype == M_STORAGE) {
    if(jtype == S_FAILED)
      storage_dstate_ = "failed";
    else
      storage_dstate_ = "done";
  } else {
    if(jtype == C_FAILED)
      computer_dstate_ = "failed";
    else
      computer_dstate_ = "done";
  }

  if(storage_dstate_.empty() && computer_dstate_.empty())
    RollbackMetaTables();
  else if(storage_dstate_.empty() && computer_dstate_ == "done")
    RollbackMetaTables();
  else if(storage_dstate_ == "done" && computer_dstate_.empty())
    RollbackMetaTables();
  else if(storage_dstate_ == "done" && computer_dstate_ == "done")
    RollbackMetaTables();
}

void CreateClusterMission::DelShardCallCB(Json::Value& root, void *arg) {
  CreateClusterMission *mission = static_cast<CreateClusterMission*>(arg);
  Json::FastWriter writer;
  std::string result = writer.write(root);
  KLOG_INFO("DelShardCallCB result {}", result);

  std::string status = root["status"].asString();
  if(status == "failed") {
    mission->UpdateRollbackState(M_STORAGE, S_FAILED);
    return;
  }

  mission->UpdateRollbackState(M_STORAGE, S_DONE);
}

void CreateClusterMission::DelComputerCallCB(Json::Value& root, void *arg) {
  CreateClusterMission *mission = static_cast<CreateClusterMission*>(arg);
  Json::FastWriter writer;
  std::string result = writer.write(root);
  KLOG_INFO("DelComputerCallCB result {}", result);

  std::string status = root["status"].asString();
  if(status == "failed") {
    mission->UpdateRollbackState(M_COMPUTER, C_FAILED);
    return;
  }

  mission->UpdateRollbackState(M_COMPUTER, C_DONE);
}

void CreateClusterMission::RollbackMetaTables() {
  KLOG_INFO("create cluster failed, rollback meta tables...");
  MysqlResult result;

  std::string ddl_log_tblname="ddl_ops_log_"+cluster_name_;
  std::string sql = string_sprintf("drop table %s.%s",
          KUNLUN_METADATA_DB_NAME, ddl_log_tblname.c_str());
  bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  if(ret) 
    KLOG_ERROR("rollback to drop table {} failed", ddl_log_tblname);

  std::string commit_log_tbname = "commit_log_"+cluster_name_;
  sql = string_sprintf("drop table %s.%s",
          KUNLUN_METADATA_DB_NAME, commit_log_tbname.c_str());
  ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  if(ret) 
    KLOG_ERROR("rollback to drop table {} failed", commit_log_tbname);

  sql = string_sprintf("delete from %s.db_clusters where name='%s'",
          KUNLUN_METADATA_DB_NAME, cluster_name_.c_str());
  ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  if(ret) 
    KLOG_ERROR("rollback to delete cluster_name {} failed", cluster_name_);

  System::get_instance()->stop_cluster(cluster_name_);

  if(job_status_ == "done" || job_status_ == "failed") {
    //std::unique_lock<std::mutex> lock{ cond_mux_ };
    //cond_.notify_all();
    cond_.SignalAll();
  }
}

void CreateClusterMission::UpdateMetaAndOperationStat(JobType jtype) {
  KlWrapGuard<KlWrapMutex> guard(update_mux_);

  std::string str_sql,memo;
  Json::Value memo_json;
  Json::FastWriter writer;

  if(jtype == C_PREPARE || jtype == S_PREPARE) {
    if(jtype == S_PREPARE)
      storage_state_ = "prepare";
    else
      computer_state_ = "prepare";
  } else if(jtype == C_DISPATCH || jtype == S_DISPATCH) {
    if(jtype == S_DISPATCH)
      storage_state_ = "dispatch";

    if(jtype == C_DISPATCH)
      computer_state_ = "dispatch";
  } else if(jtype == S_FAILED || jtype == C_FAILED) {
    if(jtype == S_FAILED) {
      storage_state_ = "failed";
      job_status_ = "failed";
      err_code_ = CREATE_CLUSTER_INSTALL_STORAGE_ERROR;
      memo_json["shard_error_info"] = shard_error_info_;
      if(computer_state_ == "done") {
        RollbackComputerJob();
      }
    }

    if(jtype == C_FAILED) {
      computer_state_ = "failed";
      job_status_ = "failed";
      err_code_ = CREATE_CLUSTER_INSTALL_COMPUTER_ERROR;
      memo_json["comp_error_info"] = comp_error_info_;
      if(storage_state_ == "done") {
        RollbackStorageJob();
      }
    } 
  } else if(jtype == S_DONE || jtype == C_DONE) {
    if(jtype == S_DONE) {
      if(computer_state_ == "failed")
        RollbackStorageJob();

      storage_state_ = "done";
    }

    if(jtype == C_DONE) {
      if(storage_state_ == "failed") 
        RollbackComputerJob();

      computer_state_ = "done";
    }
  }

  if(!storage_state_.empty())
    memo_json["storage_state"] = storage_state_;
  if(!computer_state_.empty())
    memo_json["computer_state"] = computer_state_;
  if(cluster_id_ != -2) 
    memo_json["cluster_id"] = std::to_string(cluster_id_);

  if(!computer_id_.empty())
    memo_json["computer_id"] = computer_id_;
  if(!shard_id_.empty())
    memo_json["shard_id"] = shard_id_;

  if(!cluster_name_.empty())
    memo_json["cluster_name"] = cluster_name_;
    
  memo_json["computer_user"] = computer_user_;
  memo_json["computer_pwd"] = computer_pwd_;
  memo_json["error_code"] = err_code_;
  memo_json["error_info"] = GlobalErrorNum(err_code_).get_err_num_str();
  writer.omitEndingLineFeed();
  memo = writer.write(memo_json);
  if(storage_state_ == "done" && computer_state_ == "done") {
    job_status_ = "done";
    
    if(!PostCreateCluster()) {
      job_status_ = "failed";
      RollbackStorageJob();
      RollbackComputerJob();
      //RollbackMetaTables();
    }
  }

  str_sql = "UPDATE cluster_general_job_log set status='" + job_status_ + "',memo='" + memo;
  str_sql += "',when_ended=current_timestamp(6) where id=" + job_id_;
  if(System::get_instance()->execute_metadate_opertation(SQLCOM_UPDATE, str_sql)) {
    KLOG_ERROR( "execute_metadate_opertation error");
    return;
  }
  KLOG_INFO("update cluster_general_job_log success: {}", str_sql);
  if(job_status_ == "done") {
    //std::unique_lock<std::mutex> lock{ cond_mux_ };
    //cond_.notify_all();
    cond_.SignalAll();
  } else if(storage_state_ == "failed" && computer_state_ == "failed") {
    RollbackMetaTables();
  }
}


bool CreateClusterMission::PostCreateCluster() {
  Json::Value paras;
	std::string str_sql;
  std::string memo;

  paras["ha_mode"] = ha_mode_;
  paras["shards"] = std::to_string(shards_);
  paras["nodes"] = std::to_string(nodes_);
  paras["comps"] = std::to_string(comps_);
  paras["max_storage_size"] = std::to_string(max_storage_size_);
  paras["max_connections"] = std::to_string(max_connections_);
  paras["cpu_cores"] = std::to_string(cpu_cores_);
  paras["computer_user"] = computer_user_;
  paras["computer_passwd"] = computer_pwd_;
  paras["innodb_size"] = std::to_string(innodb_size_);
  paras["dbcfg"] = std::to_string(dbcfg_);

  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  memo = writer.write(paras);

  str_sql = string_sprintf("update %s.db_clusters set memo='%s' where name='%s'",
            KUNLUN_METADATA_DB_NAME, memo.c_str(), cluster_name_.c_str());
	if(System::get_instance()->execute_metadate_opertation(SQLCOM_UPDATE, str_sql))	{
    KLOG_ERROR("update cluster info error");
		return false;
	}

  return true;
}

bool CreateClusterMission::TearDownMission() {
  //std::unique_lock<std::mutex> lock{ cond_mux_ };
	//cond_.wait(lock, [this] {
	//    return (job_status_ == "done" || job_status_ == "failed");
	//});
  cond_.Wait();
  //delete this;
  return true;
}

void GetCreateClusterStatus(Json::Value& doc) {
  doc["job_steps"] = "shard,computer";
  std::string shard_id, computer_id;
  if(doc.isMember("shard_id"))
    shard_id = doc["shard_id"].asString();

  if(doc.isMember("computer_id"))
    computer_id = doc["computer_id"].asString();

  MysqlResult result;
  std::string sql;

  Json::Reader reader;
  Json::Value memo_json;
  if(!shard_id.empty()) {
    sql = string_sprintf("select memo from %s.cluster_general_job_log where id=%s",
              KUNLUN_METADATA_DB_NAME, shard_id.c_str());
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
      KLOG_ERROR("get shard_id {} memo from cluster_general_job_log failed {}", shard_id, 
                g_node_channel_manager->getErr());
    } else {
      if(result.GetResultLinesNum() == 1) {
        if(reader.parse(result[0]["memo"], memo_json)) {
          doc["shard_step"].append(memo_json);
        }
      }
    }
  }

  if(!computer_id.empty()) {
    sql = string_sprintf("select memo from %s.cluster_general_job_log where id=%s",
              KUNLUN_METADATA_DB_NAME, computer_id.c_str());
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
      KLOG_ERROR("get computer_id {} memo from cluster_general_job_log failed {}", computer_id, 
                g_node_channel_manager->getErr());
    } else {
      if(result.GetResultLinesNum() == 1) {
        if(reader.parse(result[0]["memo"], memo_json)) {
          doc["computer_step"].append(memo_json);
        }
      }
    }
  }
}

}