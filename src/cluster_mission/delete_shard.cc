/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "delete_shard.h"
#include "kl_mentain/shard.h"
#include "kl_mentain/sys.h"
#include "cluster_operator/shardInstallMission.h"
#include "request_framework/handleRequestThread.h"
#include "cluster_collection/prometheus_control.h"
#include "coldbackup/coldbackup.h"

extern HandleRequestThread *g_request_handle_thread;
extern kunlun::KPrometheusControl* g_prometheus_manager;

namespace kunlun
{

bool DeleteShardMission::SetUpMisson() {
    job_id_ = get_request_unique_id();
    if(get_init_by_recover_flag() == true) {
        KLOG_INFO("Delete shard ignore recover state in setup phase");
        return true;
    }

    KLOG_INFO("delete shard setup phase");
    
    ClusterRequestTypes request_type = get_request_type();
    
    bool ret = true;
    int i = 0;
    Json::Value paras;
    if(init_flag_) {
        KLOG_ERROR("delete shard called by internal");
        goto end;
    }

    if (!super::get_body_json_document().isMember("paras")) {
        err_code_ = DELETE_SHARD_QUEST_LOST_PARAS_ERROR;
        KLOG_ERROR("missing `paras` key-value pair in the request body");
        goto end;
    }
    paras = super::get_body_json_document()["paras"];

    if (!paras.isMember("cluster_id")) {
      KLOG_ERROR("missing `cluster_id` key-value pair in the request body");
      err_code_ = DELETE_SHARD_QUEST_MISS_CLUSTERID_ERROR;
      goto end;
    }
    if(!CheckStringIsDigit(paras["cluster_id"].asString())) {
        err_code_ = DELETE_SHARD_QUEST_MISS_CLUSTERID_ERROR;
        goto end;
    }
    cluster_id_ = atoi(paras["cluster_id"].asCString());

    if (!paras.isMember("shard_id")) {
      KLOG_ERROR("missing `shard_id` key-value pair in the request body");
      err_code_ = DELETE_SHARD_QUEST_MISS_SHARDID_ERROR;
      goto end;
    }
    if(!CheckStringIsDigit(paras["shard_id"].asString())) {
        err_code_ = DELETE_SHARD_QUEST_MISS_SHARDID_ERROR;
        goto end;
    }
    shard_id_ = atoi(paras["shard_id"].asCString());

    CheckDeleteShardState();

end:
    if(err_code_) {
        ret = false;
        job_status_ = "failed";
        UpdateOperationRecord();
    } else 
        job_status_ = "ongoing";

    return ret;
}

void DeleteShardMission::CheckDeleteShardState() {
    std::string sql = string_sprintf("select id from %s.shards where db_cluster_id=%d",
                    KUNLUN_METADATA_DB_NAME, cluster_id_);
    MysqlResult result;
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("get shards number sql from metadb failed");
        err_code_ = DELETE_SHARD_GET_SHARD_SQL_ERROR;
        return;
    }
    int nrows = result.GetResultLinesNum();
    if(nrows <= 1) {
        KLOG_ERROR("cluster_id {} just have one shard, so can't delete", cluster_id_);
        err_code_ = DELETE_SHARD_GET_SHARD_NUM_ERROR;
        return;
    }

    sql = string_sprintf("select name from %s.db_clusters where id=%d",
                KUNLUN_METADATA_DB_NAME, cluster_id_);
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret || result.GetResultLinesNum() != 1) {
        KLOG_ERROR("get shards number sql from metadb failed");
        err_code_ = DELETE_SHARD_GET_SHARD_SQL_ERROR;
        return;
    }
    cluster_name_ = result[0]["name"];
}

void DeleteShardMission::DealLocal() {
    KLOG_INFO("delete shard deal local phase");
    if(job_status_ == "failed" || job_status_ == "done") {
        return;
    }

    storage_state_ = "prepare";
    MysqlResult result;
    //1. get shard_nodes
    std::vector<int> del_shards;
    if(shard_ids_.size() == 0)
        del_shards.push_back(shard_id_);
    else {
        for(size_t i=0; i<shard_ids_.size(); i++)
            del_shards.push_back(shard_ids_[i]);
    }

    ObjectPtr<MetadataShard> metashard = System::get_instance()->get_MetadataShard();
    if(metashard.Invalid())
        metashard->update_shard_delete_state(cluster_id_, del_shards);

    std::string sql;
    for(size_t i=0; i<del_shards.size(); i++) {
        sql = string_sprintf("select hostaddr, port from %s.shard_nodes where shard_id=%d and db_cluster_id=%d",
                        KUNLUN_METADATA_DB_NAME, del_shards[i], cluster_id_);
        bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
        if(ret) {
            err_code_ = DELETE_SHARD_GET_STORAGE_NODES_ERROR;
            job_status_ = "failed";
            KLOG_ERROR("get shard nodes from meta failed {}", g_node_channel_manager->getErr());
            UpdateOperationRecord();
            return;
        }

        int nrows = result.GetResultLinesNum();
        for(int i=0; i<nrows; i++) {
            std::string ip = result[i]["hostaddr"];
            int port = atoi(result[i]["port"]);
            IComm_Param dparam{ip, port};
            storage_dparams_.emplace_back(dparam);
        }
    }

    if(!rollback_flag_) {
        std::vector<std::string> key_name;
        key_name.push_back("memo.computer_user");
        key_name.push_back("memo.computer_passwd");
        CMemo_Params memo_paras = GetClusterMemoParams(cluster_id_, key_name);
        if(!std::get<0>(memo_paras)) {
            err_code_ = std::get<1>(memo_paras);
            job_status_ = "failed";
            UpdateOperationRecord();
            return;
        }

        std::map<std::string, std::string> key_vals = std::get<2>(memo_paras);
        computer_user_ = key_vals["computer_user"];
        computer_pwd_ = key_vals["computer_passwd"];
    }

    if(!DelShardJobs()) {
        job_status_ = "failed";
        UpdateOperationRecord();
        return;
    }
    storage_state_ = "dispatch";
    return;
}

bool DeleteShardMission::InitFromInternal() {
    Json::Value doc;
    FillCommonJobJsonDoc(doc);
    doc["job_type"] = "internal_del_shard";

    Json::Value para;
    para["cluster_id"] = std::to_string(cluster_id_);
    for(size_t i=0; i<shard_ids_.size(); i++) {
        para["shard_ids"].append(shard_ids_[i]);
    }
    doc["paras"] = para;
    set_body_json_document(doc);
    CompleteGeneralJobInfo();
    return true;
}

void DeleteShardMission::CompleteGeneralJobInfo() {
    Json::FastWriter writer;
    writer.omitEndingLineFeed();
    std::string job_info = writer.write(get_body_json_document());

    kunlun::MysqlResult result;
    std::string sql = string_sprintf("update %s.cluster_general_job_log set "
                "job_type='internal_del_shard',job_info='%s' where id = %s",
                KUNLUN_METADATA_DB_NAME ,job_info.c_str(), get_request_unique_id().c_str());
    
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if (ret) {
        KLOG_ERROR("update cluster_general_job_log faild: {}", g_node_channel_manager->getErr());
        return;
    }
    KLOG_INFO("update cluster_general_job_log success: {}", sql);
}

bool DeleteShardMission::ArrangeRemoteTask() {
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
        int init_flag = atoi(memo_json["rollback_flag"].asCString());
        if(init_flag) {
            KLOG_INFO("delete shard called by other, so quit to restart");
            return true;
        }

        std::string storage_state = memo_json["storage_state"].asString();
        int cluster_id_ = ::atoi(memo_json["cluster_id"].asString().c_str());
        if(memo_json.isMember("shard_id"))
            shard_id_ = atoi(memo_json["shard_id"].asCString());
        
        if(memo_json.isMember("shard_ids")) {
            std::string shard_ids = memo_json["shard_ids"].asString();
            std::vector<std::string> vec_ids = StringTokenize(shard_ids, ",");
            for(auto it : vec_ids) {
                int id = atoi(it.c_str());
                shard_ids_.emplace_back(id);
            }
        }

        if(storage_state == "failed") {
            job_status_ = "failed";
        } else if(storage_state == "done") {
            if(!PostDeleteShard()) {
                job_status_ = "failed";
                err_code_ = DELETE_SHARD_POST_DEAL_ERROR; 
            } else 
                job_status_ = "done";
        } 
    }
    UpdateOperationRecord();
    return true;
}

bool DeleteShardMission::DelShardJobs() {
    ObjectPtr<KDelShardMission> mission(new KDelShardMission());
    mission->set_cb(DeleteShardMission::DelShardCallCB);
    mission->set_cb_context(this);

    for(size_t i=0; i<storage_dparams_.size(); i++) {
        struct InstanceInfoSt st1 ;
        st1.ip = std::get<0>(storage_dparams_[i]);
        st1.port = std::get<1>(storage_dparams_[i]);
        st1.exporter_port = std::get<1>(storage_dparams_[i]) + 1;
        st1.mgr_port = std::get<1>(storage_dparams_[i]) + 2;
        st1.xport = std::get<1>(storage_dparams_[i]) + 3;
        st1.innodb_buffer_size_M = 1;
        if(i == 0)
            st1.role = "master";
        else 
            st1.role = "slave";

        mission->setInstallInfoOneByOne(st1);
    }

    bool ret = mission->InitFromInternal();
    storage_del_id_ = mission->get_request_unique_id();
    if(!ret){
        KLOG_ERROR("DeleteShard mission failed {}", mission->getErr());
        storage_state_ = "failed";
        err_code_ = DELETE_SHARD_DISPATCH_STORAGE_JOB_ERROR;
        return false;
    }
    ObjectPtr<ClusterRequest> base_request(dynamic_cast<ClusterRequest*>(mission.GetTRaw()));
    g_request_handle_thread->DispatchRequest(base_request);
    //g_request_handle_thread->DispatchRequest(mission);
    return true;
}

void DeleteShardMission::DelShardCallCB(Json::Value& root, void *arg) {
    DeleteShardMission* mission = static_cast<DeleteShardMission*>(arg);
    Json::FastWriter writer;
    std::string result = writer.write(root);
    KLOG_INFO("DelShardCallCB result {}", result);

    std::string status = root["status"].asString();
    if(status == "failed") {
        mission->UpdateCallCBFail(root["error_info"].asString());
        return;
    }
    mission->UpdateCallCBDone();
}

void DeleteShardMission::UpdateCallCBFail(const std::string& error_info) {
    job_status_ = "failed";
    storage_state_ = "failed";
    err_code_ = DELETE_SHARD_DELETE_STORAGE_ERROR;
    UpdateOperationRecord(error_info);
}

void DeleteShardMission::UpdateCallCBDone() {
    job_status_ = "done";
    storage_state_ = "done";
    if(!PostDeleteShard()) {
        job_status_ = "failed";
        err_code_ = DELETE_SHARD_POST_DEAL_ERROR;
    }

    UpdateOperationRecord();
}

void DeleteShardMission::PackUsedPorts(std::map<std::string, std::vector<int> >& host_ports) {
    for(size_t i=0; i<storage_dparams_.size(); i++) {
      std::string hostaddr = std::get<0>(storage_dparams_[i]);
      std::vector<int> ports;
      if(host_ports.find(hostaddr) != host_ports.end()) {
        ports = host_ports[hostaddr];
      }
      ports.push_back(std::get<1>(storage_dparams_[i]));
      host_ports[hostaddr] = ports;
    }
    return;
}

bool DeleteShardMission::DelShardFromPgTables(const std::string& hostaddr, const std::string& port) {
    PGConnectionOption option;
    option.ip = hostaddr;
    option.port_num = atoi(port.c_str());
    option.user = computer_user_;
    option.password = computer_pwd_;
    option.database = "postgres";

    PGConnection pg_conn(option);
    if(!pg_conn.Connect()) {
        KLOG_ERROR("connect pg failed {}", pg_conn.getErr());
        return false;
    }

    PgResult pgresult;
    std::string sql;

    sql = "start transaction";
    if(pg_conn.ExecuteQuery(sql.c_str(), &pgresult) == -1) {
      KLOG_ERROR("postgres start transaction failed {}", pg_conn.getErr());
      return false;
    }

    sql = string_sprintf("delete from pg_shard where id=%d", shard_id_);
    if(pg_conn.ExecuteQuery(sql.c_str(), &pgresult) == -1) {
      KLOG_ERROR("postgres delete pg_shard failed {}", pg_conn.getErr());
      return false;
    }

    sql = string_sprintf("delete from pg_shard_node where shard_id=%d",
                shard_id_);
    if(pg_conn.ExecuteQuery(sql.c_str(), &pgresult) == -1) {
      KLOG_ERROR("postgres delete pg_shard_node failed {}", pg_conn.getErr());
      return false;
    }

    sql = "commit";
    if(pg_conn.ExecuteQuery(sql.c_str(), &pgresult) == -1) {
      KLOG_ERROR("postgres commit failed {}", pg_conn.getErr());
      return false;
    }
    return true;
}

bool DeleteShardMission::DeleteShardFromComputers() {
    std::string sql = string_sprintf("select hostaddr, port from %s.comp_nodes where db_cluster_id=%d",
                    KUNLUN_METADATA_DB_NAME, cluster_id_);
    MysqlResult result;
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("get computer hostaddr, port from comp_nodes failed");
        return false;
    }

    int nrows = result.GetResultLinesNum();
    for(int i=0; i<nrows; i++) {
        for(int j=0; j < 10; j++) {
            ret = DelShardFromPgTables(result[i]["hostaddr"], result[i]["port"]);
            if(!ret)
                sleep(2);
            else
                break;
        }
        if(!ret)
            return false;
    }
    return true;
}

bool DeleteShardMission::PostDeleteShard() {
    std::map<std::string, std::vector<int> > host_ports;
    PackUsedPorts(host_ports);
    if(!UpdateUsedPort(K_CLEAR, M_STORAGE, host_ports))
        return false;

    std::vector<int> del_shards;
    if(shard_ids_.size() == 0)
        del_shards.push_back(shard_id_);
    else {
        for(size_t i=0; i<shard_ids_.size(); i++)
            del_shards.push_back(shard_ids_[i]);
    }
    std::vector<std::string> storage_hosts;
    for(size_t i=0; i<storage_dparams_.size(); i++) {
        std::string hostaddr = std::get<0>(storage_dparams_[i]);
        std::string port = std::to_string(std::get<1>(storage_dparams_[i])+1);
        storage_hosts.emplace_back(hostaddr+":"+port);
    }

    MysqlResult result;
    std::string sql;
    bool ret;
    for(auto it : del_shards) {
        sql = string_sprintf("delete from %s.shard_nodes where shard_id=%d and db_cluster_id=%d",
                    KUNLUN_METADATA_DB_NAME, it, cluster_id_);
        ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
        if(ret) {
            KLOG_ERROR("delete shard_id {} from shard_nodes failed", it);
            continue;
        }

        sql = string_sprintf("delete from %s.shards where id=%d", 
                    KUNLUN_METADATA_DB_NAME, it);
        ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
        if(ret) {
            KLOG_ERROR("delete shard_id {} from shards failed", it);
        }
    }

    if(!rollback_flag_)
        DeleteShardFromComputers();
    System::get_instance()->del_shard_cluster_memory(cluster_id_, del_shards);
    ret = g_prometheus_manager->DelStorageConf(storage_hosts);
    if(ret) {
        KLOG_ERROR("delete prometheus mysqld_exporter config failed");
        //return false;
    }

    UpdateShardTopologyAndBackup();
    return true;
}

void DeleteShardMission::UpdateShardTopologyAndBackup() {
  std::string sql = string_sprintf("select id from %s.shards where db_cluster_id=%d", 
        KUNLUN_METADATA_DB_NAME, cluster_id_);
  MysqlResult result;
  bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  if(ret) {
    KLOG_ERROR("get cluster_id {} shard ids from shards", cluster_id_);
    return;
  }

  std::vector<int> shard_ids;
  std::vector<std::string> backup_ips;
  int nrows = result.GetResultLinesNum();
  for(int i=0; i<nrows; i++) {
    int id = atoi(result[i]["id"]);
    shard_ids.emplace_back(id);

    sql = string_sprintf("select hostaddr from %s.shard_nodes where db_cluster_id=%d and shard_id=%d and backup_node='ON'",
                KUNLUN_METADATA_DB_NAME, cluster_id_, id);
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret || result.GetResultLinesNum() != 1) {
        KLOG_ERROR("get back_node hostaddr from shard_nodes failed");
        continue;
    }
    backup_ips.emplace_back(result[0]["hostaddr"]);
  }

  sql = string_sprintf("select count(*) from %s.commit_log_%s", KUNLUN_METADATA_DB_NAME,
        cluster_name_.c_str());
  ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  if(ret || result.GetResultLinesNum() != 1) {
    KLOG_ERROR("get cluster_id {} commit log records", cluster_id_);
    return;
  }
  int max_commit_log_id = atoi(result[0]["count(*)"]);

  sql = string_sprintf("select max(id) from %s.ddl_ops_log_%s", KUNLUN_METADATA_DB_NAME,
          cluster_name_.c_str());
  ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  if(ret || result.GetResultLinesNum() != 1) {
    KLOG_ERROR("get cluster_id {} ddl ops log max id", cluster_id_);
    return;
  }
  int max_ddl_log_id = atoi(result[0]["max(id)"]);

  UpdateClusterShardTopology(cluster_id_, shard_ids, max_commit_log_id, max_ddl_log_id);
  for(auto bk_ip : backup_ips) {
    KColdBackUpMission * mission = new KColdBackUpMission(bk_ip.c_str());
    ret = mission->InitFromInternal();
    if (!ret){
        KLOG_ERROR("init coldbackupMisson faild: {}",mission->getErr());
        delete mission;
        continue;
      }
      KLOG_INFO("init coldbackupMisson successfully for host {}", bk_ip);
      //dispatch current mission
      g_request_handle_thread->DispatchRequest(mission);
  }
}

bool DeleteShardMission::UpdateOperationRecord(const std::string& error_info) {
    std::string str_sql,memo;
    Json::Value memo_json;
    Json::FastWriter writer;
    memo_json["rollback_flag"] = std::to_string(init_flag_);

    memo_json["cluster_id"] = std::to_string(cluster_id_);
    if(shard_ids_.empty())
        memo_json["shard_id"] = std::to_string(shard_id_);
    else {
        std::string shard_ids;
        for(auto id : shard_ids_) {
            shard_ids += std::to_string(id)+",";
        }
        memo_json["shard_ids"] = shard_ids;
    }

    memo_json["error_code"] = err_code_;
    if(error_info.empty())
        memo_json["error_info"] = GlobalErrorNum(err_code_).get_err_num_str();
    else {
        memo_json["error_info"] = error_info;
        error_info_ = error_info;
    }

    if(!storage_state_.empty()) 
        memo_json["storage_state"] = storage_state_;
    
    if(!storage_del_id_.empty())
        memo_json["storage_id"] = storage_del_id_;

    if(storage_dparams_.size() > 0) {
        std::string hosts;
        for(size_t i=0; i<storage_dparams_.size(); i++) {
        hosts += std::get<0>(storage_dparams_[i]) + "_" + std::to_string(std::get<1>(storage_dparams_[i])) + ";";
        }
        memo_json["storage_hosts"] = hosts;
    }

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

bool DeleteShardMission::TearDownMission() {
    //std::unique_lock<std::mutex> lock{ cond_mux_ };
	//cond_.wait(lock, [this] {
	//    return (storage_state_ == "done" || storage_state_ == "failed");
	//});
    cond_.Wait();

    Json::Value root;
    root["request_id"] = job_id_;
    root["status"] = job_status_;
    root["error_code"] = err_code_;
    if(error_info_.empty())
        root["error_info"] = GlobalErrorNum(err_code_).get_err_num_str();
    else
        root["error_info"] = error_info_;
    Json::FastWriter writer;
    writer.omitEndingLineFeed();
    std::string result = writer.write(root);
    KLOG_INFO("delete shard mission result: {}", result);
    SetSerializeResult(result);
    //delete this;
    return true; 
}

}
