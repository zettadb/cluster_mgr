/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "delete_node.h"
#include "request_framework/handleRequestThread.h"
#include "cluster_operator/shardInstallMission.h"
#include "zettalib/op_log.h"
#include "kl_mentain/shard.h"
#include "kl_mentain/sys.h"
#include "zettalib/op_pg.h"
#include "cluster_comm.h"
#include "cluster_collection/prometheus_control.h"

extern HandleRequestThread *g_request_handle_thread;
extern kunlun::KPrometheusControl* g_prometheus_manager;

namespace kunlun
{

bool DeleteNodeMission::ArrangeRemoteTask() {
    if(get_init_by_recover_flag() == true) {
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

        if(memo_json.isMember("storage_state"))
            storage_dstate_ = memo_json["storage_state"].asString();
        if(memo_json.isMember("computer_user"))
            computer_user_ = memo_json["computer_user"].asString();
        if(memo_json.isMember("computer_pwd"))
            computer_pwd_ = memo_json["computer_pwd"].asString();

        if(storage_dstate_ == "post") {
            if(PostDelNode()) {
                storage_dstate_ = "done";
                job_status_ = "done";
                UpdateOperationRecord();
                return true;
            }
        } 
    }
    return true;
}

bool DeleteNodeMission::SetUpMisson() {
    job_id_ = get_request_unique_id();
    if(get_init_by_recover_flag() == true) {
        KLOG_INFO("Delete node ignore recover state in setup phase");
        return true;
    }
    
    KLOG_INFO("Delete node setup phase");
    
    ClusterRequestTypes request_type = get_request_type();
    
    bool ret = true;
    Json::Value paras;

    if (!super::get_body_json_document().isMember("paras")) {
        err_code_ = DEL_NODE_QUEST_LOST_PARAS_ERROR;
        KLOG_ERROR("missing `paras` key-value pair in the request body");
        goto end;
    }
    paras = super::get_body_json_document()["paras"];

    if (!paras.isMember("cluster_id")) {
      err_code_ = DEL_NODE_QUEST_MISS_CLUSTERID_ERROR;
      KLOG_ERROR("missing `cluster` key-value pair in the request body");
      goto end;
    }
    if(!CheckStringIsDigit(paras["cluster_id"].asString())) {
      err_code_ = DEL_NODE_QUEST_MISS_CLUSTERID_ERROR;
      KLOG_ERROR("missing `cluster` key-value pair in the request body");
      goto end;
    }
    cluster_id_ = atoi(paras["cluster_id"].asCString());

    if (!paras.isMember("shard_id")) {
      err_code_ = DEL_NODE_QUEST_MISS_SHARDID_ERROR;
      KLOG_ERROR("missing `shards` key-value pair in the request body");
      goto end;
    }
    if(!CheckStringIsDigit(paras["shard_id"].asString())) {
      err_code_ = DEL_NODE_QUEST_MISS_SHARDID_ERROR;
      KLOG_ERROR("missing `shards` key-value pair in the request body");
      goto end;
    }
    shard_id_ = atoi(paras["shard_id"].asCString());

    if (!paras.isMember("hostaddr")) {
      err_code_ = DEL_NODE_QUEST_MISS_HOSTADDR_ERROR;
      KLOG_ERROR("missing `hostaddr` key-value pair in the request body");
      goto end;
    }
    hostaddr_ = paras["hostaddr"].asString();

    if (!paras.isMember("port")) {
      err_code_ = DEL_NODE_QUEST_MISS_PORT_ERROR;
      KLOG_ERROR("missing `port` key-value pair in the request body");
      goto end;
    }
    port_ = paras["port"].asString();

    CheckInputParams();

end:
    if(err_code_) {
      ret = false;
      job_status_ = "failed";
      storage_dstate_ = "failed";
      UpdateOperationRecord();
    } else {
      job_status_ = "ongoing";
      storage_dstate_ = "prepare";
    }

    return ret;
}

bool DeleteNodeMission::CheckInputParams() {
    MysqlResult result;
    std::string sql = string_sprintf("select hostaddr, port, member_state from %s.shard_nodes where shard_id=%d and db_cluster_id=%d",
            KUNLUN_METADATA_DB_NAME, shard_id_, cluster_id_);
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("get hostaddr and port from shard_nodes by shard_id {} failed", shard_id_);
        err_code_ = DEL_NODE_GET_SHARD_NODES_SQL_ERROR;
        return false;
    }
    int nrows = result.GetResultLinesNum();
    if(nrows <= 1) {
        KLOG_ERROR("get shard {} just have only node, so can't delete");
        err_code_ = DELETE_NODE_GET_NODE_NUM_ERROR;
        return false;
    }

    int match_flag = 0;
    for(int i=0; i<nrows; i++) {
        std::string ip = result[i]["hostaddr"];
        std::string port = result[i]["port"];
        std::string member_state = result[i]["member_state"];
        if(ip == hostaddr_ && port == port_) {
            match_flag = 1;
            if(member_state == "source") {
                KLOG_ERROR("delete node {}_{} is master node, so can't delete", hostaddr_, port_);
                err_code_ = DEL_NODE_IS_MASTER_NODE_ERROR;
                return false;
            }
            break;
        }
    }
    if(!match_flag) {
        KLOG_ERROR("input hostaddr {} and port {} is not shard_nodes", hostaddr_, port_);
        err_code_ = DEL_NODE_NOT_IN_SHARD_ERROR;
        return false;
    }

    std::vector<std::string> key_name;
    key_name.push_back("memo.computer_user");
    key_name.push_back("memo.computer_passwd");
    CMemo_Params memo_paras = GetClusterMemoParams(cluster_id_, key_name);
    if(!std::get<0>(memo_paras)) {
        err_code_ = std::get<1>(memo_paras);
        return false;
    }

    std::map<std::string, std::string> key_vals = std::get<2>(memo_paras);
    computer_user_ = key_vals["computer_user"];
    computer_pwd_ = key_vals["computer_passwd"];

    return true;
}

void DeleteNodeMission::DealLocal() {
    if(job_status_ == "failed" || job_status_ == "done")
        return;

    storage_dstate_ = "dispatch";
    ObjectPtr<KDelShardMission> mission(new KDelShardMission());
    mission->set_cb(DeleteNodeMission::DeleteStorageCB);
    mission->set_cb_context(this);

    struct InstanceInfoSt st1 ;
    st1.ip = hostaddr_;
    st1.port = atoi(port_.c_str());
    st1.exporter_port = st1.port + 1;
    st1.innodb_buffer_size_M = 1;
    st1.role = "slave";

    mission->setInstallInfoOneByOne(st1);
    bool ret = mission->InitFromInternal();
    if(!ret){
        KLOG_ERROR("delete storage mission failed {}", mission->getErr());
        err_code_ = DEL_NODE_DISPATCH_JOB_ERROR;
        job_status_ = "failed";
        UpdateOperationRecord();
        return;
    }
    
    ObjectPtr<ClusterRequest> base_request(dynamic_cast<ClusterRequest*>(mission.GetTRaw()));
    g_request_handle_thread->DispatchRequest(base_request);
    return;
}

void DeleteNodeMission::DeleteStorageCB(Json::Value& root, void* arg) {
    DeleteNodeMission *mission = static_cast<DeleteNodeMission*>(arg);

    Json::FastWriter writer;
    std::string result = writer.write(root);
    KLOG_INFO("DeleteStorageCB result {}", result);

    std::string status = root["status"].asString();
    if(status == "failed") {
        mission->UpdateJobState("failed", root["error_info"].asString());
        return;
    }

    mission->UpdateJobState("done", "ok");
}

bool DeleteNodeMission::TearDownMission() {
    //std::unique_lock<std::mutex> lock{ cond_mux_ };
	//cond_.wait(lock, [this] {
	//    return (job_status_ == "done" || job_status_ == "failed");
	//});
    cond_.Wait();

    return true;
}

bool DeleteNodeMission::PostDelNode() {
    std::map<std::string, std::vector<int> > host_ports;
    std::vector<int> ports;
    ports.push_back(atoi(port_.c_str()));
    host_ports[hostaddr_] = ports;
    if(!UpdateUsedPort(K_CLEAR, M_STORAGE, host_ports)) {
        KLOG_ERROR("update server_nodes used_ports failed");
        return false;
    }

    MysqlResult result;
    std::string sql = string_sprintf("select name, num_nodes from %s.shards where id=%d", 
                    KUNLUN_METADATA_DB_NAME, shard_id_);
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("get shards by shard_id {} failed", shard_id_);
        return false;
    }
    if(result.GetResultLinesNum() != 1) {
        KLOG_ERROR("get shards by shard_id {} records too many", shard_id_);
        return false;
    }
    shard_name_ = result[0]["name"];
    int num_nodes = atoi(result[0]["num_nodes"]);

    sql = "begin";
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("set session begin failed");
        return false;
    }

    sql = string_sprintf("update %s.shards set num_nodes=%d where id=%d", 
                KUNLUN_METADATA_DB_NAME, num_nodes-1, shard_id_);
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("update shard num_nodes failed");
        return false;
    }

    sql = string_sprintf("delete from %s.shard_nodes where hostaddr='%s' and port=%s",
            KUNLUN_METADATA_DB_NAME, hostaddr_.c_str(), port_.c_str());
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("delete hostaddr {} and port {} from shard_nodes failed", hostaddr_, port_);
        return false;
    }
    std::vector<std::string> storage_hosts;
    std::string host = hostaddr_ +"_"+ port_;
    int port_in = atoi(port_.c_str()) + 1;
    storage_hosts.emplace_back(hostaddr_ +":"+ std::to_string(port_in));
    sql = string_sprintf("delete from %s.node_map_master where node_host='%s'",
                KUNLUN_METADATA_DB_NAME, host.c_str());
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("delete host {} from node_map_master failed", host);
        return false;
    }
    
    sql = "commit";
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("execute commit sql failed");
        return false;
    }

    System::get_instance()->del_node_shard_memory(cluster_id_, shard_id_, host);
    DelShardFromComputerNode();
    ret = g_prometheus_manager->DelStorageConf(storage_hosts);
    if(ret) {
        KLOG_ERROR("delete prometheus mysqld_exporter config failed");
        //return false;
    }
    return true;
}

bool DeleteNodeMission::DelFromPgTables(const std::string& pg_hostaddr, const std::string& pg_port) {
    PGConnectionOption option;
    option.ip = pg_hostaddr;
    option.port_num = atoi(pg_port.c_str());
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

    sql = string_sprintf("select num_nodes from pg_shard where name='%s' and db_cluster_id=%d",
            shard_name_.c_str(), cluster_id_);
    if(pg_conn.ExecuteQuery(sql.c_str(), &pgresult) == -1) {
      KLOG_ERROR("postgres select pg_shard failed {}", pg_conn.getErr());
      return false;
    }

    if(pgresult.GetNumRows() != 1) {
        KLOG_ERROR("get too many records by name {} and db_cluster_id {}", shard_name_, cluster_id_);
        return false;
    }

    int num_nodes = atoi(pgresult[0]["num_nodes"]);
    sql = string_sprintf("update pg_shard set num_nodes=%d where name='%s' and db_cluster_id=%d",
            num_nodes-1, shard_name_.c_str(), cluster_id_);
    if(pg_conn.ExecuteQuery(sql.c_str(), &pgresult) == -1) {
      KLOG_ERROR("postgres update pg_shard failed {}", pg_conn.getErr());
      return false;
    }

    sql = string_sprintf("delete from pg_shard_node where port=%s and hostaddr='%s'",
                port_.c_str(), hostaddr_.c_str());
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

bool DeleteNodeMission::DelShardFromComputerNode() {
    MysqlResult result;
    std::string sql = string_sprintf("select * from %s.comp_nodes where db_cluster_id=%d",
                KUNLUN_METADATA_DB_NAME, cluster_id_);
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("get comp_nodes failed {}", g_node_channel_manager->getErr());
        return false;
    }

    int nrows = result.GetResultLinesNum();
    for(int i=0; i<nrows; i++) {
        for(int j=0; j < 10; j++) {
            ret = DelFromPgTables(result[i]["hostaddr"], result[i]["port"]);
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

void DeleteNodeMission::UpdateJobState(const std::string& state, const std::string& error_info) {
    if(state == "failed") {
        job_status_ = "failed";
        storage_dstate_ = "failed";
        error_info_ = error_info;
    } else {
        job_status_ = "done";
        storage_dstate_ = "post";
        if(!PostDelNode()) {
            job_status_ = "failed";
            err_code_ = DEL_NODE_POST_DEAL_ERROR;
        }
        storage_dstate_ = "done";
    }
    UpdateOperationRecord();
    if(job_status_ == "done" || job_status_ == "failed") {
        //std::unique_lock<std::mutex> lock{ cond_mux_ };
        //cond_.notify_all();
        cond_.SignalAll();
    }
}

bool DeleteNodeMission::UpdateOperationRecord() {
    std::string str_sql,memo;
    Json::Value memo_json;
    Json::FastWriter writer;

    memo_json["error_code"] = err_code_;
    if(error_info_.empty())
        memo_json["error_info"] = GlobalErrorNum(err_code_).get_err_num_str();
    else
        memo_json["error_info"] = error_info_;

    if(!storage_dstate_.empty())
        memo_json["storage_state"] = storage_dstate_;
    if(!computer_user_.empty())
        memo_json["computer_user"] = computer_user_;
    if(!computer_pwd_.empty())
        memo_json["computer_pwd"] = computer_pwd_;

    memo_json["cluster_id"] = std::to_string(cluster_id_);
    memo_json["shard_id"] = std::to_string(shard_id_);
    
    writer.omitEndingLineFeed();
    memo = writer.write(memo_json);

    str_sql = "UPDATE cluster_general_job_log set status='" + job_status_ + "',memo='" + memo;
    str_sql += "',when_ended=current_timestamp(6) where id=" + job_id_;

    if(System::get_instance()->execute_metadate_opertation(SQLCOM_UPDATE, str_sql)) {
        KLOG_ERROR( "execute_metadate_opertation error");
        return false;
    }
    KLOG_INFO("update cluster_general_job_log success sql: {}", str_sql);

    return true;
}


} // namespace kunlun

