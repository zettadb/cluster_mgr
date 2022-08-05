/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "delete_cluster.h"
#include "kl_mentain/shard.h"
#include "kl_mentain/sys.h"
#include "delete_shard.h"
#include "delete_computer.h"
#include "request_framework/handleRequestThread.h"
#include "zettalib/tool_func.h"

extern HandleRequestThread *g_request_handle_thread;

namespace kunlun
{

bool DeleteClusterMission::SetUpMisson() {
    job_id_ = get_request_unique_id();
    if(get_init_by_recover_flag() == true) {
        KLOG_INFO("Delete cluster ignore recover state in setup phase");
        return true;
    }

    KLOG_INFO("delete cluster setup phase");
    
    ClusterRequestTypes request_type = get_request_type();
    bool ret = true;
    int i = 0;
    std::string cluster_name, cluster_id;
    Json::Value paras;

    if(init_flag_) {
        KLOG_ERROR("delete cluster called by internal");
        goto end;
    }

    if (!super::get_body_json_document().isMember("paras")) {
        err_code_ = DELETE_CLUSTER_QUEST_LOST_PARAS_ERROR;
        KLOG_ERROR("missing `paras` key-value pair in the request body");
        goto end;
    }
    paras = super::get_body_json_document()["paras"];

    if (paras.isMember("cluster_name")) {
      cluster_name = paras["cluster_name"].asString();
    }

    if (paras.isMember("cluster_id")) {
      cluster_id = paras["cluster_id"].asString();
    }

    if(cluster_id.empty() && cluster_name.empty()) {
        err_code_ = DELETE_CLUSTER_QUEST_MISSING_PARAM_ERROR;
        KLOG_ERROR("missing delete cluster parameter cluster_id or cluster_name in the request body");
        goto end;
    }

    if(!cluster_id.empty()) {
        if(CheckInputClusterId(cluster_id))
            goto end;
    } else {
        if(CheckInputClusterName(cluster_name))
            goto end;
    }
    storage_state_ = "prepare";
    computer_state_ = "prepare";

end:
    if(err_code_) {
        ret = false;
        job_status_ = "failed";
        UpdateOperationRecord();
    } else 
        job_status_ = "ongoing";

    return ret;
}

void DeleteClusterMission::DealLocal() {
    KLOG_INFO("delete cluster deal local phase");
    if(job_status_ == "failed" || job_status_ == "done")
        return;

    System::get_instance()->update_cluster_delete_state(cluster_id_);

    if(!GetDeleteParamsByClusterId()) {
        job_status_ = "failed";
        UpdateOperationRecord();
        return;
    }

    MysqlResult result;
    std::string sql = string_sprintf("select hostaddr, port from %s.shard_nodes where db_cluster_id=%d",
                    KUNLUN_METADATA_DB_NAME, cluster_id_);
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("get cluster_id {} shard_nodes failed", cluster_id_);
        job_status_ = "failed";
        err_code_ = DELETE_CLUSTER_GET_SHARDS_ERROR;
        UpdateOperationRecord();
        return;
    }
    if(result.GetResultLinesNum() > 0) {
        if(!DelShardJobs()) {
            job_status_ = "failed";
            UpdateOperationRecord();
            return;
        }
        storage_state_ = "dispatch";
    } else {
        storage_state_ = "done";
        sql = string_sprintf("delete from %s.shards where db_cluster_id=%d",
                    KUNLUN_METADATA_DB_NAME, cluster_id_);
        ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
        if(ret)
            KLOG_ERROR("delete cluster_id {} from shards failed", cluster_id_);
    }

    sql = string_sprintf("select hostaddr, port from %s.comp_nodes where db_cluster_id=%d",
                        KUNLUN_METADATA_DB_NAME, cluster_id_);
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("get cluster_id {} comp_nodes failed", cluster_id_);
        job_status_ = "failed";
        err_code_ = DELETE_CLUSTER_GET_COMPS_ERROR;
        UpdateOperationRecord();
        return;
    }

    if(result.GetResultLinesNum() > 0) {
        if(!DelComputerJobs()) {
            job_status_ = "failed";
            UpdateOperationRecord();
            return;
        }
        computer_state_ = "dispatch";
    } else 
        computer_state_ = "done";
    
    if(storage_state_ == "done" && computer_state_ == "done") {
        PostDeleteCluster();
        job_status_ = "done";
    }

    UpdateOperationRecord();
    return;
}

bool DeleteClusterMission::GetDeleteParamsByClusterId() {
    MysqlResult result;
    std::string sql = string_sprintf("select id from %s.shards where db_cluster_id=%d",
                    KUNLUN_METADATA_DB_NAME, cluster_id_);
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        err_code_ = DELETE_CLUSTER_GET_SHARDS_ERROR;
        KLOG_ERROR("get shard_id from shards by db_cluster_id {} failed", cluster_id_);
        return false;
    }

    int nrows = result.GetResultLinesNum();
    for(int i=0; i<nrows; i++) {
        int shard_id = atoi(result[i]["id"]);
        shard_ids_.emplace_back(shard_id);
    }

    sql = string_sprintf("select id from %s.comp_nodes where db_cluster_id=%d",
                    KUNLUN_METADATA_DB_NAME, cluster_id_);
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        err_code_ = DELETE_CLUSTER_GET_COMPS_ERROR;
        KLOG_ERROR("get comp_id from comp_nodes by db_cluster_id {} failed", cluster_id_);
        return false;
    }

    nrows = result.GetResultLinesNum();
    for(int i=0; i<nrows; i++) {
        int comp_id = atoi(result[i]["id"]);
        comp_ids_.emplace_back(comp_id);
    }

    return true;
}

bool DeleteClusterMission::ArrangeRemoteTask() {
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
        std::string storage_state = memo_json["storage_state"].asString();
        std::string computer_state = memo_json["computer_state"].asString(); 
        std::string cluster_name_ = memo_json["cluster_name"].asString();
        int cluster_id_ = atoi(memo_json["cluster_id"].asCString());
        if(storage_state == "failed" || computer_state == "failed") {
            job_status_ = "failed";
        } else if(storage_state == "done" && computer_state == "done") {
            if(!PostDeleteCluster()) {
                job_status_ = "failed";
                err_code_ = DELETE_CLUSTER_POST_DEAL_ERROR; 
            } else 
                job_status_ = "done";
        } 
    }
    UpdateOperationRecord();
    return true;
}

bool DeleteClusterMission::DelShardJobs() {
    ObjectPtr<DeleteShardMission> mission(new DeleteShardMission(cluster_id_, shard_ids_));
    mission->SetRollbackFlag(true);
    mission->set_cb(DeleteClusterMission::DelShardCallCB);
    mission->set_cb_context(this);

    mission->InitFromInternal();
    ObjectPtr<ClusterRequest> base_request(dynamic_cast<ClusterRequest*>(mission.GetTRaw()));
    g_request_handle_thread->DispatchRequest(base_request);
    //g_request_handle_thread->DispatchRequest(mission);
    storage_del_id_ = mission->get_request_unique_id();
    KLOG_INFO("get storage delete id {}", storage_del_id_);
    return true;
}

bool DeleteClusterMission::DelComputerJobs() {
    ObjectPtr<DeleteComputerMission> mission(new DeleteComputerMission(cluster_id_, comp_ids_));
    mission->set_cb(DeleteClusterMission::DelComputerCallCB);
    mission->set_cb_context(this);
    
    mission->InitFromInternal();
    ObjectPtr<ClusterRequest> base_request(dynamic_cast<ClusterRequest*>(mission.GetTRaw()));
    g_request_handle_thread->DispatchRequest(base_request);
    //g_request_handle_thread->DispatchRequest(mission);
    computer_del_id_ = mission->get_request_unique_id();
    KLOG_INFO("get computer delete id {}", computer_del_id_);
    return true;
}

void DeleteClusterMission::DelShardCallCB(Json::Value& root, void *arg) {
    DeleteClusterMission* mission = static_cast<DeleteClusterMission*>(arg);
    Json::FastWriter writer;
    std::string result = writer.write(root);
    KLOG_INFO("DelShardCallCB result {}", result);

    std::string status = root["status"].asString();
    if(status == "failed") {
        mission->UpdateCallCBFail("storage", root["error_info"].asString());
        return;
    }
    mission->UpdateCallCBDone("storage");
}

void DeleteClusterMission::DelComputerCallCB(Json::Value& root, void *arg) {
    DeleteClusterMission* mission = static_cast<DeleteClusterMission*>(arg);
    Json::FastWriter writer;
    std::string result = writer.write(root);
    KLOG_INFO("DelComputerCallCB result {}", result);

    std::string status = root["status"].asString();
    if(status == "failed") {
        mission->UpdateCallCBFail("computer", root["error_info"].asString());
        return;
    }
    mission->UpdateCallCBDone("computer");
}

void DeleteClusterMission::UpdateCallCBFail(const std::string& type, const std::string& error_info) {
    KlWrapGuard<KlWrapMutex> guard(update_mux_);
    if(type == "storage") {
        storage_state_ = "failed";
        err_code_ = DELETE_CLUSTER_DELETE_STORAGE_ERROR;
    }
    if(type == "computer") {
        computer_state_ = "failed";
        err_code_ = DELETE_CLUSTER_DELETE_COMPUTER_ERROR;
    }

    if((storage_state_ == "failed" && computer_state_ == "failed") || 
            (storage_state_ == "failed" && computer_state_ == "done") ||
            (storage_state_ == "done" && computer_state_ == "failed"))
        job_status_ = "failed";
    
    UpdateOperationRecord(error_info);
}

void DeleteClusterMission::UpdateCallCBDone(const std::string& type) {
    KlWrapGuard<KlWrapMutex> guard(update_mux_);
    if(type == "storage")
        storage_state_ = "done";
    if(type == "computer")
        computer_state_ = "done";

    if(storage_state_ == "done" && computer_state_ == "done") {
        job_status_ = "done";
        if(!PostDeleteCluster()) {
            job_status_ = "failed";
            err_code_ = DELETE_CLUSTER_POST_DEAL_ERROR;
        }
    } else if((storage_state_ == "done" && computer_state_ == "failed") ||
                (storage_state_ == "failed" && computer_state_ == "done")) {
        job_status_ = "failed";
    }

    UpdateOperationRecord();
}

bool DeleteClusterMission::PostDeleteCluster() {
    if(!System::get_instance()->stop_cluster(cluster_name_))	{
		KLOG_ERROR("stop_cluster error");
        return false;
	}
    return true;
}

bool DeleteClusterMission::CheckInputClusterId(const std::string& cluster_id) {
    std::string sql = string_sprintf("select * from %s.db_clusters where id=%s", 
                        KUNLUN_METADATA_DB_NAME, cluster_id.c_str()); 
    MysqlResult result;
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        err_code_ = DELETE_CLUSTER_CHECK_ID_BY_CLUSTER_ID_ERROR;
        KLOG_ERROR("get cluster_id info from db_clusters failed {}", g_node_channel_manager->getErr());
        return false;
    }

    if(result.GetResultLinesNum() != 1) {
        err_code_ = DELETE_CLUSTER_CHECK_ID_BY_CLUSTER_ID_ERROR;
        KLOG_ERROR("input cluster_id {} can't match db_clusters", cluster_id);
        return false;
    }

    cluster_id_ = atoi(cluster_id.c_str());
    cluster_name_ = result[0]["name"];
    return ret;
}
    
bool DeleteClusterMission::CheckInputClusterName(const std::string& cluster_name) {
    std::string sql = string_sprintf("select id from %s.db_clusters where name='%s'", 
                                KUNLUN_METADATA_DB_NAME, cluster_name.c_str());
    MysqlResult result;
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        err_code_ = DELETE_CLUSTER_CHECK_ID_BY_CLUSTER_NAME_ERROR;
        KLOG_ERROR("get cluster_name info from db_clusters failed {}", g_node_channel_manager->getErr());
        return false;
    }

    if(result.GetResultLinesNum() != 1) {
        err_code_ = DELETE_CLUSTER_CHECK_ID_BY_CLUSTER_NAME_ERROR;
        KLOG_ERROR("input cluster_name {} can't match db_clusters", cluster_name);
        return false;
    }

    cluster_id_ = atoi(result[0]["id"]);
    cluster_name_ = cluster_name;
    return ret;
}

bool DeleteClusterMission::TearDownMission() {
    //std::unique_lock<std::mutex> lock{ cond_mux_ };
	//cond_.wait(lock, [this] {
	//    return (job_status_ == "done" || job_status_ == "failed");
	//});
    cond_.Wait();

    //delete this;
    return true;  
}

bool DeleteClusterMission::UpdateOperationRecord(const std::string& error_info) {
    std::string str_sql,memo;
    Json::Value memo_json;
    Json::FastWriter writer;

    memo_json["cluster_id"] = std::to_string(cluster_id_);
    memo_json["cluster_name"] = cluster_name_;

    memo_json["error_code"] = err_code_;
    if(error_info.empty())
        memo_json["error_info"] = GlobalErrorNum(err_code_).get_err_num_str();
    else 
        memo_json["error_info"] = error_info;

    if(!storage_state_.empty()) 
        memo_json["storage_state"] = storage_state_;
    if(!computer_state_.empty())
        memo_json["computer_state"] = computer_state_;
    
    if(!storage_del_id_.empty())
        memo_json["shard_id"] = storage_del_id_;
    if(!computer_del_id_.empty())
        memo_json["computer_id"] = computer_del_id_;

    writer.omitEndingLineFeed();
    memo = writer.write(memo_json);

    str_sql = "UPDATE cluster_general_job_log set status='" + job_status_ + "',memo='" + memo;
    str_sql += "',when_ended=current_timestamp(6) where id=" + job_id_;

    if(System::get_instance()->execute_metadate_opertation(SQLCOM_UPDATE, str_sql)) {
        KLOG_ERROR( "execute_metadate_opertation error");
        return false;
    }
    KLOG_INFO("update cluster_general_job_log sql: {}", str_sql);

    if(job_status_ == "done" || job_status_ == "failed") {
        //std::unique_lock<std::mutex> lock{ cond_mux_ };
        //cond_.notify_all();
        cond_.SignalAll();
    }
    return true;
}

void GetDeleteClusterStatus(Json::Value& doc) {
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