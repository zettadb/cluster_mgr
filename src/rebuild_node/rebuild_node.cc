/*
   Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "rebuild_node.h"
#include "http_server/node_channel.h"
#include "util_func/meta_info.h"
#include "zettalib/tool_func.h"
#include "json/json.h"
#include "zettalib/op_log.h"
#include "kl_mentain/shard.h"
#include "json/json.h"
#include "request_framework/handleRequestThread.h"


extern std::string meta_svr_user;
extern std::string meta_svr_pwd;
extern GlobalNodeChannelManager* g_node_channel_manager;
extern HandleRequestThread * g_request_handle_thread;

bool RebuildNodeMission::SetUpMisson() {
    if(get_init_by_recover_flag() == true) {
        KLOG_INFO("Rebuild node ignore recover state in setup phase");
        return true;
    }

    KLOG_INFO("rebuild node mission setup phase");

    job_id_ = get_request_unique_id();
    if(init_flag_) {
        KLOG_INFO("rebuild node called by internal");
        return true;
    }

    Json::Value root = get_body_json_document();
    if(!root["paras"].isMember("shard_id")) {
        KLOG_ERROR("input param no shard_id value");
        error_code_ = JOB_PARAMS_LOST_SHARD_ID;
        return false;
    }
    shard_id_ = atoi(root["paras"]["shard_id"].asCString());

    if(!root["paras"].isMember("cluster_id")) {
        KLOG_ERROR("input param no cluster_id value");
        error_code_ = JOB_PARAMS_LOST_CLUSTER_ID;
        return false;
    }
    cluster_id_ = atoi(root["paras"]["cluster_id"].asCString());

    if(!root["paras"].isMember("rb_nodes")) {
        KLOG_ERROR("input param no rb_nodes value");
        error_code_ = JOB_PARAMS_LOST_REBUILD_NODE;
        return false;
    }

    Json::Value rb_nodes = root["paras"]["rb_nodes"];
    for(unsigned int i=0; i<rb_nodes.size(); i++) {
        if(!rb_nodes[i].isMember("hostaddr")) {
            KLOG_ERROR("input param rb_nodes format wrong");
            error_code_ = JOB_PARAMS_REBUILD_NODE_NOT_HOSTADDR;
            return false;
        }
        std::string hostaddr = rb_nodes[i]["hostaddr"].asString();
        if(!rb_nodes[i].isMember("port")) {
            KLOG_ERROR("input param rb_nodes format wrong");
            error_code_ = JOB_PARAMS_REBUILD_NODE_NOT_PORT;
            return false;
        }
        std::string port = rb_nodes[i]["port"].asString();
        int backup = 0;
        if(rb_nodes[i].isMember("need_backup")) {
            backup = atoi(rb_nodes[i]["need_backup"].asCString());    
        }

        std::string hdfs_host="no_hdfs";
        if(backup) {
            if(rb_nodes[i].isMember("hdfs_host"))
                hdfs_host = rb_nodes[i]["hdfs_host"].asString();
            else {
                KLOG_ERROR("need_backup=1, need input hdfs_host");
                error_code_ = JOB_PARAMS_NEED_HDFS_HOST_IN_NEEDBACKUP;
                return false;
            }
        }

        int pv_limit = 10485760;  //default 10M
        if(rb_nodes[i].isMember("pv_limit"))
            pv_limit = atoi(rb_nodes[i]["pv_limit"].asString().c_str()) * 1024 * 1024;

        RN_Param rparam {hostaddr+"_"+port, backup, hdfs_host, pv_limit};
        rb_backup_.emplace_back(rparam);
    }

    if(rb_backup_.size() == 0) {
        KLOG_ERROR("input param rb_nodes is empty");
        error_code_ = JOB_PARAMS_LOST_REBUILD_NODE;
        return false;
    }

    if(root["paras"].isMember("allow_pull_from_master")) {
        std::string allow_pull_master = root["paras"]["allow_pull_from_master"].asString();
        if(atoi(allow_pull_master.c_str()))
            allow_pull_from_master_ = true;
    }

    if(root["paras"].isMember("allow_replica_delay")) {
        std::string allow_replica_delay = root["paras"]["allow_replica_delay"].asString();
        allow_delay_ = atoi(allow_replica_delay.c_str());
    }

    if(!GetPullAndMasterHost())
        return false;

    return true;
}

bool RebuildNodeMission::GetPullAndMasterHost() {
    std::string sql = kunlun::string_sprintf("select id, hostaddr, port, member_state, replica_delay from shard_nodes"
                        " where shard_id=%d and db_cluster_id=%d", shard_id_, cluster_id_);
    kunlun::MysqlResult result;
    bool sql_ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if (sql_ret) {
        setErr("%s", g_node_channel_manager->getErr());
        error_code_ = REBUILD_NODE_GET_HOSTADDR_STATE_FROM_METADB;
        return false;
    }

    int num_rows = result.GetResultLinesNum();
    if (num_rows == 0) {
        setErr("Can't get MySQL instance address by shard id %d",
           shard_id_);
        error_code_ = REBUILD_NODE_GET_HOSTADDR_STATE_FROM_METADB;
        return false;
    }

    int min_delay = 2147483647;
    std::vector<std::string> slave_hosts;
    for(int j=0; j<num_rows; j++) {
        std::string member_state = result[j]["member_state"];
        std::string hostaddr = result[j]["hostaddr"];
        std::string port = result[j]["port"];
        std::string host = hostaddr +"_"+ port;
        int replica_delay = atoi(result[j]["replica_delay"]);
        if(member_state == "source") {
            master_host_ = host;
        } else {
            int exist_flag = 0;
            for(size_t i=0; i<rb_backup_.size(); i++) {
                if(std::get<0>(rb_backup_[i]) == host) {
                    exist_flag = 1;
                    break;
                }
            }

            if(!exist_flag) {
                if(min_delay > replica_delay) {
                    min_delay = replica_delay;
                    pull_host_ = host;
                }
            }
            slave_hosts.push_back(host);
        }
    }

    if(!init_flag_) {
        for(auto it : rb_backup_) {
            std::string host = std::get<0>(it);
            if(std::find(slave_hosts.begin(), slave_hosts.end(), host) 
                        == slave_hosts.end()) {
                KLOG_ERROR("input hostaddr: {} is not in shard", host);
                error_code_ = REBUILD_NODE_NOT_IN_SHARD;
                return false;
            }
        }
    }

    if(min_delay > allow_delay_) {
        if(allow_pull_from_master_) {
            pull_host_ = master_host_;
        } else 
            pull_host_.clear();
    }

    if(pull_host_.empty()) {
        KLOG_ERROR("Can't find pull physical data host");
        error_code_ = REBUILD_NODE_GET_PULL_HOST_ERROR;
        return false;
    }
    return true;
}

void RebuildNodeMission::UpdateRbNodeMapState(const std::string& host, RbNodeState state, int type) {
    if(rbnode_state_.find(host) != rbnode_state_.end()) {
        rbnode_state_[host] = state;
    }

    if(state == RB_FAILED) {
        std::string request_id = rbnode_relation_id_[host];
        std::string sql = string_sprintf("select memo from %s.cluster_general_job_log where id=%s",
                KUNLUN_METADATA_DB_NAME, request_id.c_str());
        MysqlResult result;
        bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
        if(ret) {
            KLOG_ERROR("get request_id {} memo from cluster_general_job_log failed", request_id);
        }

        if(result.GetResultLinesNum() == 1) {
            std::string memo = result[0]["memo"];
            if(!memo.empty() && memo != "NULL") {
                Json::Value memo_json;
                Json::Reader reader;
                ret = reader.parse(memo, memo_json);
                if (!ret) {
                    KLOG_ERROR("parse memo json failed: {}", reader.getFormattedErrorMessages());
                } else {
                    error_code_ = atoi(memo_json["error_code"].asCString());
                    error_info_ = memo_json["error_info"].asString();
                }
            }
        }
    }
    UpdateRebuildNodeMissionState(type);
}

void RebuildNodeMission::UpdateRebuildNodeMissionState(int type) {
    int finish_num = 0;
    bool job_state = true;

    if(type) {
        for(size_t i=0; i<rb_backup_.size(); i++) {
            finish_num++;
            job_state = false;
            std::string host = std::get<0>(rb_backup_[i]);
            rbnode_state_[host] = RB_FAILED;
        }
    } else {
        for(size_t i=0; i<rb_backup_.size(); i++) {
            std::string host = std::get<0>(rb_backup_[i]);
            if(rbnode_state_.find(host) != rbnode_state_.end()) {
                if(rbnode_state_[host] == RB_DONE || rbnode_state_[host] == RB_FAILED) {
                    finish_num++;
                    if(rbnode_state_[host] == RB_FAILED)
                        job_state = false;
                }
            }
        }
    }

    if(finish_num == (int)rb_backup_.size()) {
        if(job_state)
            job_status_ = DONE;
        else
            job_status_ = FAILED;
        
        update_operation_record();
        if(job_status_ == DONE || job_status_ == FAILED) {
            //std::unique_lock<std::mutex> lock{ cond_mux_ };
            //cond_.notify_all();
            cond_.SignalAll();
        }
    }
}


void RebuildNodeMission::ParallelRbNodeCallBack(Json::Value& root, void *arg) {
    RbNodeParams* rb_params = static_cast<RbNodeParams*>(arg);

    Json::FastWriter writer;
    writer.omitEndingLineFeed();
    std::string result = writer.write(root);
    KLOG_INFO("parallel rbnode callback result: {}", result);
    
    std::string status = root["status"].asString();
    if(status == "failed") {
        KLOG_ERROR("rebuild node failed: {}", root["info"].asString());
        rb_params->rb_mission_->UpdateRbNodeMapState(rb_params->host_, RB_FAILED);
        return;
    }

    rb_params->rb_mission_->UpdateRbNodeMapState(rb_params->host_, RB_DONE);
    delete rb_params;
}

void RebuildNodeMission::ParallelRebuildNode() {
    for(size_t i=1; i<rb_backup_.size(); i++) {
        RN_Param rparam = rb_backup_[i];
        std::string pull_host = std::get<0>(rb_backup_[0]);
        ObjectPtr<RbNodeMission> rbnode(new RbNodeMission(std::get<0>(rparam), pull_host, std::get<1>(rparam),
                        std::get<2>(rparam), std::get<3>(rparam), master_host_));
        RbNodeParams* rb_params = new RbNodeParams();
        rb_params->rb_mission_ = this;
        rb_params->host_ = std::get<0>(rparam);
        rbnode->set_cb(RebuildNodeMission::ParallelRbNodeCallBack);
        rbnode->set_cb_context(rb_params);

        rbnode_state_[std::get<0>(rparam)] = RB_ONGOING;
        rbnode->InitFromInternal();
        rbnode_relation_id_[std::get<0>(rparam)] = rbnode->get_request_unique_id();
        update_operation_record();

        ObjectPtr<ClusterRequest> base_request(dynamic_cast<ClusterRequest*>(rbnode.GetTRaw()));
        g_request_handle_thread->DispatchRequest(base_request);
    }
}

void RebuildNodeMission::RebuildNodeCallBack(Json::Value& root, void *arg) {
    //RebuildNodeMission *rb_mission = static_cast<RebuildNodeMission*>(arg);
    RbNodeParams* rb_params = static_cast<RbNodeParams*>(arg);

    Json::FastWriter writer;
    writer.omitEndingLineFeed();
    std::string result = writer.write(root);
    KLOG_INFO("rebuild node callback result: {}", result);

    std::string status = root["status"].asString();
    if(status == "failed") {
        KLOG_ERROR("rebuild node failed: {}", root["error_info"].asString());
        //rb_mission->SetRbNodeState(2);
        rb_params->rb_mission_->UpdateRbNodeMapState(rb_params->host_, RB_FAILED, 1);
    } else {
        //rb_mission->SetRbNodeState(1);
        rb_params->rb_mission_->UpdateRbNodeMapState(rb_params->host_, RB_DONE);
        rb_params->rb_mission_->ParallelRebuildNode();
    }
    delete rb_params;
}

bool RebuildNodeMission::InitFromInternal() {
    Json::Value doc;
    FillCommonJobJsonDoc(doc);
    doc["job_type"] = "internal_rebuild_hosts";

    Json::Value para;
    para["cluster_id"] = std::to_string(cluster_id_);
    para["shard_id"] = std::to_string(shard_id_);
    para["allow_delay"] = std::to_string(allow_delay_);
    para["allow_pull_from_master"] = std::to_string(allow_pull_from_master_);
    for(size_t i=0; i<rb_backup_.size(); i++) {
        Json::Value tmp;
        tmp["host"] = std::get<0>(rb_backup_[i]);
        tmp["need_hdfs"] = std::to_string(std::get<1>(rb_backup_[i]));
        tmp["hdfs_path"] = std::get<2>(rb_backup_[i]);
        tmp["pv_limit"] = std::to_string(std::get<3>(rb_backup_[i]));
        para["rb_backup"].append(tmp);
    }

    doc["paras"] = para;
    set_body_json_document(doc);
    CompleteGeneralJobInfo();
    return true;
}

void RebuildNodeMission::CompleteGeneralJobInfo() {
    Json::FastWriter writer;
    writer.omitEndingLineFeed();
    std::string job_info = writer.write(get_body_json_document());

    kunlun::MysqlResult result;
    std::string sql = string_sprintf("update %s.cluster_general_job_log set "
                "job_type='internal_rebuild_hosts',job_info='%s' where id = %s",
                KUNLUN_METADATA_DB_NAME ,job_info.c_str(), get_request_unique_id().c_str());
    
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if (ret) {
        KLOG_ERROR("update cluster_general_job_log faild: {}", g_node_channel_manager->getErr());
        return;
    }
    KLOG_INFO("update cluster_general_job_log success: {}", sql);
}

bool RebuildNodeMission::update_operation_record() {
    std::string str_sql,memo;
    Json::Value job_memo_json;
    Json::FastWriter writer;

    job_memo_json["error_code"] = std::to_string(error_code_);
    if(error_info_.empty())
        job_memo_json["error_info"] = GlobalErrorNum(error_code_).get_err_num_str();
    else
        job_memo_json["error_info"] = error_info_;

    Json::Value tmp;
    for(auto &it : rbnode_state_) {
        tmp[it.first] = (int)(it.second);
        job_memo_json["rbnode_state"].append(tmp);
    }
    for(auto &it : rbnode_relation_id_) {
        tmp[it.first] = it.second;
        job_memo_json["rbnode_relation_id"].append(tmp); 
    }
    writer.omitEndingLineFeed();
    memo = writer.write(job_memo_json);

    str_sql = string_sprintf("UPDATE cluster_general_job_log set status=%d, memo='%s', when_ended=current_timestamp(6) where id=%s",
            job_status_, memo.c_str(), job_id_.c_str());

    MysqlResult result;
    bool ret = g_node_channel_manager->send_stmt(str_sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("update cluster_general_job status failed");
        return false;
    }
    KLOG_INFO("update cluster_general_job_log success sql: {}", str_sql);
    return true;
}

void RebuildNodeMission::DealLocal() {
    if(get_init_by_recover_flag() == true)
        return;

    if(error_code_) {
        job_status_ = FAILED;
        update_operation_record();
        return;
    }

    if(init_flag_) {
        if(!GetPullAndMasterHost()) {
            job_status_ = FAILED;
            update_operation_record();
            return;
        }
    }

    RN_Param rparam = rb_backup_[0];
    ObjectPtr<RbNodeMission> rbnode(new RbNodeMission(std::get<0>(rparam), pull_host_, std::get<1>(rparam),
                    std::get<2>(rparam), std::get<3>(rparam), master_host_));

    rbnode->set_cb(RebuildNodeMission::RebuildNodeCallBack);
    RbNodeParams* rb_params = new RbNodeParams();
    rb_params->rb_mission_ = this;
    rb_params->host_ = std::get<0>(rparam);
    rbnode->set_cb_context(rb_params);

    rbnode_state_[std::get<0>(rparam)] = RB_ONGOING;
    rbnode->InitFromInternal();
    rbnode_relation_id_[std::get<0>(rparam)] = rbnode->get_request_unique_id();
    job_status_ = ON_GOING;
    update_operation_record();

    ObjectPtr<ClusterRequest> base_request(dynamic_cast<ClusterRequest*>(rbnode.GetTRaw()));
    g_request_handle_thread->DispatchRequest(base_request);
} 

bool RebuildNodeMission::TearDownMission() {
    //std::unique_lock<std::mutex> lock{ cond_mux_ };
	//cond_.wait(lock, [this] {
	//    return (job_status_ == RequestStatus::DONE || job_status_ == RequestStatus::FAILED);
	//});
    cond_.Wait();
    sleep(2);

    Json::Value root;
    root["request_id"] = job_id_;
    if(job_status_ == RequestStatus::DONE)
        root["status"] = "done";
    if(job_status_ == RequestStatus::FAILED)
        root["status"] = "failed";
    
    root["error_code"] = error_code_;
    if(error_info_.empty())
        root["error_info"] = GlobalErrorNum(error_code_).get_err_num_str();
    else
        root["error_info"] = error_info_;
    Json::FastWriter writer;
    writer.omitEndingLineFeed();
    std::string result = writer.write(root);
    KLOG_INFO("rebuild node mission result: {}", result);
    SetSerializeResult(result);
    //delete this;
    return true; 
}

bool RbNodeMission::InitFromInternal() {
    // Based on ip_port_, initialize the Json object
    Json::Value doc;
    FillCommonJobJsonDoc(doc);
    doc["job_type"] = "rb_shard_node";

    Json::Value para;
    para["rebuild_host"] = rebuild_host_;
    para["pull_host"] = pull_host_;
    para["need_backup"] = need_backup_;
    para["hdfs_host"] = hdfs_host_;
    para["pv_limit"] = pv_limit_;
    doc["paras"] = para;
    set_body_json_document(doc);
    CompleteGeneralJobInfo();
    return true;
}

void RbNodeMission::CompleteGeneralJobInfo() {
    // set job_type,job_info
    Json::FastWriter writer;
    writer.omitEndingLineFeed();
    std::string job_info = writer.write(get_body_json_document());

    kunlun::MysqlResult result;
    std::string sql = string_sprintf("update %s.cluster_general_job_log set "
                "job_type='rb_shard_node',job_info='%s' where id = %s",
                KUNLUN_METADATA_DB_NAME ,job_info.c_str(), get_request_unique_id().c_str());
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if (ret) {
        KLOG_ERROR("update cluster_general_job_log faild: {}", g_node_channel_manager->getErr());
        return;
    }
    KLOG_INFO("update cluster_general_job_log success: {}", sql);
    // fetch backup storage info
}

bool RbNodeMission::SetUpMisson() {
    if(get_init_by_recover_flag() == true) {
        KLOG_INFO("Rb node ignore recover state in setup phase");
        return true;
    }

    job_id_ = get_request_unique_id();
    if(need_backup_) {
        if(!GetShardInfo(rebuild_host_)) {
            KLOG_ERROR("get host: {} shard_namd and cluster_name failed", rebuild_host_);
            error_code_ = RB_GET_SHARD_CLUSTER_AND_SHARD_NAME_ERROR;
            return false;
        }
    } else {
        cluster_name_ = "kunlun";
        shard_name_ = "kunlun";
    }
    return true;
}

void RbNodeMission::UpdateRbNodeFailedStat() {
    Json::Value root;
    root["request_id"] = job_id_;
    root["status"] = "failed";
    root["error_code"] = error_code_;
    root["error_info"] = GlobalErrorNum(error_code_).get_err_num_str();
    
    Json::FastWriter writer;
    writer.omitEndingLineFeed();
    std::string result = writer.write(root);
    KLOG_INFO("rb node mission result: {}", result);
    SetSerializeResult(result);
    return; 
}

bool RbNodeMission::ArrangeRemoteTask() {
    if (get_init_by_recover_flag() == true) {
        KLOG_INFO("Rb node ignore recover state in arrange remote task step");
        return true;
    }

    if(error_code_) {
        update_operation_record();
        UpdateRbNodeFailedStat();
        return false;
    }

    std::string ip = rebuild_host_.substr(0, rebuild_host_.rfind("_"));
    brpc::Channel *channel =
      g_node_channel_manager->getNodeChannel(ip.c_str());
    
    std::string task_spec_info = kunlun::string_sprintf(
                            "rb_shard_node_%s", job_id_.c_str());

    RbNodeTask *rb_task = new RbNodeTask(task_spec_info.c_str(), job_id_);
    rb_task->AddNodeSubChannel(ip.c_str(), channel);

    std::string m_ip = pull_host_.substr(0, pull_host_.rfind("_"));
    std::string port = rebuild_host_.substr(rebuild_host_.rfind("_")+1);

    Json::Value root;
    root["cluster_mgr_request_id"] = job_id_;
    root["task_spec_info"] = task_spec_info;
    root["job_type"] = "rebuild_node";
    root["paras"]["command_para"].append(rebuild_host_);
    root["paras"]["command_para"].append(pv_limit_);
    root["paras"]["command_para"].append(pull_host_);
    root["paras"]["command_para"].append(master_host_);
    root["paras"]["command_para"].append(need_backup_);
    root["paras"]["command_para"].append(hdfs_host_);
    root["paras"]["command_para"].append(cluster_name_);
    root["paras"]["command_para"].append(shard_name_);
    root["paras"]["command_para"].append(job_id_);
    root["paras"]["port"] = port;
    rb_task->SetPara(ip.c_str(), root);

    get_task_manager()->PushBackTask(rb_task);
    update_operation_record();
    return true;
}

void RbNodeMission::GetNodeMgrRbHostResult() {
    MysqlResult result;
    std::string sql = string_sprintf("select status, memo from %s.cluster_general_job_log where id=%s",
                    KUNLUN_METADATA_DB_NAME, job_id_.c_str());
    bool ret = true;
    while(1) {
        ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
        if(ret) {
            KLOG_ERROR("get job_id {} task status from cluster_general_job_log failed", job_id_);
        } else {
            if(result.GetResultLinesNum() == 1) {
                std::string status = result[0]["status"];
                if(status == "done" || status == "failed") {
                    Json::Value root, memo_json;
                    root["request_id"] = job_id_;
                    root["status"] = status;
                    
                    Json::Reader reader;
                    ret = reader.parse(result[0]["memo"], memo_json);
                    if (!ret) {
                        KLOG_ERROR("JSON parse error: {}, JSON string: {}",
                            reader.getFormattedErrorMessages().c_str(), result[0]["memo"]);
                        root["status"] = "failed";
                        root["error_code"] = std::to_string(RB_MISSION_PARSE_MEMO_JSON_ERROR);
                        root["error_info"] = GlobalErrorNum(RB_MISSION_PARSE_MEMO_JSON_ERROR).get_err_num_str();
                    } else {
                        root["error_code"] = memo_json["error_code"].asString();
                        root["error_info"] = memo_json["error_info"].asString();
                    }
                    
                    Json::FastWriter writer;
                    writer.omitEndingLineFeed();
                    std::string result = writer.write(root);
                    KLOG_INFO("rebuild node mission result: {}", result);
                    SetSerializeResult(result);
                    break;
                }
            }
        }
    }
}

bool RbNodeMission::TearDownMission() {
  bool ok = get_task_manager()->ok();
  if (ok) {
    return true;
  }

  KLOG_INFO("Rb Node RemoteTask result {}", get_task_manager()->serialized_result_);
  
  auto remote_tasks = get_task_manager()->get_remote_task_vec();
  for(auto rt : remote_tasks) {
      if(rt->get_response()->get_request_id() == job_id_) {
          if(!rt->get_response()->ok()) {
              KLOG_ERROR("Rb node job_id {} remote task rpc failed, so reset result", job_id_);
              GetNodeMgrRbHostResult();
          }
      }
  }
  return true;
}

void RbNodeMission::update_operation_record() {
    std::string str_sql,memo;
    Json::Value job_memo_json;
    Json::FastWriter writer;
    kunlun::RequestStatus job_status;

    job_memo_json["error_code"] = std::to_string(error_code_);
    job_memo_json["error_info"] = GlobalErrorNum(error_code_).get_err_num_str();
    writer.omitEndingLineFeed();
    memo = writer.write(job_memo_json);
    if(error_code_) {
        job_status = FAILED;
    } else 
        job_status = ON_GOING;

    str_sql = string_sprintf("UPDATE %s.cluster_general_job_log set status=%d, memo='%s', when_ended=current_timestamp(6) where id=%s",
            KUNLUN_METADATA_DB_NAME ,job_status, memo.c_str(), job_id_.c_str());

        //syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());
    KLOG_INFO("str_sql: {}", str_sql);

    MysqlResult result;
    bool ret = g_node_channel_manager->send_stmt(str_sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("update cluster_general_job status failed");
    }
}

bool RbNodeTask::TaskReportImpl() {
  // write result into cluster
#if 0
  std::string memo;
  Json::Value root = get_response()->get_all_response_json();
  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  memo = writer.write(root);
  bool ok = get_response()->ok();
  kunlun::RequestStatus status;
  if(ok)
    status = DONE;
  else 
    status = FAILED;

  std::string sql = string_sprintf("update %s.cluster_general_job_log set status = %d ,"
        "when_ended = CURRENT_TIMESTAMP(6) , memo = '%s' where id = %s",
            KUNLUN_METADATA_DB_NAME ,status, memo.c_str(), related_id_.c_str());

  kunlun::MysqlResult result;
  int ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  if (ret) {
    KLOG_ERROR("Report Request status sql: {} ,failed: {}", sql,
               g_node_channel_manager->getErr());
  } else {
    KLOG_INFO("Report Request status sql: {} ", sql);
  }
#endif

  return true;
}

bool RbNodeMission::GetShardInfo(const std::string& host) {
    std::string ip = host.substr(0, host.rfind("_"));
    std::string port = host.substr(host.rfind("_")+1);
    kunlun::MysqlConnectionOption options;
    options.autocommit = true;
    options.ip = ip;
    options.port_num = atoi(port.c_str());
    options.user = meta_svr_user;
    options.password = meta_svr_pwd;

    kunlun::MysqlConnection mysql_conn(options);
    if (!mysql_conn.Connect()) {
        KLOG_ERROR("connect to metadata db failed: {}", mysql_conn.getErr());
        return false;
    }

    kunlun::MysqlResult res;
    char sql[2048] = {0};
    sprintf(sql,
            "select cluster_name, shard_name from kunlun_sysdb.cluster_info limit 1");

    int ret = mysql_conn.ExcuteQuery(sql, &res, true);
    if (ret != 0) {
        KLOG_ERROR("get cluster_name query:[{}] failed: {}", sql,
            mysql_conn.getErr());
        return false;
    }

    if(res.GetResultLinesNum() != 1) {
        KLOG_ERROR("get cluster_name and shard_name empty");
        return false;
    }
    cluster_name_ = res[0]["cluster_name"];
    shard_name_ = res[0]["shard_name"];
    return true;
}

namespace kunlun {
void GetRebuildNodeStatus(Json::Value& doc) {
    doc["job_step"] = "check_param, xtracback_data, checksum_data, backup_old_data, clear_old_data, recover_data, rebuild_sync, done";
    
    std::map<std::string, int> rb_map_states;
    std::map<std::string, std::string> rb_map_ids;
    if(doc.isMember("rbnode_state")) {
        Json::Value rb_state = doc["rbnode_state"];
        for(unsigned int i=0; i<rb_state.size(); i++) {
            Json::Value::Members mem = rb_state[i].getMemberNames();      
            for (auto iter = mem.begin(); iter != mem.end(); iter++)  {
                int state = rb_state[i][*iter].asInt();
                rb_map_states[*iter] = state;
            }
        }
    }

    if(doc.isMember("rbnode_relation_id")) {
        Json::Value rb_relation_id = doc["rbnode_relation_id"];
        for(unsigned int i=0; i<rb_relation_id.size(); i++) {
            Json::Value::Members mem = rb_relation_id[i].getMemberNames();
            for(auto iter = mem.begin(); iter != mem.end(); iter++) {
                std::string id = rb_relation_id[i][*iter].asString();
                rb_map_ids[*iter] = id;
            }
        }
    }

    for( auto& it : rb_map_states) {
        Json::Value rb_json;
        int state = it.second;
        if(state == 1) {
            rb_json["status"] = "done";
            rb_json["error_code"] = EOK;
            rb_json["error_info"] = GlobalErrorNum(EOK).get_err_num_str();
            rb_json["step"] = "done";
        } else {
            if(rb_map_ids.find(it.first) != rb_map_ids.end()) {
                std::string id = rb_map_ids[it.first];

                MysqlResult result;
                std::string sql = string_sprintf("select id,job_type,status,memo from %s.cluster_general_job_log where id=%s",
                            KUNLUN_METADATA_DB_NAME, id.c_str());

                bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
                if (ret) {
                    rb_json["status"] = "failed";
                    rb_json["error_code"] = CONNECT_META_EXECUTE_SQL_ERROR;
                    rb_json["error_info"] = GlobalErrorNum(CONNECT_META_EXECUTE_SQL_ERROR).get_err_num_str();
                    return;
                }

                if(result.GetResultLinesNum() != 1) {
                    rb_json["status"] = "failed";
                    rb_json["error_code"] = GET_REBUILD_NODE_RB_NODE_TASK_ERROR;
                    rb_json["error_info"] = GlobalErrorNum(GET_REBUILD_NODE_RB_NODE_TASK_ERROR).get_err_num_str();
                    return;
                }

                std::string status = result[0]["status"];
                Json::Value memo;
                Json::Reader reader;
                if (!reader.parse(result[0]["memo"], memo)){
                    rb_json["error_code"] = PARSE_GENERAL_JOB_MEMO_JSON_ERROR;
                    rb_json["error_info"] = GlobalErrorNum(PARSE_GENERAL_JOB_MEMO_JSON_ERROR).get_err_num_str();
                    rb_json["status"] = "failed";
                    return;
                }
                rb_json["status"] = status;
                rb_json["error_code"] = memo["error_code"].asString();
                rb_json["error_info"] = memo["error_info"].asString();
                rb_json["step"] = memo["step"].asString();
            }
        }
        doc[it.first] = rb_json;
    }
    doc.removeMember("rbnode_relation_id");
    doc.removeMember("rbnode_state");
}
}