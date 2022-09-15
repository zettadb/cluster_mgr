/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "delete_computer.h"
#include "kl_mentain/shard.h"
#include "kl_mentain/sys.h"
#include "cluster_operator/computeInstallMission.h"
#include "request_framework/handleRequestThread.h"
#include "cluster_collection/prometheus_control.h"

extern HandleRequestThread *g_request_handle_thread;
extern std::string meta_svr_user;
extern std::string meta_svr_pwd;
extern std::string meta_group_seeds;
extern kunlun::KPrometheusControl* g_prometheus_manager;

namespace kunlun
{

bool DeleteComputerMission::SetUpMisson() {
    job_id_ = get_request_unique_id();
    if(get_init_by_recover_flag() == true) {
        KLOG_INFO("Delete computer ignore recover state in setup phase");
        return true;
    }
    
    KLOG_INFO("delete cluster setup phase");
    
    ClusterRequestTypes request_type = get_request_type();
    
    bool ret = true;
    int i = 0;
    Json::Value paras;

    if(init_flag_) {
        KLOG_INFO("delete computer called by internal");
        goto end;
    }

    if (!super::get_body_json_document().isMember("paras")) {
        err_code_ = DELETE_COMPUTER_QUEST_LOST_PARAS_ERROR;
        KLOG_ERROR("missing `paras` key-value pair in the request body");
        goto end;
    }
    paras = super::get_body_json_document()["paras"];
    if (!paras.isMember("cluster_id")) {
      KLOG_ERROR("missing `cluster_id` key-value pair in the request body");
      err_code_ = DELETE_COMPUTER_QUEST_MISS_CLUSTERID_ERROR;
      goto end;
    }
    if(!CheckStringIsDigit(paras["cluster_id"].asString())) {
        err_code_ = DELETE_COMPUTER_QUEST_MISS_CLUSTERID_ERROR;
        goto end;
    }
    cluster_id_ = atoi(paras["cluster_id"].asCString());

    if (!paras.isMember("comp_id")) {
      KLOG_ERROR("missing `comp_id` key-value pair in the request body");
      err_code_ = DELETE_COMPUTER_QUEST_MISS_COMPID_ERROR;
      goto end;
    }
    if(!CheckStringIsDigit(paras["comp_id"].asString())) {
        err_code_ = DELETE_COMPUTER_QUEST_MISS_COMPID_ERROR;
        goto end;
    }
    comp_id_ = atoi(paras["comp_id"].asCString());

    CheckDeleteCompState();

end:
    if(err_code_) {
        ret = false;
        job_status_ = "failed";
        UpdateOperationRecord();
    } else 
        job_status_ = "ongoing";

    return ret;
}

void DeleteComputerMission::CheckDeleteCompState() {
    std::string sql = string_sprintf("select id from %s.comp_nodes where db_cluster_id=%d",
                KUNLUN_METADATA_DB_NAME, cluster_id_);
    MysqlResult result;
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        err_code_ = DELETE_COMPUTER_GET_COMP_NODES_SQL_ERROR;
        KLOG_ERROR("get computer nodes from comp_nodes failed");
        return;
    }

    int nrows = result.GetResultLinesNum();
    if(nrows <= 1) {
        err_code_ = DELETE_COMPUTER_GET_COMP_NODES_NUM_ERROR;
        KLOG_ERROR("cluster_id {} is only one computer node, so can't delete", cluster_id_);
        return;
    }
}

void DeleteComputerMission::DealLocal() {
    KLOG_INFO("delete cluster deal local phase");
    if(job_status_ == "failed" || job_status_ == "done") {
        return;
    }
    //System::get_instance()->update_cluster_delete_state(cluster_id_);
    
    computer_state_ = "prepare";
    MysqlResult result;
    //1. get computer_nodes
    std::vector<int> del_comps;
    if(comp_ids_.size() == 0)
        del_comps.push_back(comp_id_);
    else {
        for(size_t i=0; i<comp_ids_.size(); i++)
            del_comps.push_back(comp_ids_[i]);
    }

    std::string sql;
    bool ret = false;
    int nrows = 0;
    for(size_t i=0; i < del_comps.size(); i++) {
        sql = string_sprintf("select hostaddr, port from %s.comp_nodes where db_cluster_id=%d and id=%d",
                        KUNLUN_METADATA_DB_NAME, cluster_id_, del_comps[i]);
        ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
        if(ret) {
            err_code_ = DELETE_COMPUTER_GET_COMPUTER_NODES_ERROR;
            job_status_ = "failed";
            KLOG_ERROR("get computer nodes from meta failed {}", g_node_channel_manager->getErr());
            UpdateOperationRecord();
            return;
        }
        nrows = result.GetResultLinesNum();
        for(int i=0; i<nrows; i++) {
            std::string ip = result[i]["hostaddr"];
            int port = atoi(result[i]["port"]);
            IComm_Param dparam{ip, port};
            computer_dparams_.emplace_back(dparam);
        }
    }

    sql = string_sprintf("select name from %s.db_clusters where id=%d",
                KUNLUN_METADATA_DB_NAME, cluster_id_);
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        err_code_ = DELETE_COMPUTER_GET_CLUSTER_NAME_ERROR;
        job_status_ = "failed";
        KLOG_ERROR("get cluster_name from meta failed {}", g_node_channel_manager->getErr());
        UpdateOperationRecord();
        return;
    }
    if(result.GetResultLinesNum() != 1) {
        err_code_ = DELETE_COMPUTER_GET_CLUSTER_NAME_NUM_ERROR;
        job_status_ = "failed";
        KLOG_ERROR("get cluster_name from meta too many records");
        UpdateOperationRecord();
        return;
    }
    cluster_name_ = result[0]["name"];
    
    if(!DelComputerJobs()) {
        job_status_ = "failed";
        UpdateOperationRecord();
        return;
    }
    computer_state_ = "dispatch";
    UpdateOperationRecord();
    return;
}

bool DeleteComputerMission::ArrangeRemoteTask() {
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
            KLOG_INFO("delete computer called by other, so quit to restart");
            return true;
        }

        std::string storage_state = memo_json["storage_state"].asString();
        std::string computer_state = memo_json["computer_state"].asString(); 
        if(memo_json.isMember("cluster_id"))
            cluster_id_ = atoi(memo_json["cluster_id"].asCString());
        if(memo_json.isMember("computer_hosts")) {
            std::string hosts = memo_json["computer_hosts"].asString();
            std::vector<std::string> host_vec = StringTokenize(hosts, ",");
            for(size_t i=0; i<host_vec.size(); i++) {
                std::string ip = host_vec[i].substr(0, host_vec[i].rfind("_"));
                int port = atoi(host_vec[i].substr(host_vec[i].rfind("_")+1).c_str());
                IComm_Param iparam{ip, port};
                computer_dparams_.emplace_back(iparam);
            }
        }

        if(computer_state == "failed") {
            job_status_ = "failed";
        } else if(computer_state == "done") {
            if(!PostDeleteComputer()) {
                job_status_ = "failed";
                err_code_ = DELETE_CLUSTER_POST_DEAL_ERROR; 
            } else 
                job_status_ = "done";
        } 

    }
    UpdateOperationRecord();
    return true;
}

bool DeleteComputerMission::InitFromInternal() {
    Json::Value doc;
    FillCommonJobJsonDoc(doc);
    doc["job_type"] = "internal_del_comp";

    Json::Value para;
    para["cluster_id"] = std::to_string(cluster_id_);
    for(size_t i=0; i<comp_ids_.size(); i++) {
        para["comp_ids"].append(comp_ids_[i]);
    }
    doc["paras"] = para;
    set_body_json_document(doc);
    CompleteGeneralJobInfo();
    return true;
}

void DeleteComputerMission::CompleteGeneralJobInfo() {
    Json::FastWriter writer;
    writer.omitEndingLineFeed();
    std::string job_info = writer.write(get_body_json_document());

    kunlun::MysqlResult result;
    std::string sql = string_sprintf("update %s.cluster_general_job_log set "
                "job_type='internal_del_comp',job_info='%s' where id = %s",
                KUNLUN_METADATA_DB_NAME ,job_info.c_str(), get_request_unique_id().c_str());
    
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if (ret) {
        KLOG_ERROR("update cluster_general_job_log faild: {}", g_node_channel_manager->getErr());
        return;
    }
    KLOG_INFO("update cluster_general_job_log success: {}", sql);
}

bool DeleteComputerMission::DelComputerJobs() {
    ObjectPtr<KDelComputeMission> mission(new KDelComputeMission());
    mission->set_cb(DeleteComputerMission::DelComputerCallCB);
    mission->set_cb_context(this);

    for(size_t i=0; i< computer_dparams_.size(); i++) {
        struct ComputeInstanceInfoSt st2;
        st2.ip = std::get<0>(computer_dparams_[i]);
        st2.pg_port = std::get<1>(computer_dparams_[i]);
        st2.mysql_port = std::get<1>(computer_dparams_[i]) + 1;
        st2.exporter_port = std::get<1>(computer_dparams_[i]) + 2;
        st2.init_user = "abc";
        st2.init_pwd = "abc";
        st2.compute_id = i+1;

        mission->setInstallInfoOneByOne(st2);
    }
        
    bool ret = mission->InitFromInternal();
    computer_del_id_ = mission->get_request_unique_id();
    if(!ret){
        err_code_ = DELETE_COMPUTER_DISPATCH_COMPUTER_JOB_ERROR;
        computer_state_ = "failed";
        KLOG_ERROR("Delcomputer mission failed {}",mission->getErr());
        return false;
    }
    ObjectPtr<ClusterRequest> base_request(dynamic_cast<ClusterRequest*>(mission.GetTRaw()));
    g_request_handle_thread->DispatchRequest(base_request);
    //g_request_handle_thread->DispatchRequest(mission);
    return true;
}

void DeleteComputerMission::DelComputerCallCB(Json::Value& root, void *arg) {
    DeleteComputerMission* mission = static_cast<DeleteComputerMission*>(arg);
    Json::FastWriter writer;
    std::string result = writer.write(root);
    KLOG_INFO("DelComputerCallCB result {}", result);

    std::string status = root["status"].asString();
    if(status == "failed") {
        mission->UpdateCallCBFail(root["error_info"].asString());
        return;
    }
    mission->UpdateCallCBDone();
}

void DeleteComputerMission::UpdateCallCBFail(const std::string& error_info) {
    job_status_ = "failed";
    computer_state_ = "failed";
    err_code_ = DELETE_COMPUTER_DELETE_COMPUTER_ERROR;
    UpdateOperationRecord(error_info);
}

void DeleteComputerMission::UpdateCallCBDone() {
    job_status_ = "done";
    computer_state_ = "done";
    if(!PostDeleteComputer()) {
        job_status_ = "failed";
        err_code_ = DELETE_COMPUTER_POST_DEAL_ERROR;
    }

    UpdateOperationRecord();
}

void DeleteComputerMission::PackUsedPorts(std::map<std::string, std::vector<int> >& host_ports) {
    for(size_t i=0; i<computer_dparams_.size(); i++) {
      std::string hostaddr = std::get<0>(computer_dparams_[i]);
      std::vector<int> ports;
      if(host_ports.find(hostaddr) != host_ports.end()) {
        ports = host_ports[hostaddr];
      }
      ports.push_back(std::get<1>(computer_dparams_[i]));
      host_ports[hostaddr] = ports;
    }
    return;
}

bool DeleteComputerMission::PostDeleteComputer() {
    std::map<std::string, std::vector<int> > host_ports;
    PackUsedPorts(host_ports);
    if(!UpdateUsedPort(K_CLEAR, M_COMPUTER, host_ports))
        return false;

    std::string sql;
    MysqlResult result;
    bool ret = false;
    std::vector<std::string> comp_hosts;
    std::vector<std::string> comps_hosts;
    for(auto ci : computer_dparams_) {
        std::string ip = std::get<0>(ci);
        int port = std::get<1>(ci);
        std::string host = ip +"_"+std::to_string(port);
        comps_hosts.emplace_back(host);
        comp_hosts.emplace_back(ip +":"+std::to_string(port+2));
        sql = string_sprintf("select id from %s.comp_nodes where hostaddr='%s' and port=%d",
                            KUNLUN_METADATA_DB_NAME, ip.c_str(), port);
        ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
        if(ret) {
            KLOG_ERROR("get host {}_{} id from comp_nodes failed", ip, port);
            return false;
        }
        if(result.GetResultLinesNum() != 1) {
            KLOG_ERROR("get host {}_{} id from comp_nodes record too many", ip, port);
            return false;
        }

        sql = string_sprintf("alter table %s.commit_log_%s drop partition p%s", 
                        KUNLUN_METADATA_DB_NAME, cluster_name_.c_str(), result[0]["id"]);
        ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
        if(ret) {
            KLOG_ERROR("drop commit_log patition sql: {} failed", sql);
            return false;
        }

        sql = string_sprintf("delete from %s.comp_nodes where hostaddr='%s' and port=%d",
                    KUNLUN_METADATA_DB_NAME, ip.c_str(), port);
        ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
        if(ret) {
            KLOG_ERROR("delete host {}_{} from comp_nodes failed", ip, port);
            return false;
        }
    }

    System::get_instance()->del_computer_cluster_memory(cluster_id_, comps_hosts);
    ret = g_prometheus_manager->DelComputerConf(comp_hosts);
    if(ret) {
        KLOG_ERROR("delete prometheus postgres_exporter config failed");
        //return false;
    }
    return true;
}

bool DeleteComputerMission::UpdateOperationRecord(const std::string& error_info) {
    std::string str_sql,memo;
    Json::Value memo_json;
    Json::FastWriter writer;

    memo_json["rollback_flag"] = std::to_string(init_flag_);
    
    memo_json["error_code"] = err_code_;
    if(error_info.empty())
        memo_json["error_info"] = GlobalErrorNum(err_code_).get_err_num_str();
    else {
        memo_json["error_info"] = error_info;
        error_info_ = error_info;
    }

    if(!computer_state_.empty())
        memo_json["computer_state"] = computer_state_;
    
    if(!computer_del_id_.empty())
        memo_json["computer_id"] = computer_del_id_;

    memo_json["cluster_id"] = std::to_string(cluster_id_);
    if(computer_dparams_.size() > 0) {
        std::string hosts;
        for(size_t i=0; i<computer_dparams_.size(); i++) {
        hosts += std::get<0>(computer_dparams_[i]) + "_" + std::to_string(std::get<1>(computer_dparams_[i])) + ";";
        }
        memo_json["computer_hosts"] = hosts;
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

bool DeleteComputerMission::TearDownMission() {
    //std::unique_lock<std::mutex> lock{ cond_mux_ };
	//cond_.wait(lock, [this] {
	//    return (computer_state_ == "done" || computer_state_ == "failed");
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
    KLOG_INFO("delete computer mission result: {}", result);
    SetSerializeResult(result);
    //delete this;
    return true; 
}

}
