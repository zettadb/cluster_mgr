/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "machine_mission.h"
#include "kl_mentain/shard.h"
#include "kl_mentain/sys.h"
#include "zettalib/tool_func.h"
#include "zettalib/op_mysql.h"
#include "cluster_collection/prometheus_control.h"

extern kunlun::KPrometheusControl* g_prometheus_manager;

namespace kunlun
{

bool MachineRequest::SetUpMisson() {
    bool ret = true;
    ClusterRequestTypes request_type = get_request_type();
    job_id_ = get_request_unique_id();
	
    if (!super::get_body_json_document().isMember("paras")) {
        err_code_ = MACHINE_QUEST_LOST_PARAS_ERROR;
        KLOG_ERROR("missing `paras` key-value pair in the request body");
        goto end;
    }

    switch(request_type) {
        case kunlun::kCreateMachineType:
            return CreateMachine();
        case kunlun::kDeleteMachineType:
            return DeleteMachine();
        case kunlun::kUpdateMachineType:
            return UpdateMachine();
    }

end:
    if(err_code_) {
        ret = false;
        job_status_ = "failed";
    } else
        job_status_ = "ongoing";
    
    UpdateOperationRecord();
    return ret;
}  

bool MachineRequest::CheckMachineState() {
    ClusterRequestTypes request_type = get_request_type();
    if(request_type == kunlun::kCreateMachineType) {
        std::string nodemgr_port = g_node_channel_manager->GetNodeMgrPort(hostaddr_);
        SyncNodeChannel sync_chan(10, hostaddr_, nodemgr_port);
        bool ret = sync_chan.Init();
        if(ret) {
            std::string host = hostaddr_+"_"+nodemgr_port;
            Json::Value root;
            Json::Value paras;
            root["cluster_mgr_request_id"] = "ping_pong";
            root["task_spec_info"] = "ping_pong";
            root["job_type"] = "ping_pong";
            root["paras"] = paras;

            if(sync_chan.ExecuteCmd(root)) {
                KLOG_ERROR("check hostaddr {} is unalive, {}", hostaddr_, sync_chan.getErr());
                ret = false;
            }
        } else
            KLOG_ERROR("sync channel init failed {}", sync_chan.getErr());
        
        if(!ret) {
            err_code_ = CREATE_MACHINE_REPORT_ALIVE_ERROR;
            job_status_ = "failed";
            std::string sql = string_sprintf("update %s.server_nodes set node_stats='%s' where hostaddr='%s' and machine_type='%s'",
                        KUNLUN_METADATA_DB_NAME, hostaddr_.c_str(), machine_type_.c_str());
            MysqlResult result;
            ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
            if(ret) {
                KLOG_ERROR("update hostaddr {} node_stats failed", hostaddr_);
                return false;
            }
            return false;
        }
    }
    return true;
}

void MachineRequest::DealLocal() {
    return;
}

bool MachineRequest::CreateMachine() {
    Json::Value paras = super::get_body_json_document()["paras"];
    
    bool ret = true;
    if(!paras.isMember("machine_type")) {
        err_code_ = MACHINE_QUEST_MISS_MACHINE_TYPE_ERROR;
        KLOG_ERROR("create machine paras miss machine_type parameter");
        goto end;
    }

    machine_type_ = paras["machine_type"].asString();
    if(!(machine_type_ == "storage" || machine_type_ == "computer")) {
        err_code_ = MACHINE_QUEST_MACHINE_TYPE_ERROR;
        KLOG_ERROR("input machine_type {} is not support", machine_type_);
        goto end;
    }

    if (!paras.isMember("hostaddr")) {
        err_code_ = CREATE_MACHINE_MISS_HOSTADDR_ERROR;
        KLOG_ERROR("missing `hostaddr` key-value pair in the request body");
        goto end;
    }
    hostaddr_ = paras["hostaddr"].asString();

    if(!CheckMachineState()) {
        goto end;
    }

    if(machine_type_ == "storage") 
        ret = CreateStorageMachine(paras);
    else 
        ret = CreateComputerMachine(paras);

end:
    if(err_code_) {
        job_status_ = "failed";
        ret = false;
    } else
        job_status_ = "done";

    UpdateOperationRecord();
    return ret;
}

bool MachineRequest::CreateStorageMachine(Json::Value& doc) {
    if (!doc.isMember("rack_id")) {
        err_code_ = CREATE_MACHINE_MISS_RACKID_ERROR;
        KLOG_ERROR("missing `rack_id` key-value pair in the request body");
        return false;
    }
    std::string rack_id = doc["rack_id"].asString();

    if (!doc.isMember("datadir")) {
        err_code_ = CREATE_MACHINE_MISS_DATADIR_ERROR;
        KLOG_ERROR("missing `datadir` key-value pair in the request body");
        return false;
    }
    std::string datadir = doc["datadir"].asString();

    if (!doc.isMember("logdir")) {
        err_code_ = CREATE_MACHINE_MISS_LOGDIR_ERROR;
        KLOG_ERROR("missing `logdir` key-value pair in the request body");
        return false;
    }
    std::string logdir = doc["logdir"].asString();

    if (!doc.isMember("wal_log_dir")) {
        err_code_  = CREATE_MACHINE_MISS_WALLOGDIR_ERROR;
        KLOG_ERROR("missing `wal_log_dir` key-value pair in the request body");
        return false;
    }
    std::string wal_log_dir = doc["wal_log_dir"].asString();

    std::string port_range = "57000-58000";
    if(doc.isMember("port_range")) {
        port_range = doc["port_range"].asString();
    }

    if (!doc.isMember("total_mem")) {
        err_code_ = CREATE_MACHINE_MISS_TOTALMEM_ERROR;
        KLOG_ERROR("missing `total_mem` key-value pair in the request body");
        return false;
    }
    int total_mem = stoi(doc["total_mem"].asString());

    if (!doc.isMember("total_cpu_cores")) {
        err_code_ = CREATE_MACHINE_MISS_CPUCORES_ERROR;
        KLOG_ERROR("missing `total_cpu_cores` key-value pair in the request body");
        return false;
    }
    int total_cpu_cores = stoi(doc["total_cpu_cores"].asString());

    if(!g_prometheus_manager->ReportAddMachine(hostaddr_, "storage")) {
        err_code_ = ADD_NODE_EXPORTER_TO_PROMETHEUS_ERROR;
        KLOG_ERROR("add node_exporter to prometheus failed");
        return false;
    }

    //check hostaddr is in server_nodes;
    std::string sql = string_sprintf("select * from %s.server_nodes where hostaddr='%s' and machine_type='storage' and node_stats='running'",
                KUNLUN_METADATA_DB_NAME, hostaddr_.c_str());
    MysqlResult result;
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("check hostaddr is in server node sql failed: {}", g_node_channel_manager->getErr());
        err_code_  = CREATE_MACHINE_CHECK_HOSTADDR_SQL_ERROR;
        return false;
    }

    int nrows = result.GetResultLinesNum();
    if(nrows != 0) {
        KLOG_ERROR("Input hostaddr {} storage node exist", hostaddr_);
        err_code_ = CREATE_MACHINE_HOSTADDR_EXIST_ERROR;
        return false;
    }

    sql = string_sprintf("select nodemgr_port, nodemgr_bin_path, nodemgr_tcp_port, machine_type, nodemgr_prometheus_port, port_range from %s.server_nodes where hostaddr='%s'"
                , KUNLUN_METADATA_DB_NAME, hostaddr_.c_str());
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("select server node sql failed: {}", g_node_channel_manager->getErr());
        err_code_  = CREATE_MACHINE_CHECK_HOSTADDR_SQL_ERROR;
        return false;
    }
    std::string nodemgr_port, nodemgr_bin_path, nodemgr_tcp_port, machine_type, nodemgr_prometheus_port;
    nrows = result.GetResultLinesNum();
    if(!( nrows == 1 || nrows == 0 || nrows == 2)) {
        KLOG_ERROR("get hostaddr {} exist record too many", hostaddr_);
        err_code_ = CREATE_MACHINE_GET_HOSTADDR_RECORD_ERROR;
        return false;
    }
    
    if(nrows == 1 || nrows == 2) {
        machine_type = result[0]["machine_type"];
        nodemgr_port = result[0]["nodemgr_port"];
        nodemgr_bin_path = result[0]["nodemgr_bin_path"];
        nodemgr_tcp_port = result[0]["nodemgr_tcp_port"];
        nodemgr_prometheus_port = result[0]["nodemgr_prometheus_port"];
        //if(machine_type == "NULL") 
        //    sql = string_sprintf("update %s.server_nodes set datadir='%s', logdir='%s', wal_log_dir='%s', total_mem=%d, total_cpu_cores=%d, port_range='%s', machine_type='storage' where hostaddr='%s'",
        //        KUNLUN_METADATA_DB_NAME, datadir.c_str(), logdir.c_str(), wal_log_dir.c_str(), total_mem, total_cpu_cores, port_range.c_str(), hostaddr_.c_str());
        //else
        sql = string_sprintf("insert into %s.server_nodes (hostaddr, rack_id, datadir, logdir, wal_log_dir, total_mem, total_cpu_cores, port_range, nodemgr_port, nodemgr_bin_path, nodemgr_tcp_port, nodemgr_prometheus_port, machine_type) value('%s', '%s', '%s', '%s', '%s', %d, %d, '%s', %s, '%s', %s, %s, 'storage')"
                        , KUNLUN_METADATA_DB_NAME, hostaddr_.c_str(), rack_id.c_str(), datadir.c_str(), logdir.c_str(), wal_log_dir.c_str(), total_mem, total_cpu_cores, port_range.c_str(), nodemgr_port.c_str(), nodemgr_bin_path.c_str(), nodemgr_tcp_port.c_str(), nodemgr_prometheus_port.c_str());
    } else {
        sql = string_sprintf("insert into %s.server_nodes (hostaddr, rack_id, datadir, logdir, wal_log_dir, total_mem, total_cpu_cores, port_range, machine_type) value('%s', '%s', '%s', '%s', '%s', %d, %d, '%s', 'storage')"
                        , KUNLUN_METADATA_DB_NAME, hostaddr_.c_str(), rack_id.c_str(), datadir.c_str(), logdir.c_str(), wal_log_dir.c_str(), total_mem, total_cpu_cores, port_range.c_str());
    }
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("insert hostaddr into server node sql failed: {}", g_node_channel_manager->getErr());
        err_code_  = CREATE_MACHINE_INSERT_HOSTADDR_SQL_ERROR;
        return false;
    }
    return true;
}

bool MachineRequest::CreateComputerMachine(Json::Value& doc) {
    if (!doc.isMember("rack_id")) {
        err_code_ = CREATE_MACHINE_MISS_RACKID_ERROR;
        KLOG_ERROR("missing `rack_id` key-value pair in the request body");
        return false;
    }
    std::string rack_id = doc["rack_id"].asString();

    if (!doc.isMember("comp_datadir")) {
        err_code_ = CREATE_MACHINE_MISS_COMPDATADIR_ERROR;
        KLOG_ERROR("missing `comp_datadir` key-value pair in the request body");
        return false;
    }
    std::string comp_datadir = doc["comp_datadir"].asString();

    std::string port_range = "47000-48000";
    if(doc.isMember("port_range")) {
        port_range = doc["port_range"].asString();
    }

    if (!doc.isMember("total_mem")) {
        err_code_ = CREATE_MACHINE_MISS_TOTALMEM_ERROR;
        KLOG_ERROR("missing `total_mem` key-value pair in the request body");
        return false;
    }
    int total_mem = stoi(doc["total_mem"].asString());

    if (!doc.isMember("total_cpu_cores")) {
        err_code_ = CREATE_MACHINE_MISS_CPUCORES_ERROR;
        KLOG_ERROR("missing `total_cpu_cores` key-value pair in the request body");
        return false;
    }
    int total_cpu_cores = stoi(doc["total_cpu_cores"].asString());

    if(!g_prometheus_manager->ReportAddMachine(hostaddr_, "computer")) {
        err_code_ = ADD_NODE_EXPORTER_TO_PROMETHEUS_ERROR;
        KLOG_ERROR("add node_exporter to prometheus failed");
        return false;
    }

    std::string sql = string_sprintf("select * from %s.server_nodes where hostaddr='%s' and machine_type='computer'",
                KUNLUN_METADATA_DB_NAME, hostaddr_.c_str());
    MysqlResult result;
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("check hostaddr is in server node sql failed: {}", g_node_channel_manager->getErr());
        err_code_  = CREATE_MACHINE_CHECK_HOSTADDR_SQL_ERROR;
        return false;
    }

    int nrows = result.GetResultLinesNum();
    if(nrows != 0) {
        KLOG_ERROR("Input hostaddr {} computer node exist", hostaddr_);
        err_code_ = CREATE_MACHINE_HOSTADDR_EXIST_ERROR;
        return false;
    }

    sql = string_sprintf("select nodemgr_port, nodemgr_bin_path, nodemgr_tcp_port, machine_type, nodemgr_prometheus_port from %s.server_nodes where hostaddr='%s'"
                , KUNLUN_METADATA_DB_NAME, hostaddr_.c_str());
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("select server node sql failed: {}", g_node_channel_manager->getErr());
        err_code_  = CREATE_MACHINE_CHECK_HOSTADDR_SQL_ERROR;
        return false;
    }
    std::string nodemgr_port, nodemgr_bin_path, nodemgr_tcp_port, machine_type, nodemgr_prometheus_port;
    nrows = result.GetResultLinesNum();
    if(!( nrows == 2 || nrows == 1 || nrows == 0)) {
        KLOG_ERROR("get hostaddr {} exist record too many", hostaddr_);
        err_code_ = CREATE_MACHINE_GET_HOSTADDR_RECORD_ERROR;
        return false;
    }
    
    if(nrows == 2 || nrows == 1) {
        machine_type = result[0]["machine_type"];
        nodemgr_port = result[0]["nodemgr_port"];
        nodemgr_bin_path = result[0]["nodemgr_bin_path"];
        nodemgr_tcp_port = result[0]["nodemgr_tcp_port"];
        nodemgr_prometheus_port = result[0]["nodemgr_prometheus_port"];

        //if(machine_type == "NULL") 
            //sql = string_sprintf("update %s.server_nodes set comp_datadir='%s', total_mem=%d, total_cpu_cores=%d, port_range='%s', machine_type='computer' where hostaddr='%s'",
            //    KUNLUN_METADATA_DB_NAME, comp_datadir.c_str(), total_mem, total_cpu_cores, port_range.c_str(), hostaddr_.c_str());
        //else
        sql = string_sprintf("insert into %s.server_nodes (hostaddr, rack_id, comp_datadir, total_mem, total_cpu_cores, port_range, nodemgr_port, nodemgr_bin_path, nodemgr_tcp_port, nodemgr_prometheus_port, machine_type) value('%s', '%s', '%s', %d, %d, '%s', %s, '%s', %s, %s, 'computer')"
                        , KUNLUN_METADATA_DB_NAME, hostaddr_.c_str(), rack_id.c_str(), comp_datadir.c_str(), total_mem, total_cpu_cores, port_range.c_str(), nodemgr_port.c_str(), nodemgr_bin_path.c_str(), nodemgr_tcp_port.c_str(), nodemgr_prometheus_port.c_str());
    } else {
        sql = string_sprintf("insert into %s.server_nodes (hostaddr, rack_id, comp_datadir, total_mem, total_cpu_cores, port_range, machine_type) value('%s', '%s', '%s', %d, %d, '%s', 'computer')"
                        , KUNLUN_METADATA_DB_NAME, hostaddr_.c_str(), rack_id.c_str(), comp_datadir.c_str(), total_mem, total_cpu_cores, port_range.c_str());
    }
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("insert hostaddr into server node sql failed: {}", g_node_channel_manager->getErr());
        err_code_  = CREATE_MACHINE_INSERT_HOSTADDR_SQL_ERROR;
        return false;
    }
    return true;
}

bool MachineRequest::DeleteMachine() {
    Json::Value paras = super::get_body_json_document()["paras"];
    bool ret = true;
    bool retg;
    int nrows = 0;
    MysqlResult result;
    std::string sql, hostaddr, machine_type, used_port, id;
    if(!paras.isMember("machine_type")) {
        err_code_ = MACHINE_QUEST_MISS_MACHINE_TYPE_ERROR;
        KLOG_ERROR("create machine paras miss machine_type parameter");
        goto end;
    }

    machine_type = paras["machine_type"].asString();
    if(!(machine_type == "storage" || machine_type == "computer")) {
        err_code_ = MACHINE_QUEST_MACHINE_TYPE_ERROR;
        KLOG_ERROR("input machine_type {} is not support", machine_type);
        goto end;
    }

    if (!paras.isMember("hostaddr")) {
        err_code_ = DELETE_MACHINE_MISS_HOSTADDR_ERROR;
        KLOG_ERROR("missing `hostaddr` key-value pair in the request body");
        goto end;
    }
    hostaddr = paras["hostaddr"].asString();

    if(!g_prometheus_manager->ReportDelMachine(hostaddr, machine_type)) {
        err_code_ = DEL_NODE_EXPORTER_TO_PROMETHEUS_ERROR;
        KLOG_ERROR("delete node_exporter to prometheus failed");
        return false;
    }

    sql = string_sprintf("select * from %s.server_nodes where hostaddr='%s' and machine_type='%s'",
                KUNLUN_METADATA_DB_NAME, hostaddr.c_str(), machine_type.c_str());
    
    retg = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(retg) {
        KLOG_ERROR("check hostaddr is in server node sql failed: {}", g_node_channel_manager->getErr());
        err_code_  = DELETE_MACHINE_CHECK_HOSTADDR_SQL_ERROR;
        goto end;
    }

    nrows = result.GetResultLinesNum();
    if(nrows != 1) {
        KLOG_ERROR("Input hostaddr {} machine_type {} node don't exist", hostaddr, machine_type);
        err_code_ = DELETE_MACHINE_HOSTADDR_NOT_EXIST_ERROR;
        goto end;
    }
    
    used_port = result[0]["used_port"];
    if(!(used_port == "NULL" || used_port.empty())) {
        KLOG_ERROR("hostaddr {} machine_type {} used port is not NULL", hostaddr, machine_type);
        err_code_ = DELETE_MACHINE_HAS_USED_PORT_ERROR;
        goto end;
    }

    id = result[0]["id"];
    sql = string_sprintf("delete from %s.server_nodes_stats where id=%s",
                        KUNLUN_METADATA_DB_NAME, id.c_str());
    retg = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(retg) {
        KLOG_ERROR("delete id from server node stats sql failed: {}", g_node_channel_manager->getErr());
        err_code_  = DELETE_MACHINE_UPDATE_HOSTADDR_STATE_SQL_ERROR;
        goto end;
    }

    sql = string_sprintf("delete from %s.server_nodes where hostaddr='%s' and machine_type='%s'",
                        KUNLUN_METADATA_DB_NAME, hostaddr.c_str(), machine_type.c_str());

    retg = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(retg) {
        KLOG_ERROR("delete hostaddr from server node sql failed: {}", g_node_channel_manager->getErr());
        err_code_  = DELETE_MACHINE_UPDATE_HOSTADDR_STATE_SQL_ERROR;
        goto end;
    }

end:
    if(err_code_) {
        job_status_ = "failed";
        ret = false;
    } else
        job_status_ = "done";
    UpdateOperationRecord();
    return ret;
}

bool MachineRequest::UpdateMachine() {
    return true;
}

bool MachineRequest::UpdateOperationRecord() {
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
    return true;
}

}