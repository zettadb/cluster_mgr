/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "add_node.h"
#include "request_framework/handleRequestThread.h"
#include "cluster_operator/mysqlInstallRemote.h"
#include "cluster_operator/shardInstallMission.h"
#include "zettalib/op_log.h"
#include "kl_mentain/shard.h"
#include "kl_mentain/sys.h"
#include "rebuild_node/rebuild_node.h"
#include "cluster_collection/prometheus_control.h"

extern HandleRequestThread *g_request_handle_thread;
extern kunlun::KPrometheusControl* g_prometheus_manager;

namespace kunlun 
{

bool AddNodeMission::ArrangeRemoteTask() {
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

        if(memo_json.isMember("shard_hosts")) {
            Json::Value shard_hosts = memo_json["shard_hosts"];
            for(unsigned int i=0; i<shard_hosts.size(); i++) {
                Json::Value::Members members;
                members = shard_hosts[i].getMemberNames();
                for(Json::Value::Members::iterator it = members.begin(); it != members.end(); it++) {
                    int shard_id = atoi((*it).c_str());
                    std::string hosts = shard_hosts[i][*it].asString();
                    std::vector<std::string> host_vec = StringTokenize(hosts, ";");
                    std::vector<IComm_Param> iparams;
                    for(size_t i=0; i<host_vec.size(); i++) {
                        std::string ip = host_vec[i].substr(0, host_vec[i].rfind("_"));
                        int port = atoi(host_vec[i].substr(host_vec[i].rfind("_")+1).c_str());
                        IComm_Param iparam{ip, port};
                        iparams.emplace_back(iparam);
                    }
                    storage_iparams_[shard_id] = iparams;
                }
            }
        }
        if(memo_json.isMember("cluster_id"))
            cluster_id_ = atoi(memo_json["cluster_id"].asCString());
        if(memo_json.isMember("computer_user"))
            computer_user_ = memo_json["computer_user"].asString();
        if(memo_json.isMember("computer_pwd"))
            computer_pwd_ = memo_json["computer_pwd"].asString();

        if(memo_json.isMember("shard_state")) {
            Json::Value shard_state = memo_json["shard_state"];
            for(unsigned int i=0; i<shard_state.size(); i++) {
                Json::Value::Members members;
                members = shard_state[i].getMemberNames();
                for(Json::Value::Members::iterator it = members.begin(); it != members.end(); it++) {
                    int shard_id = atoi((*it).c_str());
                    Node_State nstate = static_cast<Node_State>(shard_state[i][*it].asInt());
                    shard_map_states_[shard_id] = nstate;
                    if(nstate == N_DREBUILD) {
                        if(!PostAddNode(shard_id)) {
                            RollbackStorageInstall(shard_id);
                            continue;
                        }
                    } else if(!(nstate == N_DONE || nstate == N_FAILED)) {
                        RollbackStorageInstall(shard_id);
                        err_code_ = CLUSTER_MGR_RESTART_ERROR;
                        continue;
                    }
                }

                int done_num=0, failed_num=0;
                for(auto &it : shard_map_states_) {
                    if(it.second == N_FAILED || it.second == N_DONE) {
                        done_num++;
                        if(it.second == N_FAILED)
                            failed_num++;
                    }
                }
                if(done_num >= (int)storage_iparams_.size()) {
                    job_status_ = "done";
                    if(failed_num >= (int)storage_iparams_.size())
                        job_status_ = "failed";
                    UpdateOperationRecord();
                    return true;
                }
            }
        }
        return true;
    }

    DealInternal();
    return true;
}

bool AddNodeMission::SetUpMisson() {
    job_id_ = get_request_unique_id();
    if(get_init_by_recover_flag() == true) {
         KLOG_INFO("Add node ignore recover state in setup phase");
        return true;
    }
    
    KLOG_INFO("Add node setup phase");
    
    ClusterRequestTypes request_type = get_request_type();
    
    bool ret = true;
    int i = 0;
    Json::Value paras, storage_iplists;

    if (!super::get_body_json_document().isMember("paras")) {
        err_code_ = ADD_NODE_QUEST_LOST_PARAS_ERROR;
        KLOG_ERROR("missing `paras` key-value pair in the request body");
        goto end;
    }
    paras = super::get_body_json_document()["paras"];

    if (!paras.isMember("cluster_id")) {
      err_code_ = ADD_NODE_QUEST_MISS_CLUSTERID_ERROR;
      KLOG_ERROR("missing `shards` key-value pair in the request body");
      goto end;
    }
    if(!CheckStringIsDigit(paras["cluster_id"].asString())) {
      err_code_ = ADD_NODE_QUEST_MISS_CLUSTERID_ERROR;
      KLOG_ERROR("missing `shards` key-value pair in the request body");
      goto end;
    }
    cluster_id_ = atoi(paras["cluster_id"].asCString());

    if (paras.isMember("shard_id")) {
      if(CheckStringIsDigit(paras["shard_id"].asString()))
        shard_id_ = atoi(paras["shard_id"].asCString());
    }
    
    if (!paras.isMember("nodes")) {
      err_code_ = ADD_NODE_QUEST_MISS_NODES_ERROR;
      KLOG_ERROR("missing `nodes` key-value pair in the request body");
      goto end;
    }
    if(!CheckStringIsDigit(paras["nodes"].asString())) {
      err_code_ = ADD_NODE_QUEST_MISS_NODES_ERROR;
      KLOG_ERROR("missing `nodes` key-value pair in the request body");
      goto end;
    }
    nodes_ = atoi(paras["nodes"].asCString());
    if(nodes_ < 1 || nodes_ > 256) {
      err_code_ = ADD_NODE_QUEST_NODES_PARAM_ERROR;
      KLOG_ERROR("nodes error(must in 1-256)");
      goto end;
    }

    if(paras.isMember("storage_iplists")) {
      storage_iplists = paras["storage_iplists"];
      for(i=0; i<storage_iplists.size();i++) {
        storage_iplists_.push_back(storage_iplists[i].asString());
      }
      if(!CheckMachineIplists(M_STORAGE, storage_iplists_)) {
        err_code_ = ADD_NODE_ASSIGN_STORAGE_IPLISTS_ERROR;
        goto end;
      }
    }

    if(GetInstallNodeParamsByClusterId()) {
        goto end;
    }

end:
    if(err_code_) {
      ret = false;
      job_status_ = "failed";
      UpdateOperationRecord();
    } else
      job_status_ = "ongoing";

    return ret;
}

bool AddNodeMission::GetInstallNodeParamsByClusterId() {
    std::vector<std::string> key_name;
    key_name.push_back("name");
    key_name.push_back("memo.ha_mode");
    key_name.push_back("memo.computer_user");
    key_name.push_back("memo.computer_passwd");
    key_name.push_back("memo.innodb_size");
    key_name.push_back("memo.dbcfg");
    key_name.push_back("memo.fullsync_level");
    CMemo_Params memo_paras = GetClusterMemoParams(cluster_id_, key_name);
    if(!std::get<0>(memo_paras)) {
        err_code_ = std::get<1>(memo_paras);
        return false;
    }

    std::map<std::string, std::string> key_vals = std::get<2>(memo_paras);
    cluster_name_ = key_vals["name"];
    ha_mode_ = key_vals["ha_mode"];
    computer_user_ = key_vals["computer_user"];
    computer_pwd_ = key_vals["computer_passwd"];
    innodb_size_ = atoi(key_vals["innodb_size"].c_str());
    fullsync_level_ = atoi(key_vals["fullsync_level"].c_str());
    dbcfg_ = atoi(key_vals["dbcfg"].c_str());

    return true;
}

void AddNodeMission::DealInternal() {
    KLOG_INFO("add node deal internal phase");
    if(job_status_ == "failed") {
        return;
    }

    if(shard_id_ == -2) {
        std::string sql = string_sprintf("select id from %s.shards where db_cluster_id=%d",
                    KUNLUN_METADATA_DB_NAME, cluster_id_);
        MysqlResult result;
        bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
        if(ret) {
            KLOG_ERROR("get cluster_id {} shard failed", cluster_id_);
            err_code_ = ADD_NODE_GET_SHARD_ID_ERROR;
            UpdateOperationRecord();
            return;
        }
        int nrows = result.GetResultLinesNum();
        for(int i=0; i<nrows; i++) {
            int id = atoi(result[i]["id"]);
            shard_ids_.emplace_back(id);
        }
    } else 
        shard_ids_.push_back(shard_id_);

    if(!SeekStorageIplists()) {
        job_status_ = "failed";
        UpdateOperationRecord();
        return;
    }
    storage_state_ = "prepare";
    std::map<std::string, std::vector<int> > host_ports;
    PackInstallPorts(host_ports);
    UpdateInstallingPort(K_APPEND, M_STORAGE, host_ports);

    if(!AddStorageJobs()) {
        job_status_ = "failed";
        UpdateOperationRecord();
        return;
    }
    storage_state_ = "dispatch";
    UpdateOperationRecord();
}

void AddNodeMission::PackInstallPorts(std::map<std::string, std::vector<int> >& host_ports) {
  for(auto it : storage_iparams_) {
    std::vector<IComm_Param> s_iparams = it.second;
    for(size_t i=0; i<s_iparams.size(); i++) {
      std::string hostaddr = std::get<0>(s_iparams[i]);
      std::vector<int> ports;
      if(host_ports.find(hostaddr) != host_ports.end()) {
        ports = host_ports[hostaddr];
      }
      ports.push_back(std::get<1>(s_iparams[i]));
      host_ports[hostaddr] = ports;
    }
  }
  return;
}

bool AddNodeMission::SeekStorageIplists() {
    bool ret = false;
    if(storage_iplists_.size() > 0)
        ret = AssignStorageIplists();
    else
        ret = FetchStorageIplists();

    return ret;
}

bool AddNodeMission::AssignStorageIplists() {
    for(size_t i=0; i<storage_iplists_.size(); i++) {
        std::string hostaddr = storage_iplists_[i];
        if(!GetStorageInstallHost(hostaddr, avail_hosts_, host_ports_, err_code_)) 
            return false;
    }

    for(int i=0; i < shard_ids_.size(); i++) {
        s_iparams_.clear();
        if(!GetBestInstallHosts(avail_hosts_, nodes_, s_iparams_, host_ports_))
            return false;

        if((int)s_iparams_.size() < nodes_) {
            err_code_ = ADD_NODE_GET_STORAGE_MATCH_NODES_ERROR;
            KLOG_ERROR("get storage node is not match nodes {}", nodes_);
            return false;
        }
        storage_iparams_[shard_ids_[i]] = s_iparams_;
    }
    return true;
}

bool AddNodeMission::FetchStorageIplists() {
    std::string sql = string_sprintf("select hostaddr from %s.server_nodes where machine_type='storage' and node_stats='running'",
                KUNLUN_METADATA_DB_NAME);
  
    MysqlResult result;
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        err_code_ = ADD_NODE_STORAGE_HOSTADDR_SERVER_NODES_SQL_ERROR;
        KLOG_ERROR("get storage hostaddr from server nodes failed {}", g_node_channel_manager->getErr());
        return false;
    }

    int nrows = result.GetResultLinesNum();
    for(int i=0; i<nrows; i++) {
        std::string hostaddr = result[i]["hostaddr"];
        if(!GetStorageInstallHost(hostaddr, avail_hosts_, host_ports_, err_code_)) 
            return false;
    }

    for(int i=0; i < shard_ids_.size(); i++) {
        s_iparams_.clear();
        if(!GetBestInstallHosts(avail_hosts_, nodes_, s_iparams_, host_ports_))
            return false;

        if((int)s_iparams_.size() < nodes_) {
            err_code_ = ADD_NODE_GET_STORAGE_MATCH_NODES_ERROR;
            KLOG_ERROR("get storage node is not match nodes {}", nodes_);
            return false;
        }
        storage_iparams_[shard_ids_[i]] = s_iparams_;
    }
    return true;
}

bool AddNodeMission::AddStorageJobs() {
    bool ret = false;
    for(auto sid : storage_iparams_) {
        ret = DispatchShardInstallJob(sid.first);
        if(!ret) {
            storage_state_ = "failed";
            job_status_ = "failed";
            return false;
        }
    }
    return true;
}

std::string AddNodeMission::CreateShardInstallId() {
    std::string request_id;

    MysqlResult result;
    std::string sql = "begin";
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("execute begin sql failed");
        return request_id;
    }

    result.Clean();
    sql = "insert into kunlun_metadata_db.cluster_general_job_log set user_name='internal_user'";
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("generate unique request id faild: {}", g_node_channel_manager->getErr());
        return request_id;
    }
  
    result.Clean();
    sql = "select last_insert_id() as insert_id";
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if (ret) {
    KLOG_ERROR(
        "fetch last_insert_id during request unique id generation faild: {}",
        g_node_channel_manager->getErr());
    return request_id;
  }
  request_id = result[0]["insert_id"];

  sql = "commit";
  ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);

  return request_id;
}

bool AddNodeMission::DispatchShardInstallJob(int shard_id) {
    std::string request_id = CreateShardInstallId();
    if(request_id.empty()) {
        err_code_ = ADD_NODE_GET_INSTALL_REQUEST_ID_ERROR;
        return false;
    }

    std::string task_spec_info = kunlun::string_sprintf(
      "install_mysql_parallel_%s", request_id.c_str());
    kunlun::MySQLInstallRemoteTask *task = new kunlun::MySQLInstallRemoteTask(
        task_spec_info.c_str(), request_id);
    std::vector<IComm_Param> iparam = storage_iparams_[shard_id];
    for(size_t i=0; i<iparam.size(); i++) {
        bool ret = task->InitInstanceInfoOneByOne(
            std::get<0>(iparam[i]), std::get<1>(iparam[i]), std::get<1>(iparam[i])+1, innodb_size_,
            dbcfg_);
        if(!ret) {
            KLOG_ERROR("package install mysql parameter failed {}", task->getErr());
            err_code_ = ADD_NODE_DISPATH_INSTALL_TASK_ERROR;
            delete task;
            return false;
        }
    }
    shard_map_states_[shard_id] = N_DISPATCH;
    get_task_manager()->PushBackTask(task);
    shard_map_ids_[shard_id] = request_id;
    return true;
}

void AddNodeMission::RollbackCB(Json::Value& root, void* arg) {
    NodeParam *nparam = static_cast<NodeParam*>(arg);

    Json::FastWriter writer;
    std::string result = writer.write(root);
    KLOG_INFO("RollbackCB result {}", result);

    nparam->GetMission()->UpdateNodeMapState(nparam->GetShardId(), N_ROLLBACK);
    delete nparam;
}


void AddNodeMission::RollbackStorageInstall(int shard_id) {
    ObjectPtr<KDelShardMission> mission(new KDelShardMission());
    NodeParam* nparam = new NodeParam(this, shard_id);
    mission->set_cb(AddNodeMission::RollbackCB);
    mission->set_cb_context(nparam);

    std::vector<IComm_Param> iparams = storage_iparams_[shard_id];
    //KLOG_INFO("shard_id {} iparams size {}", shard_id, iparams.size());
    for(size_t i=0; i<iparams.size(); i++) {
        struct InstanceInfoSt st1 ;
        st1.ip = std::get<0>(iparams[i]);
        st1.port = std::get<1>(iparams[i]);
        st1.exporter_port = std::get<1>(iparams[i]) + 1;
        st1.innodb_buffer_size_M = innodb_size_;
        if(i == 0)
            st1.role = "master";
        else 
            st1.role = "slave";

        mission->setInstallInfoOneByOne(st1);
    }
    bool ret = mission->InitFromInternal();
    if(!ret){
        KLOG_ERROR("Rollback to add node mission failed {}", mission->getErr());
        return;
    }
    
    ObjectPtr<ClusterRequest> base_request(dynamic_cast<ClusterRequest*>(mission.GetTRaw()));
    g_request_handle_thread->DispatchRequest(base_request);
}

int AddNodeMission::GetShardIdByRequestId(const std::string& request_id) {
    int id = -2;
    for(auto &it : shard_map_ids_) {
        if(it.second == request_id) {
            id = it.first;
            break;
        }
    }
    return id;
}

bool AddNodeMission::PostAddNode(int shard_id) {
    std::vector<IComm_Param> iparams = storage_iparams_[shard_id];
    MysqlResult result;
    std::string sql = string_sprintf("select name, num_nodes from %s.shards where id=%d", KUNLUN_METADATA_DB_NAME,
                shard_id);
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("get shards by shard_id {} failed", shard_id);
        return false;
    }
    if(result.GetResultLinesNum() != 1) {
        KLOG_ERROR("get shards by shard_id {} records too many", shard_id);
        return false;
    }
    std::string shard_name = result[0]["name"];
    int num_nodes = atoi(result[0]["num_nodes"]);

    sql = string_sprintf("select hostaddr, port from %s.shard_nodes where shard_id=%d and db_cluster_id=%d and member_state='source'",
                KUNLUN_METADATA_DB_NAME, shard_id, cluster_id_);
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("get shard_id {} master node failed", shard_id);
        return false;
    }
    if(result.GetResultLinesNum() != 1) {
        KLOG_ERROR("get shard_id {} master node records too many", shard_id);
        return false;
    }
    std::string ip = result[0]["hostaddr"];
    std::string port = result[0]["port"];
    std::string master_host = ip + "_" + port;

    sql = string_sprintf("select name from %s.db_clusters where id=%d", 
                KUNLUN_METADATA_DB_NAME, cluster_id_);
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("get cluster name by cluster_id {} failed", cluster_id_);
        return false;
    }
    if(result.GetResultLinesNum() != 1) {
        KLOG_ERROR("get cluster name by cluster_id {} records too many", cluster_id_);
        return false;
    }
    std::string cluster_name = result[0]["name"];

    sql = "begin";
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("set session begin failed");
        return false;
    }

    sql = string_sprintf("update %s.shards set num_nodes=%d where id=%d", 
                KUNLUN_METADATA_DB_NAME, num_nodes+(int)iparams.size(), shard_id);
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("update shard num_nodes failed");
        return false;
    }

    std::unordered_map<std::string, std::vector<IComm_Param> > storage_iparams;
    std::unordered_map<std::string, std::vector<std::string> > shard_map_nids;
    std::unordered_map<std::string, std::string> shard_map_sids;
    storage_iparams[shard_name] = iparams;
    shard_map_sids[shard_name] = std::to_string(shard_id);
    std::vector<std::string> slave_hosts;
    std::vector<std::string> node_ids;
    for(size_t i=0; i<iparams.size(); i++) {
        slave_hosts.push_back(std::get<0>(iparams[i]) +"_"+std::to_string(std::get<1>(iparams[i])));
        sql = string_sprintf("insert into %s.shard_nodes(hostaddr, port, user_name, passwd, shard_id, db_cluster_id, svr_node_id, member_state) values('%s', %d, 'pgx', 'pgx_pwd', %d, %d, 1, 'replica')",
                    KUNLUN_METADATA_DB_NAME, std::get<0>(iparams[i]).c_str(), std::get<1>(iparams[i]), 
                    shard_id, cluster_id_);
        KLOG_INFO("insert into shard_nodes sql: {}", sql);
        ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
        if(ret) {
            KLOG_ERROR("insert shard_nodes failed {}", g_node_channel_manager->getErr());
            return false;
        }

        sql = "select last_insert_id() as id";
        ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
        if(ret) {
            KLOG_ERROR("select last insert id failed {}", g_node_channel_manager->getErr());
            return false;
        }
        std::string node_id = result[0]["id"];
        KLOG_INFO("get insert node id {}", node_id);
        node_ids.emplace_back(node_id);
    }

    shard_map_nids[shard_name] = node_ids;

    if(ha_mode_ == "rbr") {
        std::string uuid = GetMysqlUuidByHost(shard_name, cluster_name, master_host);
		if(uuid.empty()) {
			KLOG_ERROR("get master host {} uuid failed", master_host);
            return false;
        }

        sql = string_sprintf("insert into node_map_master (cluster_id, node_host, master_host, master_uuid) values(");
        for(size_t i=0; i < slave_hosts.size(); i++) {
            sql = sql + std::to_string(cluster_id_) + ",'"+slave_hosts[i]+"','" + master_host + "','" + uuid +"'),("; 
        }

        sql = sql.substr(0, sql.length()-2);
        KLOG_INFO("insert node_map_master sql: {}", sql);
        ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
        if(ret) {
            KLOG_ERROR("insert node_map_master failed {}", g_node_channel_manager->getErr());
            return false;
        }

        if(!PersistFullsyncLevel(slave_hosts, fullsync_level_)) {
            KLOG_ERROR("persist fullsync_level {} to db failed", fullsync_level_);
            return false;
        }
    }

    sql = "commit";
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("commit failed {}", g_node_channel_manager->getErr());
        return false;
    }

    std::vector<std::string> sql_stmts;
    AddShardToComputerNode(cluster_id_, computer_user_, computer_pwd_, storage_iparams, 
                        shard_map_nids, shard_map_sids, sql_stmts, 1);

    //if(!WriteSqlToDdlLog(cluster_name_, sql_stmts)) {
    //    KLOG_ERROR("write sql into meta ddl_log failed");
    //    return false;
    //}

    std::vector<std::string> storage_hosts;
    std::vector<CNode_Type> node_params;
    for(size_t i=0; i < iparams.size(); i++) {
        int node_id = atoi(node_ids[i].c_str());
        CNode_Type ct{node_id, std::get<0>(iparams[i]), std::get<1>(iparams[i]), 1};
        node_params.emplace_back(ct);
        storage_hosts.emplace_back(std::get<0>(iparams[i])+":"+std::to_string(std::get<1>(iparams[i])+1));
    }

    ret = g_prometheus_manager->AddStorageConf(storage_hosts);
    if(ret) {
        KLOG_ERROR("add prometheus mysqld_exporter config failed");
        return false;
    }

    System::get_instance()->add_node_shard_memory(cluster_id_, shard_id_, node_params);
    return true;
}

void AddNodeMission::PackInstallPortsByShardId(int shard_id, 
                std::map<std::string, std::vector<int> >& host_ports) {
    std::vector<IComm_Param> iparams = storage_iparams_[shard_id];
    for(size_t i=0; i<iparams.size(); i++) {
        std::string ip = std::get<0>(iparams[i]);
        std::vector<int> ports;
        if(host_ports.find(ip) != host_ports.end()) {
            ports = host_ports[ip];
        }
        ports.push_back(std::get<1>(iparams[i]));
        host_ports[ip] = ports;
    }
}

void AddNodeMission::UpdateNodeMapState(int shard_id, Node_State nstate) {
    KlWrapGuard<KlWrapMutex> guard(up_mux_);
    if(nstate == N_FAILED) {
        RollbackStorageInstall(shard_id);
        shard_map_states_[shard_id] = N_ROLLBACK;
        return;
    } 
    if(nstate == N_DREBUILD) {
        if(!PostAddNode(shard_id)) {
            err_code_ = ADD_NODE_POST_DEAL_ERROR;
            RollbackStorageInstall(shard_id);
            shard_map_states_[shard_id] = N_ROLLBACK;
            return;
        }
        nstate = N_DONE;
    }

    if(nstate == N_ROLLBACK)
        nstate = N_FAILED;

    shard_map_states_[shard_id] = nstate;
    
    std::map<std::string, std::vector<int> > done_ports;
    std::map<std::string, std::vector<int> > failed_ports;
    int done_state = 0, failed_state=0;
    for(auto &it : shard_map_states_) {
        if(it.second == N_DONE || it.second == N_FAILED) {
            done_state++;
            if(it.second == N_FAILED) {
                failed_state++;
                PackInstallPortsByShardId(it.first, failed_ports);
            } else {
                PackInstallPortsByShardId(it.first, done_ports);
            }
        }
    }
    UpdateUsedPort(K_APPEND, M_STORAGE, done_ports);
    UpdateInstallingPort(K_CLEAR, M_STORAGE, done_ports);
    UpdateInstallingPort(K_CLEAR, M_STORAGE, failed_ports);

    if(done_state >= (int)shard_map_states_.size()) {
        job_status_ = "done";
        storage_state_ = "done";
        if(failed_state >= (int)shard_map_states_.size()) {
            job_status_ = "failed";
            storage_state_ = "failed";
        }

        UpdateOperationRecord();
        if(storage_state_ == "done" || storage_state_ == "failed") {
            //std::unique_lock<std::mutex> lock{ cond_mux_ };
            //cond_.notify_all();
            cond_.SignalAll();
        }
    }
}

void AddNodeMission::RebuildRelationCB(Json::Value& root, void* arg) {
    NodeParam *nparam = static_cast<NodeParam*>(arg);

    Json::FastWriter writer;
    std::string result = writer.write(root);
    KLOG_INFO("RebuildRelationCB result {}", result);

    std::string status = root["status"].asString();
    if(status == "failed") {
        nparam->GetMission()->UpdateErrorCodeInfo(atoi(root["error_code"].asCString()), root["error_info"].asString());
        nparam->GetMission()->UpdateNodeMapState(nparam->GetShardId(),N_FAILED);
        delete nparam;
        return;
    }

    nparam->GetMission()->UpdateNodeMapState(nparam->GetShardId(), N_DREBUILD);
    delete nparam;
}

void AddNodeMission::UpdateErrorCodeInfo(int error_code, const std::string& error_info) {
    error_info_ = error_info;
    err_code_ = error_code;
}

bool AddNodeMission::NodeRebuildRelation(int shard_id) {
    std::vector<RN_Param> rb_backups;
    std::vector<IComm_Param> iparams = storage_iparams_[shard_id];
    for(size_t i=0; i<iparams.size(); i++) {
        std::string host = std::get<0>(iparams[i])+"_"+std::to_string(std::get<1>(iparams[i]));
        RN_Param rb_param{host, 0, "hdfs", 10485760};
        rb_backups.emplace_back(rb_param);
    }
    
    ObjectPtr<RebuildNodeMission> mission(new RebuildNodeMission(true, 15, rb_backups, 
                    cluster_id_, shard_id));
    mission->set_cb(AddNodeMission::RebuildRelationCB);
    NodeParam* nparam = new NodeParam(this, shard_id);
    mission->set_cb_context(nparam);

    mission->InitFromInternal();
    ObjectPtr<ClusterRequest> base_request(dynamic_cast<ClusterRequest*>(mission.GetTRaw()));
    g_request_handle_thread->DispatchRequest(base_request);
    return true;
}

bool AddNodeMission::CheckInstallMysqlResult() {
    int failed_num = 0;
    std::map<std::string, std::vector<int> > host_ports;
    for(auto &it : shard_map_states_) {
        if(it.second == N_FAILED) {
            failed_num++;
            std::vector<IComm_Param> iparams = storage_iparams_[it.first];
            for(size_t i=0; i<iparams.size(); i++) {
                std::string ip = std::get<0>(iparams[i]);
                std::vector<int> ports;
                if(host_ports.find(ip) != host_ports.end()) {
                    ports = host_ports[ip];
                }
                ports.push_back(std::get<1>(iparams[i]));
                host_ports[ip] = ports;
            }
        }
    }
    UpdateInstallingPort(K_CLEAR, M_STORAGE, host_ports);

    if(failed_num == (int)shard_map_states_.size())
        return false;
    return true;
}

bool AddNodeMission::TearDownMission() {
    std::vector<RemoteTask *> task_vec = get_task_manager()->get_remote_task_vec();
    for(auto it = task_vec.begin(); it != task_vec.end(); it++) {
        std::string request_id = (*it)->get_response()->get_request_id();
        int shard_id = GetShardIdByRequestId(request_id);
        if(shard_id != -2) {
            if (!(*it)->get_response()->ok()) {
                KLOG_ERROR("shard_id {} install mysql failed {}", shard_id, 
                 (*it)->get_response()->get_error_info());
                error_info_ += (*it)->get_response()->get_error_info() + "|";
                shard_map_states_[shard_id] = N_FAILED;
            } else {
                shard_map_states_[shard_id] = N_SREBUILD;
                if(!NodeRebuildRelation(shard_id)) {
                    shard_map_states_[shard_id] = N_FAILED;
                }
            }
        }
    }

    if(!CheckInstallMysqlResult()) {
        job_status_ = "failed";
        storage_state_ = "failed";
        UpdateOperationRecord();
        return true;
    }
    UpdateOperationRecord();

    //std::unique_lock<std::mutex> lock{ cond_mux_ };
	//cond_.wait(lock, [this] {
	//    return (storage_state_ == "done" || storage_state_ == "failed");
	//});
    cond_.Wait();
    return true;
}

bool AddNodeMission::UpdateOperationRecord() {
    std::string str_sql,memo;
    Json::Value memo_json;
    Json::FastWriter writer;

    memo_json["error_code"] = err_code_;
    if(error_info_.empty())
        memo_json["error_info"] = GlobalErrorNum(err_code_).get_err_num_str();
    else
        memo_json["error_info"] = error_info_;

    if(!computer_user_.empty())
        memo_json["computer_user"] = computer_user_;
    if(!computer_pwd_.empty())
        memo_json["computer_pwd"] = computer_pwd_;

    memo_json["cluster_id"] = std::to_string(cluster_id_);
    memo_json["shard_id"] = std::to_string(shard_id_);
    memo_json["nodes"] = std::to_string(nodes_);
    if(storage_iparams_.size() > 0) {
        for(auto &it : storage_iparams_) {
            Json::Value tmp;
            std::string hosts;
            std::vector<IComm_Param> s_iparams = it.second;
            for(size_t i=0; i<s_iparams.size(); i++) {
                hosts += std::get<0>(s_iparams[i]) + "_" + std::to_string(std::get<1>(s_iparams[i])) + ";";
            }
            std::string shard_id = std::to_string(it.first);
            tmp[shard_id] = hosts;
            memo_json["shard_hosts"].append(tmp);
        }
    }

    if(shard_map_ids_.size() > 0) {
        for(auto &it : shard_map_ids_) { 
            Json::Value tmp;
            std::string shard_id = std::to_string(it.first);
            tmp[shard_id] = it.second;
            memo_json["shard_map_ids"].append(tmp);
        }
    }

    if(shard_map_states_.size() > 0) {
        for(auto &it : shard_map_states_) { 
            Json::Value tmp;
            std::string shard_id = std::to_string(it.first);
            tmp[shard_id] = it.second;
            memo_json["shard_state"].append(tmp);
        }
    }

    if(!storage_state_.empty()) {
        memo_json["storage_state"] = storage_state_;
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

    return true;
}

}