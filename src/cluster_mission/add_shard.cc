/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "add_shard.h"
#include "cluster_operator/shardInstallMission.h"
#include "request_framework/handleRequestThread.h"
#include "zettalib/op_log.h"
#include "kl_mentain/shard.h"
#include "kl_mentain/sys.h"
#include "zettalib/op_mysql.h"
#include "cluster_collection/prometheus_control.h"
#include "coldbackup/coldbackup.h"

extern HandleRequestThread *g_request_handle_thread;
extern kunlun::KPrometheusControl* g_prometheus_manager;

namespace kunlun
{

bool AddShardMission::ArrangeRemoteTask() {
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
            KLOG_INFO("add shard called by other, so quit to restart");
            return true;
        }

        std::string storage_state = memo_json["storage_state"].asString();
        if(storage_state != "prepare" && memo_json.isMember("shard_hosts")) {
            Json::Value shard_hosts = memo_json["shard_hosts"];
            for(unsigned int i=0; i < shard_hosts.size(); i++) {
                Json::Value::Members mem = shard_hosts[i].getMemberNames();
                for(auto iter = mem.begin(); iter != mem.end(); iter++) {
                    std::string hosts = shard_hosts[i][*iter].asString();
                    std::vector<std::string> host_vec = StringTokenize(hosts, ",");
                    std::vector<IComm_Param> iparams;
                     for(size_t i=0; i<host_vec.size(); i++) {
                        std::string ip = host_vec[i].substr(0, host_vec[i].rfind("_"));
                        int port = atoi(host_vec[i].substr(host_vec[i].rfind("_")+1).c_str());
                        IComm_Param iparam{ip, port};
                        iparams.emplace_back(iparam);
                    }
                    storage_iparams_[*iter] = iparams;
                }
            }
        }
        if(memo_json.isMember("ha_mode"))
            ha_mode_ = memo_json["ha_mode"].asString();
        if(memo_json.isMember("cluster_name"))
            cluster_name_ = memo_json["cluster_name"].asString();
        if(memo_json.isMember("cluster_id"))
            cluster_id_ = atoi(memo_json["cluster_id"].asCString());
        if(memo_json.isMember("nick_name"))
            nick_name_ = memo_json["nick_name"].asString();

        if(storage_state == "done") {
            bool ret = PostAddShard();
            if(ret) {
                job_status_ = "done";
                std::vector<Shard_Type> shard_params;
                for(auto &sp : storage_iparams_) {
                    std::string shard_name = sp.first;
                    int  shard_id = atoi(shard_map_sids_[shard_name].c_str());
                    std::vector<std::string> node_ids = shard_map_nids_[shard_name];
                    std::vector<IComm_Param> iparams = sp.second;
                    std::vector<CNode_Type> node_params;
                    for(size_t i=0; i < iparams.size(); i++) {
                        int node_id = atoi(node_ids[i].c_str());
                        CNode_Type ct{node_id, std::get<0>(iparams[i]), std::get<1>(iparams[i]), 1};
                        node_params.emplace_back(ct);
                    }
                    Shard_Type st{shard_id, shard_name, "pgx", "pgx_pwd", 1, node_params};
                    shard_params.emplace_back(st);
                }
                System::get_instance()->add_shard_cluster_memory(cluster_id_, cluster_name_, 
                                nick_name_, ha_mode_, shard_params);
            } else {
                job_status_ = "failed";
                err_code_ = ADD_SHARD_DEAL_POST_ERROR;
                RollbackStorageJob();
            }
            UpdateOperationRecord();
            return ret;
        } else {
            RollbackStorageJob();
            err_code_ = CLUSTER_MGR_RESTART_ERROR;
            job_status_ = "failed";
        }
        
        UpdateOperationRecord();
    }
    return true;
} 

bool AddShardMission::SetUpMisson() {
    job_id_ = get_request_unique_id();
    if(get_init_by_recover_flag() == true) {
         KLOG_INFO("Add shard ignore recover state in setup phase");
        return true;
    }
    
    KLOG_INFO("Add shard setup phase");
    
    ClusterRequestTypes request_type = get_request_type();
    
    bool ret = true;
    int i = 0;
    Json::Value paras, storage_iplists;

    if(init_flag_) {
        KLOG_INFO("add shard called by internal");
        goto end;
    }

    if (!super::get_body_json_document().isMember("paras")) {
        err_code_ = ADD_SHARD_QUEST_LOST_PARAS_ERROR;
        KLOG_ERROR("missing `paras` key-value pair in the request body");
        goto end;
    }
    paras = super::get_body_json_document()["paras"];

    if (!paras.isMember("cluster_id")) {
      err_code_ = ADD_SHARD_QUEST_MISS_CLUSTERID_ERROR;
      KLOG_ERROR("missing `shards` key-value pair in the request body");
      goto end;
    }
    if(!CheckStringIsDigit(paras["cluster_id"].asString())) {
      err_code_ = ADD_SHARD_QUEST_MISS_CLUSTERID_ERROR;
      KLOG_ERROR("missing `shards` key-value pair in the request body");
      goto end;
    }
    cluster_id_ = atoi(paras["cluster_id"].asCString());

    if (!paras.isMember("shards")) {
      err_code_ = ADD_SHARD_QUEST_MISS_SHARDS_ERROR;
      KLOG_ERROR("missing `shards` key-value pair in the request body");
      goto end;
    }
    if(!CheckStringIsDigit(paras["shards"].asString())) {
      err_code_ = ADD_SHARD_QUEST_MISS_SHARDS_ERROR;
      KLOG_ERROR("missing `shards` key-value pair in the request body");
      goto end;
    }
    shards_ = atoi(paras["shards"].asCString());
    if(shards_ < 1 || shards_ > 256) {
      err_code_ = ADD_SHARD_QUEST_SHARDS_PARAM_ERROR;
      KLOG_ERROR("shards error(must in 1-256)");
      goto end;
    }

    if (!paras.isMember("nodes")) {
      err_code_ = ADD_SHARD_QUEST_MISS_NODES_ERROR;
      KLOG_ERROR("missing `nodes` key-value pair in the request body");
      goto end;
    }
    if(!CheckStringIsDigit(paras["nodes"].asString())) {
      err_code_ = ADD_SHARD_QUEST_MISS_NODES_ERROR;
      KLOG_ERROR("missing `nodes` key-value pair in the request body");
      goto end;
    }
    nodes_ = atoi(paras["nodes"].asCString());
    if(nodes_ < 1 || nodes_ > 256) {
      err_code_ = ADD_SHARD_QUEST_NODES_PARAM_ERROR;
      KLOG_ERROR("nodes error(must in 1-256)");
      goto end;
    }

    if(paras.isMember("storage_iplists")) {
      storage_iplists = paras["storage_iplists"];
      for(i=0; i<storage_iplists.size();i++) {
        storage_iplists_.push_back(storage_iplists[i].asString());
      }

      if(!CheckMachineIplists(M_STORAGE, storage_iplists_)) {
          err_code_ = ADD_SHARD_ASSIGN_STORAGE_IPLISTS_ERROR;
          goto end;
      }
    }

    if(GetInstallShardParamsByClusterId()) {
        goto end;
    }

end:
    if(err_code_) {
      ret = false;
      job_status_ = "failed";
      storage_state_ = "failed";
      UpdateOperationRecord();
    } else
      job_status_ = "ongoing";

    return ret;
}

bool AddShardMission::GetInstallShardParamsByClusterId() {
    std::vector<std::string> key_name;
    key_name.push_back("name");
    key_name.push_back("memo.ha_mode");
    key_name.push_back("memo.computer_user");
    key_name.push_back("memo.computer_passwd");
    key_name.push_back("memo.innodb_size");
    key_name.push_back("memo.dbcfg");
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
    dbcfg_ = atoi(key_vals["dbcfg"].c_str());
    
    return true;
}

bool AddShardMission::InitFromInternal() {
    Json::Value doc;
    FillCommonJobJsonDoc(doc);
    doc["job_type"] = "internal_add_shard";

    Json::Value para;
    para["ha_mode"] = ha_mode_;
    para["cluster_id"] = std::to_string(cluster_id_);
    para["cluster_name"] = cluster_name_;
    para["shards"] = std::to_string(shards_);
    para["nodes"] = std::to_string(nodes_);
    para["innodb_size"] = std::to_string(innodb_size_);
    para["dbcfg"] = std::to_string(dbcfg_);
    for(size_t i=0; i<storage_iplists_.size(); i++) {
        para["storage_iplists"].append(storage_iplists_[i]);
    }
    doc["paras"] = para;
    set_body_json_document(doc);
    CompleteGeneralJobInfo();
    return true;
}

void AddShardMission::CompleteGeneralJobInfo() {
    Json::FastWriter writer;
    writer.omitEndingLineFeed();
    std::string job_info = writer.write(get_body_json_document());

    kunlun::MysqlResult result;
    std::string sql = string_sprintf("update %s.cluster_general_job_log set "
                "job_type='internal_add_shard',job_info='%s' where id = %s",
                KUNLUN_METADATA_DB_NAME ,job_info.c_str(), get_request_unique_id().c_str());
    
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if (ret) {
        KLOG_ERROR("update cluster_general_job_log faild: {}", g_node_channel_manager->getErr());
        return;
    }
    KLOG_INFO("update cluster_general_job_log success: {}", sql);
}

void AddShardMission::DealLocal() {
    KLOG_INFO("add shard deal local phase");
    if(job_status_ == "failed" || job_status_ == "done") {
        return;
    }
    
    if(!SeekStorageIplists()) {
        job_status_ = "failed";
        storage_state_ = "failed";
        UpdateOperationRecord();
        return;
    }
    UpdateMetaAndOperationStat(S_PREPARE);

    bool ret = AddShardJobs();
    if(!ret) {
        job_status_ = "failed";
        UpdateMetaAndOperationStat(S_FAILED);
        return;
    }
    UpdateMetaAndOperationStat(S_DISPATCH);
}

bool AddShardMission::SeekStorageIplists() {
  bool ret = true;
  if(storage_iplists_.size() > 0) {
    ret = AssignStorageIplists();
  } else
    ret = FetchStorageIplists();
  
  return ret;
}

int AddShardMission::GetShardNum() {
    MysqlResult result;
    std::string sql = string_sprintf("select name from %s.shards where db_cluster_id=%d",
                KUNLUN_METADATA_DB_NAME, cluster_id_);
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("get cluster_id {} shards failed", cluster_id_);
        err_code_ = ADD_SHARD_GET_SHARDS_FROM_METADB_ERROR;
        return -1;
    }
    int pos = 0;
    int nrows = result.GetResultLinesNum();
    for(size_t i=0; i< nrows ; i++) {
        std::string name = result[i]["name"];
        std::string s_num = name.substr(name.rfind("_")+1);
        int num = atoi(s_num.c_str());
        if(num > pos) {
            pos = num;
        }
    }
    return pos;
    //return (result.GetResultLinesNum());
}

bool AddShardMission::AssignStorageIplists() {
  for(size_t i=0; i<storage_iplists_.size(); i++) {
    std::string hostaddr = storage_iplists_[i];
    if(!GetStorageInstallHost(hostaddr, avail_hosts_, host_ports_, err_code_)) 
      return false;
  }
  
  int pos = GetShardNum();
  if(pos == -1)
    return false;
  
  for(int i=0; i<shards_; i++) {
    s_iparams_.clear();
    if(!GetBestInstallHosts(avail_hosts_, nodes_, s_iparams_, host_ports_))
      return false;

    if((int)s_iparams_.size() < nodes_) {
      err_code_ = ADD_SHARD_GET_STORAGE_MATCH_NODES_ERROR;
      KLOG_ERROR("get storage node is not match nodes {}", nodes_);
      return false;
    }
    std::string shard_name = "shard_"+std::to_string(pos+i+1);
    storage_iparams_[shard_name] = s_iparams_;
  }
  return true;
}

bool AddShardMission::FetchStorageIplists() {
  std::string sql = string_sprintf("select hostaddr from %s.server_nodes where machine_type='storage' and node_stats='running'",
                KUNLUN_METADATA_DB_NAME);
  
  MysqlResult result;
  bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  if(ret) {
    err_code_ = ADD_SHARD_STORAGE_HOSTADDR_SERVER_NODES_SQL_ERROR;
    KLOG_ERROR("get storage hostaddr from server nodes failed {}", g_node_channel_manager->getErr());
    return false;
  }

  int nrows = result.GetResultLinesNum();
  for(int i=0; i<nrows; i++) {
    std::string hostaddr = result[i]["hostaddr"];
    if(!GetStorageInstallHost(hostaddr, avail_hosts_, host_ports_, err_code_)) 
      return false;
  }
  int pos = GetShardNum();
  if(pos == -1)
    return false;

  for(int i=0; i<shards_; i++) {
    s_iparams_.clear();
    if(!GetBestInstallHosts(avail_hosts_, nodes_, s_iparams_, host_ports_))
      return false;

    if((int)s_iparams_.size() < nodes_) {
      err_code_ = ADD_SHARD_GET_STORAGE_MATCH_NODES_ERROR;
      KLOG_ERROR("get storage node is not match nodes {}", nodes_);
      return false;
    }
    std::string shard_name="shard_"+std::to_string(pos+i+1);
    storage_iparams_[shard_name] = s_iparams_;
  }

  return true;
}

std::string AddShardMission::GetProcUuid() {
  std::string uuid;
  FILE *fp = fopen("/proc/sys/kernel/random/uuid", "rb");
	if (fp == NULL)	{
		KLOG_ERROR("open proc random uuid error");
		return uuid;
	}

	char buf[60];
	memset(buf, 0, 60);
	size_t n = fread(buf, 1, 36, fp);
	fclose(fp);
	
	if(n != 36)
		return uuid;
	
	uuid.assign(buf, n);
  return uuid;
}

bool AddShardMission::AddShardJobs() {
  for(auto it : storage_iparams_) {
    ObjectPtr<KAddShardMission>mission(new KAddShardMission());
    mission->set_cb(AddShardMission::AddShardCallCB);
    ShardJobParam *jparam = new ShardJobParam(this, it.first);
    mission->set_cb_context(jparam);

    std::string uuid = GetProcUuid();
    std::vector<IComm_Param> s_iparams = it.second;
    std::string mgr_seed;
    for(size_t i=0; i<s_iparams.size(); i++)
      mgr_seed += std::get<0>(s_iparams[i]) + ":" + std::to_string(std::get<1>(s_iparams_[i])+2) + ",";

    mgr_seed = mgr_seed.substr(0, mgr_seed.length()-1);

    for(size_t i=0; i<s_iparams.size(); i++) {
      struct InstanceInfoSt st1 ;
      st1.ip = std::get<0>(s_iparams[i]);
      st1.port = std::get<1>(s_iparams[i]);
      st1.exporter_port = std::get<1>(s_iparams[i])+1;
      st1.mgr_port = std::get<1>(s_iparams_[i])+2;
      st1.xport = std::get<1>(s_iparams_[i])+3;
      st1.mgr_seed = mgr_seed;
      st1.mgr_uuid = uuid;
      st1.innodb_buffer_size_M = innodb_size_;
      st1.db_cfg = dbcfg_;
      if(i == 0)
        st1.role = "master";
      else 
        st1.role = "slave";

      mission->setInstallInfoOneByOne(st1);
    }

    bool ret = mission->InitFromInternal();
    if(!ret){
      KLOG_ERROR("AddShard mission failed {}", mission->getErr());
      err_code_ = ADD_SHARD_INIT_MISSION_ERROR;
      return false;
    }

    ObjectPtr<ClusterRequest> base_request(dynamic_cast<ClusterRequest*>(mission.GetTRaw()));
    g_request_handle_thread->DispatchRequest(base_request);
    //g_request_handle_thread->DispatchRequest(mission);
    shard_map_id_[it.first] = mission->get_request_unique_id();
    shard_map_state_[it.first] = 0;
  }

  return true;
}

void AddShardMission::PackInstallPorts(std::map<std::string, std::vector<int> >& host_ports) {
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

void AddShardMission::UpdateMetaAndOperationStat(JobType jtype) {
  std::string str_sql,memo;
  Json::Value memo_json;
  Json::FastWriter writer;
  memo_json["error_code"] = err_code_;
  memo_json["error_info"] = GlobalErrorNum(err_code_).get_err_num_str();
  memo_json["rollback_flag"] = std::to_string(init_flag_);

  if(jtype == S_PREPARE) {
    storage_state_ = "prepare";
  } else if(jtype == S_DISPATCH) {
    storage_state_ = "dispatch";

    std::map<std::string, std::vector<int> > install_storage_ports;
    PackInstallPorts(install_storage_ports);
    if(!UpdateInstallingPort(K_APPEND, M_STORAGE, install_storage_ports))
      return;
  } else if(jtype == S_FAILED) {
    std::map<std::string, std::vector<int> > install_storage_ports;
    PackInstallPorts(install_storage_ports);

    storage_state_ = "failed";
    job_status_ = "failed";
    err_code_ = ADD_SHARD_INSTALL_STORAGE_ERROR;
    memo_json["error_code"] = ADD_SHARD_INSTALL_STORAGE_ERROR;
    memo_json["error_info"] = error_info_;
    
    if(!UpdateInstallingPort(K_CLEAR, M_STORAGE, install_storage_ports))
      return;
    RollbackStorageJob();
  } else if(jtype == S_DONE) {
    storage_state_ = "done";
    std::map<std::string, std::vector<int> > install_storage_ports;
    PackInstallPorts(install_storage_ports);
    if(!UpdateInstallingPort(K_CLEAR, M_STORAGE, install_storage_ports))
        return;
    
    if(!UpdateUsedPort(K_APPEND, M_STORAGE, install_storage_ports))
        return;
  }

  if(!shard_map_id_.empty()) {
    for(auto &it : shard_map_id_) { 
      Json::Value tmp;
      tmp[it.first] = it.second;
      memo_json["shard_ids"].append(tmp);
    }
  }

  if(!storage_state_.empty())
    memo_json["storage_state"] = storage_state_;
  
  if(!cluster_name_.empty())
    memo_json["cluster_name"] = cluster_name_;
  memo_json["cluster_id"] = std::to_string(cluster_id_);
  if(!nick_name_.empty())
    memo_json["nick_name"] = nick_name_;
  if(!ha_mode_.empty()) 
    memo_json["ha_mode"] = ha_mode_;

  if(storage_iparams_.size() > 0) {
    Json::Value tmp;
    for(auto &it : storage_iparams_) {
      std::string hosts;
      std::vector<IComm_Param> s_iparams = it.second;
      for(size_t i=0; i<s_iparams.size(); i++) {
        hosts += std::get<0>(s_iparams[i]) + "_" + std::to_string(std::get<1>(s_iparams[i])) + ";";
      }
      tmp[it.first] = hosts;
    }
    memo_json["shard_hosts"].append(tmp);
  }

  if(storage_state_ == "done") {
    job_status_ = "done";
    if(!PostAddShard()) {
      job_status_ = "failed";
      std::map<std::string, std::vector<int> > install_storage_ports;
      PackInstallPorts(install_storage_ports);
      UpdateUsedPort(K_CLEAR, M_STORAGE, install_storage_ports);

      RollbackStorageJob();
      err_code_ = ADD_SHARD_DEAL_POST_ERROR;
      memo_json["error_code"] = ADD_SHARD_DEAL_POST_ERROR;
      memo_json["error_info"] = GlobalErrorNum(err_code_).get_err_num_str();;
    } else {
        std::vector<Shard_Type> shard_params;
        for(auto &sp : storage_iparams_) {
            std::string shard_name = sp.first;
            int shard_id = atoi(shard_map_sids_[shard_name].c_str());
            std::vector<std::string> node_ids = shard_map_nids_[shard_name];
            std::vector<IComm_Param> iparams = sp.second;
            std::vector<CNode_Type> node_params;
            for(size_t i=0; i < iparams.size(); i++) {
                int node_id = atoi(node_ids[i].c_str());
                CNode_Type ct{node_id, std::get<0>(iparams[i]), std::get<1>(iparams[i]), 1};
                node_params.emplace_back(ct);
            }
            Shard_Type st{shard_id, shard_name, "pgx", "pgx_pwd", 1, node_params};
            shard_params.emplace_back(st);
        }

        System::get_instance()->add_shard_cluster_memory(cluster_id_, cluster_name_,
                            nick_name_, ha_mode_, shard_params);
    }
  }
  
  writer.omitEndingLineFeed();
  memo = writer.write(memo_json);
  str_sql = "UPDATE cluster_general_job_log set status='" + job_status_ + "',memo='" + memo;
  str_sql += "',when_ended=current_timestamp(6) where id=" + job_id_;
  if(System::get_instance()->execute_metadate_opertation(SQLCOM_UPDATE, str_sql)) {
    KLOG_ERROR( "execute_metadate_opertation error");
    return;
  }
  KLOG_INFO("update cluster_general_job_log success: {}", str_sql);
  if(job_status_ == "done" || job_status_ == "failed") {
    //std::unique_lock<std::mutex> lock{ cond_mux_ };
    //cond_.notify_all();
    cond_.SignalAll();
  }
}

bool AddShardMission::PostAddShard() {
    MysqlResult result;
    std::string sql = "start transaction";
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("start transaction failed {}", g_node_channel_manager->getErr());
        return false;
    }

    std::vector<std::string> storage_hosts;
    std::vector<std::string> master_host_vec;
    for(auto shard : storage_iparams_) {
        std::vector<IComm_Param> iparams = shard.second;
        sql = string_sprintf("insert into %s.shards(name, num_nodes, db_cluster_id, sync_num) values('%s', %d, %d, %d)",
            KUNLUN_METADATA_DB_NAME, shard.first.c_str(), iparams.size(), cluster_id_, 1);
        ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
        if(ret) {
            KLOG_ERROR("insert shard failed {}", g_node_channel_manager->getErr());
            return false;
        }

        sql = "select last_insert_id() as id";
        ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
        if(ret) {
            KLOG_ERROR("select last insert id failed {}", g_node_channel_manager->getErr());
            return false;
        }

        std::string shard_id = result[0]["id"];
        KLOG_INFO("get insert shard id {}", shard_id);
        std::string member_state = "replica";
        std::string master_host;
        std::vector<std::string> node_ids;
        std::vector<std::string> slave_hosts;
        std::vector<std::string> hosts;
        for(size_t i=0; i<iparams.size(); i++) {
            if(i == 0) {
                if(ha_mode_ == "rbr") 
                    member_state = "source";
                master_host = std::get<0>(iparams[i])+"_"+std::to_string(std::get<1>(iparams[i]));
                hosts.push_back(master_host);
                storage_hosts.emplace_back(std::get<0>(iparams[i])+":"+std::to_string(std::get<1>(iparams[i])+1));
            } else { 
                std::string host = std::get<0>(iparams[i]) +"_"+std::to_string(std::get<1>(iparams[i]));
                slave_hosts.push_back(host);
                hosts.push_back(host);
                storage_hosts.emplace_back(std::get<0>(iparams[i]) +":"+std::to_string(std::get<1>(iparams[i])+1));
                member_state = "replica";
            }
            sql = string_sprintf("insert into %s.shard_nodes(hostaddr, port, user_name, passwd, shard_id, db_cluster_id, svr_node_id, member_state) values('%s', %d, 'pgx', 'pgx_pwd', %s, %d, 1, '%s')",
                    KUNLUN_METADATA_DB_NAME, std::get<0>(iparams[i]).c_str(), std::get<1>(iparams[i]), 
                    shard_id.c_str(), cluster_id_, member_state.c_str());
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

        master_host_vec.push_back(master_host);
        shard_map_nids_[shard.first] = node_ids;
        shard_map_sids_[shard.first] = shard_id;

        if(ha_mode_ == "rbr") {
            std::string uuid = GetMysqlUuidByHost(shard.first, cluster_name_, master_host);
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
        } else {
          GetMysqlUuidByHost(shard.first, cluster_name_, master_host);
        }
    }

    sql = "commit";
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("commit failed {}", g_node_channel_manager->getErr());
        return false;
    }

    std::vector<std::string> sql_stmts;
    AddShardToComputerNode(cluster_id_, computer_user_, computer_pwd_, storage_iparams_, 
                    shard_map_nids_, shard_map_sids_, sql_stmts);
                    
    //if(!WriteSqlToDdlLog(cluster_name_, sql_stmts)) {
    //  KLOG_ERROR("write sql into meta ddl log failed");
    //  return false;
    //}

    ret = g_prometheus_manager->AddStorageConf(storage_hosts);
    if(ret) {
        KLOG_ERROR("add prometheus mysqld_exporter config failed");
        //return false;
    }

    UpdateShardTopologyAndBackup();
    for(auto bk_ip : master_host_vec) {
      KColdBackUpMission * mission = new KColdBackUpMission(bk_ip.c_str());
      bool ret = mission->InitFromInternal();
      if (!ret){
        KLOG_ERROR("init coldbackupMisson faild: {}",mission->getErr());
        delete mission;
        continue;
      }
      KLOG_INFO("init coldbackupMisson successfully for host {}", bk_ip);
      //dispatch current mission
      g_request_handle_thread->DispatchRequest(mission);
    }
    return true;
}

void AddShardMission::UpdateShardTopologyAndBackup() {
  std::string sql = string_sprintf("select id from %s.shards where db_cluster_id=%d", 
        KUNLUN_METADATA_DB_NAME, cluster_id_);
  MysqlResult result;
  bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  if(ret) {
    KLOG_ERROR("get cluster_id {} shard ids from shards", cluster_id_);
    return;
  }

  std::vector<int> shard_ids;
  int nrows = result.GetResultLinesNum();
  for(int i=0; i<nrows; i++) {
    int id = atoi(result[i]["id"]);
    shard_ids.emplace_back(id);
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
}

void AddShardMission::RollbackStorageJob() {
    ObjectPtr<KDelShardMission>mission(new KDelShardMission());
    for(auto it : storage_iparams_) {
        std::vector<IComm_Param> s_iparams = it.second;
        for(size_t i=0; i<s_iparams.size(); i++) {
        struct InstanceInfoSt st1 ;
        st1.ip = std::get<0>(s_iparams[i]);
        st1.port = std::get<1>(s_iparams[i]);
        st1.exporter_port = std::get<1>(s_iparams[i]) + 1;
        st1.mgr_port = std::get<1>(s_iparams[i]) + 2;
        st1.xport = std::get<1>(s_iparams[i]) + 3;
        st1.innodb_buffer_size_M = innodb_size_;
        if(i == 0)
            st1.role = "master";
        else 
            st1.role = "slave";

        mission->setInstallInfoOneByOne(st1);
        }
    }
    bool ret = mission->InitFromInternal();
    if(!ret){
        KLOG_ERROR("Rollback to add shard mission failed {}", mission->getErr());
        return;
    }
    
    ObjectPtr<ClusterRequest> base_request(dynamic_cast<ClusterRequest*>(mission.GetTRaw()));
    g_request_handle_thread->DispatchRequest(base_request);
}

bool AddShardMission::TearDownMission() {
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
    SetSerializeResult(result);
    //delete this;
    return true; 
}

void AddShardMission::SetInstallErrorInfo(const std::string& error_info) {
  error_info_ = error_info;
}

void AddShardMission::UpdateShardInstallState(const std::string& shard_name, JobType jtype) {
    KlWrapGuard<KlWrapMutex> guard(update_mux_);
    if(shard_map_state_.find(shard_name) == shard_map_state_.end())
        return;

    if(jtype == S_DONE)
        shard_map_state_[shard_name] = 1;
    if(jtype == S_FAILED)
        shard_map_state_[shard_name] = 2;

    int done_num = 0, failed_num = 0;
    for(auto &it : shard_map_state_) {
        if(it.second == 1) {
            done_num++;
        }

        if(it.second == 2) {
            failed_num++;
            done_num++;
        }
    }

    if(done_num >= (int)shard_map_state_.size()) {
        if(failed_num > 0)
            UpdateMetaAndOperationStat(S_FAILED);
        else
            UpdateMetaAndOperationStat(S_DONE);
    }

}

void AddShardMission::AddShardCallCB(Json::Value& root, void *arg) {
    ShardJobParam *jparam = static_cast<ShardJobParam*>(arg);
    Json::FastWriter writer;
    std::string result = writer.write(root);
    KLOG_INFO("AddShardCallCB result {}", result);
    
    std::string shard_name = jparam->GetShardName();
    std::string status = root["status"].asString();
    if(status == "failed") {
      std::string errinfo;
      Json::Value json_res = root["result"];
        for(unsigned int k=0; k< json_res.size(); k++) { 
        if(json_res[k].isMember("response_array")) {
          Json::Value resp_arr = json_res[k]["response_array"];
          for(unsigned int i=0; i<resp_arr.size(); i++) {
            std::string status = resp_arr[i]["info"]["status"].asString();
            if(status == "failed") {
              errinfo += "host:"+resp_arr[i]["info"]["Extra"].asString()+" -- info:"+
                    resp_arr[i]["info"]["info"].asString()+"|";
            }
          }
        }
      }

      if(errinfo.empty())
        errinfo = root["error_info"].asString();

      jparam->GetJobMission()->SetInstallErrorInfo(errinfo);
      jparam->GetJobMission()->UpdateShardInstallState(shard_name, S_FAILED);
      delete jparam;
      return;
    }

    jparam->GetJobMission()->UpdateShardInstallState(shard_name, S_DONE);
    delete jparam;
}

bool AddShardMission::UpdateOperationRecord() {
    std::string str_sql,memo;
    Json::Value memo_json;
    Json::FastWriter writer;

    memo_json["error_code"] = err_code_;
    memo_json["error_info"] = GlobalErrorNum(err_code_).get_err_num_str();
    memo_json["rollback_flag"] = std::to_string(init_flag_);
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
        cond_.SignalAll();
    }
    return true;
}

}