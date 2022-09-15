/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "add_computer.h"
#include "cluster_operator/computeInstallMission.h"
#include "request_framework/handleRequestThread.h"
#include "zettalib/op_log.h"
#include "kl_mentain/shard.h"
#include "kl_mentain/sys.h"
#include "zettalib/op_pg.h"
#include "cluster_collection/prometheus_control.h"


extern HandleRequestThread *g_request_handle_thread;
extern std::string meta_svr_user;
extern std::string meta_svr_pwd;
extern std::string meta_group_seeds;
extern kunlun::KPrometheusControl* g_prometheus_manager;

namespace kunlun
{

bool AddComputerMission::ArrangeRemoteTask() {
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

        std::string computer_state = memo_json["computer_state"].asString(); 
        int init_flag = atoi(memo_json["rollback_flag"].asCString());
        if(init_flag) {
            KLOG_INFO("add computer called by other, so quit to restart");
            return true;
        }

        if(computer_state != "prepare") {
            std::string hosts = memo_json["computer_hosts"].asString();
            std::vector<std::string> host_vec = StringTokenize(hosts, ",");
            for(size_t i=0; i<host_vec.size(); i++) {
                std::string ip = host_vec[i].substr(0, host_vec[i].rfind("_"));
                int port = atoi(host_vec[i].substr(host_vec[i].rfind("_")+1).c_str());
                IComm_Param iparam{ip, port};
                computer_iparams_.emplace_back(iparam);
            }
        }

        if(memo_json.isMember("cluster_name"))
            cluster_name_ = memo_json["cluster_name"].asString();
        if(memo_json.isMember("cluster_id"))
            cluster_id_ = atoi(memo_json["cluster_id"].asCString());
        if(memo_json.isMember("nick_name"))
            nick_name_ = memo_json["nick_name"].asString();
        if(memo_json.isMember("computer_user"))
            computer_user_ = memo_json["computer_user"].asString();
        if(memo_json.isMember("computer_pwd"))
            computer_pwd_ = memo_json["computer_pwd"].asString();
        if(computer_state == "done") {
            bool ret = PostAddComputer();
            if(ret) {
                job_status_ = "done";
                std::vector<Comps_Type> comps_params;
                for(auto ci : computer_iparams_) {
                    Comps_Type ct {std::get<0>(ci), std::get<1>(ci), computer_user_, computer_pwd_};
                    comps_params.emplace_back(ct);
                }
                System::get_instance()->add_computer_cluster_memory(cluster_id_, cluster_name_, 
                                nick_name_, comps_params);
            } else {
                job_status_ = "failed";
                err_code_ = ADD_COMPUTER_DEAL_POST_ERROR;
                RollbackComputerJob();
            }

            //std::unique_lock<std::mutex> lock{ cond_mux_ };
            //cond_.notify_all();
            cond_.SignalAll();
            UpdateOperationRecord();
            return ret;
        } else {
            RollbackComputerJob();
        }
        
        err_code_ = CLUSTER_MGR_RESTART_ERROR;
        job_status_ = "failed";
        UpdateOperationRecord();
    }
    return true;
} 

bool AddComputerMission::SetUpMisson() {
    job_id_ = get_request_unique_id();
    if(get_init_by_recover_flag() == true) {
        KLOG_INFO("Add computer ignore recover state in setup phase");
        return true;
    }

    KLOG_INFO("Add computer setup phase");
    
    this->set_default_del_request(false);
    ClusterRequestTypes request_type = get_request_type();
    bool ret = true;
    int i = 0;
    Json::Value paras, computer_iplists;
    if(init_flag_) {
        KLOG_INFO("Add computer called by internal job");
        goto end;
    }

    if (!super::get_body_json_document().isMember("paras")) {
        err_code_ = ADD_COMPUTER_QUEST_LOST_PARAS_ERROR;
        KLOG_ERROR("missing `paras` key-value pair in the request body");
        goto end;
    }
    paras = super::get_body_json_document()["paras"];

    if (!paras.isMember("cluster_id")) {
      err_code_ = ADD_COMPUTER_QUEST_MISS_CLUSTERID_ERROR;
      KLOG_ERROR("missing `shards` key-value pair in the request body");
      goto end;
    }

    if(!CheckStringIsDigit(paras["cluster_id"].asString())) {
      err_code_ = ADD_COMPUTER_QUEST_MISS_CLUSTERID_ERROR;
      KLOG_ERROR("missing `shards` key-value pair in the request body");
      goto end;
    }
    cluster_id_ = atoi(paras["cluster_id"].asCString());

    if (!paras.isMember("comps")) {
      err_code_ = ADD_COMPUTER_QUEST_MISS_COMPS_ERROR;
      KLOG_ERROR("missing `comps` key-value pair in the request body");
      goto end;
    }

    if(!CheckStringIsDigit(paras["comps"].asString())) {
      err_code_ = ADD_COMPUTER_QUEST_MISS_COMPS_ERROR;
      KLOG_ERROR("missing `comps` key-value pair in the request body");
      goto end;
    }
    comps_ = atoi(paras["comps"].asCString());
    if( comps_ < 1 || comps_ > 256) {
      err_code_ = ADD_COMPUTER_QUEST_COMPS_PARAM_ERROR ;
      KLOG_ERROR("comps error(must in 1-256)");
      goto end;
    }

    if(paras.isMember("computer_iplists")) {
      computer_iplists = paras["computer_iplists"];
      for(i=0; i<computer_iplists.size();i++) {
        computer_iplists_.push_back(computer_iplists[i].asString());
      }
      if(!CheckMachineIplists(M_COMPUTER, computer_iplists_)) {
        err_code_ = ADD_COMPUTER_ASSIGN_COMPUTER_IPLISTS_ERROR;
        goto end;
      }
    }

    GetInstallCompsParamsByClusterId();

end:
    if(err_code_) {
      ret = false;
      job_status_ = "failed";
      UpdateOperationRecord();
    } else
      job_status_ = "ongoing";

    return ret;
}

bool AddComputerMission::GetInstallCompsParamsByClusterId() {
    std::vector<std::string> key_name;
    key_name.push_back("name");
    key_name.push_back("memo.ha_mode");
    key_name.push_back("memo.computer_user");
    key_name.push_back("memo.computer_passwd");
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
    return true;
}

void AddComputerMission::GetExistMaxCompId() {
    KlWrapGuard<KlWrapMutex> guard(cid_mux_);

    MysqlResult result;
    std::string sql = string_sprintf("select max(id) from %s.comp_nodes_id_seq",
                    KUNLUN_METADATA_DB_NAME);
    int ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("get max id from comp_nodes_id_seq record failed {}", g_node_channel_manager->getErr());
        job_status_ = "failed";
        err_code_ = ADD_COMPUTER_GET_EXIST_COMPS_FROM_METADB_ERROR;
        UpdateOperationRecord();
        return;
    }

    int nrows = result.GetResultLinesNum();
    if( nrows != 1) {
        if(nrows != 0) {
            job_status_ = "failed";
            err_code_ = ADD_COMPUTER_GET_EXIST_COMPS_FROM_METADB_ERROR;
            UpdateOperationRecord();
            return;
        }
    } else
        exist_comps_ = atoi(result[0]["max(id)"]);

    sql = string_sprintf("insert into %s.comp_nodes_id_seq(id) values(%d)",
                KUNLUN_METADATA_DB_NAME, exist_comps_ + comps_);
    KLOG_INFO("insert comp_nodes_id_seq sql: {}", sql);
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("insert cluster_name {} comp_nodes_id_seq into db_clusters failed {}", cluster_name_, g_node_channel_manager->getErr());
        job_status_ = "failed";
        err_code_ = ADD_COMPUTER_GET_EXIST_COMPS_FROM_METADB_ERROR;
        UpdateOperationRecord();
        return;
    }
}

void AddComputerMission::DealLocal() {
    KLOG_INFO("add computer deal local phase");
    if(job_status_ == "failed" || job_status_ == "done") {
        return;
    }

    GetExistMaxCompId();

    if(!SeekComputerIplists()) {
        job_status_ = "failed";
        UpdateOperationRecord();
        return;
    }
    UpdateMetaAndOperationStat(C_PREPARE);

    bool ret = AddComputerJobs();
    if(!ret) {
        job_status_ = "failed";
        UpdateMetaAndOperationStat(C_FAILED);
        return;
    }
    UpdateMetaAndOperationStat(C_DISPATCH);
}

bool AddComputerMission::InitFromInternal() {
    Json::Value doc;
    FillCommonJobJsonDoc(doc);
    doc["job_type"] = "internal_add_comps";

    Json::Value para;
    para["comps"] = std::to_string(comps_);
    para["cluster_id"] = std::to_string(cluster_id_);
    para["cluster_name"] = cluster_name_;
    para["computer_user"] = computer_user_;
    para["computer_passwd"] = computer_pwd_;

    for(size_t i=0; i<computer_iplists_.size(); i++) {
        para["computer_iplists"].append(computer_iplists_[i]);
    }
    doc["paras"] = para;
    set_body_json_document(doc);
    CompleteGeneralJobInfo();
    return true;
}

void AddComputerMission::CompleteGeneralJobInfo() {
    Json::FastWriter writer;
    writer.omitEndingLineFeed();
    std::string job_info = writer.write(get_body_json_document());

    kunlun::MysqlResult result;
    std::string sql = string_sprintf("update %s.cluster_general_job_log set "
                "job_type='internal_add_comps',job_info='%s' where id = %s",
                KUNLUN_METADATA_DB_NAME ,job_info.c_str(), get_request_unique_id().c_str());
    
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if (ret) {
        KLOG_ERROR("update cluster_general_job_log faild: {}", g_node_channel_manager->getErr());
        return;
    }
    KLOG_INFO("update cluster_general_job_log success: {}", sql);
}

bool AddComputerMission::SeekComputerIplists() {
  bool ret = true;
  if(computer_iplists_.size() > 0) {
    ret = AssignComputerIplists();
  } else
    ret = FetchComputerIplists();

  return ret;
}

bool AddComputerMission::AssignComputerIplists() {
  for(size_t i=0; i<computer_iplists_.size(); i++) {
    std::string hostaddr = computer_iplists_[i];
    if(!GetComputerInstallHost(hostaddr, avail_hosts_, host_ports_, err_code_)) 
      return false;
  }
  
  if(!GetBestInstallHosts(avail_hosts_, comps_, computer_iparams_, host_ports_))
    return false;

  if((int)computer_iparams_.size() < comps_) {
    err_code_ = ADD_COMPUTER_GET_COMPUTER_MATCH_COMPS_ERROR;
    KLOG_ERROR("get computer node is not match comps {}", comps_);
    return false;
  }
  return true;
}

bool AddComputerMission::FetchComputerIplists() {
  std::string sql = string_sprintf("select hostaddr from %s.server_nodes where machine_type='computer' and node_stats='running'",
                KUNLUN_METADATA_DB_NAME);
  
  MysqlResult result;
  bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  if(ret) {
    err_code_ = ADD_COMPUTER_HOSTADDR_SERVER_NODES_SQL_ERROR;
    KLOG_ERROR("get computer hostaddr from server nodes failed {}", g_node_channel_manager->getErr());
    return false;
  }

  int nrows = result.GetResultLinesNum();
  for(int i=0; i<nrows; i++) {
    std::string hostaddr = result[i]["hostaddr"];
    if(!GetComputerInstallHost(hostaddr, avail_hosts_, host_ports_, err_code_)) 
      return false;
  }

  if(!GetBestInstallHosts(avail_hosts_, comps_, computer_iparams_, host_ports_))
    return false;

  if((int)computer_iparams_.size() < comps_) {
    err_code_ = ADD_COMPUTER_GET_COMPUTER_MATCH_COMPS_ERROR;
    KLOG_ERROR("get computer node is not match comps {}", comps_);
    return false;
  }
  
  return true;
}

void AddComputerMission::SetInstallErrorInfo(const std::string& error_info) {
    error_info_ = error_info;
}

void AddComputerMission::AddComputerCallCB(Json::Value& root, void *arg) {
    AddComputerMission *mission = static_cast<AddComputerMission*>(arg);
    Json::FastWriter writer;
    std::string result = writer.write(root);
    KLOG_INFO("AddComputerCallCB result {}", result);

    std::string status = root["status"].asString();
    if(status == "failed") {
        std::string errinfo;
        Json::Value json_res = root["result"];
        for(unsigned int k=0; k<json_res.size(); k++) {
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

        mission->SetInstallErrorInfo(errinfo);
        mission->UpdateMetaAndOperationStat(C_FAILED);
        //mission->RollbackComputerJob();
        return;
    }

    mission->UpdateMetaAndOperationStat(C_DONE);
}

bool AddComputerMission::AddComputerJobs() {
  ObjectPtr<KComputeInstallMission> mission(new KComputeInstallMission());
  mission->set_cb(AddComputerMission::AddComputerCallCB);
  mission->set_cb_context(this);

  for(size_t i=0; i< computer_iparams_.size(); i++) {
    struct ComputeInstanceInfoSt st2;
    st2.ip = std::get<0>(computer_iparams_[i]);
    st2.pg_port = std::get<1>(computer_iparams_[i]);
    st2.mysql_port = std::get<1>(computer_iparams_[i]) + 1;
    st2.exporter_port = std::get<1>(computer_iparams_[i]) + 2;
    st2.init_user = computer_user_;
    st2.init_pwd = computer_pwd_;
    st2.compute_id = i+1+exist_comps_;

    mission->setInstallInfoOneByOne(st2);
  }
    
  bool ret = mission->InitFromInternal();
  if(!ret){
    KLOG_ERROR("Add computer mission failed {}",mission->getErr());
    err_code_ = ADD_COMPUTER_DISPATCH_MISSION_ERROR;
    return false;
  }
  ObjectPtr<ClusterRequest> base_request(dynamic_cast<ClusterRequest*>(mission.GetTRaw()));
  g_request_handle_thread->DispatchRequest(base_request);
  //g_request_handle_thread->DispatchRequest(mission);
  computer_id_ = mission->get_request_unique_id();
  return true;
}

void AddComputerMission::UpdateMetaAndOperationStat(JobType jtype) {
    std::string str_sql,memo;
    Json::Value memo_json;
    Json::FastWriter writer;
    memo_json["error_code"] = err_code_;
    memo_json["error_info"] = GlobalErrorNum(err_code_).get_err_num_str();
    memo_json["rollback_flag"] = std::to_string(init_flag_);

    if(jtype == C_PREPARE) {
      computer_state_ = "prepare";
  } else if(jtype == C_DISPATCH) {
      computer_state_ = "dipatch";

    std::map<std::string, std::vector<int> > install_computer_ports;
    PackInstallPorts(install_computer_ports);
    if(!UpdateInstallingPort(K_APPEND, M_COMPUTER, install_computer_ports))
      return;
  } else if(jtype == C_FAILED) {
    std::map<std::string, std::vector<int> > install_computer_ports;
    PackInstallPorts(install_computer_ports);

    if(jtype == C_FAILED) {
      computer_state_ = "failed";
      job_status_ = "failed";
      err_code_ = ADD_COMPUTER_INSTALL_COMPUTER_ERROR;
      memo_json["error_code"] = ADD_COMPUTER_INSTALL_COMPUTER_ERROR;
      memo_json["error_info"] = error_info_;
    }
    
    if(!UpdateInstallingPort(K_CLEAR, M_COMPUTER, install_computer_ports))
      return;
      
  } else if(jtype == C_DONE) {
    computer_state_ = "done";
    std::map<std::string, std::vector<int> > install_computer_ports;
    PackInstallPorts(install_computer_ports);
    if(!UpdateInstallingPort(K_CLEAR, M_COMPUTER, install_computer_ports))
        return;
    
    if(!UpdateUsedPort(K_APPEND, M_COMPUTER, install_computer_ports))
        return;
  }

  if(!computer_state_.empty())
    memo_json["computer_state"] = computer_state_;

  if(!computer_id_.empty())
    memo_json["computer_id"] = computer_id_;
  
  if(!cluster_name_.empty())
    memo_json["cluster_name"] = cluster_name_;
  memo_json["cluster_id"] = std::to_string(cluster_id_);
  if(!nick_name_.empty())
    memo_json["nick_name"] = nick_name_;
  if(!computer_user_.empty())
    memo_json["computer_user"] = computer_user_;
  if(!computer_pwd_.empty())
    memo_json["computer_pwd"] = computer_pwd_;

  if(computer_iparams_.size() > 0) {
    std::string hosts;
    for(size_t i=0; i<computer_iparams_.size(); i++) {
      hosts += std::get<0>(computer_iparams_[i]) + "_" + std::to_string(std::get<1>(computer_iparams_[i])) + ";";
    }
    memo_json["computer_hosts"] = hosts;
  }
  
  if(computer_state_ == "done") {
    job_status_ = "done";
    if(!PostAddComputer()) {
      job_status_ = "failed";
      err_code_ = ADD_COMPUTER_DEAL_POST_ERROR;
      memo_json["error_code"] = err_code_;
      memo_json["error_info"] = GlobalErrorNum(err_code_).get_err_num_str();
      std::map<std::string, std::vector<int> > install_computer_ports;
      PackInstallPorts(install_computer_ports);
      UpdateUsedPort(K_CLEAR, M_COMPUTER, install_computer_ports);
      RollbackComputerJob();
    } else {
        std::vector<Comps_Type> comps_params;
        for(auto ci : computer_iparams_) {
            Comps_Type ct {std::get<0>(ci), std::get<1>(ci), computer_user_, computer_pwd_};
            comps_params.emplace_back(ct);
        }
        System::get_instance()->add_computer_cluster_memory(cluster_id_, cluster_name_, 
                        nick_name_, comps_params);
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

void AddComputerMission::PackInstallPorts(std::map<std::string, std::vector<int> >& host_ports) {
    for(size_t i=0; i<computer_iparams_.size(); i++) {
        std::string hostaddr = std::get<0>(computer_iparams_[i]);
        std::vector<int> ports;
        if(host_ports.find(hostaddr) != host_ports.end()) {
            ports = host_ports[hostaddr];
        }
        ports.push_back(std::get<1>(computer_iparams_[i]));
        host_ports[hostaddr] = ports;
    }
    return;
}

void AddComputerMission::RollbackComputerJob() {
    ObjectPtr<KDelComputeMission>mission(new KDelComputeMission());
    for(size_t i=0; i< computer_iparams_.size(); i++) {
        struct ComputeInstanceInfoSt st2;
        st2.ip = std::get<0>(computer_iparams_[i]);
        st2.pg_port = std::get<1>(computer_iparams_[i]);
        st2.mysql_port = std::get<1>(computer_iparams_[i]) + 1;
        st2.exporter_port = std::get<1>(computer_iparams_[i]) + 2;
        st2.init_user = computer_user_;
        st2.init_pwd = computer_pwd_;
        st2.compute_id = i+1+exist_comps_;

        mission->setInstallInfoOneByOne(st2);
    }
        
    bool ret = mission->InitFromInternal();
    if(!ret){
        KLOG_ERROR("rollback add computer mission failed {}",mission->getErr());
        return;
    }
    ObjectPtr<ClusterRequest> base_request(dynamic_cast<ClusterRequest*>(mission.GetTRaw()));
    g_request_handle_thread->DispatchRequest(base_request);
    //g_request_handle_thread->DispatchRequest(mission);
}

bool AddComputerMission::UpdateOperationRecord() {
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
        //std::unique_lock<std::mutex> lock{ cond_mux_ };
        //cond_.notify_all();
        cond_.SignalAll();
    }
    return true;
}

bool AddComputerMission::PostAddComputer() {
    MysqlResult result;
    std::string sql = string_sprintf("select id, @@server_id as svrid from %s.db_clusters where name='%s'", 
              KUNLUN_METADATA_DB_NAME, cluster_name_.c_str());
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("get cluster_name {} from db_clusters failed {}", cluster_name_, g_node_channel_manager->getErr());
        return false;
    }
    if(result.GetResultLinesNum() != 1) {
        KLOG_ERROR("get cluster_name {} records too many", cluster_name_);
        return false;
    }
    std::string cluster_master_svrid = result[0]["svrid"];

    sql = "start transaction";
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("start transaction failed {}", g_node_channel_manager->getErr());
        return false;
    }

    std::vector<std::string> comp_hosts;
    for(size_t i=0; i<computer_iparams_.size(); i++) {
        std::string cp_name = "comp"+std::to_string(exist_comps_+i+1);
        sql = string_sprintf("insert into %s.comp_nodes(id, name, hostaddr, port, db_cluster_id, user_name, passwd, svr_node_id) values(%d, '%s', '%s', %d, %d, '%s', '%s', 1)",
                    KUNLUN_METADATA_DB_NAME, exist_comps_+i+1, cp_name.c_str(), std::get<0>(computer_iparams_[i]).c_str(), std::get<1>(computer_iparams_[i]),
                    cluster_id_, computer_user_.c_str(), computer_pwd_.c_str());
        KLOG_INFO("insert comp_nodes sql: {}", sql);
        ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
        if(ret) {
            KLOG_ERROR("insert cluster_name {} comp nodes into db_clusters failed {}", cluster_name_, g_node_channel_manager->getErr());
            return false;
        }

        sql = string_sprintf("alter table %s.commit_log_%s add partition(partition p%d values in (%d))",
                KUNLUN_METADATA_DB_NAME, cluster_name_.c_str(), exist_comps_+i+1, exist_comps_+i+1);
        KLOG_INFO("alter commit_log sql: {}", sql);
        ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
        if(ret) {
            if(!CheckTablePartitonExist(exist_comps_+i+1)) {
                KLOG_ERROR("alter commit_log for cluster_name {} failed {}", cluster_name_, g_node_channel_manager->getErr());
                return false;
            }
        }

        UpdateComputerCatalogTables(std::get<0>(computer_iparams_[i]), std::get<1>(computer_iparams_[i]),
                            cp_name, exist_comps_+i+1);
        comp_hosts.emplace_back(std::get<0>(computer_iparams_[i])+":"+std::to_string(std::get<1>(computer_iparams_[i])+2));
    }

    sql = "commit";
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("commit failed {}", g_node_channel_manager->getErr());
        return false;
    }

    ret = g_prometheus_manager->AddComputerConf(comp_hosts);
    if(ret) {
        KLOG_ERROR("add prometheus postgres_exporter config failed");
        //return false;
    }
    
    //check computer_node replay all ddl_log
    ComputerReplayDdlLogAndUpdateMeta();
    return true;
}

bool AddComputerMission::CheckMetaCompNodesStat(const std::string& hostaddr, int port) {
    std::string sql = string_sprintf("select status from %s.comp_nodes where hostaddr='%s' and port=%d",
                KUNLUN_METADATA_DB_NAME, hostaddr.c_str(), port);
    MysqlResult result;
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret || result.GetResultLinesNum() != 1) {
        KLOG_ERROR("select comp_nodes by hostaddr {}, port {} failed", hostaddr, port);
        return false;
    }
    std::string status = result[0]["status"];
    if(status == "active" || status == "inactive")
        return true;
    
    return false;
}

bool AddComputerMission::UpdateMetaCompNodeStat(const std::string& hostaddr, int port, bool is_ok) {
    std::string status = "inactive";
    if(is_ok)
        status = "active";
    MysqlResult result;
    std::string sql = string_sprintf("update %s.comp_nodes set status='%s' where hostaddr='%s' and port=%d",
                KUNLUN_METADATA_DB_NAME, status.c_str(), hostaddr.c_str(), port);
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("update comp_nodes status by hostaddr {}, port {} failed", hostaddr, port);
        return false;
    }
    return true;
}

/*
* return value
* 0 -- replay over
* 1 -- failed, can't connect pg
* 2 -- running
*/
int AddComputerMission::PgReplayDdlLogStat(const std::string& hostaddr, int port, int max_id) {
    PGConnectionOption option;
    option.ip = hostaddr;
    option.port_num = port;
    option.user = computer_user_;
    option.password = computer_pwd_;
    option.database = "postgres";

    PGConnection pg_conn(option);
    if(!pg_conn.Connect()) {
        KLOG_ERROR("connect pg failed {}", pg_conn.getErr());
        return 1;
    }

    PgResult presult;
    int ret = pg_conn.ExecuteQuery("select max(ddl_op_id) from pg_ddl_log_progress", &presult);
    if(ret == -1 || presult.GetNumRows() != 1) {
        KLOG_ERROR("select pg_ddl_log_progress failed {}", pg_conn.getErr());
        return 1;
    }

    int ddl_op_id = atoi(presult[0]["max"]);
    if(ddl_op_id >= max_id)
        return 0;
        
    return 2;
}

void AddComputerMission::ComputerReplayDdlLogAndUpdateMeta() {
    std::string sql = string_sprintf("select max(id) from %s.ddl_ops_log_%s", 
                KUNLUN_METADATA_DB_NAME, cluster_name_.c_str());
    MysqlResult result;    
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret || result.GetResultLinesNum() != 1) {
        KLOG_ERROR("get cluster {} ddl_ops_log max id failed", cluster_name_);
        return;
    }

    int max_id = atoi(result[0]["max(id)"]);

    std::map<std::string, int> retry_nums;
    int finish_num = 0;
    while(true) {
        for(size_t i=0; i<computer_iparams_.size(); i++) {
            std::string hostaddr = std::get<0>(computer_iparams_[i]);
            int port = std::get<1>(computer_iparams_[i]);
            if(CheckMetaCompNodesStat(hostaddr, port)) {
                finish_num++;
                continue;
            }

            int replay_ret = PgReplayDdlLogStat(hostaddr, port, max_id);
            if(replay_ret == 1) {
                std::string host = hostaddr+"_"+std::to_string(port);
                if(retry_nums.find(host) == retry_nums.end())
                    retry_nums[host] = 1;
                else {
                    int num = retry_nums[host];
                    if(num > 10) {
                        retry_nums.erase(host);
                        UpdateMetaCompNodeStat(hostaddr, port, false);
                    } else
                        retry_nums[host] = num+1;
                }
            } else if(replay_ret == 0) {
                std::string host = hostaddr+"_"+std::to_string(port);
                if(retry_nums.find(host) != retry_nums.end())
                    retry_nums.erase(host);
                
                UpdateMetaCompNodeStat(hostaddr, port, true);
            }
        }

        if(finish_num >= (int)computer_iparams_.size())
            break;

        sleep(2);
    }
}

bool AddComputerMission::CheckTablePartitonExist(int part_no) {
    MysqlResult result;
    std::string table_name = "commit_log_"+cluster_name_;
    std::string sql = string_sprintf("select distinct PARTITION_NAME, PARTITION_DESCRIPTION from information_schema.partitions where TABLE_SCHEMA='%s' and TABLE_NAME='%s'",
            KUNLUN_METADATA_DB_NAME, table_name.c_str());
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("get cluster_name {} commit_log partition failed", cluster_name_);
        return false;
    }

    std::string s_desc = std::to_string(part_no);
    std::string t_name = "p"+s_desc;
    KLOG_INFO("check comit_log {} partition_name: {} and partition_description: {}", table_name, t_name, s_desc);
    
    int nrows = result.GetResultLinesNum();
    for(int i=0; i<nrows; i++) {
        std::string name = result[i]["PARTITION_NAME"];
        std::string desc = result[i]["PARTITION_DESCRIPTION"];
        KLOG_INFO("get commit_log {} partition_name: {} and partition_description: {} from information_schema.partitions", table_name, name, desc);
        if((name == t_name) && (desc == s_desc))
            return true;
    }
    return false;
}

bool AddComputerMission::UpdateComputerCatalogTables(const std::string ip, int port, 
                    const std::string& cp_name, int pos) {
    ObjectPtr<MetadataShard> metashard = System::get_instance()->get_MetadataShard();
    
    PGConnectionOption option;
    option.ip = ip;
    option.port_num = port;
    option.user = computer_user_;
    option.password = computer_pwd_;
    option.database = "postgres";

    PGConnection pg_conn(option);
    if(!pg_conn.Connect()) {
        KLOG_ERROR("connect pg failed {}", pg_conn.getErr());
        return false;
    }

    std::string sql = "set skip_tidsync=true";
    PgResult pgresult;
    if(pg_conn.ExecuteQuery(sql.c_str(), &pgresult) == -1) {
        KLOG_ERROR("postgres set skip_tidsync failed {}", pg_conn.getErr());
        return false;
    }

    sql = "start transaction";
    if(pg_conn.ExecuteQuery(sql.c_str(), &pgresult) == -1) {
        KLOG_ERROR("postgres start transaction failed {}", pg_conn.getErr());
        return false;
    }

    Shard::HAVL_mode havl_mode;
    if(ha_mode_ == "mgr")
        havl_mode = Shard::HAVL_mode::HA_mgr;
    else if(ha_mode_ == "rbr")
        havl_mode = Shard::HAVL_mode::HA_rbr;
    else
        havl_mode = Shard::HAVL_mode::HA_no_rep;

    sql = string_sprintf("insert into pg_cluster_meta(comp_node_id, cluster_id, cluster_master_id, ha_mode, cluster_name, comp_node_name, meta_ha_mode) values(%d, %d, 0, %d, '%s', '%s', %d)",
                    pos, cluster_id_, havl_mode, cluster_name_.c_str(), cp_name.c_str(), metashard->get_mode());
    KLOG_INFO("insert pg_cluster_meta sql: {}", sql);
    if(pg_conn.ExecuteQuery(sql.c_str(), &pgresult) == -1) {
        KLOG_ERROR("postgres insert pg_cluster_meta failed {}", pg_conn.getErr());
        return false;
    }

    for(auto it : metashard->get_nodes()) {
        std::string ip;
        int port;
        it->get_ip_port(ip, port);
        bool is_master = it->is_master();
        sql = string_sprintf("insert into pg_cluster_meta_nodes(server_id, cluster_id, is_master, port, user_name, hostaddr, passwd) values (%d, %d, '%d', %d, 'pgx', '%s', 'pgx_pwd')",
            it->get_id(), cluster_id_, is_master, port, ip.c_str());
        KLOG_INFO("insert pg_cluster_meta_nodes sql: {}", sql);
        if(pg_conn.ExecuteQuery(sql.c_str(), &pgresult) == -1) {
            KLOG_ERROR("insert pg_cluster_meta_nodes failed {}", pg_conn.getErr());
            return false;
        }
    }

    sql = "select id from pg_shard";
    if(pg_conn.ExecuteQuery(sql.c_str(), &pgresult) == -1) {
        KLOG_ERROR("get id from pg_shard_node failed {}", pg_conn.getErr());
        return false;
    }

    std::map<std::string, int> shard_nrows;
    std::vector<std::string> shardids;
    uint32_t nrows = pgresult.GetNumRows();
    for(uint32_t i=0; i<nrows; i++) {
        std::string id = pgresult[i]["id"];
        shardids.emplace_back(id);
    }

    MysqlResult result;
    sql = string_sprintf("select * from %s.shards where db_cluster_id=%d", 
                KUNLUN_METADATA_DB_NAME, cluster_id_);
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("get cluster_id {} shards from metadb failed {}", cluster_id_, g_node_channel_manager->getErr());
        return false;
    }

    nrows = result.GetResultLinesNum();
    for(uint32_t i=0; i<nrows; i++) {
        std::string id = result[i]["id"];
        if(std::find(shardids.begin(), shardids.end(), id) != shardids.end())
            continue;

        shard_nrows[id] = 0;
        sql = string_sprintf("insert into pg_shard(name, id, master_node_id, num_nodes, space_volumn, num_tablets, db_cluster_id, when_created) values('%s', %s, 0, 0, %s, %s, %s, '%s')",
                result[i]["name"], id.c_str(), result[i]["space_volumn"], result[i]["num_tablets"], result[i]["db_cluster_id"], result[i]["when_created"]);
        KLOG_INFO("insert pg_shard sql: {}", sql);
        if(pg_conn.ExecuteQuery(sql.c_str(), &pgresult) == -1) {
            KLOG_ERROR("postgres insert pg_shard failed {}", pg_conn.getErr());
            return false;
        }
    }

    sql = "select id from pg_shard_node";
    if(pg_conn.ExecuteQuery(sql.c_str(), &pgresult) == -1) {
        KLOG_ERROR("get id from pg_shard_node failed {}", pg_conn.getErr());
        return false;
    }

    std::vector<std::string> shardnodeids;
    nrows = pgresult.GetNumRows();
    for(uint32_t i=0; i<nrows; i++) {
        std::string id = pgresult[i]["id"];
        shardnodeids.emplace_back(id);
    }

    sql = string_sprintf("select * from %s.shard_nodes where db_cluster_id=%d", 
                KUNLUN_METADATA_DB_NAME, cluster_id_);
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("get cluster_id {} shard_nodes from metadb failed {}", cluster_id_, g_node_channel_manager->getErr());
        return false;
    }

    nrows = result.GetResultLinesNum();
    for(uint32_t i=0; i<nrows; i++) {
        std::string id = result[i]["id"];
        std::string shard_id = result[i]["shard_id"];
        if(std::find(shardnodeids.begin(), shardnodeids.end(), id) != shardnodeids.end())
            continue;

        if(shard_nrows.find(shard_id) != shard_nrows.end())
            shard_nrows[shard_id] += 1;
        else
            shard_nrows[shard_id] = 1;

        sql = string_sprintf("insert into pg_shard_node (id, port, shard_id, svr_node_id, ro_weight, ping, latency, user_name, hostaddr, passwd, when_created) values(%s, %s, %s, 0, 0, 0, 0, '%s', '%s', '%s', '%s')",
                id.c_str(), result[i]["port"], shard_id.c_str(), result[i]["user_name"], result[i]["hostaddr"], 
                result[i]["passwd"], result[i]["when_created"]);
        KLOG_INFO("insert pg_shard_node sql: {}", sql);
        if(pg_conn.ExecuteQuery(sql.c_str(), &pgresult) == -1) {
            KLOG_ERROR("postgres insert pg_shard_node failed {}", pg_conn.getErr());
            return false;
        }

        sql = string_sprintf("update pg_shard set master_node_id=%s where master_node_id=0 and id=%s",
            id.c_str(), shard_id.c_str());
        KLOG_INFO("update pg_shard sql: {}", sql);
        if(pg_conn.ExecuteQuery(sql.c_str(), &pgresult) == -1) {
            KLOG_ERROR("postgres update pg_shard master_node failed {}", pg_conn.getErr());
            return false;
        }
    }

    for(auto &it : shard_nrows) {
        sql = string_sprintf("update pg_shard set num_nodes=%d where id=%s",
                it.second, it.first.c_str());
        KLOG_INFO("update pg_shard sql: {}", sql);
        if(pg_conn.ExecuteQuery(sql.c_str(), &pgresult) == -1) {
            KLOG_ERROR("postgres update pg_shard num_nodes failed {}", pg_conn.getErr());
            return false;
        }
    }

    sql = string_sprintf("update pg_cluster_meta set cluster_master_id=%d where cluster_name='%s'",
            metashard->get_master()->get_id(), cluster_name_.c_str());
    KLOG_INFO("update pg_cluster_meta sql: {}", sql);
    if(pg_conn.ExecuteQuery(sql.c_str(), &pgresult) == -1) {
        KLOG_ERROR("postgres update pg_cluster_meta failed {}", pg_conn.getErr());
        return false;
    }

    sql = "select oid from pg_database where datname='postgres'";
    if(pg_conn.ExecuteQuery(sql.c_str(), &pgresult) == -1) {
        KLOG_ERROR("postgres get pg_database failed {}", pg_conn.getErr());
        return false;
    }
    
    nrows = pgresult.GetNumRows();
    if( nrows != 1) {
        KLOG_ERROR("get postgres from pg_database too many, {}", nrows);
    } else {
        sql = string_sprintf("insert into pg_ddl_log_progress values(%s, 0, 0)", pgresult[0]["oid"]);
        KLOG_INFO("insert pg_ddl_log_progress sql: {}", sql);
        if(pg_conn.ExecuteQuery(sql.c_str(), &pgresult) == -1) {
            KLOG_ERROR("postgres insert pg_ddl_log_progress failed {}", pg_conn.getErr());
            return false;
        }
    }
    sql = "commit";
    if(pg_conn.ExecuteQuery(sql.c_str(), &pgresult) == -1) {
        KLOG_ERROR("postgres commit failed {}", pg_conn.getErr());
        return false;
    }
    return true;
}

bool AddComputerMission::TearDownMission() {
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

}