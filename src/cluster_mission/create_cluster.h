/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef _CLUSTER_MGR_CREATE_CLUSTER_H_
#define _CLUSTER_MGR_CREATE_CLUSTER_H_
#include "http_server/node_channel.h"
#include "request_framework/missionRequest.h"
#include "cluster_comm.h"
#include <unordered_map>
//#include <condition_variable>
//#include <mutex>

namespace kunlun
{

class CreateClusterMission : public MissionRequest {
    typedef MissionRequest super;
public:
    explicit CreateClusterMission(Json::Value *doc) : super(doc),
          err_code_(0), init_flag_(false), cluster_id_(-2), dbcfg_(0), computer_user_("abc"), 
          computer_pwd_("abc"), storage_dstate_(""), computer_dstate_("") {}
    
    virtual ~CreateClusterMission() {}

    virtual bool ArrangeRemoteTask() override final;
    virtual bool SetUpMisson() override final;
    virtual void DealLocal() override final;
    virtual bool TearDownMission() override final;
    virtual bool FillRequestBodyStImpl() override final { return true; }
    virtual void ReportStatus() override final { return; }

    void SetInitFlag();
    bool UpdateOperationRecord();
    void RollbackStorageJob();
    void RollbackComputerJob();
    void UpdateMetaAndOperationStat(JobType jtype);
    void GenerateClusterName();
    void GetTimestampStr(std::string& str_ts);
    bool PostCreateCluster();
    void SetInstallErrorInfo(MachType mtype, const std::string& error_info);
    void UpdateShardInstallState(const std::string& shard_name);
    void RollbackMetaTables();
    void UpdateRollbackState(MachType mtype, JobType jtype);

public:
    static void AddShardCallCB(Json::Value& root, void *arg);
    static void DelShardCallCB(Json::Value& root, void *arg);
    static void AddComputerCallCB(Json::Value& root, void *arg);
    static void DelComputerCallCB(Json::Value& root, void *arg);
    
protected:
    bool AddShardJobs();
    bool AddComputerJobs();

private:
    int err_code_;
    std::string job_id_;
    std::string job_status_;
    std::string shard_error_info_;
    std::string comp_error_info_;
    bool init_flag_;

    std::string cluster_name_;
    int cluster_id_;
    std::string nick_name_;
    std::string ha_mode_;
    int shards_;
    int nodes_;
    int comps_;
    int max_storage_size_;
    int max_connections_;
    int cpu_cores_;
    int innodb_size_;
    int dbcfg_;
    std::string computer_user_;
    std::string computer_pwd_;
    std::vector<std::string> storage_iplists_;
    std::vector<std::string> computer_iplists_;
    std::string shard_id_;
    std::string computer_id_;
    std::string storage_state_;
    std::string computer_state_;
    std::string storage_dstate_;
    std::string computer_dstate_;

    //std::mutex update_mux_;
    //std::mutex roll_mux_;
    KlWrapMutex update_mux_;
    KlWrapMutex roll_mux_;
    //std::mutex cond_mux_;
    //std::condition_variable cond_;
    KlWrapCond cond_;
};

void GetCreateClusterStatus(Json::Value& doc);

}

#endif