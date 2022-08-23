/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef _CLUSTER_MGR_ADD_SHARD_H_
#define _CLUSTER_MGR_ADD_SHARD_H_
#include "http_server/node_channel.h"
#include "request_framework/missionRequest.h"
#include <vector>
//#include <condition_variable>
//#include <mutex>
#include "cluster_comm.h"

namespace kunlun
{

class AddShardMission : public MissionRequest {
    typedef MissionRequest super;
public:
    explicit AddShardMission(Json::Value *doc) : super(doc),
          err_code_(0), init_flag_(false) {}

    explicit AddShardMission(const std::string& ha_mode, const std::string& cluster_name, int cluster_id,
                    int shards, int nodes, const std::vector<std::string>& storage_iplists, 
                    int innodb_size, int dbcfg, const std::string& computer_user, const std::string& computer_pwd) : err_code_(0), init_flag_(true),
            ha_mode_(ha_mode), cluster_name_(cluster_name), cluster_id_(cluster_id), shards_(shards), 
            nodes_(nodes), storage_iplists_(storage_iplists), innodb_size_(innodb_size), dbcfg_(dbcfg), 
            computer_user_(computer_user), computer_pwd_(computer_pwd) {}
    virtual ~AddShardMission() {}

    virtual bool ArrangeRemoteTask() override final;
    virtual bool SetUpMisson() override final;
    virtual void DealLocal() override final;
    virtual bool InitFromInternal() override;
    virtual void CompleteGeneralJobInfo() override;
    virtual bool TearDownMission() override final;
    virtual bool FillRequestBodyStImpl() override final { return true; }
    virtual void ReportStatus() override final { return; }

    bool UpdateOperationRecord();
  
    void UpdateMetaAndOperationStat(JobType jtype);
    void SetInstallErrorInfo(const std::string& error_info);
    void UpdateShardInstallState(const std::string& shard_name, JobType jtype);

    void SetNickName(const std::string& nick_name) {
        nick_name_ = nick_name;
    }
public:
    static void AddShardCallCB(Json::Value& root, void* arg);

protected:
    bool SeekStorageIplists();
    bool AssignStorageIplists();
    bool FetchStorageIplists();
    bool AddShardJobs();
    bool PostAddShard();

    void PackInstallPorts(std::map<std::string, std::vector<int> >& host_ports);
    bool GetInstallShardParamsByClusterId();
    void RollbackStorageJob();
    int GetShardNum();
    void UpdateShardTopologyAndBackup();
    std::string GetProcUuid();

private:
    int err_code_;
    std::string job_id_;
    std::string job_status_;
    std::string error_info_;

    bool init_flag_;
    std::string ha_mode_;
    std::string cluster_name_;
    int cluster_id_;
    int shards_;
    int nodes_;
    std::vector<std::string> storage_iplists_;
    int innodb_size_;
    int dbcfg_;
    std::string computer_user_;
    std::string computer_pwd_;
    std::string nick_name_;

    std::unordered_map<std::string, std::string> shard_map_id_;
    std::unordered_map<std::string, int> shard_map_state_;
    std::unordered_map<std::string, std::vector<std::string> > shard_map_nids_;
    std::unordered_map<std::string, std::string> shard_map_sids_;
    std::unordered_map<std::string, std::vector<IComm_Param> > storage_iparams_;
    std::vector<IComm_Param> s_iparams_;
    std::unordered_map<std::string, std::vector<int> > host_ports_;
    std::vector<Host_Param> avail_hosts_;
    std::string storage_state_;

    //std::mutex update_mux_;
    //std::mutex cond_mux_;
    //std::condition_variable cond_;
    KlWrapMutex update_mux_;
    KlWrapCond cond_;
}; 

class ShardJobParam {
public:
    ShardJobParam(AddShardMission* job_mission, const std::string& shard_name) :
                        job_mission_(job_mission), shard_name_(shard_name) {}
    virtual ~ShardJobParam() {}

    AddShardMission* GetJobMission() {
        return job_mission_;
    }
    const std::string& GetShardName() const {
        return shard_name_;
    }

private:
    AddShardMission* job_mission_;
    std::string shard_name_;
};

}
#endif
