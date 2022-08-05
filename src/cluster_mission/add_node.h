/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef _CLUSTER_MGR_ADD_NODE_H_
#define _CLUSTER_MGR_ADD_NODE_H_
#include "http_server/node_channel.h"
#include "request_framework/missionRequest.h"
#include <vector>
#include <unordered_map>
//#include <condition_variable>
//#include <mutex>
#include "cluster_comm.h"

namespace kunlun
{

typedef enum {
    N_PREPARE,
    N_DISPATCH,
    N_SREBUILD,
    N_DREBUILD,
    N_ROLLBACK,
    N_DONE,
    N_FAILED
} Node_State;

class AddNodeMission : public MissionRequest {
    typedef MissionRequest super;
public:
    explicit AddNodeMission(Json::Value *doc) : super(doc),
          err_code_(0), shard_id_(-2) {}
    virtual ~AddNodeMission() {}

    virtual bool ArrangeRemoteTask() override final;
    virtual bool SetUpMisson() override final;
    virtual void DealLocal() override final { return; }
    virtual bool TearDownMission() override final;
    virtual bool FillRequestBodyStImpl() override final { return true; }
    virtual void ReportStatus() override final { return; }

    void DealInternal();
    bool UpdateOperationRecord();
    void UpdateErrorCodeInfo(int error_code, const std::string& error_info);
public:
    static void RebuildRelationCB(Json::Value& root, void* arg);
    static void RollbackCB(Json::Value& root, void* arg);

protected:
    bool GetInstallNodeParamsByClusterId();
    bool SeekStorageIplists();
    bool AssignStorageIplists();
    bool FetchStorageIplists();
    bool AddStorageJobs();
    bool DispatchShardInstallJob(int shard_id);
    std::string CreateShardInstallId();
    void RollbackStorageInstall(int shard_id);
    void PackInstallPorts(std::map<std::string, std::vector<int> >& host_ports);
    void PackInstallPortsByShardId(int shard_id, std::map<std::string, std::vector<int> >& host_ports);
    int GetShardIdByRequestId(const std::string& request_id);
    bool NodeRebuildRelation(int shard_id);
    bool CheckInstallMysqlResult();
    void UpdateNodeMapState(int shard_id, Node_State nstate);
    bool PostAddNode(int shard_id);

private:
    int err_code_;
    std::string job_id_;
    std::string job_status_;
    std::string error_info_;

    int cluster_id_;
    int shard_id_;
    int nodes_;
    int innodb_size_;
    int dbcfg_;
    int fullsync_level_;
    std::string computer_user_;
    std::string computer_pwd_;
    std::string ha_mode_;
    std::vector<int> shard_ids_;
    std::string cluster_name_;
    std::vector<std::string> storage_iplists_;
    std::unordered_map<int, std::vector<IComm_Param> > storage_iparams_;
    std::unordered_map<int, std::string> shard_map_ids_;
    std::unordered_map<int, Node_State> shard_map_states_;
    std::string storage_state_;
    std::vector<IComm_Param> s_iparams_;
    std::unordered_map<std::string, std::vector<int> > host_ports_;
    std::vector<Host_Param> avail_hosts_;
    //std::mutex up_mux_;
    KlWrapMutex up_mux_;
    //std::mutex cond_mux_;
    //std::condition_variable cond_;
    KlWrapCond cond_;
};

class NodeParam {
public:
    NodeParam(AddNodeMission* mission, int shard_id) : mission_(mission), shard_id_(shard_id) {}
    virtual ~NodeParam() {}

    AddNodeMission* GetMission() const {
        return mission_;
    }
    int GetShardId() const {
        return shard_id_;
    }
private:
    AddNodeMission* mission_;
    int shard_id_;    
};
 
}

#endif