/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef _CLUSTER_MGR_DELETE_SHARD_H_
#define _CLUSTER_MGR_DELETE_SHARD_H_
#include <vector>
#include <map>
//#include <condition_variable>
//#include <mutex>
#include "http_server/node_channel.h"
#include "request_framework/missionRequest.h"
#include "cluster_comm.h"

namespace kunlun
{

class DeleteShardMission : public MissionRequest {
    typedef MissionRequest super;
public:
    explicit DeleteShardMission(Json::Value *doc) : super(doc),
          err_code_(0), init_flag_(false), rollback_flag_(false) {}

    explicit DeleteShardMission(int cluster_id, const std::vector<int> shard_ids) : 
                err_code_(0), init_flag_(true), rollback_flag_(false), cluster_id_(cluster_id), 
                shard_ids_(shard_ids) {}
    virtual ~DeleteShardMission() {}

    virtual bool ArrangeRemoteTask() override final;
    virtual bool SetUpMisson() override final;
    virtual void DealLocal() override final;
    virtual bool InitFromInternal() override;
    virtual void CompleteGeneralJobInfo() override;
    virtual bool TearDownMission() override final;
    virtual bool FillRequestBodyStImpl() override final { return true; }
    virtual void ReportStatus() override final { return; }
    void SetRollbackFlag(bool rollback_flag) {
        rollback_flag_ = rollback_flag;
    }
    void UpdateCallCBFail(const std::string& error_info);
    void UpdateCallCBDone();
    bool UpdateOperationRecord(const std::string& error_info = "");
    void PackUsedPorts(std::map<std::string, std::vector<int> >& host_ports);
    bool PostDeleteShard();
    bool DeleteShardFromComputers();
    bool DelShardFromPgTables(const std::string& hostaddr, const std::string& port);
    void CheckDeleteShardState();
    void UpdateShardTopologyAndBackup();
    
public:
    static void DelShardCallCB(Json::Value& root, void* arg);

protected:
    bool DelShardJobs();
private:
    int err_code_;
    std::string job_id_;
    std::string job_status_;
    std::string error_info_;

    bool init_flag_;
    bool rollback_flag_;
    int cluster_id_;
    int shard_id_;
    std::string cluster_name_;
    std::string computer_user_;
    std::string computer_pwd_;
    std::vector<IComm_Param> storage_dparams_;
    std::vector<int> shard_ids_;
    std::string storage_state_;
    std::string storage_del_id_;

    //std::mutex cond_mux_;
    //std::condition_variable cond_;
    KlWrapCond cond_;
};

}
#endif