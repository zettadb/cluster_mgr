/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef _CLUSTER_MGR_DELETE_NODE_H_
#define _CLUSTER_MGR_DELETE_NODE_H_

#include "http_server/node_channel.h"
#include "request_framework/missionRequest.h"
#include "cluster_comm.h"
//#include <condition_variable>
//#include <mutex>

namespace kunlun
{

class DeleteNodeMission : public MissionRequest {
    typedef MissionRequest super;
public:
    explicit DeleteNodeMission(Json::Value *doc) : super(doc),
          err_code_(0) {}
    virtual ~DeleteNodeMission() {}

    virtual bool ArrangeRemoteTask() override final;
    virtual bool SetUpMisson() override final;
    virtual void DealLocal() override final;
    virtual bool TearDownMission() override final;
    virtual bool FillRequestBodyStImpl() override final { return true; }
    virtual void ReportStatus() override final { return; }

    bool UpdateOperationRecord();
    void UpdateJobState(const std::string& state, const std::string& error_info);

public:
    static void DeleteStorageCB(Json::Value& root, void* arg);
    
protected:
    bool CheckInputParams();
    bool PostDelNode();
    bool DelShardFromComputerNode();
    bool DelFromPgTables(const std::string& pg_hostaddr, const std::string& pg_port);

private:
    int err_code_;
    std::string job_id_;
    std::string job_status_;
    std::string error_info_;

    int cluster_id_;
    int shard_id_;
    std::string hostaddr_;
    std::string port_;
    std::string computer_user_;
    std::string computer_pwd_;
    std::string shard_name_;
    std::string storage_dstate_;

    //std::mutex cond_mux_;
    //std::condition_variable cond_;
    KlWrapCond cond_;
};
 
}

#endif