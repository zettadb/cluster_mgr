/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef _CLUSTER_MGR_RAFT_MISSION_H_
#define _CLUSTER_MGR_RAFT_MISSION_H_
#include "http_server/node_channel.h"
#include "request_framework/missionRequest.h"

namespace kunlun
{

class RaftMission : public MissionRequest {
    typedef MissionRequest super;
public:
    RaftMission(Json::Value *doc) : super(doc), err_code_(0) {}
    virtual ~RaftMission() {}

    virtual bool ArrangeRemoteTask() override final { return true; }
    virtual bool SetUpMisson() override final;
    virtual void DealLocal() override final;
    virtual bool InitFromInternal() override { return true; }
    virtual void CompleteGeneralJobInfo() override {}
    virtual bool TearDownMission() override final { return true; }
    virtual bool FillRequestBodyStImpl() override final { return true; }
    virtual void ReportStatus() override final { return; }

    bool UpdateOperationRecord();

private:
    bool TransferLeaderParse(Json::Value& doc);
    bool AddPeerParse(Json::Value& doc);
    bool RemovePeerParse(Json::Value& doc);

    void TransferLeader();
    void AddPeer();
    void RemovePeer();

    void GetCurrentConfPeer();
    bool SyncNewConfToDisk();
private:
    int err_code_;
    std::string job_id_;
    std::string job_status_;
    std::string error_info_;

    std::string task_type_;
    std::string target_leader_;
    std::string peer_;
    std::vector<std::string> cur_peers_;
};

} // namespace kunlun

#endif