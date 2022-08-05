/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef _CLUSTER_MGR_DELETE_CLUSTER_H_
#define _CLUSTER_MGR_DELETE_CLUSTER_H_
#include "http_server/node_channel.h"
#include "request_framework/missionRequest.h"
#include "cluster_comm.h"
#include "zettalib/tool_func.h"
#include <tuple>
#include <vector>
//#include <condition_variable>
//#include <mutex>

namespace kunlun
{

class DeleteClusterMission : public MissionRequest {
    typedef MissionRequest super;
public:
    explicit DeleteClusterMission(Json::Value *doc) : super(doc),
          err_code_(0), init_flag_(false) {}
    virtual ~DeleteClusterMission() {}

    virtual bool ArrangeRemoteTask() override final;
    virtual bool SetUpMisson() override final;
    virtual void DealLocal() override final;
    virtual bool TearDownMission() override final ;
    virtual bool FillRequestBodyStImpl() override final { return true; }
    virtual void ReportStatus() override final { return; }

    bool UpdateOperationRecord(const std::string& error_info = "");
    void UpdateCallCBFail(const std::string& type, const std::string& error_info);
    void UpdateCallCBDone(const std::string& type);
    bool GetDeleteParamsByClusterId();

public:
    static void DelShardCallCB(Json::Value& root, void *arg);
    static void DelComputerCallCB(Json::Value& root, void *arg);

protected:
    bool CheckInputClusterId(const std::string& cluster_id);
    bool CheckInputClusterName(const std::string& cluster_name);

    bool DelShardJobs();
    bool DelComputerJobs();
    bool PostDeleteCluster();
    void PackUsedPorts(MachType itype, 
                  std::map<std::string, std::vector<int> >& host_ports);
private:
    int err_code_;
    std::string job_id_;
    std::string job_status_;
    int cluster_id_;
    std::string cluster_name_;

    bool init_flag_;
    std::vector<int> shard_ids_;
    std::vector<int> comp_ids_;
    std::string storage_del_id_;
    std::string computer_del_id_;
    std::string storage_state_;
    std::string computer_state_;

    //std::mutex update_mux_;
    KlWrapMutex update_mux_;
    //std::mutex cond_mux_;
    //std::condition_variable cond_;
    KlWrapCond cond_;
};

void GetDeleteClusterStatus(Json::Value& doc);

}

#endif