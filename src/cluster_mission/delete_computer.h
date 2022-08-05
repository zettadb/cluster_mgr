/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef _CLUSTER_MGR_DELETE_COMPUTER_H_
#define _CLUSTER_MGR_DELETE_COMPUTER_H_
#include <vector>
#include <map>
//#include <condition_variable>
//#include <mutex>
#include "http_server/node_channel.h"
#include "request_framework/missionRequest.h"
#include "cluster_comm.h"

namespace kunlun
{

class DeleteComputerMission : public MissionRequest {
    typedef MissionRequest super;
public:
    explicit DeleteComputerMission(Json::Value *doc) : super(doc),
          err_code_(0), init_flag_(false) {}
    explicit DeleteComputerMission(int cluster_id, const std::vector<int> comp_ids) :
            err_code_(0), init_flag_(true), cluster_id_(cluster_id), 
            comp_ids_(comp_ids) {}

    virtual ~DeleteComputerMission() {}

    virtual bool ArrangeRemoteTask() override final;
    virtual bool SetUpMisson() override final;
    virtual void DealLocal() override final;
    virtual bool InitFromInternal() override;
    virtual void CompleteGeneralJobInfo() override;
    virtual bool TearDownMission() override final;
    virtual bool FillRequestBodyStImpl() override final { return true; }
    virtual void ReportStatus() override final { return; }

    void UpdateCallCBFail(const std::string& error_info);
    void UpdateCallCBDone();
    bool UpdateOperationRecord(const std::string& error_info = "");
    bool PostDeleteComputer();
    void PackUsedPorts(std::map<std::string, std::vector<int> >& host_ports);
    void CheckDeleteCompState();
public:
    static void DelComputerCallCB(Json::Value& root, void* arg);

protected:
    bool DelComputerJobs();

private:
    int err_code_;
    std::string job_id_;
    std::string job_status_;
    std::string error_info_;

    bool init_flag_;
    int cluster_id_;
    std::string cluster_name_;
    int comp_id_;
    std::vector<IComm_Param> computer_dparams_;
    std::vector<int> comp_ids_;
    std::string computer_del_id_;
    std::string computer_state_;

    //std::mutex cond_mux_;
    //std::condition_variable cond_;
    KlWrapCond cond_;
};

}
#endif