/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef _CLUSTER_MGR_ADD_COMPUTER_H_
#define _CLUSTER_MGR_ADD_COMPUTER_H_

#include "http_server/node_channel.h"
#include "request_framework/missionRequest.h"
#include "cluster_comm.h"
//#include <condition_variable>
//#include <mutex>

namespace kunlun
{

class AddComputerMission : public MissionRequest {
    typedef MissionRequest super;
public:
    explicit AddComputerMission(Json::Value *doc) : super(doc),
          err_code_(0), init_flag_(false), exist_comps_(0) {}
    explicit AddComputerMission(int comps, const std::string& cluster_name, int cluster_id, const std::vector<std::string> computer_iplists,
                    const std::string& computer_user, const std::string& computer_pwd, const std::string ha_mode) : err_code_(0), init_flag_(true),
                    exist_comps_(0), comps_(comps), cluster_name_(cluster_name), cluster_id_(cluster_id), computer_iplists_(computer_iplists),
                    computer_user_(computer_user), computer_pwd_(computer_pwd), ha_mode_(ha_mode) {}

    virtual ~AddComputerMission() {}

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
    bool PostAddComputer();
    void SetNickName(const std::string& nick_name) {
        nick_name_ = nick_name;
    }

public:
    static void AddComputerCallCB(Json::Value& root, void *arg);

protected:
    bool SeekComputerIplists();
    bool AssignComputerIplists();
    bool FetchComputerIplists();
    void PackInstallPorts(std::map<std::string, std::vector<int> >& host_ports);
    bool AddComputerJobs();
    void RollbackComputerJob();
    bool UpdateComputerCatalogTables(const std::string ip, int port, 
                    const std::string& cp_name, int pos);
    bool GetInstallCompsParamsByClusterId();
    bool CheckTablePartitonExist(int part_no);
    void GetExistMaxCompId();
    void ComputerReplayDdlLogAndUpdateMeta();
    bool CheckMetaCompNodesStat(const std::string& hostaddr, int port);
    bool UpdateMetaCompNodeStat(const std::string& hostaddr, int port, bool is_ok);
    int PgReplayDdlLogStat(const std::string& hostaddr, int port, int max_id);
private:
    int err_code_;
    std::string job_id_;
    std::string job_status_;
    std::string error_info_;
    bool init_flag_;

    int comps_;
    std::string cluster_name_;
    int cluster_id_;
    std::vector<std::string> computer_iplists_;
    std::string computer_user_;
    std::string computer_pwd_;
    std::string ha_mode_;
    std::string nick_name_;
    int exist_comps_;

    std::string computer_id_;
    std::string computer_state_;
    std::unordered_map<std::string, std::vector<int> > host_ports_;
    std::vector<Host_Param> avail_hosts_;
    std::vector<IComm_Param> computer_iparams_;
    //std::mutex cid_mux_;
    KlWrapMutex cid_mux_;

    //std::mutex cond_mux_;
    //std::condition_variable cond_;
    KlWrapCond cond_;
};

}

#endif