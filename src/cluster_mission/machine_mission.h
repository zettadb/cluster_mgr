/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef _CLUSTER_MGR_MACHINE_MISSION_H_
#define _CLUSTER_MGR_MACHINE_MISSION_H_
#include "http_server/node_channel.h"
#include "request_framework/missionRequest.h"

namespace kunlun
{

class MachineRequest : public MissionRequest {
    typedef MissionRequest super;
public:
    explicit MachineRequest(Json::Value *doc) : super(doc),
          err_code_(0) {}
    virtual ~MachineRequest() {}

    virtual bool ArrangeRemoteTask() override final { return true; }
    virtual bool SetUpMisson() override final;
    virtual void DealLocal() override final;
    virtual bool TearDownMission() override final { return true; }
    virtual bool FillRequestBodyStImpl() override final { return true; }
    virtual void ReportStatus() override final { return; }
    virtual bool SyncTaskImpl() override final { return true; }

    virtual int GetErrorCode() override final {
        return err_code_;
    }
    bool UpdateOperationRecord();
protected:
    bool CreateMachine();
    bool CreateStorageMachine(Json::Value& doc);
    bool CreateComputerMachine(Json::Value& doc);
    bool CheckMachineState();
    bool DeleteMachine();
    bool UpdateMachine();

private:
    int err_code_;
    std::string job_id_;
    std::string job_status_;
    std::string hostaddr_;
    std::string machine_type_;
};

}

#endif