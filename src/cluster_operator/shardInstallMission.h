/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

/*
struct InstanceInfoSt {
  std::string ip = "";
  unsigned long port = 0;
  int innodb_buffer_size_M = 0;
  std::string to_string() {
    return kunlun::string_sprintf("(%s | %lu | %d)", ip.c_str(), port,
                                  innodb_buffer_size_M);
  }
};
*/
#pragma once
#include "mysqlInstallRemote.h"
#include "request_framework/missionRequest.h"
#include <vector>
#include <unordered_map>
namespace kunlun {
class KAddShardMission : public MissionRequest {
  typedef MissionRequest super;

public:
  KAddShardMission() : super(){};

  void setInstallInfo(std::vector<struct InstanceInfoSt> & infos);
  void setInstallInfoOneByOne(struct InstanceInfoSt & install_info);
  void setGeneralRequestId(std::string request_id);
  virtual bool InitFromInternal() override;
  struct InstanceInfoSt getMasterInfo();
  bool setUpRBR();
  void rollBackShardInstall();
  bool setUpMgr();

  virtual bool SetUpMisson();
  virtual bool ArrangeRemoteTask();
  virtual bool TearDownMission();
  virtual void CompleteGeneralJobInfo() override;
  virtual void ReportStatus() override;
  virtual bool FillRequestBodyStImpl() override { return true; };
private:
  bool doChangeMasterOnSlave(struct InstanceInfoSt &master,struct InstanceInfoSt &slave);
  // set super_read_only = on;
  bool enableMaster(struct InstanceInfoSt &master);

private:
  unordered_map<string,struct InstanceInfoSt> install_infos_;
};

class KDelShardMission : public MissionRequest {
  typedef MissionRequest super;

public:
  KDelShardMission() : super(){};

  void setInstallInfo(std::vector<struct InstanceInfoSt> & infos);
  void setInstallInfoOneByOne(struct InstanceInfoSt & install_info);
  void setGeneralRequestId(std::string request_id);
  virtual bool InitFromInternal() override;
  struct InstanceInfoSt getMasterInfo();
  bool setUpRBR();

  virtual bool SetUpMisson();
  virtual bool ArrangeRemoteTask();
  virtual bool TearDownMission();
  virtual void CompleteGeneralJobInfo() override;
  virtual void ReportStatus() override;
  virtual bool FillRequestBodyStImpl() override { return true; };


private:
  unordered_map<string,struct InstanceInfoSt> install_infos_;
};
} // namespace kunlun
