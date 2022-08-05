/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

/*
struct ComputeInstanceInfoSt {
  std::string ip = "";
  unsigned long pg_port = 0;
  unsigned long mysql_port = 0;
  int compute_id = 0;
  std::string init_user = "abc";
  std::string init_pwd = "abc";
  // kunlun_metadata_db mysql_pg_install table's primary key
  int related_id = -1;
  std::string to_string() {
    return kunlun::string_sprintf("(%s | pg_port:%lu | mysql_port:%lu | "
                                  "comp_id:%d | init_user:%s | init_pwd:%s)",
                                  ip.c_str(), pg_port, mysql_port, compute_id,
                                  init_user.c_str(), init_pwd.c_str());
  }
};
*/
#pragma once
#include "postgresInstallRemote.h"
#include "request_framework/missionRequest.h"
#include <vector>
#include <unordered_map>
namespace kunlun {
class KComputeInstallMission : public MissionRequest {
  typedef MissionRequest super;

public:
  KComputeInstallMission() : super(){};

  void setInstallInfo(std::vector<struct ComputeInstanceInfoSt> & infos);
  void setInstallInfoOneByOne(struct ComputeInstanceInfoSt & install_info);
  void setGeneralRequestId(std::string request_id);
  virtual bool InitFromInternal() override;

  virtual bool SetUpMisson();
  virtual bool ArrangeRemoteTask();
  virtual bool TearDownMission();
  virtual void CompleteGeneralJobInfo() override;
  virtual void ReportStatus() override;
  virtual bool FillRequestBodyStImpl() override { return true; };
  void rollBackComputeInstall();


private:
  unordered_map<string,struct ComputeInstanceInfoSt> install_infos_;
};

class KDelComputeMission : public MissionRequest {
  typedef MissionRequest super;

public:
  KDelComputeMission() : super(){};

  void setInstallInfo(std::vector<struct ComputeInstanceInfoSt> & infos);
  void setInstallInfoOneByOne(struct ComputeInstanceInfoSt & install_info);
  void setGeneralRequestId(std::string request_id);
  virtual bool InitFromInternal() override;

  virtual bool SetUpMisson();
  virtual bool ArrangeRemoteTask();
  virtual bool TearDownMission();
  virtual void CompleteGeneralJobInfo() override;
  virtual void ReportStatus() override;
  virtual bool FillRequestBodyStImpl() override { return true; };


private:
  unordered_map<string,struct ComputeInstanceInfoSt> install_infos_;
};
} // namespace kunlun
