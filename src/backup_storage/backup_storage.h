/*
  Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#ifndef _BACKUP_STORAGE_H_
#define _BACKUP_STORAGE_H_
#include "http_server/node_channel.h"
#include "request_framework/missionRequest.h"

namespace kunlun {

class BackupStorage : public ::MissionRequest {
  typedef MissionRequest super;

private:
  ClusterRequestTypes request_type_;
  std::string job_id_;

public:
  explicit BackupStorage(Json::Value *doc) : super(doc){};
  ~BackupStorage(){};

  void CreateBackupStorage();
  void UpdateBackupStorage();
  void DeleteBackupStorage();
  virtual bool ArrangeRemoteTask() override final;
  virtual bool SetUpMisson() override { return true; }
  virtual bool TearDownMission() override { return true; }
  virtual bool FillRequestBodyStImpl() override { return true; }
  virtual void ReportStatus() override {}
};
} // namespace kunlun
#endif /*_BACKUP_STORAGE_H_*/
