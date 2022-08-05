/*
  Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#ifndef _SYNC_MISSION_H_
#define _SYNC_MISSION_H_
#include "http_server/node_channel.h"
#include "request_framework/missionRequest.h"
#include "request_framework/requestValueDefine.h"

namespace kunlun {
class SyncMission : public ::MissionRequest {
  typedef MissionRequest super;

public:
  explicit SyncMission(Json::Value *doc) : super(doc){};
  ~SyncMission(){};

  bool GetStatus();
  bool GetMetaMode();
  bool GetMetaSummary();
  bool GetBackupStorage();
  bool GetClusterSummary();
  bool GetClusterDetail();
  bool GetExpandTableList();
  bool GetVariable();
  bool SetVariable();

  bool GenerateRelatedAttachment(Json::Value &doc);
  void fetchClusterRestoreResult(Json::Value &doc);
  void fetchClusterExpandResult(Json::Value &doc);

  virtual bool SyncTaskImpl() override {
    bool ret = true;
    switch (get_request_type()) {
    case kunlun::kGetStatusType:
      ret = GetStatus();
      break;
    case kunlun::kGetMetaModeType:
      ret = GetMetaMode();
      break;
    case kunlun::kGetMetaSummaryType:
      ret = GetMetaSummary();
      break;
    case kunlun::kGetBackupStorageType:
      ret = GetBackupStorage();
      break;
    case kunlun::kGetClusterDetailType:
      ret = GetClusterDetail();
      break;
    case kunlun::kGetExpandTableListType:
      ret = GetExpandTableList();
      break;
    case kunlun::kGetVariableType:
      ret = GetVariable();
      break;
    case kunlun::kSetVariableType:
      ret = SetVariable();
      break;

    default:
      break;
    }
    return ret;
  };
  virtual bool ArrangeRemoteTask() override  { return true; }
  virtual bool SetUpMisson() override  { return true; }
  virtual bool TearDownMission() override  { return true; }
  virtual bool FillRequestBodyStImpl() override  { return true; }
  virtual void ReportStatus() override {}
};
} // namespace kunlun
#endif /*_SYNC_MISSION_H_*/