/*
   Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef _CLUSTER_MGR_CLUSTER_DEBUG_H_
#define _CLUSTER_MGR_CLUSTER_DEBUG_H_

#ifndef NDEBUG
#include "request_framework/missionRequest.h"

namespace kunlun {
   
class ClusterDebug : public ::MissionRequest {
   typedef MissionRequest super;

public:
  explicit ClusterDebug(Json::Value *doc) : super(doc) {
  }
  virtual ~ClusterDebug(){};

  virtual bool ArrangeRemoteTask() override { return true;}
  virtual bool SetUpMisson() override { return true; }
  virtual bool TearDownMission() override { return true; }
  virtual bool FillRequestBodyStImpl() override { return true; }
  virtual void ReportStatus() override {}
  virtual bool SyncTaskImpl() override {
     ParseJson();
     return true;
  }

private:
   void ParseJson();
};

}

int _cm_keyword(const char* keyword, int state);

#define CM_DEBUG_EXECUTE_IF(keyword, a1)    \
   do {                                     \
      if(_cm_keyword((keyword), 1)) {       \
         a1                                 \
      }                                     \
   } while(0)

#else

#define CM_DEBUG_EXECUTE_IF(keywork, a1) \
do {                                   \
} while(0)

#endif

#endif