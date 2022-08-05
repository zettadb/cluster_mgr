/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#pragma once
#include "cluster_operator/mysqlInstallRemote.h"
#include "request_framework/remoteTask.h"
#include <time.h>
#include <unordered_map>

namespace kunlun {
extern void rebuildRbrAfterRestore(void *para);
class ShardRestoreRemoteTask : public RemoteTask {
  typedef RemoteTask super;

public:
  ShardRestoreRemoteTask(const char *task_name, std::string general_request_id)
      : super(task_name), request_id_(general_request_id) {
    srcShardName_ = "";
    srcClusterName_ = "";
    restore_time_str_ = "";
//    this->Set_call_back(rebuildRbrAfterRestore);
//    this->Set_cb_context((void *)this);
  };
  virtual ~ShardRestoreRemoteTask(){};
  bool InitShardsInfo(int srcShardId, int dstShardId, std::string restore_time);

  bool virtual TaskReportImpl() override;
  void virtual SetUpStatus() override;
  struct InstanceInfoSt getMasterInfo();
  bool setUpRBR();

private:
  bool ComposeOneNodeChannelAndPara();
  bool ComposeNodeChannelsAndParas();
  bool InitRestoreLogId();
  // set super_read_only = on;
  bool enableMaster(struct InstanceInfoSt &master);
  bool doChangeMasterOnSlave(struct InstanceInfoSt &master,
                             struct InstanceInfoSt &slave);

private:
  std::string request_id_;
  std::string srcShardName_;
  std::string srcClusterName_;
  std::unordered_map<std::string, InstanceInfoSt> destMysqlInfos_;
  std::string hdfs_addr_;
  std::string restore_log_id_;
  std::string restore_time_str_;
};
} // namespace kunlun
