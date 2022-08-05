/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#pragma once
#include "cluster_operator/postgresInstallRemote.h"
#include "request_framework/remoteTask.h"
#include <time.h>
#include <unordered_map>

namespace kunlun {
class ComputeRestoreRemoteTask : public RemoteTask {
  typedef RemoteTask super;

public:
  ComputeRestoreRemoteTask(const char *task_name,
                           std::string general_request_id)
      : super(task_name), request_id_(general_request_id) {
    srcClusterName_ = "";
    restore_time_ = "";
  };
  virtual ~ComputeRestoreRemoteTask(){};
  bool InitComputesInfo(int srcClusterId, int dstClusterId,
                        std::string shard_id_map, std::string restore_time);

  bool virtual TaskReportImpl() override;
  void virtual SetUpStatus() override;

private:
  bool ComposeOneNodeChannelAndPara();
  bool ComposeNodeChannelsAndParas();
  bool InitRestoreLogId();

private:
  std::string request_id_;
  std::string srcClusterName_;
  std::unordered_map<std::string, ComputeInstanceInfoSt> dstPostgresInfos_;
  std::string hdfs_addr_;
  std::string restore_log_id_;
  // relation bettwen src cluster shard and dest Cluster shard
  std::string shard_id_map_;
  std::string restore_time_;
};
} // namespace kunlun
