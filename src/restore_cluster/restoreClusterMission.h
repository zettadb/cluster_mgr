/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#pragma once
#include "request_framework/missionRequest.h"
#include "restore_cluster/restoreComputeTask.h"
#include "restore_cluster/restoreShardTask.h"
#include <utility>
#include <vector>
#include "kl_mentain/kl_cluster.h"
#include "kl_mentain/shard.h"
#include "kl_mentain/sys.h"

namespace kunlun {
class KRestoreClusterMission : public MissionRequest {
  typedef MissionRequest super;

public:
  explicit KRestoreClusterMission(Json::Value *doc)
      : super(doc), src_cluster_id_(-1), dst_cluster_id_(-1),
        rollback_if_failed_(true),inited_(false),restore_time_(""){};
  KRestoreClusterMission(int srcClusterId, int dstClusterId)
      : src_cluster_id_(srcClusterId), dst_cluster_id_(dstClusterId),
        rollback_if_failed_(true),inited_(false),restore_time_(""){};
  ~KRestoreClusterMission(){};
  void rollBackShardRestore();

  virtual bool InitFromInternal() override;
  virtual bool SetUpMisson();
  virtual bool ArrangeRemoteTask();
  virtual bool TearDownMission();
  virtual void CompleteGeneralJobInfo() override;
  virtual void ReportStatus() override;
  virtual bool FillRequestBodyStImpl() override { return true; };
  void SetShardStatus(std::vector<ObjectPtr<Shard> > &shards,bool deleted);

private:
  bool srcAndDstClusterIsAvilable();
  bool initShardMap();
  std::string shard_map_to_string();
  bool prepareRestoreOneShardTask(std::pair<int, int> p);
  bool prepareRestoreAllCompute();

private:
  int src_cluster_id_;
  int dst_cluster_id_;
  bool rollback_if_failed_;
  bool inited_;
  std::vector<std::pair<int, int>> shard_map_;
  std::string restore_time_;
};
} // namespace kunlun
