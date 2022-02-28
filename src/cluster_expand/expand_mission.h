/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#ifndef _EXPAND_MISSION_H_
#define _EXPAND_MISSION_H_
#include "http_server/node_channel.h"
#include "request_framework/missionRequest.h"

void Expand_Call_Back(brpc::Controller *);
namespace kunlun {
class ExpandClusterTask : public RemoteTask {
  typedef RemoteTask super;

public:
  explicit ExpandClusterTask(const char *task_name) : super(task_name){
    super::Set_call_back(&Expand_Call_Back);
  };
  bool TaskReportImpl() override;
};

class ExpandClusterMission : public MissionRequest {
  typedef MissionRequest super;

public:
  explicit ExpandClusterMission(Json::Value *doc) : super(doc){};
  ~ExpandClusterMission(){};

  virtual bool ArrangeRemoteTask() override final;
  virtual bool SetUpMisson() override final;
  virtual void TearDownImpl() override final;
  virtual bool FillRequestBodyStImpl() override final;
  virtual void ReportStatus() override final;

private:
  bool MakeDir();
  bool DumpTable();
  bool LoadTable();
  bool TableCatchUp();
  bool TransferFile();

private:
  // Will be initialized in setup phase
  std::string mydumper_tmp_data_dir_;
  std::string src_shard_node_address_;
  int64_t src_shard_node_port_;
  std::string dst_shard_node_address_;
  int64_t dst_shard_node_port_;
  std::string meta_cluster_url_;
  std::string table_list_str_;
  std::string table_list_str_storage_;
};
};     // namespace kunlun
#endif /*_EXPAND_MISSION_H_*/
