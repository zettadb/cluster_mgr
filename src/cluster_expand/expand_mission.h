/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#ifndef _EXPAND_MISSION_H_
#define _EXPAND_MISSION_H_
#include "http_server/node_channel.h"
#include "request_framework/missionRequest.h"

void Expand_Call_Back(void *);
namespace kunlun {
class ExpandClusterMission;
class ExpandClusterTask : public RemoteTask {
  typedef RemoteTask super;

public:
  explicit ExpandClusterTask(const char *task_name, const char *related_id,
                             ExpandClusterMission *mission = nullptr)
      : super(task_name), related_id_(related_id), mission_ptr_(mission) {
    super::Set_call_back(&Expand_Call_Back);
    super::Set_cb_context((void *)this);
  };
  bool TaskReportImpl() override;
  // table_move_jobs id
  std::string related_id_;
  ExpandClusterMission *mission_ptr_;
};

class ExpandClusterMission : public MissionRequest {
  typedef MissionRequest super;

public:
  explicit ExpandClusterMission(Json::Value *doc) : super(doc){};
  ~ExpandClusterMission(){};

  virtual bool ArrangeRemoteTask() override final;
  virtual bool SetUpMisson() override final;
  virtual bool TearDownMission() override final;
  virtual bool FillRequestBodyStImpl() override final;
  virtual void ReportStatus() override final;

public:
  std::string get_table_list_str() const;

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
  // used by tablecatchup
  std::string table_list_str_;
  // used by mydumper/myloader
  std::string table_list_str_storage_;
  // table_move_jobs id
  std::string related_id_;
};
};     // namespace kunlun
#endif /*_EXPAND_MISSION_H_*/
