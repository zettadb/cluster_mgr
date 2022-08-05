/*
  Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#pragma once
#include "util_func/meta_info.h"
#include "request_framework/missionRequest.h"
#include "kl_mentain/kl_cluster.h"
#include "kl_mentain/sys.h"
#include "kl_mentain/shard.h"

namespace kunlun {
class KColdBackUpTask : public RemoteTask {
  typedef RemoteTask super;

public:
  explicit KColdBackUpTask(const char *task_name, std::string &related_id)
      : super(task_name), related_id_(related_id){};
  bool virtual TaskReportImpl();
  std::string related_id_;
};

class KColdBackUpMission : public MissionRequest {
  typedef MissionRequest super;

public:
  explicit KColdBackUpMission(Json::Value *doc) : super(doc){};
  explicit KColdBackUpMission(const char *ip_port, int backup_type = 0)
      : ip_port_(ip_port), backup_type_(backup_type){};

  virtual bool SetUpMisson();
  virtual bool ArrangeRemoteTask();
  virtual bool TearDownMission();
  virtual bool InitFromInternal() override;
  virtual void CompleteGeneralJobInfo() override;
  virtual void ReportStatus() override;
  virtual bool FillRequestBodyStImpl() override {return true;};

  bool IsShard();

private:
  // indicate which instance to be coldback
  std::string ip_port_;
  std::string ip_;
  std::string port_;
  std::string cluster_name_;
  std::string cluster_id_;
  std::string shard_name_;
  std::string shard_id_;
  std::string comp_name_;
  std::string comp_id_;
  std::string backup_storage_info_;
  std::string backup_storage_id_;
  brpc::Channel *dest_node_channle_;
  std::string cmd_;
  std::string cluster_coldbackups_related_id_;

  // 0 storage
  // 1 compute
  int backup_type_;
};

// {"version":"1.0","job_id":"","job_type":"update_cluster_coldback_time_period","timestamp":"1435749309","user_name":"kunlun_test","paras":{"cluster_id":"1","time_period_str":"08:00:00-24:00:00"}}

class KUpdateBackUpPeriodMission : public MissionRequest {
  typedef MissionRequest super;

public:
  explicit KUpdateBackUpPeriodMission(Json::Value *doc)
      : super(doc), cluster_id_(-1), success_(false){};
  virtual bool SetUpMisson() override;
  virtual bool ArrangeRemoteTask() override { return true; };
  virtual bool TearDownMission();
  virtual void DealLocal();
  virtual bool FillRequestBodyStImpl() override { return true; };
  void UpdateShardInfo(std::vector<ObjectPtr<Shard> > &shards);
  void UpdateComputeInfo(std::vector<ObjectPtr<Computer_node> > &comps);
  bool UpdateInstanceInfo();

private:
  int cluster_id_;
  std::string coldbackup_period_str_;
  bool success_;
};

class KManualColdBackUpMission: public MissionRequest {
  typedef MissionRequest super;

public:
  explicit KManualColdBackUpMission(Json::Value *doc)
      : super(doc), cluster_id_(-1), success_(false){};
  virtual bool SetUpMisson() override;
  virtual bool ArrangeRemoteTask() override { return true; };
  virtual bool TearDownMission();
  virtual void DealLocal();
  virtual bool FillRequestBodyStImpl() override { return true; };

private:
  int cluster_id_;
  bool success_;

};
} // namespace kunlun
