/*
   Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef _CLUSTER_MGR_REBUILD_NODE_H_
#define _CLUSTER_MGR_REBUILD_NODE_H_
#include <vector>
#include <tuple>
//#include <mutex>
//#include <condition_variable>
#include "util_func/kl_mutex.h"
#include "http_server/node_channel.h"
#include "request_framework/missionRequest.h"

namespace kunlun 
{

class RebuildNodeTask : public RemoteTask {
    typedef RemoteTask super;
public:
  explicit RebuildNodeTask(const char *task_name, std::string &related_id)
      : super(task_name), related_id_(related_id){};
  bool virtual TaskReportImpl() { return true; } 
  std::string related_id_;
};

typedef enum {
    RB_ONGOING,
    RB_DONE,
    RB_FAILED
} RbNodeState;

/*
* 1 -- rebuild host
* 2 -- need backup
* 3 -- if need backup=1, hdfs_host
* 4 -- pv_limit
*/
typedef std::tuple<std::string, int, std::string, int> RN_Param;

class RebuildNodeMission : public MissionRequest {
    typedef MissionRequest super;

public:
    explicit RebuildNodeMission(Json::Value *doc) : super(doc), 
            allow_pull_from_master_(false), allow_delay_(30), error_code_(0), init_flag_(false) {
    }
    
    explicit RebuildNodeMission(bool pull_from_master, int allow_delay, 
        const std::vector<RN_Param>& rb_backup, int cluster_id, int shard_id) : 
        allow_pull_from_master_(pull_from_master), allow_delay_(allow_delay), error_code_(0),
        rb_backup_(rb_backup), cluster_id_(cluster_id), shard_id_(shard_id),
        init_flag_(true) {}

    virtual ~RebuildNodeMission(){}

    virtual bool ArrangeRemoteTask() override final { return true; }
    virtual bool SetUpMisson() override final;
    virtual void DealLocal() override final;
    virtual bool TearDownMission() override final;
    virtual bool InitFromInternal() override;
    virtual void CompleteGeneralJobInfo() override;
    virtual bool FillRequestBodyStImpl() override final { return true; }
    virtual void ReportStatus() override final { return; }

    void ParallelRebuildNode();

    void UpdateRbNodeMapState(const std::string& host, RbNodeState state, int type=0);
    void UpdateRebuildNodeMissionState(int type=0);
    //bool ArrangeRemoteTaskForRecover();
public:
    static void RebuildNodeCallBack(Json::Value& root, void *arg);
    static void ParallelRbNodeCallBack(Json::Value& root, void *arg);
private:
    bool update_operation_record();
    bool GetPullAndMasterHost();
private:
    bool allow_pull_from_master_;
    int allow_delay_;
    std::vector<RN_Param> rb_backup_;
    std::string master_host_;
    std::string pull_host_;
    int error_code_;
    RequestStatus job_status_;
    std::map<std::string, RbNodeState> rbnode_state_;
    std::map<std::string, std::string> rbnode_relation_id_;
    std::string job_id_;
    int cluster_id_;
    int shard_id_;
    bool init_flag_;
    std::string error_info_;

    //std::mutex cond_mux_;
    //std::condition_variable cond_;
    KlWrapCond cond_;
};

class RbNodeTask : public RemoteTask {
  typedef RemoteTask super;

public:
  explicit RbNodeTask(const char *task_name, const std::string &related_id)
      : super(task_name), related_id_(related_id) {}
  bool virtual TaskReportImpl();

  std::string related_id_;
};

class RbNodeMission : public MissionRequest {
    typedef MissionRequest super;
public:
    explicit RbNodeMission(Json::Value *doc) : super(doc), error_code_(0) {}
    explicit RbNodeMission(const std::string& rebuild_host, const std::string& pull_host, 
                    int need_backup, const std::string& hdfs_host, int pv_limit, const std::string& master_host) : 
                    rebuild_host_(rebuild_host), pull_host_(pull_host), need_backup_(need_backup), hdfs_host_(hdfs_host), 
                    pv_limit_(pv_limit),master_host_(master_host), error_code_(0) {}

    virtual bool SetUpMisson();
    virtual bool ArrangeRemoteTask();
    virtual bool TearDownMission();
    virtual bool InitFromInternal() override;
    virtual void CompleteGeneralJobInfo() override;
    virtual void ReportStatus() override { return; }
    virtual bool FillRequestBodyStImpl() override {return true;}

    void UpdateRbNodeFailedStat();
    bool GetShardInfo(const std::string& host);
    void update_operation_record();
    void GetNodeMgrRbHostResult();
    
private:
    std::string rebuild_host_;
    std::string pull_host_;
    int need_backup_;
    std::string hdfs_host_;
    int pv_limit_;
    std::string shard_name_;
    std::string cluster_name_;
    std::string master_host_;
    std::string job_id_;
    int error_code_;
};

typedef struct {
    RebuildNodeMission *rb_mission_;
    std::string host_;
} RbNodeParams;

void GetRebuildNodeStatus(Json::Value& doc);
}
#endif