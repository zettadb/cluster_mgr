/*
  Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "cluster_operator/mysqlInstallRemote.h"
#include "cluster_operator/postgresInstallRemote.h"
#include "cluster_operator/shardInstallMission.h"
#include "cluster_operator/computeInstallMission.h"
#include "http_server/node_channel.h"
#include "request_framework/missionRequest.h"
#include "request_framework/handleRequestThread.h"
#include "zettalib/op_log.h"

extern HandleRequestThread *g_request_handle_thread;
namespace kunlun {
class ExampleMission : public ::MissionRequest {
  typedef MissionRequest super;

public:
  explicit ExampleMission(Json::Value *doc) : super(doc){};
  ~ExampleMission(){};

  virtual bool SetUpMisson() override {

    //InstanceInfoSt st1 ;
    //st1.ip = "192.168.0.135";
    //st1.port = 8930;
    //st1.innodb_buffer_size_M = 16;
    //st1.role = "master";
    //InstanceInfoSt st2;
    //st2.ip = "192.168.0.135";
    //st2.port = 8931;
    //st2.innodb_buffer_size_M = 16;
    //st2.role = "slave";

    //KAddShardMission * mission = new KAddShardMission();

    //mission->setInstallInfoOneByOne(st1);
    //mission->setInstallInfoOneByOne(st2);
    //bool ret = mission->InitFromInternal();
    //if(!ret){
    //  KLOG_ERROR("{}",mission->getErr());
    //}
    //g_request_handle_thread->DispatchRequest(mission);
    //KLOG_INFO("DISPATCH TEST SHARD INSTALL");

    ComputeInstanceInfoSt st1;
    st1.ip = "192.168.0.135";
    st1.pg_port = 55001;
    st1.mysql_port = 55002;
    st1.init_user = "abc";
    st1.init_pwd = "abc";
    st1.compute_id = 1;
    ComputeInstanceInfoSt st2;
    st2.ip = "192.168.0.135";
    st2.pg_port = 55003;
    st2.mysql_port = 55004;
    st2.init_user = "abc";
    st2.init_pwd = "abc";
    st2.compute_id = 2;

    KComputeInstallMission * mission = new KComputeInstallMission();
    mission->setInstallInfoOneByOne(st1);
    mission->setInstallInfoOneByOne(st2);

    bool ret = mission->InitFromInternal();
    if(!ret){
      KLOG_ERROR("{}",mission->getErr());
    }
    g_request_handle_thread->DispatchRequest(mission);
    KLOG_INFO("DISPATCH TEST COMPUTE INSTALL");

  
    return true; 
  }
  virtual bool ArrangeRemoteTask() override {
    // for each node_channel, execute the ifconfig and fetch the output
    auto node_channle_map = g_node_channel_manager->get_nodes_channel_map();
    auto iter = node_channle_map.begin();
    for (; iter != node_channle_map.end(); iter++) {
      //IfConfig((iter->first).c_str());
      //InstallMysqlTask((iter->first).c_str());
      //InstallPostgresTask((iter->first).c_str());
    }
    return true;
  }
  virtual bool TearDownMission() override { return true; }
  virtual bool FillRequestBodyStImpl() override { return true; }
  virtual void ReportStatus() override {
    KLOG_INFO("report status called here");
  }

  bool InstallPostgresTask(const char *node_ip) {
    kunlun::ComputeInstanceInfoSt info_st;
    info_st.ip = "192.168.0.135";
    info_st.pg_port = 56701;
    info_st.mysql_port = 56702;
    info_st.init_user = "abc";
    info_st.init_pwd = "abc";
    info_st.compute_id = 1;
    info_st.related_id = 100;
    
    kunlun::PostgresInstallRemoteTask *task =
        new kunlun::PostgresInstallRemoteTask("test_install_postgres_single", "100");
    bool ret = task->InitInstanceInfoOneByOne(info_st);
    if(!ret){
      KLOG_ERROR("{}",task->getErr());
    }
    get_task_manager()->PushBackTask(task);
    return true;
  }

  bool InstallMysqlTask(const char *node_ip) {
    kunlun::MySQLInstallRemoteTask *task =
        new kunlun::MySQLInstallRemoteTask("test_install_mysql_single", "100");
    bool ret = task->InitInstanceInfoOneByOne(std::string(node_ip), "7895", "7896", 16, 0);
    if(!ret){
      KLOG_ERROR("{}",task->getErr());
    }
    get_task_manager()->PushBackTask(task);
    return true;
  }

  bool IfConfig(const char *node_ip) {
    brpc::Channel *channel = g_node_channel_manager->getNodeChannel(node_ip);

    // RemoteTask is a base class which has the callback_ facility to support
    // the special requirment, see the ExpandClusterTask for detail
    RemoteTask *task = new RemoteTask("Example_Ifconfig_info");
    task->AddNodeSubChannel(node_ip, channel);
    Json::Value root;
    root["command_name"] = "ifconfig";
    root["cluster_mgr_request_id"] = get_request_unique_id();
    root["task_spec_info"] = task->get_task_spec_info();
    Json::Value paras;
    paras.append("-a 1>&2");
    root["para"] = paras;
    task->SetPara(node_ip, root);
    get_task_manager()->PushBackTask(task);
    return true;
  };
};
} // namespace kunlun
