/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "http_server/http_server.h"
#include "http_server/node_channel.h"
#include "kl_mentain/config.h"
#include "kl_mentain/global.h"
//#include "kl_mentain/log.h"
#include "kl_mentain/os.h"
#include "kl_mentain/sys.h"
//#include "kl_mentain/thread_manager.h"
#include "raft_ha/raft_ha.h"
#include "stdio.h"
#include "sys_config.h"
#include "zettalib/proc_env.h"
#include <signal.h>
#include <unistd.h>
#include "zettalib/tool_func.h"
#include "zettalib/op_log.h"
#include "cluster_collection/machine_alive.h"
#include "cluster_collection/prometheus_control.h"
#include "kl_mentain/func_timer.h"
#include "cluster_auto/refresh_shard.h"
#include "gflags/gflags.h"

extern int g_exit_signal;
int64_t thread_work_interval = 3;
extern std::string local_ip;
extern std::string dev_interface;
std::string log_file_path;
int64_t max_log_file_size = 500;

HandleRequestThread *g_request_handle_thread =nullptr;
kunlun::CMachineAlive* g_machine_alive = nullptr;
GlobalNodeChannelManager* g_node_channel_manager = nullptr;
kunlun::KPrometheusControl* g_prometheus_manager = nullptr;
kunlun::CRefreshShard* g_rfshard = nullptr;
NodeHa* g_cluster_ha_handler;
kunlun::KLTimer *g_global_timer = nullptr;

int main(int argc, char **argv) {
  if (argc != 2) {
    printf("\nUsage: cluster_mgr /config/file/path/my_cluster_cfg.conf\n");
    return 1;
  }

  kunlun::procDaemonize();
  kunlun::procInvokeKeepalive();

  google::ParseCommandLineFlags(&argc, &argv,true);
  google::SetCommandLineOption("defer_close_second", "3600");
  google::SetCommandLineOption("idle_timeout_second", "3600");
  google::SetCommandLineOption("log_connection_close", "true");
  google::SetCommandLineOption("log_idle_connection_close", "true");


  // Init config and logger
  Configs *cfg = Configs::get_instance(argv[1]);
  //Logger::create_instance();
  int ret = 0;
  if ((ret = cfg->process_config_file(argv[1]))) {
    return ret;
  }
  std::string log_path = log_file_path.substr(0, log_file_path.rfind("/"));
  std::string log_prefix = log_file_path.substr(log_file_path.rfind("/")+1);
  Op_Log::getInstance()->init(log_path, log_prefix, max_log_file_size);

  brpc::Server *httpServer = nullptr;
  brpc::Server *collectionServer = nullptr;
  g_prometheus_manager = new KPrometheusControl();
  g_global_timer = new kunlun::KLTimer();
  
  //Thread main_thd;
  System * global_instance = nullptr;
  g_cluster_ha_handler = new NodeHa;

  if (!g_cluster_ha_handler->Launch()) {
    KLOG_ERROR("g_cluster_ha launch failed: {}", g_cluster_ha_handler->getErr());
    return 1;
  }

  for (;;) {
    g_cluster_ha_handler->wait_state_change();
#ifndef NDEBUG
    KLOG_INFO("=== cluster_mgr run in Debug mode ===");
#else
    KLOG_INFO("=== cluster_mgr run in Release mode ===");
#endif

    if (g_cluster_ha_handler->is_leader()) {
      KLOG_INFO("Current Cluster_mgr is leader now");
      if (System::create_instance(argv[1])) {
        return 1;
      }

      g_node_channel_manager = new GlobalNodeChannelManager();

      global_instance = System::get_instance();
      global_instance->setup_metadata_shard();

      bool ret = g_node_channel_manager->Init();
      if (!ret) {
        KLOG_ERROR("Errcode: {}, Errinfo: {}, ExtraInfo: {}",
               g_node_channel_manager->get_err_num(),
               g_node_channel_manager->get_err_num_str(),
               g_node_channel_manager->getExtraErr());
        fprintf(stderr, "Errcode: %d, Errinfo: %s, ExtraInfo: %s",
                g_node_channel_manager->get_err_num(),
                g_node_channel_manager->get_err_num_str(),
                g_node_channel_manager->getExtraErr());

        _exit(0);
        //return -1;
      }

      // waiting for create meta shard or network connected
      while(g_cluster_ha_handler->is_leader()) {
        if (global_instance->get_cluster_mgr_working() && 
          global_instance->setup_metadata_shard() == 0) {
          // roll back job because cluster_mgr crash
          global_instance->refresh_shards_from_metadata_server();
          global_instance->refresh_computers_from_metadata_server();
          //Job::get_instance()->job_roll_back_check();
          break;
        }
        
        KLOG_ERROR("setup_metadata_shard fail, waiting ...");
        sleep(thread_work_interval * 3);
      }

      KLOG_INFO("Cluster manager started using meta-data shard node ({}:{}).",
             meta_svr_ip.c_str(), meta_svr_port);

      g_cluster_ha_handler->UpdateMetaClusterMgrState();
      g_rfshard = new CRefreshShard();
      if(g_rfshard->start())
        KLOG_ERROR("start cluster auto refresh failed");

      httpServer = NewHttpServer();
      if (httpServer == nullptr) {
        fprintf(stderr, "start http rpc server faild");
        KLOG_ERROR("start http rpc server faild");
        return 1;
      }

      if(g_prometheus_manager->start()) 
        KLOG_ERROR("start prometheus manager failed");

      g_machine_alive = new kunlun::CMachineAlive(60);
      if(g_machine_alive->start()) {
        KLOG_ERROR("start check machine alive thread failed");
      }

      while (g_cluster_ha_handler->is_leader()) {
        if (global_instance->get_cluster_mgr_working() &&
            global_instance->setup_metadata_shard() == 0) {
          //global_instance->refresh_shards_from_metadata_server();
          //global_instance->refresh_computers_from_metadata_server();
          //global_instance->meta_shard_maintenance();
          global_instance->process_recovered_prepared();
          global_instance->dispatch_shard_process_txns();
        }
        sleep(thread_work_interval);
      }
    } else {
      if(g_prometheus_manager != nullptr) {
        g_prometheus_manager->Stop();
      }
    
      if (global_instance != nullptr) {
        delete System::get_instance();
        global_instance = nullptr;
      }
      if(g_machine_alive != nullptr) {
        delete g_machine_alive;
        g_machine_alive = nullptr;
      }

      if(g_rfshard != nullptr) {
        delete g_rfshard;
        g_rfshard = nullptr;
      }

      if (httpServer != nullptr) {
        httpServer->Stop(0);
        httpServer->Join();
        delete httpServer;
        httpServer = nullptr;
      }

      //sleep(1);
      KLOG_INFO("Current Cluster_mgr is not leader");
    }
  }

  exit(0);
}
