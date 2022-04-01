/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "http_server/http_server.h"
#include "http_server/node_channel.h"
#include "kl_mentain/config.h"
#include "kl_mentain/global.h"
#include "kl_mentain/log.h"
#include "kl_mentain/os.h"
#include "kl_mentain/sys.h"
#include "kl_mentain/job.h"
#include "kl_mentain/thread_manager.h"
#include "raft_ha/raft_ha.h"
#include "stdio.h"
#include "sys_config.h"
#include "zettalib/proc_env.h"
#include <signal.h>
#include <unistd.h>
#include "zettalib/tool_func.h"

extern int g_exit_signal;
extern int64_t thread_work_interval;
extern std::string local_ip;
extern std::string dev_interface;

GlobalNodeChannelManager g_node_channel_manager;
NodeHa g_cluster_ha_handler;

int main(int argc, char **argv) {
  if (argc != 2) {
    printf("\nUsage: cluster_mgr /config/file/path/my_cluster_cfg.conf\n");
    return 1;
  }

  kunlun::procDaemonize();
  kunlun::procInvokeKeepalive();

  // Init config and logger
  Configs *cfg = Configs::get_instance(argv[1]);
  Logger::create_instance();
  int ret = 0;
  if ((ret = cfg->process_config_file(argv[1]))) {
    return ret;
  }
  if ((ret = Logger::get_instance()->init(log_file_path)) != 0) {
    return ret;
  }
//  char addr[256] = {0};
//  if (kunlun::GetIpFromInterface(dev_interface.c_str(), addr) != 0) {
//    return false;
//  }
//  local_ip = addr;
  brpc::Server *httpServer = nullptr;
  Thread main_thd;
  System * global_instance = nullptr;

  if (!g_cluster_ha_handler.Launch()) {
    fprintf(stderr, "%s", g_cluster_ha_handler.getErr());
    return 1;
  }
  g_cluster_ha_handler.start();

  for (;;) {
    if (g_cluster_ha_handler.is_leader()) {
      syslog(Logger::INFO, "Current Cluster_mgr is leader now");
      if (System::create_instance(argv[1])) {
        return 1;
      }

      global_instance = System::get_instance();

      bool ret = g_node_channel_manager.Init();
      if (!ret) {
        syslog(Logger::ERROR, "Errcode: %d, Errinfo: %s, ExtraInfo: %s",
               g_node_channel_manager.get_err_num(),
               g_node_channel_manager.get_err_num_str(),
               g_node_channel_manager.getExtraErr());
        fprintf(stderr, "Errcode: %d, Errinfo: %s, ExtraInfo: %s",
                g_node_channel_manager.get_err_num(),
                g_node_channel_manager.get_err_num_str(),
                g_node_channel_manager.getExtraErr());

        _exit(0);
        return -1;
      }

      // waiting for create meta shard or network connected
      while(g_cluster_ha_handler.is_leader()) {
        if (System::get_instance()->get_cluster_mgr_working() && 
          System::get_instance()->setup_metadata_shard() == 0) {
          // roll back job because cluster_mgr crash
          System::get_instance()->refresh_shards_from_metadata_server();
          System::get_instance()->refresh_computers_from_metadata_server();
          //Job::get_instance()->job_roll_back_check();
          break;
        }
        
        syslog(Logger::ERROR, "setup_metadata_shard fail, waiting ...");
        Thread_manager::get_instance()->sleep_wait(&main_thd, thread_work_interval * 3000);
      }

      syslog(Logger::INFO,
             "Cluster manager started using meta-data shard node (%s:%d).",
             meta_svr_ip.c_str(), meta_svr_port);

      httpServer = NewHttpServer();
      if (httpServer == nullptr) {
        fprintf(stderr, "cluster manager start faild");
        return 1;
      }

      while (g_cluster_ha_handler.is_leader()) {
        if (System::get_instance()->get_cluster_mgr_working() &&
            System::get_instance()->setup_metadata_shard() == 0) {
          System::get_instance()->refresh_shards_from_metadata_server();
          System::get_instance()->refresh_computers_from_metadata_server();
          System::get_instance()->meta_shard_maintenance();
          System::get_instance()->process_recovered_prepared();
        }
        Thread_manager::get_instance()->sleep_wait(&main_thd,
                                                   thread_work_interval * 1000);
      }
    } else {
      if (global_instance != nullptr) {
        delete System::get_instance();
        global_instance = nullptr;
      }
      if (httpServer != nullptr) {
        httpServer->Stop(0);
        httpServer->Join();
        delete httpServer;
        httpServer = nullptr;
      }
      sleep(1);
      syslog(Logger::INFO, "Current Cluster_mgr is not leader");
    }
  }

  exit(0);
}
