/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "config.h"
#include "global.h"
#include "http_server/http_server.h"
#include "http_server/node_channel.h"
#include "log.h"
#include "os.h"
#include "proc_env.h"
#include "stdio.h"
#include "sys.h"
#include "sys_config.h"
#include "thread_manager.h"
#include <signal.h>
#include <unistd.h>

extern int g_exit_signal;
extern int64_t thread_work_interval;

GlobalNodeChannelManager g_node_channel_manager;

int main(int argc, char **argv)
{
  if (argc != 2)
  {
    printf("\nUsage: cluster_mgr /config/file/path/my_cluster_cfg.conf\n");
    return 1;
  }

  kunlun::procDaemonize();
  kunlun::procInvokeKeepalive();

  if (System::create_instance(argv[1]))
  {
    return 1;
  }

  syslog(Logger::INFO,
         "Cluster manager started using meta-data shard node (%s:%d).",
         meta_svr_ip.c_str(), meta_svr_port);

  Thread main_thd;

  bool ret = g_node_channel_manager.Init();
  // if (!ret || !g_node_channel_manager.Ok())
  if (!ret)
  {
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

  brpc::Server *httpServer = NewHttpServer();
  if (httpServer == nullptr)
  {
    fprintf(stderr, "cluster manager start faild");
    return 1;
  }

  //httpServer->RunUntilAskedToQuit();

  while (!Thread_manager::do_exit)
  {
    if (System::get_instance()->get_cluster_mgr_working() &&
        System::get_instance()->setup_metadata_shard() == 0)
    {
      System::get_instance()->refresh_shards_from_metadata_server();
      System::get_instance()->refresh_computers_from_metadata_server();
      System::get_instance()->meta_shard_maintenance();
      System::get_instance()->process_recovered_prepared();
    }

    Thread_manager::get_instance()->sleep_wait(&main_thd,
                                               thread_work_interval * 1000);
  }

  if (g_exit_signal)
    syslog(Logger::INFO, "Instructed to exit by signal %d.", g_exit_signal);
  else
    syslog(Logger::INFO, "Exiting because of internal error.");

  delete System::get_instance();
}
