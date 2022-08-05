/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "sys.h"
#include "config.h"
#include "global.h"
//#include "log.h"
#include "zettalib/op_log.h"
#include "zettalib/tool_func.h"
#include "shard.h"
#include "func_timer.h"
#include "sys_config.h"
//#include "thread_manager.h"
#include <utility>

System *System::m_global_instance = NULL;
//extern std::string log_file_path;
extern kunlun::KLTimer *g_global_timer;
std::string local_ip;

int64_t num_worker_threads = 6;
int64_t storage_sync_interval = 60;
int64_t commit_log_retention_hours = 24;

using namespace kunlun;

System::System(const std::string& cfg_path) : cluster_mgr_working(true), config_path(cfg_path) {
	tpool_ = new CThreadPool(num_worker_threads);
  amysql_mgr_ = new CAsyncMysqlManager();
  amysql_mgr_->start();

  ObjectPtr<MetadataShard> ms(new MetadataShard());
  meta_shard = ms;
  //add timer for async mysql
  g_global_timer->AddLoopTimerUnit(CAsyncMysqlManager::CheckMysqlSocketTimeout, 
                    amysql_mgr_, 5);
}

System::~System() {
  //Thread_manager::do_exit = 1;
  //Thread_manager::get_instance()->join_all();
  cluster_mgr_working.store(false);
}

int System::setup_metadata_shard_mgr(ObjectPtr<Shard_node> sn, std::string& master_ip, 
              int& master_port) {
  bool is_master = false;
  int nrows = 0;
  //std::string master_ip;

  MysqlResult res;
  int ret = sn->send_stmt(
        "select MEMBER_HOST, MEMBER_PORT from performance_schema.replication_group_members where MEMBER_ROLE = 'PRIMARY' and MEMBER_STATE = 'ONLINE'", &res, 
      stmt_retries);
  if (ret)
    return ret;

  char *endptr = NULL;
  ObjectPtr<Shard_node> master_sn;

  nrows = res.GetResultLinesNum();
  int num = 0;
  for(uint32_t i=0; i<nrows; i++) {
    int port = strtol(res[i]["MEMBER_PORT"], &endptr, 10);
    Assert(endptr == NULL || *endptr == '\0');

    /*
      meta_svr_ip:meta_svr_port is the current master node,
    */
    if (strncmp(res[i]["MEMBER_HOST"], meta_svr_ip.c_str(),
                std::min(meta_svr_ip.length(), strlen(res[i]["MEMBER_HOST"]))) == 0 &&
        port == meta_svr_port) {
      is_master = true;
      master_sn = sn;
      master_ip = meta_svr_ip;
      master_port = meta_svr_port;
      meta_shard->set_master(master_sn);
    } else {
      master_ip = res[i]["MEMBER_HOST"];
      master_port = strtol(res[i]["MEMBER_PORT"], &endptr, 10);
      Assert(endptr == NULL || *endptr == '\0');
    }

    num++;
    if (num > 1) {
      KLOG_ERROR("Multiple primary nodes found: {}:{}.", res[i]["MEMBER_HOST"],
             port);
    }
  }

  if (nrows > 1)
    return -1;

  if (nrows == 0) {
    /*
      This node is out of the meta shard, and we don't know any other node
      of the meta shard, so suppose it is the master, and
      connect to it to get the list of metadata shard nodes. If it contains
      nodes not in current meta shard, kunlun DDC won't be able
      to use the latest meta data.
    */
    is_master = true;
    sn->get_ip_port(master_ip, master_port);
    if (meta_shard->set_master(sn)) {
      KLOG_WARN(
             "Suppose primary node of shard({}.{}, {}) to be ({}:{}, {}) since "
             "it's out of the meta-shard MGR cluster. It must have latest list "
             "of metadata nodes otherwise Kunlun DDC won't be able to work "
             "correctly.",
             meta_shard->get_cluster_name(),
             meta_shard->get_name(), meta_shard->get_id(),
             master_ip, master_port, sn->get_id());
    }
  }

  /*
    If sn is already the current master node, fetch all meta shard node from it;
    otherwise fetch only the current master's user/pwd from it and then connect
    to the current master to fetch all other meta shard nodes.
  */
  meta_shard->fetch_meta_shard_nodes(sn, is_master, master_ip.c_str(),
                                    master_port);
  if (!is_master) {
    ret = meta_shard->fetch_meta_shard_nodes(sn, false, master_ip.c_str(),
                                            master_port);
  }
  
  return ret;
}

int System::setup_metadata_shard_rbr(ObjectPtr<Shard_node> sn) {
  bool is_master = false;
  int nrows = 0, master_port = 0;
  std::string master_ip;
  char *endptr = NULL;

  MysqlResult res, result;
  int ret = sn->send_stmt(
      "select hostaddr, port, member_state from meta_db_nodes", &res, stmt_retries);
  if (ret) {
    KLOG_ERROR("get meta configuration from meta_db_nodes failed");
    return ret;
  }
  nrows = res.GetResultLinesNum();
  if(nrows < 1) {
    KLOG_ERROR("get meta configuration record zero from meta_db_nodes");
    return -1;
  }

  ret = sn->send_stmt("select host, port from performance_schema.replication_connection_configuration where channel_name='kunlun_repl'",
      &result, stmt_retries);
  if(ret) {
    KLOG_ERROR("get host and port from replication_connection_configuration failed");
    return ret;
  }
  
  int rows = result.GetResultLinesNum();
  if(rows == 0) {
    sn->get_ip_port(master_ip, master_port);
    is_master = true;
  } else if(rows == 1) {
    master_ip = result[0]["host"];
    master_port = atoi(result[0]["port"]);
  } else {
    KLOG_ERROR("get host and port too many records");
    return -1;
  }

  if(is_master) {
    meta_shard->set_master(sn);
  } else {
    if(master_ip.empty() || master_port == 0) {
      /*
        This node is out of the meta shard, and we don't know any other node
        of the meta shard, so suppose it is the master, and
        connect to it to get the list of metadata shard nodes. If it contains
        nodes not in current meta shard, kunlun DDC won't be able
        to use the latest meta data.
      */
      is_master = true;
      sn->get_ip_port(master_ip, master_port);
      if (meta_shard->set_master(sn)) {
        KLOG_WARN(
              "Suppose primary node of shard({}.{}, {}) to be ({}:{}, {}) since "
              "it's out of the meta-shard MGR cluster. It must have latest list "
              "of metadata nodes otherwise Kunlun DDC won't be able to work "
              "correctly.",
              meta_shard->get_cluster_name(),
              meta_shard->get_name(), meta_shard->get_id(),
              master_ip, master_port, sn->get_id());
      }
    } 
  }

  /*
    If sn is already the current master node, fetch all meta shard node from it;
    otherwise fetch only the current master's user/pwd from it and then connect
    to the current master to fetch all other meta shard nodes.
  */
  meta_shard->fetch_meta_shard_nodes(sn, is_master, master_ip.c_str(),
                                    master_port);
  if (!is_master) {
    ret = meta_shard->fetch_meta_shard_nodes(sn, false, master_ip.c_str(),
                                            master_port);
  }
  return ret;
}

/*
  We assume the metadata cluster nodes in sn's pg_cluster_meta_nodes are always
  up to date, and we allow the metadata shard master connection configs to be
  obsolete as long as it's still in the metadata shard and all metadata shard
  nodes it has in pg_cluster_meta_nodes belong to this shard, although some of
  them could be out of the GR cluster currently.
  @retval: -1: invalid MGR topology;
                   -2: invalid meta group seeds
*/
int System::setup_metadata_shard() {
  KlWrapGuard<KlWrapMutex> guard(mtx);
  int ret = 0;
  int retry = 0;
  bool is_master = false;
  int nrows = 0, master_port = 0;
  std::string master_ip;
  ObjectPtr<Shard_node> sn = meta_shard->get_master();

retry_group_seeds:
  if (!sn.Invalid()) {
    if (retry < vec_meta_ip_port.size()) {
      meta_svr_ip = vec_meta_ip_port[retry].first;
      meta_svr_port = vec_meta_ip_port[retry].second;
      retry++;
    } else
      return -2;

    sn = meta_shard->get_node_by_ip_port(meta_svr_ip, meta_svr_port);
  } else {
    is_master = true;
    sn->get_ip_port(master_ip, master_port);
    if (meta_shard->set_master(sn))
      KLOG_INFO(
             "Found primary node of shard({}.{}, {}) to be ({}:{}, {})",
             meta_shard->get_cluster_name(),
             meta_shard->get_name(), meta_shard->get_id(),
             master_ip, master_port, sn->get_id());
  }

  MysqlResult res;
  if (!sn.Invalid()) {
    ObjectPtr<Shard> ms(dynamic_cast<Shard*>(meta_shard.GetTRaw()));
    ObjectPtr<Shard_node> dn(new Shard_node(0, ms, meta_svr_ip.c_str(), meta_svr_port,
                        meta_svr_user.c_str(), meta_svr_pwd.c_str(), 0));
    sn = dn;

    meta_shard->add_node(sn);

    // set meta shard HAVL_mode
    ret = sn->send_stmt(
            "select value from global_configuration where name='meta_ha_mode'", &res, 
        stmt_retries);
    if (ret == 0) {
      if(res.GetResultLinesNum() > 0) {
        if (res[0]["value"] != NULL) {
          if (strcmp(res[0]["value"], "no_rep") == 0)
            meta_shard->set_mode(Shard::HAVL_mode::HA_no_rep);
          else if (strcmp(res[0]["value"], "mgr") == 0)
            meta_shard->set_mode(Shard::HAVL_mode::HA_mgr);
          else if (strcmp(res[0]["value"], "rbr") == 0)
            meta_shard->set_mode(Shard::HAVL_mode::HA_rbr);
          KLOG_INFO("set meta shard HAVL_mode as {}", res[0]["value"]);
        }
      }
    } else {
      meta_shard->remove_node(0);
      sn.SetTRaw(nullptr);
      goto retry_group_seeds;
    }
  }

  if(meta_shard->get_mode() == Shard::HAVL_mode::HA_no_rep) {
    std::vector<ObjectPtr<Shard_node> > nodes = meta_shard->get_nodes();
    if(nodes.size() > 0) {
      meta_shard->set_master(nodes[0]);
    }
  } else if(meta_shard->get_mode() == Shard::HAVL_mode::HA_mgr) {
    std::string master_ip;
    int master_port=0;
    ret = setup_metadata_shard_mgr(sn, master_ip, master_port);
    if(ret)
      return ret;
  } else if(meta_shard->get_mode() == Shard::HAVL_mode::HA_rbr) {
    ret = setup_metadata_shard_rbr(sn);
    if(ret)
      return ret;
    
    ret = sn->send_stmt(
            "select sync_num, degrade_conf_state, degrade_conf_time, degrade_run_state from meta_db_nodes where member_state='source'", 
                &res, stmt_retries);
    if(ret) {
      KLOG_ERROR("In rbr mode, get meta_db_nodes failed, {}", sn->get_mysql_err());
      return ret;
    }

    if(res.GetResultLinesNum() > 0) {
      std::string sync_num = res[0]["sync_num"];
      meta_shard->set_fullsync_level(atoi(sync_num.c_str()));
      bool conf_state = (strcmp(res[0]["degrade_conf_state"],"ON") ==0 ? true : false);
      meta_shard->set_degrade_conf_state(conf_state);
      std::string conf_time = res[0]["degrade_conf_time"];
      meta_shard->set_degrade_conf_time(atoi(conf_time.c_str()));
      bool run_state = (strcmp(res[0]["degrade_run_state"],"ON") == 0 ? true : false);
      meta_shard->set_degrade_run_state(run_state);
    }
  }
  
  return ret;
}

/*
  Connect to meta data master node, get all shards' configs, and update each
  node. If a node isn't in the query result, remove it. If a node doesn't exist,
  create it and add it to its shard.
*/
int System::refresh_shards_from_metadata_server() {
  KlWrapGuard<KlWrapMutex> guard(mtx);
  return meta_shard->refresh_shards(kl_clusters);
}

/*
  Connect to meta data master node, get all computer' configs, and update each
  node. If a node isn't in the query result, remove it. If a node doesn't exist,
  create it and add it to its computers.
*/
int System::refresh_computers_from_metadata_server() {
  KlWrapGuard<KlWrapMutex> guard(mtx);
  return meta_shard->refresh_computers(kl_clusters);
}

/*
  Connect to storage node, get tables's rows & pages, and update to computer
  nodes.
*/
int System::refresh_storages_info_to_computers() {
  KlWrapGuard<KlWrapMutex> guard(mtx);
  for (auto &cluster : kl_clusters) {
    if(cluster->get_cluster_delete_state())
      continue;
    cluster->refresh_storages_to_computers();
  }
  return 0;
}

/*
  Connect to storage node, get num_tablets & space_volumn, and update to
  computer nodes and meta shard.
*/
int System::refresh_storages_info_to_computers_metashard() {
  KlWrapGuard<KlWrapMutex> guard(mtx);
  for (auto &cluster : kl_clusters) {
    if(cluster->get_cluster_delete_state())
      continue;
    cluster->refresh_storages_to_computers_metashard(meta_shard);
  }
  return 0;
}

/*
  Connect to meta data master node, truncate unused commit log partitions
*/
int System::truncate_commit_log_from_metadata_server() {
  KlWrapGuard<KlWrapMutex> guard(mtx);
  if(kl_clusters.size() <= 0)
    return 0;
    
  kl_clusters[0]->truncate_commit_log_from_metadata_server(kl_clusters,
                                                           meta_shard);
  return 0;
}

/*
* deal create database/table left in SN node
*/
void System::scan_cn_ddl_undoing_jobs() {
  for(auto cluster : kl_clusters) {
    if(cluster->get_cluster_delete_state())
      continue;

    cluster->deal_cn_ddl_undoing_jobs(meta_shard);
  }
}

/*
  Read config file and initialize config settings;
  Connect to metadata shard and get the storage shards to work on, set up
  Shard objects and the connections to each shard node.
*/
int System::create_instance(const std::string &cfg_path) {
  m_global_instance = new System(cfg_path);
  // Configs *cfg = Configs::get_instance();
  int ret = 0;

  m_global_instance->get_tpool()->commit([=]{
    int commit_log_count = 0;
	  int commit_log_count_max = commit_log_retention_hours*60*60/storage_sync_interval;
    
    while(true) {
      if(m_global_instance->get_cluster_mgr_working()) {
        m_global_instance->refresh_storages_info_to_computers();
        m_global_instance->refresh_storages_info_to_computers_metashard();
        m_global_instance->scan_cn_ddl_undoing_jobs();

        if(commit_log_count++ >= commit_log_count_max) {
          commit_log_count = 0;
          m_global_instance->truncate_commit_log_from_metadata_server();
        }
      } else
        break;

      sleep(storage_sync_interval);
    }
  });

  // if ((ret = Logger::create_instance()))
  //  goto end;
  // if ((ret = cfg->process_config_file(cfg_path)))
  //  goto end;
  // if ((ret = Logger::get_instance()->init(log_file_path)) != 0)
  //  goto end;
  //if ((ret = (Thread_manager::get_instance() == NULL)) != 0)
  //  goto end;
  //if ((ret = (Machine_info::get_instance() == NULL)) != 0)
  //  goto end;

end:
  return ret;
}

/*
  Find a proper shard for worker thread 'thd' to work on.
  return true if one is found and associated with 'thd', false otherwise.

  This method should be called in the main thread, only by which the
  storage_shards is modified, so no need for a mtx lock.
*/
/*bool System::acquire_shard(Thread *thd, bool force) {
  for (auto &cluster : kl_clusters)
    for (auto &sd : cluster->storage_shards) {
      if (sd->set_thread_handler(thd, force))
        return true;
    }

  return false;
}*/

// the next several function for auto cluster operation
int System::execute_metadate_opertation(enum_sql_command command,
                                        const std::string &str_sql) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  return meta_shard->execute_metadate_opertation(command, str_sql);
}

int System::get_max_cluster_id(int &cluster_id) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  return meta_shard->get_max_cluster_id(cluster_id);
}

int System::get_max_shard_name_id(std::string &cluster_name, int &shard_id) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  // storage shard
  for (auto &cluster : kl_clusters) {
    if (cluster_name != cluster->get_name())
      continue;

    // compare every shard name id
    for (auto &shard : cluster->storage_shards) {
      char *p = (char *)(shard->get_name().c_str());
      if (p == NULL)
        continue;

      // skip no digit
      while (*p < '0' || *p > '9')
        p++;

      int i = atoi(p);
      if (shard_id < i)
        shard_id = i;
    }

    break;
  }

  return 0;
}

int System::get_max_comp_name_id(std::string &cluster_name, int &comp_id) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  // storage shard
  for (auto &cluster : kl_clusters) {
    if (cluster_name != cluster->get_name())
      continue;

    // compare every shard name id
    for (auto &comp : cluster->computer_nodes) {
      char *p = (char *)(comp->get_name().c_str());
      if (p == NULL)
        continue;

      // skip no digit
      while (*p < '0' || *p > '9')
        p++;

      int i = atoi(p);
      if (comp_id < i)
        comp_id = i;
    }

    break;
  }

  return 0;
}





bool System::get_backup_info_from_metadata(
    std::string &cluster_name, std::string &timestamp,
    std::vector<std::string> &vec_shard) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  if (meta_shard->get_backup_info_from_metadata(cluster_name, timestamp,
                                               vec_shard)) {
    // syslog(Logger::ERROR, "get_backup_info_from_metadata error");
    return false;
  }

  return true;
}

bool System::get_cluster_info_from_metadata(std::string &cluster_name,
                                            std::string &json_buf) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  if (meta_shard->get_cluster_info_from_metadata(cluster_name, json_buf)) {
    // syslog(Logger::ERROR, "get_cluster_info_from_metadata error");
    return false;
  }

  return true;
}

bool System::get_machine_info_from_metadata(
    std::vector<std::string> &vec_machine) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  if (meta_shard->get_machine_info_from_metadata(vec_machine)) {
    // syslog(Logger::ERROR, "get_machine_info_from_metadata error");
    return false;
  }

  return true;
}

bool System::get_server_nodes_stats_id(std::string &hostaddr, std::string &id){
  KlWrapGuard<KlWrapMutex> guard(mtx);

  if (meta_shard->get_server_nodes_stats_id(hostaddr, id)) {
    // syslog(Logger::ERROR, "get_server_nodes_stats_id error");
    return false;
  }

  return true;
}

bool System::check_backup_storage_name(std::string &name) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  if (meta_shard->check_backup_storage_name(name)) {
    // syslog(Logger::ERROR, "check_backup_storage error");
    return false;
  }

  return true;
}

bool System::check_machine_hostaddr(std::string &hostaddr) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  if (meta_shard->check_machine_hostaddr(hostaddr)) {
    // syslog(Logger::ERROR, "check_machine_hostaddr error");
    return false;
  }

  return true;
}

bool System::check_cluster_name(std::string &cluster_name) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

	for (auto &cluster:kl_clusters)
		if(cluster_name == cluster->get_name())
			return true;

	return false;
}

bool System::check_nick_name(std::string &nick_name) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  if (meta_shard->check_nick_name(nick_name)) {
    // syslog(Logger::ERROR, "check_nick_name error");
    return false;
  }

  return true;
}

bool System::rename_cluster(std::string &cluster_name, std::string &nick_name) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  std::string str_sql;
  str_sql = "UPDATE db_clusters set nick_name='" + nick_name +
            "' where name='" + cluster_name + "'";
  // syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

  if (meta_shard->execute_metadate_opertation(SQLCOM_UPDATE, str_sql)) {
    // syslog(Logger::ERROR, "rename_cluster error");
    return false;
  }

  for (auto &cluster : kl_clusters) {
    if (cluster_name != cluster->get_name())
      continue;

    // rename nick name
    cluster->set_nick_name(nick_name);

    break;
  }

  return true;
}



bool System::check_cluster_shard_ip_port(std::string &cluster_name, 
                                std::string &shard_name, 
                                Tpye_Ip_Port &ip_port){
  KlWrapGuard<KlWrapMutex> guard(mtx);

  for (auto &cluster : kl_clusters) {
    if (cluster_name == cluster->get_name()){
      for (auto &shard : cluster->storage_shards) {
        if (shard_name == shard->get_name()){
          for (auto &node : shard->get_nodes()) {
            if(node->matches_ip_port(ip_port.first, ip_port.second))
              return true;
          }
          break;
        }
      }
      break;
    }
  }

  return false;
}


bool System::get_cluster_shard_name(std::string &cluster_name,
                                    std::vector<std::string> &vec_shard_name) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  // storage shard
  for (auto &cluster : kl_clusters) {
    if (cluster_name != cluster->get_name())
      continue;

    // get every shard name
    for (auto &shard : cluster->storage_shards) {
      vec_shard_name.emplace_back(shard->get_name());
    }

    break;
  }
  return (vec_shard_name.size() > 0);
}



bool System::update_instance_status(Tpye_Ip_Port &ip_port, std::string &status,
                                    int &type) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  if (meta_shard->update_instance_status(ip_port, status, type)) {
    // syslog(Logger::ERROR, "update_instance_status error");
    return false;
  }

  return true;
}

bool System::get_backup_storage_string(std::string &name,
                                       std::string &backup_storage_id,
                                       std::string &backup_storage_str) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  if (meta_shard->get_backup_storage_string(name, backup_storage_id,
                                           backup_storage_str)) {
    // syslog(Logger::ERROR, "get_backup_storage_string error");
    return false;
  }

  return true;
}

bool System::get_shards_ip_port(std::string &cluster_name,
    std::vector<std::vector<Tpye_Ip_Port>> &vec_vec_shard) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  for (auto &cluster : kl_clusters) {
    if (cluster_name != cluster->get_name())
      continue;

    // get ip and port
    for (auto &shard : cluster->storage_shards) {
      std::vector<Tpye_Ip_Port> vec_storage_ip_port;
      for (auto &node : shard->get_nodes()) {
        std::string ip;
        int port;
        node->get_ip_port(ip, port);
        vec_storage_ip_port.emplace_back(std::make_pair(ip, port));
      }
      vec_vec_shard.emplace_back(vec_storage_ip_port);
    }

    break;
  }

  return true;
}

bool System::get_shards_ip_port(std::string &cluster_name,
                                std::string &shard_name,
                                std::vector<Tpye_Ip_Port> &vec_shard) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  for (auto &cluster : kl_clusters) {
    if (cluster_name != cluster->get_name())
      continue;

    // get ip and port
    for (auto &shard : cluster->storage_shards) {
      if (shard_name != shard->get_name())
        continue;

      for (auto &node : shard->get_nodes()) {
        std::string ip;
        int port;
        node->get_ip_port(ip, port);
        vec_shard.emplace_back(std::make_pair(ip, port));
      }

      break;
    }

    break;
  }

  return (vec_shard.size() > 0);
}

bool System::get_comps_ip_port(std::string &cluster_name,
                               std::vector<Tpye_Ip_Port> &vec_comp) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  for (auto &cluster : kl_clusters) {
    if (cluster_name != cluster->get_name())
      continue;

    // get ip and port
    for (auto &comp : cluster->computer_nodes) {
      std::string ip;
      int port;
      comp->get_ip_port(ip, port);
      vec_comp.emplace_back(std::make_pair(ip, port));
    }

    break;
  }

  return true;
}

bool System::get_comps_ip_port(std::string &cluster_name,
                               std::string &comp_name,
                               std::vector<Tpye_Ip_Port> &vec_comp) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  for (auto &cluster : kl_clusters) {
    if (cluster_name != cluster->get_name())
      continue;

    // get ip and port
    for (auto &comp : cluster->computer_nodes) {
      if (comp_name != comp->get_name())
        continue;

      std::string ip;
      int port;
      comp->get_ip_port(ip, port);
      vec_comp.emplace_back(std::make_pair(ip, port));

      break;
    }

    break;
  }

  return true;
}

bool System::get_meta_ip_port(std::vector<Tpye_Ip_Port> &vec_meta) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

	for(auto &node: meta_shard->get_nodes())	{
		std::string ip;
		int port;
		node->get_ip_port(ip, port);

		vec_meta.emplace_back(std::make_pair(ip, port));
	}

	return (vec_meta.size() > 0);
}



bool System::stop_cluster(std::string &cluster_name) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  for (auto cluster_it = kl_clusters.begin();
       cluster_it != kl_clusters.end();) {
    if (cluster_name != (*cluster_it)->get_name()) {
      cluster_it++;
      continue;
    }

    // remove cluster
    //delete *cluster_it;
    kl_clusters.erase(cluster_it);
    break;
  }

  if (meta_shard->delete_cluster_from_metadata(cluster_name)) {
    KLOG_ERROR("delete_cluster_from_metadata error");
    return false;
  }

  return true;
}


bool System::get_shard_info_for_backup(
    std::string &cluster_name, std::string &cluster_id,
    std::vector<Tpye_Shard_Id_Ip_Port_Id> &vec_shard_id_ip_port_id) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  std::string ip;
  int port;

  for (auto &cluster : kl_clusters) {
    if (cluster_name != cluster->get_name())
      continue;

    cluster_id = std::to_string(cluster->get_id());

    // sort shard by id
    sort(cluster->storage_shards.begin(), cluster->storage_shards.end(),
         [](ObjectPtr<Shard> a, ObjectPtr<Shard> b) { return a->get_id() < b->get_id(); });

    // get a no master node for backup in every shard
    for (auto &shard : cluster->storage_shards) {
      ObjectPtr<Shard_node> cur_master = shard->get_master();
      ObjectPtr<Shard_node> backup_node;
      auto vec_node = shard->get_nodes();
      if (vec_node.size() == 0) {
        return false;
      } else if (vec_node.size() == 1) {
        backup_node = vec_node[0];
      } else {
        for (auto node : vec_node)
          if (cur_master->get_id() != node->get_id()) {
            backup_node = node;
            break;
          }
      }

      if (!backup_node.Invalid())
        return false;

      backup_node->get_ip_port(ip, port);
      vec_shard_id_ip_port_id.emplace_back(std::make_tuple(
          shard->get_name(), shard->get_id(), ip, port, backup_node->get_id()));
    }

    break;
  }

  return (vec_shard_id_ip_port_id.size() > 0);
}

bool System::get_node_info_for_backup(
    std::string &cluster_name, std::string &shard_name, std::string &cluster_id,
    Tpye_Shard_Id_Ip_Port_Id &shard_id_ip_port_id) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  std::string ip;
  int port;

  for (auto &cluster : kl_clusters) {
    if (cluster_name != cluster->get_name())
      continue;

    cluster_id = cluster->get_id();

    // get a no master node for backup in every shard
    for (auto &shard : cluster->storage_shards) {
      if (shard_name != shard->get_name())
        continue;

      ObjectPtr<Shard_node> cur_master = shard->get_master();
      ObjectPtr<Shard_node> backup_node;
      auto vec_node = shard->get_nodes();
      if (vec_node.size() == 0) {
        return false;
      } else if (vec_node.size() == 1) {
        backup_node = vec_node[0];
      } else {
        for (auto node : vec_node)
          if (cur_master->get_id() != node->get_id()) {
            backup_node = node;
            break;
          }
      }

      if (!backup_node.Invalid())
        return false;

      backup_node->get_ip_port(ip, port);
      shard_id_ip_port_id = std::make_tuple(shard->get_name(), shard->get_id(),
                                            ip, port, backup_node->get_id());

      return true;
    }

    break;
  }

  return false;
}

bool System::get_shard_ip_port_restore(
    std::string &cluster_name,
    std::vector<std::vector<Tpye_Ip_Port>> &vec_vec_ip_port) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  std::string ip;
  int port;

  // storage node
  for (auto &cluster : kl_clusters) {
    if (cluster_name != cluster->get_name())
      continue;

    // sort shard by id
    sort(cluster->storage_shards.begin(), cluster->storage_shards.end(),
         [](ObjectPtr<Shard> a, ObjectPtr<Shard> b) { return a->get_id() < b->get_id(); });

    // get every node for restore in every shard
    for (auto &shard : cluster->storage_shards) {
      std::vector<Tpye_Ip_Port> vec_ip_port;
      for (auto &node : shard->get_nodes()) {
        node->get_ip_port(ip, port);
        vec_ip_port.emplace_back(std::make_pair(ip, port));
      }
      vec_vec_ip_port.emplace_back(vec_ip_port);
    }

    break;
  }

  return (vec_vec_ip_port.size() > 0);
}

bool System::get_comps_ip_port_restore(std::string &cluster_name,
                                       std::vector<Tpye_Ip_Port> &vec_ip_port) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  std::string ip;
  int port;

  // computer node
  for (auto &cluster : kl_clusters) {
    if (cluster_name != cluster->get_name())
      continue;

    // get every node for restore
    for (auto &node : cluster->computer_nodes) {
      node->get_ip_port(ip, port);
      vec_ip_port.emplace_back(std::make_pair(ip, port));
    }

    break;
  }

  return (vec_ip_port.size() > 0);
}
bool System::get_shard_map_for_restore(std::string &backup_cluster_name,
                                       std::string &restore_cluster_name,
                                       std::string &shard_map) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  std::vector<std::string> vec_backup_shard;
  std::vector<std::string> vec_restore_shard;

  for (auto &cluster : kl_clusters) {
    if (backup_cluster_name == cluster->get_name()) {
      // sort shard by id
      sort(cluster->storage_shards.begin(), cluster->storage_shards.end(),
           [](ObjectPtr<Shard> a, ObjectPtr<Shard> b) { return a->get_id() < b->get_id(); });

      // get every shard
      for (auto &shard : cluster->storage_shards)
        vec_backup_shard.emplace_back(std::to_string(shard->get_id()));
    } else if (restore_cluster_name == cluster->get_name()) {
      // sort shard by id
      sort(cluster->storage_shards.begin(), cluster->storage_shards.end(),
           [](ObjectPtr<Shard> a, ObjectPtr<Shard> b) { return a->get_id() < b->get_id(); });

      // get every shard
      for (auto &shard : cluster->storage_shards)
        vec_restore_shard.emplace_back(std::to_string(shard->get_id()));
    }
  }

  if (vec_backup_shard.size() == 0 || vec_restore_shard.size() == 0 ||
      vec_backup_shard.size() != vec_restore_shard.size())
    return false;

  shard_map = "{";
  for (int i = 0; i < vec_backup_shard.size(); i++) {
    if (i > 0)
      shard_map += ",";
    shard_map += vec_backup_shard[i] + ":" + vec_restore_shard[i];
  }
  shard_map += "}";

  return true;
}

bool System::get_cluster_shards_nodes_comps(std::string &cluster_name,
                                            int &shards, int &nodes,
                                            int &comps) {
  KlWrapGuard<KlWrapMutex> guard(mtx);
  bool ret = false;

  for (auto &cluster : kl_clusters) {
    if (cluster_name != cluster->get_name())
      continue;

    shards = cluster->storage_shards.size();
    if (shards == 0)
      return false;

    nodes = cluster->storage_shards[0]->get_nodes().size();
    comps = cluster->computer_nodes.size();
    ret = true;

    break;
  }

  return ret;
}

bool System::get_cluster_shard_variable(std::string &cluster_name, 
                                    std::string &shard_name, 
                                    std::string &variable, 
                                    std::string &value){
  KlWrapGuard<KlWrapMutex> guard(mtx);
  bool ret = false;

  // storage node
  for (auto &cluster : kl_clusters) {
    if (cluster_name == cluster->get_name()){
      for (auto &shard : cluster->storage_shards) {
        if (shard_name == shard->get_name()){
          for (auto &node : shard->get_nodes()) {
            if (node.Invalid()){
              if(node->get_variables(variable, value) == 0)
                ret = true;
              break;
            }
          }
          break;
        }
      }
      break;
    }
  }

  return ret;
}

bool System::get_cluster_no_rep_mode(std::string &cluster_name) {
  KlWrapGuard<KlWrapMutex> guard(mtx);
  bool ret = false;

  // storage node
  for (auto &cluster : kl_clusters) {
    if (cluster_name == cluster->get_name()){
      // get no_rep mode from master of shard
      for (auto &shard : cluster->storage_shards) {
        if (shard->get_mode() == Shard::HAVL_mode::HA_no_rep)
          ret = true;
        break;
      }

      break;
    }
  }

  return ret;
}

bool System::clear_cluster_shard_master(std::string &cluster_name) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  // storage node
  for (auto &cluster : kl_clusters) {
    if (cluster_name != cluster->get_name())
      continue;

    // set every shard master to NULL
    for (auto &shard : cluster->storage_shards) {
      shard->set_master(NULL);
    }

    break;
  }

  return true;
}

bool System::update_instance_cluster_info(std::string &cluster_name) {
  KlWrapGuard<KlWrapMutex> guard(mtx);
  bool ret = true;

  // storage node
  for (auto &cluster : kl_clusters) {
    if (cluster_name != cluster->get_name())
      continue;

    // check every shard master
    for (auto &shard : cluster->storage_shards) {
      if (!shard->get_master().Invalid()) {
        ret = false;
        break;
      }
    }

    // update cluster_info by master of shard
    if (ret) {
      KLOG_INFO("every shard have get master");
      for (auto &shard : cluster->storage_shards) {
        if (shard->get_master()->update_instance_cluster_info()) {
          KLOG_ERROR("update_instance_cluster_info error");
          ret = false;
          break;
        }
      }
    }

    break;
  }

  return ret;
}

bool System::update_shard_group_seeds(std::string &cluster_name, 
                              std::string &shard_name, 
                              std::string &group_seeds) {
#if 0
  Scopped_mutex sm(mtx);

  std::string variable = "group_replication_group_seeds";
  std::string type = "string";

  for (auto &cluster : kl_clusters) {
    if (cluster_name == cluster->get_name()){
      for (auto &shard : cluster->storage_shards) {
        if (shard_name == shard->get_name()){
          for (auto &node : shard->get_nodes()) {
            if(node->set_variables(variable, type, group_seeds))
              return false;
          }
          break;
        }
      }
      break;
    }
  }
#endif
  return true;
}


bool System::get_cluster_info(std::string &cluster_name, Json::Value &json){
  KlWrapGuard<KlWrapMutex> guard(mtx);

  for (auto &cluster : kl_clusters) {
    if (cluster_name == cluster->get_name()){
      json["cluster_name"] = cluster_name;
      json["cluster_id"] = std::to_string(cluster->get_id());
      json["shards"] = std::to_string(cluster->storage_shards.size());
      json["comps"] = std::to_string(cluster->computer_nodes.size());
      break;
    }
  }

  return true;
}

bool System::get_cluster_shard_info(std::string &cluster_name, 
                                    std::vector<std::string> &vec_shard_name, 
                                    Json::Value &json){
  KlWrapGuard<KlWrapMutex> guard(mtx);

  for (auto &cluster : kl_clusters) {
    if (cluster_name == cluster->get_name()){

      Json::Value shard_list;
      json["cluster_name"] = cluster_name;
      json["cluster_id"] = std::to_string(cluster->get_id());
      json["shards"] = std::to_string(cluster->storage_shards.size());
      json["comps"] = std::to_string(cluster->computer_nodes.size());

      for(auto &shard_name: vec_shard_name)
        for (auto &shard : cluster->storage_shards)
          if (shard_name == shard->get_name()){
            Json::Value shard_json;
            shard_json["shard_name"] = shard_name;
            shard_json["shard_id"] = std::to_string(shard->get_id());
            shard_list.append(shard_json);
            break;
          }

      json["list_shard"] = shard_list;
      break;
    }
  }

  return true;
}

bool System::get_cluster_comp_info(std::string &cluster_name, 
                                    std::vector<std::string> &vec_comp_name, 
                                    Json::Value &json){
  KlWrapGuard<KlWrapMutex> guard(mtx);

  for (auto &cluster : kl_clusters) {
    if (cluster_name == cluster->get_name()){

      Json::Value comp_list;
      json["cluster_name"] = cluster_name;
      json["cluster_id"] = std::to_string(cluster->get_id());
      json["shards"] = std::to_string(cluster->storage_shards.size());
      json["comps"] = std::to_string(cluster->computer_nodes.size());

      for(auto &comp_name: vec_comp_name)
        for (auto &comp : cluster->computer_nodes)
          if (comp_name == comp->get_name()){
            Json::Value comp_json;
            comp_json["comp_name"] = comp_name;
            comp_json["comp_id"] = std::to_string(comp->get_id());
            comp_list.append(comp_json);
            break;
          }

      json["list_comp"] = comp_list;
      break;
    }
  }

  return true;
}

bool System::get_cluster_shard_node_info(std::string &cluster_name, 
                                          std::vector<std::string> &vec_shard_name, 
                                          std::vector <std::vector<Tpye_Ip_Port_Paths>> &vec_shard_storage_ip_port_paths, 
                                          Json::Value &json){
  KlWrapGuard<KlWrapMutex> guard(mtx);

  for (auto &cluster : kl_clusters) {
    if (cluster_name == cluster->get_name()){

      Json::Value list_shard;
      json["cluster_name"] = cluster_name;
      json["cluster_id"] = std::to_string(cluster->get_id());
      json["shards"] = std::to_string(cluster->storage_shards.size());
      json["comps"] = std::to_string(cluster->computer_nodes.size());

      for(int i=0; i<vec_shard_name.size(); i++)
        for (auto &shard : cluster->storage_shards)
          if (vec_shard_name[i] == shard->get_name()){
            Json::Value list_node;
            Json::Value list_shard_array;

            list_shard_array["shard_name"] = vec_shard_name[i];
            list_shard_array["shard_id"] = std::to_string(shard->get_id());
            list_shard_array["nodes"] = std::to_string(shard->get_nodes().size());

            for(int j=0; j<vec_shard_storage_ip_port_paths[i].size(); j++)
              for (auto &node : shard->get_nodes()) {
                std::string hostaddr;
                int port;
                
                node->get_ip_port(hostaddr, port);
                if(hostaddr == std::get<0>(vec_shard_storage_ip_port_paths[i][j]) 
                      && port == std::get<1>(vec_shard_storage_ip_port_paths[i][j])){
                  Json::Value list_node_array;
                  list_node_array["hostaddr"] = hostaddr;
                  list_node_array["port"] = std::to_string(port);
                  list_node_array["node_id"] = std::to_string(node->get_id());
                  list_node.append(list_node_array);
                }
              }
            
            list_shard_array["list_node"] = list_node;
            list_shard.append(list_shard_array);
          }
      
      json["list_shard"] = list_shard;
      break;
    }
  }

  return true;
}

bool System::get_meta_mode(Json::Value &attachment) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  std::string mode;
  if (meta_shard->get_mode() == Shard::HAVL_mode::HA_no_rep)
    mode = "no_rep";
  else if (meta_shard->get_mode() == Shard::HAVL_mode::HA_mgr)
    mode = "mgr";
  else if (meta_shard->get_mode() == Shard::HAVL_mode::HA_rbr)
    mode = "rbr";

  attachment["mode"] = mode;
  attachment["status"] = "done";
  attachment["error_code"] = "0";
  attachment["error_info"] = "";

  return true;
}

bool System::get_meta_summary(Json::Value &attachment) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  Json::Value list_meta;
  attachment["status"] = "done";
  attachment["error_code"] = "0";
  attachment["error_info"] = "";

  for (auto &node : meta_shard->get_nodes()) {
    std::string hostaddr;
    int port;

    node->get_ip_port(hostaddr, port);

    Json::Value list_array;
    list_array["hostaddr"] = hostaddr;
    list_array["port"] = std::to_string(port);
    if (node->connect_status())
      list_array["status"] = "online";
    else
      list_array["status"] = "offline";

    if (node->is_master())
      list_array["master"] = "true";
    else
      list_array["master"] = "false";

    list_meta.append(list_array);
  }
  attachment["list_meta"] = list_meta;

  return true;
}

bool System::get_backup_storage(Json::Value &attachment) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  std::vector<Tpye_string4> vec_t_string4;
  meta_shard->get_backup_storage_list(vec_t_string4);

  Json::Value list_backup_storage;
  attachment["status"] = "done";
  attachment["error_code"] = "0";
  attachment["error_info"] = "";

  for (auto &t_string4 : vec_t_string4) {
    Json::Value list_array;
    list_array["name"] = std::get<0>(t_string4);
    list_array["stype"] = std::get<1>(t_string4);
    list_array["hostaddr"] = std::get<2>(t_string4);
    list_array["port"] = std::get<3>(t_string4);
    list_backup_storage.append(list_array);
  }
  attachment["list_backup_storage"] = list_backup_storage;

  return true;
}

bool System::get_cluster_summary(Json::Value &attachment) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  Json::Value list_cluster;
  attachment["status"] = "done";
  attachment["error_code"] = "0";
  attachment["error_info"] = "";

  for (auto &cluster : kl_clusters) {
    Json::Value list_array;

    list_array["cluster_name"] = cluster->get_name();
    list_array["nick_name"] = cluster->get_nick_name();
    list_array["shards"] = std::to_string(cluster->storage_shards.size());
    list_array["comps"] = std::to_string(cluster->computer_nodes.size());

    int shard_node_offline = 0;
    for (auto &shard : cluster->storage_shards)
      for (auto &node : shard->get_nodes())
        if (!node->connect_status())
          shard_node_offline++;
    list_array["storage_offine"] = std::to_string(shard_node_offline);

    int comp_node_offline = 0;
    for (auto &comp : cluster->computer_nodes)
      if (!comp->connect_status())
        comp_node_offline++;
    list_array["computer_offine"] = std::to_string(comp_node_offline);

    list_cluster.append(list_array);
  }
  attachment["list_cluster"] = list_cluster;

  return true;
}

bool System::get_cluster_detail(Json::Value &paras, Json::Value &attachment) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  std::string cluster_name;
  if (!paras.isMember("cluster_name")) {
    KLOG_ERROR("missing `cluster_name` key-value pair in the request body");
    return false;
  }
  cluster_name = paras["cluster_name"].asString();

  for (auto &cluster : kl_clusters) {
    if (cluster_name == cluster->get_name()){
      Json::Value list_shard;
      Json::Value list_comp;
      attachment["status"] = "done";
      attachment["error_code"] = "0";
      attachment["error_info"] = "";
      attachment["cluster_name"] = cluster->get_name();
      attachment["nick_name"] = cluster->get_nick_name();
      attachment["shards"] = std::to_string(cluster->storage_shards.size());
      attachment["comps"] = std::to_string(cluster->computer_nodes.size());

      for (auto &shard : cluster->storage_shards) {
        Json::Value list_node;
        Json::Value list_shard_array;
        list_shard_array["shard_name"] = shard->get_name();
        list_shard_array["nodes"] = std::to_string(shard->get_nodes().size());

        for (auto &node : shard->get_nodes()) {
          Json::Value list_node_array;
          std::string hostaddr;
          int port;

          node->get_ip_port(hostaddr, port);
          list_node_array["hostaddr"] = hostaddr;
          list_node_array["port"] = std::to_string(port);

          if (node->connect_status())
            list_node_array["status"] = "online";
          else
            list_node_array["status"] = "offline";

          if (node->is_master())
            list_node_array["master"] = "true";
          else
            list_node_array["master"] = "false";

          list_node.append(list_node_array);
        }
        list_shard_array["list_node"] = list_node;
        list_shard.append(list_shard_array);
      }
      attachment["list_shard"] = list_shard;

      for (auto &comp : cluster->computer_nodes) {
        Json::Value list_array;
        std::string hostaddr;
        int port;

        comp->get_ip_port(hostaddr, port);
        list_array["comp_name"] = comp->get_name();
        list_array["hostaddr"] = hostaddr;
        list_array["port"] = std::to_string(port);

        if (comp->connect_status())
          list_array["status"] = "online";
        else
          list_array["status"] = "offline";

        list_comp.append(list_array);
      }
      attachment["list_comp"] = list_comp;

      break;
    }
  }

  return true;
}

bool System::get_variable(Json::Value &paras, Json::Value &attachment) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  std::string variable, hostaddr, value;
  int port;
  if (!paras.isMember("hostaddr")) {
    KLOG_ERROR("missing `hostaddr` key-value pair in the request body");
    return false;
  }
  hostaddr = paras["hostaddr"].asString();

  if (!paras.isMember("port")) {
    KLOG_ERROR("missing `port` key-value pair in the request body");
    return false;
  }
  port = stoi(paras["port"].asString());

  if (!paras.isMember("variable")) {
    KLOG_ERROR("missing `variable` key-value pair in the request body");
    return false;
  }
  variable = paras["variable"].asString();

  attachment["status"] = "done";
  attachment["error_code"] = "0";
  attachment["error_info"] = "";

/*
  for (auto &node : meta_shard.get_nodes()) {
    if (node->matches_ip_port(hostaddr, port)) {
      if (node->get_variables(variable, value) == 0){
        attachment["result"] = "true";
        attachment["value"] = value;
      }else{
        attachment["result"] = "false";
      }
      return true;
    }
  }
*/

  for (auto &cluster : kl_clusters) {
    for (auto &shard : cluster->storage_shards) {
      for (auto &node : shard->get_nodes()) {
        if (node->matches_ip_port(hostaddr, port)) {
          if (node->get_variables(variable, value) == 0){
            attachment["result"] = "true";
            attachment["value"] = value;
          }else{
            attachment["result"] = "false";
          }
          return true;
        }
      }
    }

    for (auto &comp : cluster->computer_nodes) {
      if (comp->matches_ip_port(hostaddr, port)) {
        if (comp->get_variables(variable, value) == 0){
          attachment["result"] = "true";
          attachment["value"] = value;
        }else{
          attachment["result"] = "false";
        }
        return true;
      }
    }
  }

  return false;
}

bool System::set_variable(Json::Value &paras, Json::Value &attachment) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  std::string variable, hostaddr, result, type, value;
  int port;

  if (!paras.isMember("hostaddr")) {
    KLOG_ERROR("missing `hostaddr` key-value pair in the request body");
    return false;
  }
  hostaddr = paras["hostaddr"].asString();

  if (!paras.isMember("port")) {
    KLOG_ERROR("missing `port` key-value pair in the request body");
    return false;
  }
  port = stoi(paras["port"].asString());

  if (!paras.isMember("variable")) {
    KLOG_ERROR("missing `variable` key-value pair in the request body");
    return false;
  }
  variable = paras["variable"].asString();

  if (!paras.isMember("variable")) {
    KLOG_ERROR("missing `variable` key-value pair in the request body");
    return false;
  }
  variable = paras["variable"].asString();

  if (!paras.isMember("type")) {
    KLOG_ERROR("missing `type` key-value pair in the request body");
    return false;
  }
  type = paras["type"].asString();

  if (!paras.isMember("value")) {
    KLOG_ERROR("missing `value` key-value pair in the request body");
    return false;
  }
  value = paras["value"].asString();

  attachment["status"] = "done";
  attachment["error_code"] = "0";
  attachment["error_info"] = "";
  
/*
  for (auto &node : meta_shard.get_nodes()) {
    if (node->matches_ip_port(ip, port)) {
      if (node->set_variables(variable, type, value) == 0){
        attachment["result"] = "true";
      }else{
        attachment["result"] = "false";
      }
      return true;
    }
  }
*/

  for (auto &cluster : kl_clusters) {
    for (auto &shard : cluster->storage_shards) {
      for (auto &node : shard->get_nodes()) {
        if (node->matches_ip_port(hostaddr, port)) {
          if (node->set_variables(variable, type, value) == 0){
            attachment["result"] = "true";
          }else{
            attachment["result"] = "false";
          }
          return true;
        }
      }
    }

    for (auto &comp : cluster->computer_nodes) {
      if (comp->matches_ip_port(hostaddr, port)) {
        if (comp->set_variables(variable, type, value) == 0){
          attachment["result"] = "true";
        }else{
          attachment["result"] = "false";
        }
        return true;
      }
    }
  }

  return false;
}

std::vector<ObjectPtr<Shard> > System::get_storage_shards() {
  std::vector<ObjectPtr<Shard> > storage_shards;
  for (auto &cluster : kl_clusters) {
    if(cluster->get_cluster_delete_state())
      continue;

    storage_shards.insert(storage_shards.end(), cluster->storage_shards.begin(),
                      cluster->storage_shards.end());
  }

  ObjectPtr<Shard> shard(dynamic_cast<Shard*>(meta_shard.GetTRaw()));
  storage_shards.emplace_back(shard);
  return storage_shards;
}

std::vector<ObjectPtr<Shard> > System::get_shard_by_cluster_id(uint cluster_id) {
  std::vector<ObjectPtr<Shard> > cluster_shards;
  for (auto &cluster : kl_clusters) {
    if(cluster->get_id() == cluster_id) {
      cluster_shards.insert(cluster_shards.end(), cluster->storage_shards.begin(),
                      cluster->storage_shards.end());
    }
  }
  return cluster_shards;
} 

bool System::update_cluster_delete_state(uint cluster_id) {
  KlWrapGuard<KlWrapMutex> guard(mtx);
  for (auto &cluster : kl_clusters) {
    if(cluster->get_id() == cluster_id) {
      cluster->set_cluster_delete_state(true);
    }
  }
  return true;
}

void System::add_shard_cluster_memory(int cluster_id, const std::string& cluster_name, 
			const std::string& nick_name, const std::string& ha_mode,
			const std::vector<Shard_Type>& shard_params) {
  KlWrapGuard<KlWrapMutex> guard(mtx);
  meta_shard->add_shard_cluster_memory(kl_clusters, cluster_id, cluster_name, nick_name,
            ha_mode, shard_params);
}

void System::add_computer_cluster_memory(int cluster_id, const std::string& cluster_name, 
			const std::string& nick_name, const std::vector<Comps_Type>& comps_params) {
  KlWrapGuard<KlWrapMutex> guard(mtx);
  meta_shard->add_computer_cluster_memory(kl_clusters, cluster_id, cluster_name, 
          nick_name, comps_params);
}

void System::add_node_shard_memory(int cluster_id, int shard_id, 
              const std::vector<CNode_Type>& node_params) {
  KlWrapGuard<KlWrapMutex> guard(mtx);
  meta_shard->add_node_shard_memory(kl_clusters, cluster_id, shard_id, 
          node_params);
}

void System::del_shard_cluster_memory(int cluster_id, const std::vector<int>& shard_ids) {
  KlWrapGuard<KlWrapMutex> guard(mtx);

  for (auto cluster_it = kl_clusters.begin();
       cluster_it != kl_clusters.end();) {
    if (cluster_id != (*cluster_it)->get_id()) {
      cluster_it++;
      continue;
    }

    // remove storage and shard
    for (auto shard_it = (*cluster_it)->storage_shards.begin();
         shard_it != (*cluster_it)->storage_shards.end();) {
      int del_id = (*shard_it)->get_id();
      if(std::find(shard_ids.begin(), shard_ids.end(), del_id) == shard_ids.end()) {
        shard_it++;
        continue;
      }

      for (auto node_it = (*shard_it)->get_nodes().begin();
           node_it != (*shard_it)->get_nodes().end();) {
        //delete *node_it;
        node_it = (*shard_it)->get_nodes().erase(node_it);
      }

      //delete *shard_it;
      shard_it = (*cluster_it)->storage_shards.erase(shard_it);
    }

    break;
  }
}

void System::del_node_shard_memory(int cluster_id, int shard_id, const std::string& host) {
  KlWrapGuard<KlWrapMutex> guard(mtx);
  for (auto cluster_it = kl_clusters.begin();
       cluster_it != kl_clusters.end(); cluster_it++) {
    if (cluster_id != (*cluster_it)->get_id()) {
      continue;
    }

    for (auto shard_it = (*cluster_it)->storage_shards.begin();
         shard_it != (*cluster_it)->storage_shards.end(); shard_it++) {
      if(shard_id != (*shard_it)->get_id()) {
        continue;
      }

      std::vector<ObjectPtr<Shard_node> > snodes = (*shard_it)->get_nodes();
      for(auto node_it : snodes) {
        std::string ip;
        int port;
        node_it->get_ip_port(ip, port);
        if((ip+"_"+std::to_string(port)) == host) {
          int id = node_it->get_id();
          (*shard_it)->remove_node(id);
          return;
        }
      }
    }
  }
}

void System::del_computer_cluster_memory(int cluster_id, 
          const std::vector<std::string>& comps_hosts) {
  KlWrapGuard<KlWrapMutex> guard(mtx);
  for (auto cluster_it = kl_clusters.begin();
       cluster_it != kl_clusters.end();) {
    if (cluster_id != (*cluster_it)->get_id()) {
      cluster_it++;
      continue;
    }

    // remove storage and shard
    for (auto comp_it = (*cluster_it)->computer_nodes.begin();
         comp_it != (*cluster_it)->computer_nodes.end();) {
      std::string ip;
      int port;
      (*comp_it)->get_ip_port(ip, port);
      std::string host = ip +"_"+std::to_string(port);
      if(std::find(comps_hosts.begin(), comps_hosts.end(), host) == comps_hosts.end()) {
        comp_it++;
        continue;
      }

      //delete *comp_it;
      comp_it = (*cluster_it)->computer_nodes.erase(comp_it);
    }

    break;
  }
}

void System::update_shard_master(ObjectPtr<Shard> shard, const std::string& host) {
  KlWrapGuard<KlWrapMutex> guard(mtx);
  std::string ip = host.substr(0, host.rfind("_"));
  std::string port = host.substr(host.rfind("_")+1);
  ObjectPtr<Shard_node> node = shard->get_node_by_ip_port(ip, atoi(port.c_str()));
  if(node.Invalid()) {
    shard->set_master(node);
  }
}

ObjectPtr<Shard> System::get_shard_by_host(const std::string& host) {
  for (auto cluster : kl_clusters) {
    if(cluster->get_cluster_delete_state())
      continue;

    std::vector<ObjectPtr<Shard> > shards = cluster->storage_shards;
    for(auto ss : shards) {
      if(ss->get_is_delete())
        continue;

      std::vector<ObjectPtr<Shard_node> > nodes = ss->get_nodes();
      for(auto ns : nodes) {
        if(!ns.Invalid())
          continue;

        if(ns->match_host(host)) {
          return ss;
        }
      }
    }
  }
  return nullptr;
}

ObjectPtr<Shard_node> System::get_node_by_host(const std::string& host) {
  for (auto cluster : kl_clusters) {
    if(cluster->get_cluster_delete_state())
      continue;

    std::vector<ObjectPtr<Shard> > shards = cluster->storage_shards;
    for(auto ss : shards) {
      if(ss->get_is_delete())
        continue;

      std::vector<ObjectPtr<Shard_node> > nodes = ss->get_nodes();
      for(auto ns : nodes) {
        if(!ns.Invalid())
          continue;
          
        if(ns->match_host(host)) {
          return ns;
        }
      }
    }
  }
  return nullptr;
}

int System::chk_host_is_master_node(const std::string& host) {
  ObjectPtr<Shard> shard = get_shard_by_host(host);
  if(!shard.Invalid())
    return 2;

  ObjectPtr<Shard_node> cur_master = shard->get_master();
  if(!cur_master.Invalid())
    return 2;

  if(cur_master->match_host(host))
    return 0;

  return 1;
}

CAsyncMysql* System::get_amysql_by_host(const std::string& host) {
  ObjectPtr<Shard_node> node = get_node_by_host(host);
  if(node.Invalid())
    return node->get_async_mysql();

  return nullptr;
}

ObjectPtr<Shard> System::get_shard_by_node(ObjectPtr<Shard_node> node) {
  std::string host = node->get_host();
  return get_shard_by_host(host);
}