/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef SYS_H
#define SYS_H
#include "sys_config.h"
#include "global.h"
#include "shard.h"
#include "kl_cluster.h"
#include "machine_info.h"
#include "cjson.h"
#include <vector>
#include <map>

class Thread;

/*
  Singleton class for global settings and functionality.
*/
class System
{
private:
	MetadataShard meta_shard;
	std::vector<KunlunCluster *> kl_clusters;
	
	//stop working for backup/restore cluster
	bool cluster_mgr_working;

	std::string config_path;

	mutable pthread_mutex_t mtx;
	mutable pthread_mutexattr_t mtx_attr;

	System(const std::string&cfg_path) :
		cluster_mgr_working(true),
		config_path(cfg_path)
	{
		pthread_mutexattr_init(&mtx_attr);
		pthread_mutexattr_settype(&mtx_attr, PTHREAD_MUTEX_RECURSIVE);
		pthread_mutex_init(&mtx, &mtx_attr);
	}

	static System *m_global_instance;
	System(const System&);
	System&operator=(const System&);
public:
	void meta_shard_maintenance()
	{
		meta_shard.maintenance();
	}
	MetadataShard* get_MetadataShard()
	{
		return &meta_shard;
	}
	void set_cluster_mgr_working(bool stop)
	{
		cluster_mgr_working = stop;
	}
	bool get_cluster_mgr_working()
	{
		return cluster_mgr_working;
	}

	int process_recovered_prepared();
	bool acquire_shard(Thread *thd, bool force);
	int setup_metadata_shard();
	int refresh_shards_from_metadata_server();
	int refresh_computers_from_metadata_server();
	int refresh_storages_info_to_computers();
	int refresh_storages_info_to_computers_metashard();
	int truncate_commit_log_from_metadata_server();
	~System();
	static int create_instance(const std::string&cfg_path);
	static System* get_instance()
	{
		Assert(m_global_instance != NULL);
		return m_global_instance;
	}

	const std::string&get_config_path()const
	{
		Scopped_mutex sm(mtx);
		return config_path;
	}

	int execute_metadate_opertation(enum_sql_command command, const std::string & str_sql);
	int get_comp_nodes_id_seq(int &comps_id);
	int get_server_nodes_from_metadata(std::vector<Tpye_Ip_Paths> &vec_ip_paths);
	int get_backup_info_from_metadata(std::string &backup_id, std::string &cluster_name, std::string &timestamp, int &shards);
	bool check_cluster_name(std::string &cluster_name);
	bool get_meta_info(std::vector<Tpye_Ip_Port_User_Pwd> &meta);
	bool get_machine_instance_port(Machine* machine);
	bool get_node_instance(cJSON *root, std::string &str_ret);
	bool stop_cluster(std::vector <std::vector<Tpye_Ip_Port>> &vec_shard, std::vector<Tpye_Ip_Port> &comps, std::string &cluster_name);
	bool get_shard_ip_port_backup(std::string &cluster_name, std::vector<Tpye_Ip_Port> &vec_ip_port);
	bool get_shard_ip_port_restore(std::string &cluster_name, std::vector<std::vector<Tpye_Ip_Port>> &vec_vec_ip_port);
	
};
#endif // !SYS_H
