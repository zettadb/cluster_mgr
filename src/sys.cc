/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "sys_config.h"
#include "global.h"
#include "sys.h"
#include "log.h"
#include "config.h"
#include "shard.h"
#include "thread_manager.h"
#include <utility>

System *System::m_global_instance = NULL;
extern std::string log_file_path;

System::~System()
{
	for (auto &i:kl_clusters)
		delete i;

	delete Configs::get_instance();
	delete Logger::get_instance();

	Thread_manager::do_exit = 1;
	Thread_manager::get_instance()->join_all();
	pthread_mutex_destroy(&mtx);
	pthread_mutexattr_destroy(&mtx_attr);
}

/*
  We assume the metadata cluster nodes in sn's pg_cluster_meta_nodes are always
  up to date, and we allow the metadata shard master connection configs to be
  obsolete as long as it's still in the metadata shard and all metadata shard
  nodes it has in pg_cluster_meta_nodes belong to this shard, although some of
  them could be out of the GR cluster currently.
  @retval: -1: invalid MGR topology; 
*/
int System::setup_metadata_shard()
{
	Scopped_mutex sm(mtx);
	int ret = 0;
	bool is_master = false;
	int nrows = 0, master_port = 0;
	std::string master_ip;
	Shard_node *sn = meta_shard.get_master();
	if (!sn)
	{
		sn = meta_shard.get_node_by_ip_port(meta_svr_ip, meta_svr_port);
	}
	else
	{
		is_master = true;
		sn->get_ip_port(master_ip, master_port);
		if (meta_shard.set_master(sn))
			syslog(Logger::INFO,
		   		"Found primary node of shard(%s.%s, %u) to be (%s:%d, %u)",
		   		meta_shard.get_cluster_name().c_str(),
				meta_shard.get_name().c_str(),
		   		meta_shard.get_id(), master_ip.c_str(), master_port,
				sn->get_id());
	}

	if (!sn)
	{
		sn = new Shard_node(0,
				&meta_shard, meta_svr_ip.c_str(), meta_svr_port,
				meta_svr_user.c_str(), meta_svr_pwd.c_str());

		meta_shard.add_node(sn);
	}

	ret = sn->send_stmt(SQLCOM_SELECT, CONST_STR_PTR_LEN(
		"select MEMBER_HOST, MEMBER_PORT from performance_schema.replication_group_members where MEMBER_ROLE = 'PRIMARY' and MEMBER_STATE = 'ONLINE'"), 3);
	if (ret)
		return ret;

	MYSQL_RES *result = sn->get_result();
	MYSQL_ROW row;
	char *endptr = NULL;
	Shard_node *master_sn = NULL;

	while ((row = mysql_fetch_row(result)))
	{
		unsigned long *lengths = mysql_fetch_lengths(result);
		int port = strtol(row[1], &endptr, 10);
		Assert(endptr == NULL || *endptr == '\0');

		/*
		  meta_svr_ip:meta_svr_port is the current master node,
		*/
		if (strncmp(row[0], meta_svr_ip.c_str(),
			std::min(meta_svr_ip.length(), lengths[0])) == 0 &&
			port == meta_svr_port)
		{
			is_master = true;
			master_sn = sn;
			master_ip = meta_svr_ip;
			master_port = meta_svr_port;
			meta_shard.set_master(master_sn);
		}
		else
		{
			master_ip = row[0];
			master_port = strtol(row[1], &endptr, 10);
			Assert(endptr == NULL || *endptr == '\0');
		}

		nrows++;
		if (nrows > 1)
		{
			syslog(Logger::ERROR, "Multiple(%d) primary nodes found: %s:%d.",
				   row[0], port);
		}
	}
	sn->free_mysql_result();

	if (nrows > 1)
		return -1;

	if (nrows == 0)
	{
		/*
		  This node is out of the meta shard, and we don't know any other node
		  of the meta shard, so suppose it is the master, and
		  connect to it to get the list of metadata shard nodes. If it contains
		  nodes not in current meta shard, kunlun DDC won't be able
		  to use the latest meta data.
		*/
		is_master = true;
		sn->get_ip_port(master_ip, master_port);
		if (meta_shard.set_master(sn))
			syslog(Logger::WARNING,
		   		"Suppose primary node of shard(%s.%s, %u) to be (%s:%d, %u) since it's out of the meta-shard MGR cluster. It must have latest list of metadata nodes otherwise Kunlun DDC won't be able to work correctly.",
		   		meta_shard.get_cluster_name().c_str(),
				meta_shard.get_name().c_str(),
		   		meta_shard.get_id(), master_ip.c_str(), master_port,
				sn->get_id());
	}

	/*
	  If sn is already the current master node, fetch all meta shard node from it;
	  otherwise fetch only the current master's user/pwd from it and then connect
	  to the current master to fetch all other meta shard nodes.
	*/
	meta_shard.fetch_meta_shard_nodes(sn, is_master,
		master_ip.c_str(), master_port);
	if (!is_master)
	{
		ret = meta_shard.fetch_meta_shard_nodes(sn, false,
			master_ip.c_str(), master_port);
	}

	return ret;
}

/*
  Connect to meta data master node, get all shards' configs, and update each node.
  If a node isn't in the query result, remove it.
  If a node doesn't exist, create it and add it to its shard.
*/
int System::refresh_shards_from_metadata_server()
{
	Scopped_mutex sm(mtx);
	return meta_shard.refresh_shards(kl_clusters);
}

/*
  Connect to meta data master node, get all computer' configs, and update each node.
  If a node isn't in the query result, remove it.
  If a node doesn't exist, create it and add it to its computers.
*/
int System::refresh_computers_from_metadata_server()
{
	Scopped_mutex sm(mtx);
	return meta_shard.refresh_computers(kl_clusters);
}

/*
  Connect to storage node, get tables's rows & pages, and update to computer nodes.
*/
int System::refresh_storages_info_to_computers()
{
	Scopped_mutex sm(mtx);
	for (auto &cluster:kl_clusters)
		cluster->refresh_storages_to_computers();
	return 0;
}

/*
  Connect to storage node, get num_tablets & space_volumn, and update to computer nodes and meta shard.
*/
int System::refresh_storages_info_to_computers_metashard()
{
	Scopped_mutex sm(mtx);
	for (auto &cluster:kl_clusters)
		cluster->refresh_storages_to_computers_metashard(meta_shard);
	return 0;
}

/*
  Connect to meta data master node, truncate unused commit log partitions
*/
int System::truncate_commit_log_from_metadata_server()
{
	Scopped_mutex sm(mtx);
	kl_clusters[0]->truncate_commit_log_from_metadata_server(kl_clusters, meta_shard);
	return 0;
}

/*
  Read config file and initialize config settings;
  Connect to metadata shard and get the storage shards to work on, set up
  Shard objects and the connections to each shard node.
*/
int System::create_instance(const std::string&cfg_path)
{
	m_global_instance = new System(cfg_path);
	Configs *cfg = Configs::get_instance();
	int ret = 0;

	if ((ret = Logger::create_instance()))
		goto end;
	if ((ret = cfg->process_config_file(cfg_path)))
		goto end;
	if ((ret = Logger::get_instance()->init(log_file_path)) != 0)
		goto end;
	if ((ret = m_global_instance->setup_metadata_shard()) != 0)
		goto end;
	if ((ret = m_global_instance->refresh_shards_from_metadata_server()) != 0)
		goto end;
	if ((ret = m_global_instance->refresh_computers_from_metadata_server()) != 0)
		goto end;
	Thread_manager::get_instance();
end:
	return ret;
}

/*
  Find a proper shard for worker thread 'thd' to work on.
  return true if one is found and associated with 'thd', false otherwise.

  This method should be called in the main thread, only by which the storage_shards
  is modified, so no need for a mtx lock.
*/
bool System::acquire_shard(Thread *thd, bool force)
{
	for (auto &cluster:kl_clusters)
		for (auto &sd:cluster->storage_shards)
		{
			if (sd->set_thread_handler(thd, force))
				return true;
		}

	return false;
}

