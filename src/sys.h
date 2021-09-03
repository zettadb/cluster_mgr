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
#include <vector>
#include <map>

class Thread;

/*
  Singleton class for global settings and functionality.
*/
class System
{
private:
	std::vector<Computer_node *> computer_nodes;
	std::vector<Shard *> storage_shards;
	MetadataShard meta_shard;
	StorageShard storage_shard;

	std::string config_path;

	mutable pthread_mutex_t mtx;
	mutable pthread_mutexattr_t mtx_attr;

	System(const std::string&cfg_path) :
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

	int process_recovered_prepared();
	bool acquire_shard(Thread *thd, bool force);
	int setup_metadata_shard();
	int refresh_shards_from_metadata_server();
	int refresh_computers_from_metadata_server();
	int refresh_storages_info_to_computers();
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
};
#endif // !SYS_H
