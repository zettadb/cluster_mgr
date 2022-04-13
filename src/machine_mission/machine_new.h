/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef MACHINE_NEW_H
#define MACHINE_NEW_H
#include <errno.h>
#include "kl_mentain/global.h"
#include "kl_mentain/sys.h"
#include "kl_mentain/log.h"
#include "json/json.h"
#include "mysql/mysql.h"
#include "mysql/server/private/sql_cmd.h"

#include <mutex>
#include <map>
#include <set>
#include <vector>
#include <string>
#include <tuple>
#include <algorithm>

class Machine_New
{
public:
	std::string hostaddr;
	std::string rack_id;
	int total_mem;
	int total_cpu_cores;
	int nodemgr_port;
	//datadir_paths, logdir_paths, wal_log_dir_paths, comp_datadir_paths
	std::vector<std::string> vec_paths; 
	std::vector<std::vector<Tpye_Path_Used_Free>> vec_vec_path_used_free;
	bool available; 
	int instances;
	int instance_computer;
	int instance_storage;
	int port_computer;
	int port_storage;

	Machine_New();
	~Machine_New();
	bool update_machine_path_space(Json::Value &json_info);
	bool insert_machine_on_meta();
	bool update_machine_on_meta();
	bool delete_machine_on_meta();
};

#endif // !MACHINE_NEW_H
