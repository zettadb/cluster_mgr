/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef NODE_INFO_H
#define NODE_INFO_H
#include "sys_config.h"
#include <errno.h>
#include "global.h"
#include "json/json.h"

#include <pthread.h>
#include <mutex>
#include <map>
#include <set>
#include <vector>
#include <string>
#include <tuple>
#include <algorithm>

class Machine
{
public:
	std::string hostaddr;
	std::string rack_id;
	int total_mem;
	int total_cpu_cores;
	//datadir_paths, logdir_paths, wal_log_dir_paths, comp_datadir_paths
	std::vector<std::string> vec_paths; 
	std::vector<std::vector<Tpye_Path_Used_Free>> vec_vec_path_used_free;
	bool available; 
	int instances;
	int instance_computer;
	int instance_storage;
	int port_computer;
	int port_storage;

	Machine();
	~Machine();
};

class Machine_info
{
private:
	static Machine_info *m_inst;
	Machine_info();

	std::mutex mutex_nodes_;
	std::map<std::string, Machine*> map_machine;

public:
	~Machine_info();
	static Machine_info *get_instance()
	{
		if (!m_inst) m_inst = new Machine_info();
		return m_inst;
	}

	bool insert_machine_on_meta(Machine *machine);
	bool update_machine_on_meta(Machine *machine);
	bool delete_machine_on_meta(Machine *machine);
	bool update_machine_path_space(Machine *machine, Json::Value &json_info);
	bool get_machines_info(std::vector<Machine*> &vec_machine, std::set<std::string> &set_machine);
	bool check_machines_path(std::vector<Machine*> &vec_machine);
	bool get_storage_nodes(int nodes, int &nodes_select, 
			std::vector<Tpye_Ip_Port_Paths> &vec_ip_port_paths, std::vector<Machine*> &vec_machine);
	bool get_computer_nodes(int nodes, int &nodes_select, 
			std::vector<Tpye_Ip_Port_Paths> &vec_ip_port_paths, std::vector<Machine*> &vec_machine);
};

#endif // !NODE_INFO_H
