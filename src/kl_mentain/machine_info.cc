/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "sys_config.h"
#include "global.h"
#include "log.h"
#include "sys.h"
#include "machine_info.h"
#include "http_server/node_channel.h"
#include "request_framework/syncBrpc.h"
#include <errno.h>
#include <unistd.h>

Machine_info* Machine_info::m_inst = NULL;
extern int64_t node_mgr_http_port;

Machine::Machine():	instances(0), instance_computer(0),instance_storage(0),
	port_computer(0),port_storage(0)
{

}

Machine::~Machine()
{

}

Machine_info::Machine_info()
{

}

Machine_info::~Machine_info()
{
	for(auto &node: map_machine)
		delete node.second;
}

void Machine_info::notify_node_update(std::set<std::string> &alterant_node_ip, int type) {

	SyncBrpc syncBrpc;
	Json::Value root_node;
	Json::Value paras;
	root_node["cluster_mgr_request_id"] = "update_instance";
	root_node["task_spec_info"] = "update_instance";
	root_node["job_type"] = "update_instance";

	if(type == 0)
		paras["instance_type"] = "meta_instance";
	else if(type == 1)
		paras["instance_type"] = "storage_instance";
	else if(type == 2)
		paras["instance_type"] = "computer_instance";
	root_node["paras"] = paras;

	for(auto node_ip: alterant_node_ip)
		syncBrpc.syncBrpcToNode(node_ip, root_node);
}

bool Machine_info::insert_machine_on_meta(Machine *machine)
{
	std::string str_sql;
	std::vector<std::pair<int,int>> vec_used_free;

	for(auto &vec_path_used_free: machine->vec_vec_path_used_free) 
	{
		int used = 0;
		int free = 0;

		for(auto &path_used_free:vec_path_used_free) 
		{
			used += std::get<1>(path_used_free);
			free += std::get<2>(path_used_free);
		}
		vec_used_free.emplace_back(std::make_pair(used, free));
	}

	//insert server_nodes
	str_sql = "INSERT INTO server_nodes(hostaddr,rack_id,datadir,logdir,wal_log_dir,comp_datadir,total_mem,total_cpu_cores,svc_since) VALUES('";
	str_sql += machine->hostaddr + "','" + machine->rack_id + "','";
	str_sql += machine->vec_paths[0] + "','" + machine->vec_paths[1] + "','" + machine->vec_paths[2] + "','" + machine->vec_paths[3] + "',";
	str_sql += std::to_string(machine->total_mem) + "," + std::to_string(machine->total_cpu_cores) + ",NOW())";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_INSERT, str_sql))	
	{
		syslog(Logger::ERROR, "insert server_nodes error");
		return false;
	}

	//insert server_nodes_stats
	str_sql = "INSERT INTO server_nodes_stats(id,datadir_used,datadir_avail,wal_log_dir_used,wal_log_dir_avail,log_dir_used,log_dir_avail,";
	str_sql += "comp_datadir_used,comp_datadir_avail,avg_network_usage_pct) VALUES((select id from server_nodes where hostaddr='" + machine->hostaddr + "'),";
	str_sql += std::to_string(vec_used_free[0].first) + "," + std::to_string(vec_used_free[0].second) + ",";
	str_sql += std::to_string(vec_used_free[1].first) + "," + std::to_string(vec_used_free[1].second) + ",";
	str_sql += std::to_string(vec_used_free[2].first) + "," + std::to_string(vec_used_free[2].second) + ",";
	str_sql += std::to_string(vec_used_free[3].first) + "," + std::to_string(vec_used_free[3].second) + ",0)";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_INSERT, str_sql))	
	{
		syslog(Logger::ERROR, "insert server_nodes_stats error!");
		return false;
	}

	return true;
}

bool Machine_info::update_machine_on_meta(Machine *machine)
{
	std::string str_sql;
	std::vector<std::pair<int,int>> vec_used_free;

	for(auto &vec_path_used_free: machine->vec_vec_path_used_free) 
	{
		int used = 0;
		int free = 0;

		for(auto &path_used_free:vec_path_used_free) 
		{
			used += std::get<1>(path_used_free);
			free += std::get<2>(path_used_free);
		}
		vec_used_free.emplace_back(std::make_pair(used, free));
	}

	//update server_nodes
	str_sql = "UPDATE server_nodes set rack_id='" + machine->rack_id + "',datadir='" + machine->vec_paths[0] + "',logdir='";
	str_sql += machine->vec_paths[1] + "',wal_log_dir='" + machine->vec_paths[2] + "',comp_datadir='" + machine->vec_paths[3];
	str_sql += "',total_mem=" + std::to_string(machine->total_mem) + ",total_cpu_cores=" + std::to_string(machine->total_cpu_cores);
	str_sql += " where hostaddr='" + machine->hostaddr + "'";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_UPDATE, str_sql))	
	{
		syslog(Logger::ERROR, "update server_nodes error");
		return false;
	}

	//update server_nodes_stats
	str_sql = "UPDATE server_nodes_stats set datadir_used=" + std::to_string(vec_used_free[0].first) + ",datadir_avail=" + std::to_string(vec_used_free[0].second);
	str_sql += ",log_dir_used=" + std::to_string(vec_used_free[1].first) + ",log_dir_avail=" + std::to_string(vec_used_free[1].second);
	str_sql += ",wal_log_dir_used=" + std::to_string(vec_used_free[2].first) + ",wal_log_dir_avail=" + std::to_string(vec_used_free[2].second);
	str_sql += ",comp_datadir_used=" + std::to_string(vec_used_free[3].first) + ",comp_datadir_avail=" + std::to_string(vec_used_free[3].second);
	str_sql += " where id=(select id from server_nodes where hostaddr='" + machine->hostaddr + "')";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_UPDATE, str_sql)) 
	{
		syslog(Logger::ERROR, "update server_nodes_stats error");
		return false;
	}

	return true;
}

bool Machine_info::delete_machine_on_meta(Machine *machine)
{
	std::string str_sql;

	//delete server_nodes_stats
	str_sql = "DELETE FROM server_nodes_stats where id=(select id from server_nodes where hostaddr='" + machine->hostaddr + "')";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_DELETE, str_sql))	
	{
		syslog(Logger::ERROR, "delete server_nodes_stats error");
		return false;
	}

	//delete server_nodes
	str_sql = "DELETE FROM server_nodes where hostaddr='" + machine->hostaddr + "'";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_DELETE, str_sql))	
	{
		syslog(Logger::ERROR, "delete server_nodes error");
		return false;
	}

	return true;
}

bool Machine_info::update_machine_path_space(Machine *machine, Json::Value &json_info)
{
	std::string path;
	int used,free;

	std::vector<std::string> vec_path_index;
	vec_path_index.emplace_back("path0");
	vec_path_index.emplace_back("path1");
	vec_path_index.emplace_back("path2");
	vec_path_index.emplace_back("path3");

	machine->vec_vec_path_used_free.clear();

	for(int i=0; i<4; i++) {
		std::vector<Tpye_Path_Used_Free> vec_path_used_free;

		Json::Value json_path = json_info[vec_path_index[i]];
		int n = json_path.size();
		if(n == 0)
			return false;

		for(int j=0; j<n; j++) {
			Json::Value path_sub = json_path[j];
			path = path_sub["path"].asString();
			used = path_sub["used"].asInt();
			free = path_sub["free"].asInt();
			vec_path_used_free.emplace_back(std::make_tuple(path, used, free));
		}

		//sort the path by free size
		sort(vec_path_used_free.begin(), vec_path_used_free.end(), 
				[](Tpye_Path_Used_Free a, Tpye_Path_Used_Free b){return std::get<2>(a) > std::get<2>(b);});

		machine->vec_vec_path_used_free.emplace_back(vec_path_used_free);
	}

	return true;
}

bool Machine_info::get_machines_info(std::vector<Machine*> &vec_machine, std::set<std::string> &set_machine)
{
	std::lock_guard<std::mutex> lock(mutex_nodes_);

	/////////////////////////////////////////////////////////
	//update the machines from meta data table
	if(!System::get_instance()->update_server_nodes_from_metadata(map_machine))
	{
		syslog(Logger::ERROR, "update_server_nodes_from_metadata error");
		return false;
	}

	if(set_machine.size() > 0)
	{
		for(auto &hostaddr: set_machine)
		{
			auto iter = map_machine.find(hostaddr);
			if(iter != map_machine.end())
				vec_machine.emplace_back(iter->second);
		}
	}
	else
	{
		for(auto &machine: map_machine)
			vec_machine.emplace_back(machine.second);
	}

	if(vec_machine.size() == 0)
		return false;

	/////////////////////////////////////////////////////////
	//get the instances and ports of ervery node from meta table
	for(auto &machine: vec_machine) {
		System::get_instance()->get_machine_instance_port(machine);
	}

	//sort the nodes by instances
	sort(vec_machine.begin(), vec_machine.end(), 
			[](Machine *a, Machine *b){return a->instances < b->instances;});

	return true;
}

bool Machine_info::check_machines_path(std::vector<Machine*> &vec_machine)
{
	bool bPathOk = true;
	for(auto &machine: vec_machine)
	{
		if(machine->vec_paths.size() != 4)
		{
			bPathOk = false;
			break;
		}

		if(machine->vec_paths[0].empty() || machine->vec_paths[1].empty() ||
			machine->vec_paths[2].empty() || machine->vec_paths[3].empty())
		{
			bPathOk = false;
			break;
		}
	}		

	return bPathOk;
}

bool Machine_info::get_storage_nodes(int nodes, int &nodes_select, 
	std::vector<Tpye_Ip_Port_Paths> &vec_ip_port_paths, std::vector<Machine*> &vec_machine)
{
	std::lock_guard<std::mutex> lock(mutex_nodes_);

	if(vec_machine.size() == 0)
		return false;

	int node_allocate = nodes_select;
	int node_finish = 0;
	while(node_finish<nodes)
	{
		std::vector<std::string> vec_paths;
		vec_paths.emplace_back(std::get<0>(vec_machine[node_allocate]->vec_vec_path_used_free[0][0]));
		vec_paths.emplace_back(std::get<0>(vec_machine[node_allocate]->vec_vec_path_used_free[1][0]));
		vec_paths.emplace_back(std::get<0>(vec_machine[node_allocate]->vec_vec_path_used_free[2][0]));

		std::vector<int> vec_port;
		vec_port.emplace_back(vec_machine[node_allocate]->port_storage);
		vec_port.emplace_back(vec_machine[node_allocate]->port_storage+1);
		vec_port.emplace_back(vec_machine[node_allocate]->port_storage+2);
		vec_machine[node_allocate]->port_storage += 3;

		vec_ip_port_paths.emplace_back(std::make_tuple(vec_machine[node_allocate]->hostaddr, 
									vec_machine[node_allocate]->port_storage, vec_paths));

		vec_machine[node_allocate]->port_storage += 3;
		node_finish++;
		node_allocate++;
		node_allocate %= vec_machine.size();
	}
	
	// reset nodes_select
	if(nodes < vec_machine.size())
		nodes_select = (nodes_select+nodes)%vec_machine.size();
	else
		nodes_select = (nodes_select+1)%vec_machine.size();

	return true;
}

bool Machine_info::get_computer_nodes(int nodes, int &nodes_select, 
	std::vector<Tpye_Ip_Port_Paths> &vec_ip_port_paths, std::vector<Machine*> &vec_machine)
{
	std::lock_guard<std::mutex> lock(mutex_nodes_);

	if(vec_machine.size() == 0)
		return false;

	// allocate available node
	int node_allocate = nodes_select;
	int node_finish = 0;
	while(node_finish<nodes)
	{
		std::vector<std::string> vec_paths;
		vec_paths.emplace_back(std::get<0>(vec_machine[node_allocate]->vec_vec_path_used_free[3][0]));

		std::vector<int> vec_port;
		vec_port.emplace_back(vec_machine[node_allocate]->port_computer);
		vec_machine[node_allocate]->port_computer += 1;

		vec_ip_port_paths.emplace_back(std::make_tuple(vec_machine[node_allocate]->hostaddr, 
									vec_machine[node_allocate]->port_computer, vec_paths));
		
		vec_machine[node_allocate]->port_computer += 1;
		node_finish++;
		node_allocate++;
		node_allocate %= vec_machine.size();
	}
	
	// reset nodes_select
	nodes_select = (nodes_select+nodes)%vec_machine.size();

	return true;
}