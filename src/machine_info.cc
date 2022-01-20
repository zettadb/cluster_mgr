/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "sys_config.h"
#include "global.h"
#include "cjson.h"
#include "log.h"
#include "job.h"
#include "sys.h"
#include "machine_info.h"
#include "http_client.h"
#include <errno.h>
#include <unistd.h>

Machine_info* Machine_info::m_inst = NULL;
extern int64_t node_mgr_http_port;

Machine::Machine(std::string &ip_, std::vector<std::string> &vec_paths_, Tpye_string3 &t_string3):
	ip(ip_),rack_id(std::get<0>(t_string3)),vec_paths(vec_paths_),instances(0),
	instance_computer(0),instance_storage(0),port_computer(0),port_storage(0)
{
	total_mem = atoi(std::get<1>(t_string3).c_str());
	total_cpu_cores = atoi(std::get<2>(t_string3).c_str());
}

Machine::~Machine()
{

}

Machine_info::Machine_info()
{

}

Machine_info::~Machine_info()
{
	for(auto &node: vec_machines)
		delete node;
}

bool Machine_info::insert_machine_to_table(Machine* machine)
{
	std::string str_sql;
	std::vector<std::pair<int,int>> vec_used_free;

	for(auto &vec_path_used_free:machine->vec_vec_path_used_free)
	{
		int used =0;
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
	str_sql += machine->ip + "','" + machine->rack_id + "','";
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
	str_sql += "comp_datadir_used,comp_datadir_avail,avg_network_usage_pct) VALUES((select id from server_nodes where hostaddr='" + machine->ip + "'),";
	str_sql += std::to_string(vec_used_free[0].first) + "," + std::to_string(vec_used_free[0].second) + ",";
	str_sql += std::to_string(vec_used_free[1].first) + "," + std::to_string(vec_used_free[1].second) + ",";
	str_sql += std::to_string(vec_used_free[2].first) + "," + std::to_string(vec_used_free[2].second) + ",";
	str_sql += std::to_string(vec_used_free[3].first) + "," + std::to_string(vec_used_free[3].second) + ",0)";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_INSERT, str_sql))
	{
		syslog(Logger::ERROR, "insert server_nodes_stats error");
		return false;
	}

	return true;
}

bool Machine_info::update_machine_in_table(Machine* machine)
{
	std::string str_sql;
	std::vector<std::pair<int,int>> vec_used_free;

	for(auto &vec_path_used_free:machine->vec_vec_path_used_free)
	{
		int used =0;
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
	str_sql += " where hostaddr='" + machine->ip + "'";
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
	str_sql += " where id=(select id from server_nodes where hostaddr='" + machine->ip + "')";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_UPDATE, str_sql))
	{
		syslog(Logger::ERROR, "update server_nodes_stats error");
		return false;
	}

	return true;
}

bool Machine_info::delete_machine_from_table(std::string &ip)
{
	std::string str_sql;

	str_sql = "delete from server_nodes_stats where id=(select id from server_nodes where hostaddr='" + ip + "')";
	syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_DELETE, str_sql))
	{
		syslog(Logger::ERROR, "delete server_nodes error");
		return false;
	}

	str_sql = "delete from server_nodes where hostaddr='" + ip + "'";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_DELETE, str_sql))
	{
		syslog(Logger::ERROR, "delete server_nodes error");
		return false;
	}

	return true;
}

bool Machine_info::get_machine_path_space(Machine* machine, std::string &info)
{
	bool ret = false;
	std::string path;
	int u_used,u_free;

	cJSON *root = NULL;
	cJSON *item;
	cJSON *item_sub;
	char *cjson = NULL;

	cJSON *ret_root = NULL;
	cJSON *ret_item;
	cJSON *ret_item_paths;
	cJSON *ret_item_sub_sub;
	
	std::vector<std::string> vec_path_index;
	vec_path_index.emplace_back("paths0");
	vec_path_index.emplace_back("paths1");
	vec_path_index.emplace_back("paths2");
	vec_path_index.emplace_back("paths3");

	if(machine->vec_paths.size() != 4)
	{
		syslog(Logger::ERROR, "vec_paths.size() must be 4");	
		return false;
	}
	machine->vec_vec_path_used_free.clear();

	root = cJSON_CreateObject();
	cJSON_AddStringToObject(root, "job_type", "get_path_space");
	for(int i=0; i<4; i++)
		cJSON_AddStringToObject(root, vec_path_index[i].c_str(), machine->vec_paths[i].c_str());
	cjson = cJSON_Print(root);
	cJSON_Delete(root);
	root = NULL;
	
	std::string post_url = "http://" + machine->ip + ":" + std::to_string(node_mgr_http_port);
	//syslog(Logger::INFO, "post_url=%s",post_url.c_str());
	
	std::string result, result_str;
	int retry = 3;
	while(retry-->0)
	{
		if(Http_client::get_instance()->Http_client_post_para(post_url.c_str(), cjson, result_str)==0)
		{
			//syslog(Logger::INFO, "get_machine_path_space result_str=%s",result_str.c_str());

			ret_root = cJSON_Parse(result_str.c_str());
			if(ret_root == NULL)
			{
				syslog(Logger::ERROR, "cJSON_Parse error");	
				goto end;
			}

			ret_item = cJSON_GetObjectItem(ret_root, "result");
			if(ret_item == NULL || ret_item->valuestring == NULL)
			{
				syslog(Logger::ERROR, "get result error");
				goto end;
			}
			result = ret_item->valuestring;

			ret_item = cJSON_GetObjectItem(ret_root, "info");
			if(ret_item == NULL || ret_item->valuestring == NULL)
			{
				syslog(Logger::ERROR, "get info error");
				goto end;
			}
			info = ret_item->valuestring;

			if(result == "error")
			{
				syslog(Logger::ERROR, "get result error");
				goto end;
			}
			else if(result == "succeed")
			{
				for(int i=0; i<4; i++)
				{
					int n;
					std::vector<Tpye_Path_Used_Free> vec_path_used_free;

					ret_item_paths = cJSON_GetObjectItem(ret_root, vec_path_index[i].c_str());
					if(ret_item == NULL)
					{
						syslog(Logger::ERROR, "get %s error", vec_path_index[i].c_str());
						goto end;
					}

					n = cJSON_GetArraySize(ret_item_paths);
					for(int i=0; i<n; i++)
					{
						ret_item_sub_sub = cJSON_GetArrayItem(ret_item_paths,i);
						if(ret_item_sub_sub == NULL)
						{
							syslog(Logger::ERROR, "get sub paths error");
							goto end;
						}

						ret_item = cJSON_GetObjectItem(ret_item_sub_sub, "path");
						if(ret_item == NULL)
						{
							syslog(Logger::ERROR, "get path error");
							goto end;
						}
						path = ret_item->valuestring;

						ret_item = cJSON_GetObjectItem(ret_item_sub_sub, "used");
						if(ret_item == NULL)
						{
							syslog(Logger::ERROR, "get used error");
							goto end;
						}
						u_used = ret_item->valueint;

						ret_item = cJSON_GetObjectItem(ret_item_sub_sub, "free");
						if(ret_item == NULL)
						{
							syslog(Logger::ERROR, "get free error");
							goto end;
						}
						u_free = ret_item->valueint;

						vec_path_used_free.emplace_back(std::make_tuple(path, u_used, u_free));
					}
					machine->vec_vec_path_used_free.emplace_back(vec_path_used_free);
				}

				ret = true;
				break;
			}
		}
	}

	if(retry<0)
	{
		info = machine->ip + " is error";
	}

end:
	if(ret_root!=NULL)
		cJSON_Delete(ret_root);
	if(root!=NULL)
		cJSON_Delete(root);
	if(cjson!=NULL)
		free(cjson);
	
	return ret;
}

bool Machine_info::create_machine(std::string &ip, std::vector<std::string> &vec_paths, Tpye_string3 &t_string3, std::string &info)
{
	Machine machine(ip, vec_paths, t_string3);

	if(!get_machine_path_space(&machine, info))
	{
		syslog(Logger::ERROR, "get_machine_path_space error: %s", info.c_str());
		return false;
	}

	if(!insert_machine_to_table(&machine))
	{
		info = "insert_machine_to_table error";
		syslog(Logger::ERROR, "%s", info.c_str());
		return false;
	}

	return true;
}

bool Machine_info::update_machine(std::string &ip, std::vector<std::string> &vec_paths, Tpye_string3 &t_string3, std::string &info)
{
	Machine machine(ip, vec_paths, t_string3);

	if(!get_machine_path_space(&machine, info))
	{
		syslog(Logger::ERROR, "get_machine_path_space error: %s", info.c_str());
		return false;
	}

	if(!update_machine_in_table(&machine))
	{
		info = "update_machine_in_table error";
		syslog(Logger::ERROR, "%s", info.c_str());
		return false;
	}

	return true;
}

bool Machine_info::delete_machine(std::string &ip, std::string &info)
{
	if(!delete_machine_from_table(ip))
	{
		info = "delete_machine_from_table error";
		syslog(Logger::ERROR, "%s", info.c_str());
		return false;
	}

	return true;
}

bool Machine_info::update_machines_info()
{
	std::lock_guard<std::mutex> lock(mutex_nodes_);

	for(auto &machine: vec_machines)
		delete machine;
	vec_machines.clear();

	/////////////////////////////////////////////////////////
	//get the machines from meta data table
	if(System::get_instance()->get_server_nodes_from_metadata(vec_machines))
	{
		syslog(Logger::ERROR, "get_server_nodes_from_metadata error");
		return false;
	}

	/////////////////////////////////////////////////////////
	//get the info of path space form every machine ip
	for(auto machine=vec_machines.begin(); machine!=vec_machines.end(); )
	{
		std::string info;
		if(!get_machine_path_space(*machine, info))
		{
			syslog(Logger::ERROR, "get_machine_path_space error: %s", info.c_str());
			delete *machine;
			machine = vec_machines.erase(machine); //remove unavailable machine
		}
		else
		{
			if(!update_machine_in_table(*machine))
			{
				syslog(Logger::ERROR, "update_machine_in_table error");
				return false;
			}
			machine++;
		}
	}

	//sort the every paths of ervery machine by space
	for(auto &machine: vec_machines)
	{
		for(auto &vec_path_used_free: machine->vec_vec_path_used_free)
		{
			//sort the path by free size
			sort(vec_path_used_free.begin(), vec_path_used_free.end(), 
					[](Tpye_Path_Used_Free a, Tpye_Path_Used_Free b){return std::get<2>(a) > std::get<2>(b);});
		}
	}

	/////////////////////////////////////////////////////////
	//get the instances and ports of ervery node from meta table
	for(auto &machine: vec_machines)
	{
		System::get_instance()->get_machine_instance_port(machine);
	}

	//sort the nodes by instances
	sort(vec_machines.begin(), vec_machines.end(), [](Machine *a, Machine *b){return a->instances < b->instances;});
	//start from the least instances of node
	nodes_select = 0;

	//reset the max port of port_computer for every node
	int port_computer_max = 0;
	for(auto &machine: vec_machines)
	{
		if(machine->port_computer > port_computer_max)
			port_computer_max = machine->port_computer;
	}
	for(auto &machine: vec_machines)
		machine->port_computer = port_computer_max;

	return (vec_machines.size() > 0);
}

bool Machine_info::get_storage_nodes(int nodes, std::vector<Tpye_Ip_Port_Paths> &vec_ip_port_paths)
{
	std::lock_guard<std::mutex> lock(mutex_nodes_);

	if(vec_machines.size() == 0)
		return false;

	int node_allocate = nodes_select;
	int node_finish = 0;
	while(node_finish<nodes)
	{
		std::vector<std::string> vec_paths;
		vec_paths.emplace_back(std::get<0>(vec_machines[node_allocate]->vec_vec_path_used_free[0][0]));
		vec_paths.emplace_back(std::get<0>(vec_machines[node_allocate]->vec_vec_path_used_free[1][0]));
		vec_paths.emplace_back(std::get<0>(vec_machines[node_allocate]->vec_vec_path_used_free[2][0]));

		vec_ip_port_paths.emplace_back(std::make_tuple(vec_machines[node_allocate]->ip, 
									vec_machines[node_allocate]->port_storage, vec_paths));
		
		vec_machines[node_allocate]->port_storage += 3;
		node_finish++;

		node_allocate++;
		node_allocate %= vec_machines.size();
	}
	
	// reset nodes_select
	if(nodes < vec_machines.size())
		nodes_select = (nodes_select+nodes)%vec_machines.size();
	else
		nodes_select = (nodes_select+1)%vec_machines.size();

	return true;
}

bool Machine_info::get_computer_nodes(int nodes, std::vector<Tpye_Ip_Port_Paths> &vec_ip_port_paths)
{
	std::lock_guard<std::mutex> lock(mutex_nodes_);

	if(vec_machines.size() == 0)
		return false;

	// allocate available node
	int node_allocate = nodes_select;
	int node_finish = 0;
	while(node_finish<nodes)
	{
		std::vector<std::string> vec_paths;
		vec_paths.emplace_back(std::get<0>(vec_machines[node_allocate]->vec_vec_path_used_free[3][0]));
		vec_ip_port_paths.emplace_back(std::make_tuple(vec_machines[node_allocate]->ip, 
									vec_machines[node_allocate]->port_computer, vec_paths));
		
		vec_machines[node_allocate]->port_computer += 1;
		node_finish++;

		node_allocate++;
		node_allocate %= vec_machines.size();
	}
	
	// reset nodes_select
	nodes_select = (nodes_select+nodes)%vec_machines.size();

	return true;
}

