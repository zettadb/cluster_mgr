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

#define PATH_AVAILABLE_SIZE 	60    //G

Machine_info* Machine_info::m_inst = NULL;
extern int64_t node_mgr_http_port;

Machine::Machine(std::string &ip_, std::vector<std::string> &vec_paths_):
	ip(ip_),vec_paths(vec_paths_),instances(0),
	instance_computer(0),instance_storage(0),port_computer(0),port_storage(0)
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
	for(auto &node: vec_machines)
		delete node;
}

bool Machine_info::get_machine_path_space(Machine* machine)
{
	bool ret = false;

	cJSON *root;
	cJSON *item;
	cJSON *item_sub;
	char *cjson;
	
	root = cJSON_CreateObject();
	cJSON_AddStringToObject(root, "job_type", "get_path_space");

	item = cJSON_CreateArray();
	cJSON_AddItemToObject(root, "vec_paths", item);
	for(auto &paths: machine->vec_paths)
	{
		item_sub = cJSON_CreateObject();
		cJSON_AddItemToArray(item, item_sub);
		cJSON_AddStringToObject(item_sub, "paths", paths.c_str());
	}

	cjson = cJSON_Print(root);
	cJSON_Delete(root);
	
	std::string post_url = "http://" + machine->ip + ":" + std::to_string(node_mgr_http_port);
	//syslog(Logger::INFO, "post_url=%s",post_url.c_str());
	
	std::string result_str;
	int retry = 3;
	while(retry>0)
	{
		if(Http_client::get_instance()->Http_client_post_para(post_url.c_str(), cjson, result_str)==0)
		{
			syslog(Logger::INFO, "get_machine_path_space result_str=%s",result_str.c_str());

			cJSON *ret_root;
			cJSON *ret_item;
			cJSON *ret_item_spaces;
			cJSON *ret_item_sub;
			std::vector<Tpye_Path_Used_Free> vec_path_used_free;
			
			ret_root = cJSON_Parse(result_str.c_str());
			if(ret_root == NULL)
			{
				syslog(Logger::ERROR, "cJSON_Parse error");	
				break;
			}

			ret_item_spaces = cJSON_GetObjectItem(ret_root, "spaces");
			if(ret_item_spaces == NULL)
			{
				syslog(Logger::ERROR, "get spaces error");
				cJSON_Delete(ret_root);
				break;
			}

			machine->vec_vec_path_used_free.clear();
			int spaces = cJSON_GetArraySize(ret_item_spaces);
			for(int i=0; i<spaces; i++)
			{
				std::string path;
				int used,free;
				ret_item_sub = cJSON_GetArrayItem(ret_item_spaces,i);
				if(ret_item_sub == NULL)
				{
					syslog(Logger::ERROR, "get sub spaces error");
					continue;
				}
				
				ret_item = cJSON_GetObjectItem(ret_item_sub, "path");
				if(ret_item == NULL)
				{
					syslog(Logger::ERROR, "get sub path error");
					continue;
				}
				path = ret_item->valuestring;

				ret_item = cJSON_GetObjectItem(ret_item_sub, "used");
				if(ret_item == NULL)
				{
					syslog(Logger::ERROR, "get sub used error");
					continue;
				}
				used = ret_item->valueint;

				ret_item = cJSON_GetObjectItem(ret_item_sub, "free");
				if(ret_item == NULL)
				{
					syslog(Logger::ERROR, "get sub free error");
					continue;
				}
				free = ret_item->valueint;

				vec_path_used_free.push_back(std::make_tuple(path, used, free));
			}

			machine->vec_vec_path_used_free.push_back(vec_path_used_free);

			ret = true;
			break;
		}
		else
			retry--;
	}

	free(cjson);
	
	return ret;
}

#if 0
bool Machina_info::update_nodes()
{
	/////////////////////////////////////////////////////////
	//get the info of path space form every node ip
	for(auto node=vec_nodes.begin(); node!=vec_nodes.end(); )
	{
		if(!get_node_path_space(*node))
		{
			delete *node;
			node = vec_nodes.erase(node); //remove unavailable node
		}
		else
			node++;
	}

	/////////////////////////////////////////////////////////
	//get the instances and ports of ervery node from meta table
	for(auto &node: vec_nodes)
	{
		System::get_instance()->get_node_instance_port(node);
	}

	/////////////////////////////////////////////////////////
	//sort the paths of ervery node by space
	for(auto &node: vec_nodes)
	{
		sort(node->vec_path_space.begin(), node->vec_path_space.end(), 
			[](Tpye_Path_Space a, Tpye_Path_Space b){return a.second > b.second;});
	}
	
	//sort the nodes by instances
	sort(vec_nodes.begin(), vec_nodes.end(), [](Node *a, Node *b){return a->instances < b->instances;});
	
	//start from the least instances of node
	nodes_select = 0;

	//reset the max port of port_computer for every node
	int port_computer_max = 0;
	for(auto &node: vec_nodes)
	{
		if(node->port_computer > port_computer_max)
			port_computer_max = node->port_computer;
	}

	for(auto &node: vec_nodes)
		node->port_computer = port_computer_max;

	return (vec_nodes.size() != 0);
}
#endif

bool Machine_info::update_machine_info()
{
	std::unique_lock<std::mutex> lock(mutex_nodes_);

	for(auto &machine: vec_machines)
		delete machine;
	vec_machines.clear();

	/////////////////////////////////////////////////////////
	//get the ip&paths of every node from meta data table
	std::vector<Tpye_Ip_Paths> vec_ip_paths;

	if(System::get_instance()->get_server_nodes_from_metadata(vec_ip_paths))
	{
		syslog(Logger::ERROR, "get_server_nodes_from_metadata error");
		return false;
	}

	for(auto &ip_paths: vec_ip_paths)
	{
		Machine *machine = new Machine(ip_paths.first, ip_paths.second);
		vec_machines.push_back(machine);
	}

	/////////////////////////////////////////////////////////
	//get the info of path space form every machine ip
/*	for(auto machine=vec_machines.begin(); machine!=vec_machines.end(); )
	{
		if(!get_machine_path_space(*machine))
		{
			delete *machine;
			machine = vec_machines.erase(machine); //remove unavailable machine
		}
		else
			machine++;
	}
*/
	//sort the paths of ervery machine by space


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

	return (vec_machines.size() != 0);
}

bool Machine_info::get_first_path(std::string &paths, std::string &path)
{
	size_t pos = paths.find(";");
	if(pos == size_t(-1))
		path = paths;
	else
		path = paths.substr(0,pos);

    size_t s = path.find_first_not_of(" ");
    size_t e = path.find_last_not_of(" ");
	if(s>=e)
		path = "";
	else
    	path = path.substr(s,e-s+1);

	return true;
}

bool Machine_info::get_storage_nodes(int nodes, std::vector<Tpye_Ip_Port_Paths> &vec_ip_port_paths)
{
	std::unique_lock<std::mutex> lock(mutex_nodes_);

	if(vec_machines.size() == 0)
		return false;

	int node_allocate = nodes_select;
	int node_finish = 0;
	while(node_finish<nodes)
	{
		std::vector<std::string> vec_paths;
		std::string path;

		get_first_path(vec_machines[node_allocate]->vec_paths.at(0), path);
		vec_paths.push_back(path);
		get_first_path(vec_machines[node_allocate]->vec_paths.at(1), path);
		vec_paths.push_back(path);
		get_first_path(vec_machines[node_allocate]->vec_paths.at(2), path);
		vec_paths.push_back(path);

		vec_ip_port_paths.push_back(std::make_tuple(vec_machines[node_allocate]->ip, 
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

#if 0
	// check one available node at least
	bool unavailable = true;
	for(auto &node: vec_nodes)
	{
		if(node->vec_path_space.size()>0 &&
			node->vec_path_space[0].second>PATH_AVAILABLE_SIZE)
		{
			unavailable = false;
			break;
		}
	}
	if(unavailable)
		return false;

	// allocate available node
	int node_allocate = nodes_select;
	int node_finish = 0;
	while(node_finish<nodes)
	{
		std::vector<std::string> vec_paths;
		if(vec_nodes[node_allocate]->vec_path_space.size()>0 &&
			vec_nodes[node_allocate]->vec_path_space[0].second>PATH_AVAILABLE_SIZE)
		{
			vec_paths.push_back(vec_nodes[node_allocate]->vec_path_space[0].first);
			if(vec_nodes[node_allocate]->vec_path_space.size()>1 &&
				vec_nodes[node_allocate]->vec_path_space[1].second>PATH_AVAILABLE_SIZE)
			{
				vec_paths.push_back(vec_nodes[node_allocate]->vec_path_space[1].first);
				if(vec_nodes[node_allocate]->vec_path_space.size()>2 &&
					vec_nodes[node_allocate]->vec_path_space[2].second>PATH_AVAILABLE_SIZE)
				{
					vec_paths.push_back(vec_nodes[node_allocate]->vec_path_space[2].first);
				}
				else
				{
					vec_paths.push_back(vec_nodes[node_allocate]->vec_path_space[0].first);
				}
			}
			else
			{
				vec_paths.push_back(vec_nodes[node_allocate]->vec_path_space[0].first);
				vec_paths.push_back(vec_nodes[node_allocate]->vec_path_space[0].first);
			}
			
			vec_ip_port_paths.push_back(std::make_tuple(vec_nodes[node_allocate]->ip, 
										vec_nodes[node_allocate]->port_storage, vec_paths));
			
			vec_nodes[node_allocate]->port_storage += 3;
			node_finish++;
		}

		node_allocate++;
		node_allocate %= vec_nodes.size();
	}
	
	// reset nodes_select
	if(nodes < vec_nodes.size())
		nodes_select = (nodes_select+nodes)%vec_nodes.size();
	else
		nodes_select = (nodes_select+1)%vec_nodes.size();
	
	return true;
#endif
}

bool Machine_info::get_computer_nodes(int nodes, std::vector<Tpye_Ip_Port_Paths> &vec_ip_port_paths)
{
	std::unique_lock<std::mutex> lock(mutex_nodes_);

	if(vec_machines.size() == 0)
		return false;

	// allocate available node
	int node_allocate = nodes_select;
	int node_finish = 0;
	while(node_finish<nodes)
	{
		std::vector<std::string> vec_paths;
		std::string path;

		get_first_path(vec_machines[node_allocate]->vec_paths.at(3), path);
		vec_paths.push_back(path);
		vec_ip_port_paths.push_back(std::make_tuple(vec_machines[node_allocate]->ip, 
									vec_machines[node_allocate]->port_computer, vec_paths));
		
		vec_machines[node_allocate]->port_computer += 1;
		node_finish++;

		node_allocate++;
		node_allocate %= vec_machines.size();
	}
	
	// reset nodes_select
	nodes_select = (nodes_select+nodes)%vec_machines.size();

	return true;

#if 0
	// check one available node at least
	bool unavailable = true;
	for(auto &node: vec_nodes)
	{
		if(node->vec_path_space.size()>0 &&
			node->vec_path_space[0].second>PATH_AVAILABLE_SIZE)
		{
			unavailable = false;
			break;
		}
	}
	if(unavailable)
		return false;

	// allocate available node
	int node_allocate = nodes_select;
	int node_finish = 0;
	while(node_finish<nodes)
	{
		std::vector<std::string> vec_paths;
		if(vec_nodes[node_allocate]->vec_path_space.size()>0 &&
			vec_nodes[node_allocate]->vec_path_space[0].second>PATH_AVAILABLE_SIZE)
		{
			vec_paths.push_back(vec_nodes[node_allocate]->vec_path_space[0].first);
			vec_ip_port_paths.push_back(std::make_tuple(vec_nodes[node_allocate]->ip, 
										vec_nodes[node_allocate]->port_computer, vec_paths));
			
			vec_nodes[node_allocate]->port_computer += 1;
			node_finish++;
		}

		node_allocate++;
		node_allocate %= vec_nodes.size();
	}
	
	// reset nodes_select
	nodes_select = (nodes_select+nodes)%vec_nodes.size();

	return true;
#endif
}

