/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "sys_config.h"
#include "global.h"
#include "cjson.h"
#include "log.h"
#include "node_info.h"
#include "http_client.h"
#include <errno.h>
#include <unistd.h>

#define PATH_AVAILABLE_SIZE 	60    //G

Node_info* Node_info::m_inst = NULL;
extern int64_t node_mgr_http_port;

Node::Node(std::string &ip_, std::string &paths_):
	ip(ip_),paths(paths_),instances(0),
	instance_computer(0),instance_storage(0),
	port_computer(0),port_storage(0)
{

}

Node::~Node()
{

}

Node_info::Node_info()
{

}

Node_info::~Node_info()
{
	for(auto &node: vec_nodes)
		delete node;
}

bool Node_info::get_node_space_port(Node* node)
{
	bool ret = false;

	cJSON *root;
	char *cjson;
	
	root = cJSON_CreateObject();
	cJSON_AddStringToObject(root, "job_type", "get_space_port");
	cJSON_AddStringToObject(root, "paths", node->paths.c_str());
	
	cjson = cJSON_Print(root);
	cJSON_Delete(root);
	
	std::string post_url = "http://" + node->ip + ":" + std::to_string(node_mgr_http_port);
	//syslog(Logger::INFO, "post_url=%s",post_url.c_str());
	
	std::string result_str;
	int retry = 3;
	while(retry>0)
	{
		if(Http_client::get_instance()->Http_client_post_para(post_url.c_str(), cjson, result_str)==0)
		{
			syslog(Logger::INFO, "get_node_space_port result_str=%s",result_str.c_str());

			cJSON *ret_root;
			cJSON *ret_item;
			cJSON *ret_item_spaces;
			cJSON *ret_item_sub;
			
			ret_root = cJSON_Parse(result_str.c_str());
			if(ret_root == NULL)
			{
				syslog(Logger::ERROR, "cJSON_Parse error");	
				break;
			}

			ret_item = cJSON_GetObjectItem(ret_root, "port_computer");
			if(ret_item == NULL)
			{
				syslog(Logger::ERROR, "get port_computer error");
				cJSON_Delete(ret_root);
				break;
			}
			node->port_computer = ret_item->valueint;

			ret_item = cJSON_GetObjectItem(ret_root, "port_storage");
			if(ret_item == NULL)
			{
				syslog(Logger::ERROR, "get port_storage error");
				cJSON_Delete(ret_root);
				break;
			}
			node->port_storage = ret_item->valueint;

			ret_item = cJSON_GetObjectItem(ret_root, "instance_computer");
			if(ret_item == NULL)
			{
				syslog(Logger::ERROR, "get instance_computer error");
				cJSON_Delete(ret_root);
				break;
			}
			node->instance_computer = ret_item->valueint;
			node->instances = ret_item->valueint;

			ret_item = cJSON_GetObjectItem(ret_root, "instance_storage");
			if(ret_item == NULL)
			{
				syslog(Logger::ERROR, "get instance_storage error");
				cJSON_Delete(ret_root);
				break;
			}
			node->instance_storage = ret_item->valueint;
			node->instances += ret_item->valueint;

			ret_item_spaces = cJSON_GetObjectItem(ret_root, "spaces");
			if(ret_item_spaces == NULL)
			{
				syslog(Logger::ERROR, "get spaces error");
				cJSON_Delete(ret_root);
				break;
			}

			node->vec_path_space.clear();
			int spaces = cJSON_GetArraySize(ret_item_spaces);
			for(int i=0; i<spaces; i++)
			{
				std::string path;
				int space;
				ret_item_sub = cJSON_GetArrayItem(ret_item_spaces,i);
				if(ret_item_sub == NULL)
				{
					syslog(Logger::ERROR, "get sub spaces error");
					continue;
				}
				
				ret_item = cJSON_GetObjectItem(ret_item_sub, "path");
				if(ret_item == NULL)
				{
					syslog(Logger::ERROR, "get sub space path error");
					continue;
				}
				path = ret_item->valuestring;

				ret_item = cJSON_GetObjectItem(ret_item_sub, "space");
				if(ret_item == NULL)
				{
					syslog(Logger::ERROR, "get sub space path error");
					continue;
				}
				space = ret_item->valueint;

				node->vec_path_space.push_back(std::make_pair(path, space));
			}

			ret = true;
			break;
		}
		else
			retry--;
	}

	free(cjson);
	
	return ret;
}

bool Node_info::update_nodes()
{
	std::unique_lock<std::mutex> lock(mutex_nodes_);

	for(auto &node: vec_nodes)
		delete node;
	vec_nodes.clear();

	/////////////////////////////////////////////////////////
	//get the ip&paths of every node from meta data table

	std::string ip = "192.168.207.168";
	std::string paths = "/home/kunlun;/boot;/dev";
	Node *node = new Node(ip, paths);
	vec_nodes.push_back(node);

	std::string ip2 = "192.168.207.169";
	std::string paths2 = "/home/kunlun;";
	Node *node2 = new Node(ip2, paths2);
	vec_nodes.push_back(node2);

	std::string ip3 = "192.168.207.170";
	std::string paths3 = ";/home/kunlun";
	Node *node3= new Node(ip3, paths3);
	vec_nodes.push_back(node3);

	/////////////////////////////////////////////////////////
	//get the info of path space & port form every node ip
	for(auto node=vec_nodes.begin(); node!=vec_nodes.end(); )
	{
		if(!get_node_space_port(*node))
		{
			delete *node;
			node = vec_nodes.erase(node); //remove unavailable node
		}
		else
			node++;
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

bool Node_info::get_storage_nodes(int nodes, std::vector<Tpye_Ip_Port_Paths> &vec_ip_port_paths)
{
	std::unique_lock<std::mutex> lock(mutex_nodes_);

	if(vec_nodes.size() == 0)
		return false;

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
}

bool Node_info::get_computer_nodes(int nodes, std::vector<Tpye_Ip_Port_Paths> &vec_ip_port_paths)
{
	std::unique_lock<std::mutex> lock(mutex_nodes_);

	if(vec_nodes.size() == 0)
		return false;

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
}

