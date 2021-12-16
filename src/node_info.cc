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

Node::Node(std::string &ip_, std::vector<std::string> &vec_paths_):
	ip(ip_),vec_paths(vec_paths_),instances(0),
	instance_computer(0),instance_storage(0),port_computer(0),port_storage(0)
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

bool Node_info::get_node_path_space(Node* node)
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
	for(auto &paths: node->vec_paths)
	{
		item_sub = cJSON_CreateObject();
		cJSON_AddItemToArray(item, item_sub);
		cJSON_AddStringToObject(item_sub, "paths", paths.c_str());
	}

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
			syslog(Logger::INFO, "get_node_path_space result_str=%s",result_str.c_str());

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

	

#if 0
	bool ret = false;

	cJSON *root;
	char *cjson;
	
	root = cJSON_CreateObject();
	cJSON_AddStringToObject(root, "job_type", "get_path_space");
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
			syslog(Logger::INFO, "get_node_path_space result_str=%s",result_str.c_str());

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
#endif
}

bool Node_info::update_nodes()
{
	std::unique_lock<std::mutex> lock(mutex_nodes_);

	for(auto &node: vec_nodes)
		delete node;
	vec_nodes.clear();

	/////////////////////////////////////////////////////////
	//get the ip&paths of every node from meta data table

	std::string ip = "127.0.0.1";
	std::string paths = "/home/kunlun;/nvme2";
	Node *node = new Node(ip, paths);
	vec_nodes.push_back(node);
/*
	std::string ip2 = "192.168.189.129";
	std::string paths2 = "/home/kunlun;/nvme2";
	Node *node2 = new Node(ip2, paths2);
	vec_nodes.push_back(node2);

	std::string ip3 = "192.168.189.170";
	std::string paths3 = ";/home/kunlun";
	Node *node3= new Node(ip3, paths3);
	vec_nodes.push_back(node3);
*/
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
		Job::get_instance()->get_node_instance_port(node);
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

bool Node_info::update_nodes_info()
{
	std::unique_lock<std::mutex> lock(mutex_nodes_);

	for(auto &node: vec_nodes)
		delete node;
	vec_nodes.clear();

	/////////////////////////////////////////////////////////
	//get the ip&paths of every node from meta data table
	std::vector<Tpye_Ip_Paths> vec_ip_paths;

	if(System::get_instance()->get_MetadataShard()->get_server_nodes_from_metadata(vec_ip_paths))
	{
		syslog(Logger::ERROR, "get_server_nodes_from_metadata error");
		return false;
	}

	for(auto &ip_paths: vec_ip_paths)
	{
		Node *node = new Node(ip_paths.first, ip_paths.second);
		vec_nodes.push_back(node);
	}

	/////////////////////////////////////////////////////////
	//get the info of path space form every node ip
/*	for(auto node=vec_nodes.begin(); node!=vec_nodes.end(); )
	{
		if(!get_node_path_space(*node))
		{
			delete *node;
			node = vec_nodes.erase(node); //remove unavailable node
		}
		else
			node++;
	}
*/
	//sort the paths of ervery node by space


	/////////////////////////////////////////////////////////
	//get the instances and ports of ervery node from meta table
	for(auto &node: vec_nodes)
	{
		Job::get_instance()->get_node_instance_port(node);
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

bool Node_info::get_first_path(std::string &paths, std::string &path)
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

bool Node_info::get_storage_nodes(int nodes, std::vector<Tpye_Ip_Port_Paths> &vec_ip_port_paths)
{
	std::unique_lock<std::mutex> lock(mutex_nodes_);

	if(vec_nodes.size() == 0)
		return false;

	int node_allocate = nodes_select;
	int node_finish = 0;
	while(node_finish<nodes)
	{
		std::vector<std::string> vec_paths;
		std::string path;

		get_first_path(vec_nodes[node_allocate]->vec_paths.at(0), path);
		vec_paths.push_back(path);
		get_first_path(vec_nodes[node_allocate]->vec_paths.at(1), path);
		vec_paths.push_back(path);
		get_first_path(vec_nodes[node_allocate]->vec_paths.at(2), path);
		vec_paths.push_back(path);

		vec_ip_port_paths.push_back(std::make_tuple(vec_nodes[node_allocate]->ip, 
									vec_nodes[node_allocate]->port_storage, vec_paths));
		
		vec_nodes[node_allocate]->port_storage += 3;
		node_finish++;

		node_allocate++;
		node_allocate %= vec_nodes.size();
	}
	
	// reset nodes_select
	if(nodes < vec_nodes.size())
		nodes_select = (nodes_select+nodes)%vec_nodes.size();
	else
		nodes_select = (nodes_select+1)%vec_nodes.size();

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

bool Node_info::get_computer_nodes(int nodes, std::vector<Tpye_Ip_Port_Paths> &vec_ip_port_paths)
{
	std::unique_lock<std::mutex> lock(mutex_nodes_);

	if(vec_nodes.size() == 0)
		return false;

	// allocate available node
	int node_allocate = nodes_select;
	int node_finish = 0;
	while(node_finish<nodes)
	{
		std::vector<std::string> vec_paths;
		std::string path;

		get_first_path(vec_nodes[node_allocate]->vec_paths.at(3), path);
		vec_paths.push_back(path);
		vec_ip_port_paths.push_back(std::make_tuple(vec_nodes[node_allocate]->ip, 
									vec_nodes[node_allocate]->port_computer, vec_paths));
		
		vec_nodes[node_allocate]->port_computer += 1;
		node_finish++;

		node_allocate++;
		node_allocate %= vec_nodes.size();
	}
	
	// reset nodes_select
	nodes_select = (nodes_select+nodes)%vec_nodes.size();

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

