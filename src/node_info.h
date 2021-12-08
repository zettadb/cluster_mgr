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

#include <pthread.h>
#include <mutex>
#include <map>
#include <vector>
#include <string>
#include <tuple>
#include <algorithm>

typedef std::tuple<std::string, int, std::string, std::string> Tpye_Ip_Port_User_Pwd;
typedef std::tuple<std::string, int, std::vector<std::string>> Tpye_Ip_Port_Paths;
typedef std::pair<std::string, int> Tpye_Path_Space;

class Node
{
public:
	std::string ip;
	std::string paths;
	bool available; 
	int instances;
	int instance_computer;
	int instance_storage;
	int port_computer;
	int port_storage;
	std::vector<Tpye_Path_Space> vec_path_space;
	
	Node(std::string &ip_, std::string &paths_);
	~Node();
};

class Node_info
{
public:
	
private:
	static Node_info *m_inst;
	Node_info();

	std::mutex mutex_nodes_;
	std::vector<Node*> vec_nodes;
	int nodes_select;
public:
	~Node_info();
	static Node_info *get_instance()
	{
		if (!m_inst) m_inst = new Node_info();
		return m_inst;
	}

	bool get_node_space_port(Node* node);
	bool update_nodes();
	bool get_storage_nodes(int nodes, std::vector<Tpye_Ip_Port_Paths> &vec_ip_port_paths);
	bool get_computer_nodes(int nodes, std::vector<Tpye_Ip_Port_Paths> &vec_ip_port_paths);
};

#endif // !NODE_INFO_H
