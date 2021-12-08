/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "sys_config.h"
#include "global.h"
#include "job.h"
#include "log.h"
#include "sys.h"
#include "node_info.h"
#include "http_client.h"
#include "hdfs_client.h"
#include <signal.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netdb.h>
#include <net/if.h>
#include <arpa/inet.h>

Job* Job::m_inst = NULL;
int Job::do_exit = 0;

int64_t num_job_threads = 3;

extern int64_t node_mgr_http_port;
extern "C" void *thread_func_job_work(void*thrdarg);

Job::Job()
{
	
}

Job::~Job()
{

}

int Job::start_job_thread()
{
	int error = 0;
	pthread_mutex_init(&thread_mtx, NULL);
	pthread_cond_init(&thread_cond, NULL);

	//start job work thread
	for(int i=0; i<num_job_threads; i++)
	{
		pthread_t hdl;
		if ((error = pthread_create(&hdl,NULL, thread_func_job_work, m_inst)))
		{
			char errmsg_buf[256];
			syslog(Logger::ERROR, "Can not create http server work thread, error: %d, %s",
						error, errno, strerror_r(errno, errmsg_buf, sizeof(errmsg_buf)));
			do_exit = 1;
			return -1;
		}
		vec_pthread.push_back(hdl);
	}

	return 0;
}

void Job::join_all()
{
	do_exit = 1;
	pthread_mutex_lock(&thread_mtx);
	pthread_cond_broadcast(&thread_cond);
	pthread_mutex_unlock(&thread_mtx);
	
	for (auto &i:vec_pthread)
	{
		pthread_join(i, NULL);
	}
}

bool Job::http_para_cmd(const std::string &para, std::string &str_ret)
{
	bool ret = false;
	cJSON *root;
	cJSON *item;
	Job_type job_type;

	root = cJSON_Parse(para.c_str());
	if(root == NULL)
	{
		syslog(Logger::ERROR, "cJSON_Parse error");	
		goto end;
	}

	item = cJSON_GetObjectItem(root, "job_type");
	if(item == NULL || !Job::get_instance()->get_job_type(item->valuestring, job_type))
	{
		syslog(Logger::ERROR, "http_para_cmd get_job_type error");
		goto end;
	}

	if(job_type == JOB_GET_NODE)
	{
		ret = get_node_instance(root, str_ret);
	}
	else if(job_type == JOB_CHECK_PORT)
	{
		ret = check_port_used(root, str_ret);
	}
	else if(job_type == JOB_GET_DISK_SIZE)
	{
		ret = get_disk_size(root, str_ret);
	}
	else
		ret = false;

end:
	if(root != NULL)
		cJSON_Delete(root);

	return ret;
}

bool Job::get_meta_info(std::vector<Tpye_Ip_Port_User_Pwd> &meta)
{
	Scopped_mutex sm(System::get_instance()->mtx);
	
	for(auto &node: System::get_instance()->meta_shard.get_nodes())
	{
		std::string ip,user,pwd;
		int port;
		
		node->get_ip_port(ip, port);
		node->get_user_pwd(user, pwd);

		meta.push_back(std::make_tuple(ip, port, user, pwd));
	}

	return (meta.size()>0);
}

bool Job::get_node_instance(cJSON *root, std::string &str_ret)
{
	Scopped_mutex sm(System::get_instance()->mtx);
	
	bool ret = false;
	int node_count = 0;
	cJSON *item;

	std::vector<std::string> vec_node_ip;
	int ip_count = 0;
	while(true)
	{
		std::string node_ip = "node_ip" + std::to_string(ip_count++);
		item = cJSON_GetObjectItem(root, node_ip.c_str());
		if(item == NULL)
			break;

		vec_node_ip.push_back(item->valuestring);
	}
	
	item = cJSON_GetObjectItem(root, "node_type");
	if(item == NULL)
	{
		cJSON *ret_root;
		cJSON *ret_item;
		char *ret_cjson;
		ret_root = cJSON_CreateObject();

		//meta node
		node_count = 0;
		for(auto &node: System::get_instance()->meta_shard.get_nodes())
		{
			std::string ip;
			int port;

			node->get_ip_port(ip, port);

			bool ip_match = (vec_node_ip.size() == 0);
			for(auto &node_ip: vec_node_ip)
			{
				if(ip == node_ip)
				{
					ip_match = true;
					break;
				}
			}

			if(!ip_match)
				continue;

			std::string user,pwd;
			node->get_user_pwd(user, pwd);

			std::string str;
			ret_item = cJSON_CreateObject();
			str = "meta_node" + std::to_string(node_count++);
			cJSON_AddItemToObject(ret_root, str.c_str(), ret_item);
			
			cJSON_AddStringToObject(ret_item, "ip", ip.c_str());
			cJSON_AddNumberToObject(ret_item, "port", port);
			cJSON_AddStringToObject(ret_item, "user", user.c_str());
			cJSON_AddStringToObject(ret_item, "pwd", pwd.c_str());
		}

		//storage node
		node_count = 0;
		for (auto &cluster:System::get_instance()->kl_clusters)
			for (auto &shard:cluster->storage_shards)
				for (auto &node:shard->get_nodes())
		{
			std::string ip;
			int port;

			node->get_ip_port(ip, port);

			bool ip_match = (vec_node_ip.size() == 0);
			for(auto &node_ip: vec_node_ip)
			{
				if(ip == node_ip)
				{
					ip_match = true;
					break;
				}
			}

			if(!ip_match)
				continue;

			std::string user,pwd;
			node->get_user_pwd(user, pwd);

			std::string str;
			ret_item = cJSON_CreateObject();
			str = "storage_node" + std::to_string(node_count++);
			cJSON_AddItemToObject(ret_root, str.c_str(), ret_item);
			
			cJSON_AddStringToObject(ret_item, "ip", ip.c_str());
			cJSON_AddNumberToObject(ret_item, "port", port);
			cJSON_AddStringToObject(ret_item, "user", user.c_str());
			cJSON_AddStringToObject(ret_item, "pwd", pwd.c_str());
			cJSON_AddStringToObject(ret_item, "cluster", cluster->get_name().c_str());
			cJSON_AddStringToObject(ret_item, "shard", shard->get_name().c_str());
		}

		//computer node
		node_count = 0;
		for (auto &cluster:System::get_instance()->kl_clusters)
			for (auto &node:cluster->computer_nodes)
		{
			std::string ip;
			int port;
		
			node->get_ip_port(ip, port);
		
			bool ip_match = (vec_node_ip.size() == 0);
			for(auto &node_ip: vec_node_ip)
			{
				if(ip == node_ip)
				{
					ip_match = true;
					break;
				}
			}
		
			if(!ip_match)
				continue;
		
			std::string user,pwd;
			node->get_user_pwd(user, pwd);
		
			std::string str;
			ret_item = cJSON_CreateObject();
			str = "computer_node" + std::to_string(node_count++);
			cJSON_AddItemToObject(ret_root, str.c_str(), ret_item);
			
			cJSON_AddStringToObject(ret_item, "ip", ip.c_str());
			cJSON_AddNumberToObject(ret_item, "port", port);
			cJSON_AddStringToObject(ret_item, "user", user.c_str());
			cJSON_AddStringToObject(ret_item, "pwd", pwd.c_str());
			cJSON_AddStringToObject(ret_item, "cluster", cluster->get_name().c_str());
		}

		ret_cjson = cJSON_Print(ret_root);
		str_ret = ret_cjson;

		if(ret_root != NULL)
			cJSON_Delete(ret_root);
		if(ret_cjson != NULL)
			free(ret_cjson);
	
		return true;
	}

	if(strcmp(item->valuestring, "meta_node") == 0)
	{
		cJSON *ret_root;
		cJSON *ret_item;
		char *ret_cjson;
		ret_root = cJSON_CreateObject();
		
		for(auto &node: System::get_instance()->meta_shard.get_nodes())
		{
			std::string ip;
			int port;

			node->get_ip_port(ip, port);

			bool ip_match = (vec_node_ip.size() == 0);
			for(auto &node_ip: vec_node_ip)
			{
				if(ip == node_ip)
				{
					ip_match = true;
					break;
				}
			}

			if(!ip_match)
				continue;

			std::string user,pwd;
			node->get_user_pwd(user, pwd);

			std::string str;
			ret_item = cJSON_CreateObject();
			str = "meta_node" + std::to_string(node_count++);
			cJSON_AddItemToObject(ret_root, str.c_str(), ret_item);
			
			cJSON_AddStringToObject(ret_item, "ip", ip.c_str());
			cJSON_AddNumberToObject(ret_item, "port", port);
			cJSON_AddStringToObject(ret_item, "user", user.c_str());
			cJSON_AddStringToObject(ret_item, "pwd", pwd.c_str());
		}

		ret_cjson = cJSON_Print(ret_root);
		str_ret = ret_cjson;

		if(ret_root != NULL)
			cJSON_Delete(ret_root);
		if(ret_cjson != NULL)
			free(ret_cjson);

		ret = true;
	}
	else if(strcmp(item->valuestring, "storage_node") == 0)
	{
		cJSON *ret_root;
		cJSON *ret_item;
		char *ret_cjson;
		ret_root = cJSON_CreateObject();

		for (auto &cluster:System::get_instance()->kl_clusters)
			for (auto &shard:cluster->storage_shards)
				for (auto &node:shard->get_nodes())
		{
			std::string ip;
			int port;

			node->get_ip_port(ip, port);

			bool ip_match = (vec_node_ip.size() == 0);
			for(auto &node_ip: vec_node_ip)
			{
				if(ip == node_ip)
				{
					ip_match = true;
					break;
				}
			}

			if(!ip_match)
				continue;

			std::string user,pwd;
			node->get_user_pwd(user, pwd);

			std::string str;
			ret_item = cJSON_CreateObject();
			str = "storage_node" + std::to_string(node_count++);
			cJSON_AddItemToObject(ret_root, str.c_str(), ret_item);
			
			cJSON_AddStringToObject(ret_item, "ip", ip.c_str());
			cJSON_AddNumberToObject(ret_item, "port", port);
			cJSON_AddStringToObject(ret_item, "user", user.c_str());
			cJSON_AddStringToObject(ret_item, "pwd", pwd.c_str());
			cJSON_AddStringToObject(ret_item, "cluster", cluster->get_name().c_str());
			cJSON_AddStringToObject(ret_item, "shard", shard->get_name().c_str());
		}

		ret_cjson = cJSON_Print(ret_root);
		str_ret = ret_cjson;

		if(ret_root != NULL)
			cJSON_Delete(ret_root);
		if(ret_cjson != NULL)
			free(ret_cjson);

		ret = true;
	}
	else if(strcmp(item->valuestring, "computer_node") == 0)
	{
		cJSON *ret_root;
		cJSON *ret_item;
		char *ret_cjson;
		ret_root = cJSON_CreateObject();

		for (auto &cluster:System::get_instance()->kl_clusters)
			for (auto &node:cluster->computer_nodes)
		{
			std::string ip;
			int port;

			node->get_ip_port(ip, port);

			bool ip_match = (vec_node_ip.size() == 0);
			for(auto &node_ip: vec_node_ip)
			{
				if(ip == node_ip)
				{
					ip_match = true;
					break;
				}
			}

			if(!ip_match)
				continue;

			std::string user,pwd;
			node->get_user_pwd(user, pwd);

			std::string str;
			ret_item = cJSON_CreateObject();
			str = "computer_node" + std::to_string(node_count++);
			cJSON_AddItemToObject(ret_root, str.c_str(), ret_item);
			
			cJSON_AddStringToObject(ret_item, "ip", ip.c_str());
			cJSON_AddNumberToObject(ret_item, "port", port);
			cJSON_AddStringToObject(ret_item, "user", user.c_str());
			cJSON_AddStringToObject(ret_item, "pwd", pwd.c_str());
			cJSON_AddStringToObject(ret_item, "cluster", cluster->get_name().c_str());
		}

		ret_cjson = cJSON_Print(ret_root);
		str_ret = ret_cjson;

		if(ret_root != NULL)
			cJSON_Delete(ret_root);
		if(ret_cjson != NULL)
			free(ret_cjson);

		ret = true;
	}
	
	return ret;
}

void Job::notify_node_update(std::set<std::string> &alterant_node_ip, int type)
{
	return;
	for(auto &node_ip: alterant_node_ip)
	{
		cJSON *root;
		char *cjson;
		
		root = cJSON_CreateObject();
		cJSON_AddStringToObject(root, "job_type", "update_node");
		if(type == 0)
			cJSON_AddStringToObject(root, "node_type", "meta_node");
		else if(type == 1)
			cJSON_AddStringToObject(root, "node_type", "storage_node");
		else if(type == 2)
			cJSON_AddStringToObject(root, "node_type", "computer_node");
		
		cjson = cJSON_Print(root);
		cJSON_Delete(root);
		
		std::string post_url = "http://" + node_ip + ":" + std::to_string(node_mgr_http_port);
		//syslog(Logger::INFO, "post_url=%s",post_url.c_str());
		
		std::string result_str;
		int ret = Http_client::get_instance()->Http_client_post_para(post_url.c_str(), cjson, result_str);
		free(cjson);
	}
}

bool Job::check_port_used(cJSON *root, std::string &str_ret)
{
	cJSON *item;

	std::string ip;
	int port;
	item = cJSON_GetObjectItem(root, "ip");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get ip error");
		return false;
	}
	ip = item->valuestring;
	
	item = cJSON_GetObjectItem(root, "port");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get port error");
		return false;
	}
	port = item->valueint;

	////////////////////////////////////////////////
	// check port used by socket connect
	int socket_fd = Http_client::get_instance()->Http_client_socket(ip.c_str(), port);
	if(socket_fd > 0)
	{
		close(socket_fd);
		str_ret = "{\"port\":\"used\"}";
		return true;
	}

	////////////////////////////////////////////////
	// check port used by metadata tables
	{
		Scopped_mutex sm(System::get_instance()->mtx);
		if(System::get_instance()->meta_shard.check_port_used(ip, port))
		{
			str_ret = "{\"port\":\"used\"}";
			return true;
		}
	}

	////////////////////////////////////////////////
	str_ret = "{\"port\":\"unused\"}";
	return true;
}

bool Job::get_disk_size(cJSON *root, std::string &str_ret)
{
	cJSON *item;
	char *cjson;

	syslog(Logger::INFO, "get_disk_size");

	std::string ip;
	item = cJSON_GetObjectItem(root, "ip");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get ip error");
		return false;
	}
	ip = item->valuestring;
	
	cjson = cJSON_Print(root);
	syslog(Logger::INFO, "cjson=%s", cjson);
	
	std::string post_url = "http://" + ip + ":" + std::to_string(node_mgr_http_port);
	syslog(Logger::INFO, "post_url=%s", post_url.c_str());
	
	int ret = Http_client::get_instance()->Http_client_post_para(post_url.c_str(), cjson, str_ret);
	free(cjson);

	if(ret == 0)
		return true;
	else
		return false;
}

bool Job::get_uuid(std::string &uuid)
{
	FILE *fp = fopen("/proc/sys/kernel/random/uuid", "rb");
	if (fp == NULL)
	{
		syslog(Logger::ERROR, "open file uuid error");
		return false;
	}

	char buf[60];
	memset(buf, 0, 60);
	fread(buf, 1, 36, fp);
	fclose(fp);
	uuid = buf;
	
	return true;
}

bool Job::get_job_type(char *str, Job_type &job_type)
{
	if(strcmp(str, "get_node")==0)
		job_type = JOB_GET_NODE;
	else if(strcmp(str, "check_port")==0)
		job_type = JOB_CHECK_PORT;
	else if(strcmp(str, "get_disk_size")==0)
		job_type = JOB_GET_DISK_SIZE;
	else if(strcmp(str, "create_cluster")==0)
		job_type = JOB_CREATE_CLUSTER;
	else
	{
		job_type = JOB_NONE;
		return false;
	}

	return true;
}

bool Job::get_file_type(char *str, File_type &file_type)
{
	if(strcmp(str, "")==0)
		file_type = FILE_NONE;
	else
		file_type = FILE_NONE;

	return true;
}

bool Job::update_jobid_status(std::string &jobid, std::string &status)
{
	std::unique_lock<std::mutex> lock(mutex_stauts_);

	if(list_jobid_status.size()>=kMaxStatus)
		list_jobid_status.pop_back();

	bool is_exist = false;
	for (auto it = list_jobid_status.begin(); it != list_jobid_status.end(); ++it)
	{
		if(it->first == jobid)
		{
			it->second = status;
			is_exist = true;
			break;
		}
	}

	if(!is_exist)
		list_jobid_status.push_front(std::make_pair(jobid, status));

	return true;
}

bool Job::get_jobid_status(std::string &jobid, std::string &status)
{
	std::unique_lock<std::mutex> lock(mutex_stauts_);
	
	bool ret = false;
	for (auto it = list_jobid_status.begin(); it != list_jobid_status.end(); ++it)
	{
		if(it->first == jobid)
		{
			status = it->second;
			ret = true;
			break;
		}
	}

	return ret;
}

bool Job::job_create_shard(std::vector<Tpye_Ip_Port_Paths> &shard)
{
	cJSON *root;
	char *cjson;
	cJSON *item_node;
	cJSON *item_sub;

	bool ret = false;
	int install_id = 0;
	std::string pathdir;
	std::string uuid_shard, uuid_job_id;
	std::string post_url,get_status,result_str;
	get_uuid(uuid_shard);
	get_uuid(uuid_job_id);

	/////////////////////////////////////////////////////////
	// create json parameter
	root = cJSON_CreateObject();
	cJSON_AddStringToObject(root, "ver", "0.1");
	cJSON_AddStringToObject(root, "job_id", uuid_job_id.c_str());
	cJSON_AddStringToObject(root, "job_type", "install_storage");
	cJSON_AddNumberToObject(root, "install_id", install_id);
	cJSON_AddStringToObject(root, "group_uuid", uuid_shard.c_str());
	item_node = cJSON_CreateArray();
	cJSON_AddItemToObject(root, "nodes", item_node);

	for(auto &ip_port_paths: shard)
	{
		item_sub = cJSON_CreateObject();
		cJSON_AddItemToArray(item_node, item_sub);
		
		if(install_id++ == 0)
			cJSON_AddTrueToObject(item_sub, "is_primary");
		else
			cJSON_AddFalseToObject(item_sub, "is_primary");
		cJSON_AddStringToObject(item_sub, "ip", std::get<0>(ip_port_paths).c_str());
		cJSON_AddNumberToObject(item_sub, "port", std::get<1>(ip_port_paths));
		cJSON_AddNumberToObject(item_sub, "xport", std::get<1>(ip_port_paths)+1);
		cJSON_AddNumberToObject(item_sub, "mgr_port", std::get<1>(ip_port_paths)+2);
		pathdir = std::get<2>(ip_port_paths).at(0) + "/instance_data/data_sn/" 
					+ std::to_string(std::get<1>(ip_port_paths));
		cJSON_AddStringToObject(item_sub, "data_dir_path", pathdir.c_str());
		pathdir = std::get<2>(ip_port_paths).at(1) + "/instance_data/innodblog/" 
					+ std::to_string(std::get<1>(ip_port_paths));
		cJSON_AddStringToObject(item_sub, "innodb_log_dir_path", pathdir.c_str());
		pathdir = std::get<2>(ip_port_paths).at(2) + "/instance_data/binlog/" 
					+ std::to_string(std::get<1>(ip_port_paths));
		cJSON_AddStringToObject(item_sub, "log_dir_path", pathdir.c_str());
		cJSON_AddStringToObject(item_sub, "innodb_buffer_pool_size", "64MB");
		cJSON_AddStringToObject(item_sub, "user", "kunlun");
		cJSON_AddNumberToObject(item_sub, "election_weight", 50);
	}

	/////////////////////////////////////////////////////////
	// send json parameter to every node
	install_id = 0;
	for(auto &ip_port_paths: shard)
	{
		get_uuid(uuid_job_id);
		cJSON_ReplaceItemInObject(root, "job_id", cJSON_CreateString(uuid_job_id.c_str()));
		cJSON_ReplaceItemInObject(root, "install_id", cJSON_CreateNumber(install_id++));

		cjson = cJSON_Print(root);
		//syslog(Logger::INFO, "cjson=%s",cjson);

		/////////////////////////////////////////////////////////
		// http post parameter to node
		post_url = "http://" + std::get<0>(ip_port_paths) + ":" + std::to_string(node_mgr_http_port);
		
		int retry = 3;
		while(retry-->0 && !Job::do_exit)
		{
			if(Http_client::get_instance()->Http_client_post_para(post_url.c_str(), cjson, result_str)==0)
				break;
		}
		free(cjson);
		cjson = NULL;

		if(retry<0)
		{
			syslog(Logger::ERROR, "create storage instance fail because http post");
			goto end;
		}
		
		/////////////////////////////////////////////////////////
		// get install status from node 
		get_status = "{\"ver\":\"0.1\",\"job_id\":\"" + uuid_job_id + "\",\"job_type\":\"get_status\"}";
		
		retry = 60;
		while(retry-->0 && !Job::do_exit)
		{
			sleep(1);

			if(Http_client::get_instance()->Http_client_post_para(post_url.c_str(), get_status.c_str(), result_str)==0)
			{
				cJSON *ret_root;
				cJSON *ret_item;
				std::string status;

				//syslog(Logger::INFO, "result_str=%s",result_str.c_str());
				ret_root = cJSON_Parse(result_str.c_str());
				if(ret_root == NULL)
				{
					syslog(Logger::ERROR, "cJSON_Parse error");	
					goto end;
				}

				ret_item = cJSON_GetObjectItem(ret_root, "job_status");
				if(ret_item == NULL)
				{
					syslog(Logger::ERROR, "get job_status error");
					cJSON_Delete(ret_root);
					goto end;
				}
				status = ret_item->valuestring;
				cJSON_Delete(ret_root);

				if(status.find("error") != size_t(-1))
				{
					syslog(Logger::ERROR, "install fail %s", status.c_str());
					goto end;
				}
				else if(status == "install succeed")
				{
					syslog(Logger::INFO, "storage instance %s:%d install finish!", std::get<0>(ip_port_paths).c_str(), std::get<1>(ip_port_paths));
					break;
				}
			}
				
		}

		if(retry<0)
		{
			syslog(Logger::ERROR, "create storage instance fail %s", result_str.c_str());
			goto end;
		}
	}

	ret = true;

end:
	if(root!=NULL)
		cJSON_Delete(root);
	if(cjson!=NULL)
		free(cjson);

	return ret;
}

bool Job::job_create_comps(std::vector<Tpye_Ip_Port_Paths> &comps)
{
	cJSON *root;
	char *cjson;
	cJSON *item_node;
	cJSON *item_sub;

	bool ret = false;
	int install_id = 0;
	std::string pathdir;
	std::string name, uuid_job_id;
	std::string post_url,get_status,result_str;
	get_uuid(uuid_job_id);
	
	/////////////////////////////////////////////////////////
	// create json parameter
	root = cJSON_CreateObject();
	cJSON_AddStringToObject(root, "ver", "0.1");
	cJSON_AddStringToObject(root, "job_id", uuid_job_id.c_str());
	cJSON_AddStringToObject(root, "job_type", "install_computer");
	cJSON_AddNumberToObject(root, "install_id", install_id);
	item_node = cJSON_CreateArray();
	cJSON_AddItemToObject(root, "nodes", item_node);

	for(auto &ip_port_paths: comps)
	{
		item_sub = cJSON_CreateObject();
		cJSON_AddItemToArray(item_node, item_sub);

		cJSON_AddNumberToObject(item_sub, "id", install_id+1);
		name = "comp" + std::to_string(install_id);
		install_id++;
		cJSON_AddStringToObject(item_sub, "name", name.c_str());
		cJSON_AddStringToObject(item_sub, "ip", std::get<0>(ip_port_paths).c_str());
		cJSON_AddNumberToObject(item_sub, "port", std::get<1>(ip_port_paths));
		cJSON_AddStringToObject(item_sub, "user", "abc");
		cJSON_AddStringToObject(item_sub, "password", "abc");
		pathdir = std::get<2>(ip_port_paths).at(0) + "/instance_data/data_cn/" 
					+ std::to_string(std::get<1>(ip_port_paths));
		cJSON_AddStringToObject(item_sub, "datadir", pathdir.c_str());
	}

	/////////////////////////////////////////////////////////
	// send json parameter to every node
	install_id = 0;
	for(auto &ip_port_paths: comps)
	{
		get_uuid(uuid_job_id);
		cJSON_ReplaceItemInObject(root, "job_id", cJSON_CreateString(uuid_job_id.c_str()));
		cJSON_ReplaceItemInObject(root, "install_id", cJSON_CreateNumber(install_id++));

		cjson = cJSON_Print(root);
		//syslog(Logger::INFO, "cjson=%s",cjson);
		
		/////////////////////////////////////////////////////////
		// http post parameter to node
		post_url = "http://" + std::get<0>(ip_port_paths) + ":" + std::to_string(node_mgr_http_port);
		
		int retry = 3;
		while(retry-->0 && !Job::do_exit)
		{
			if(Http_client::get_instance()->Http_client_post_para(post_url.c_str(), cjson, result_str)==0)
				break;
		}
		free(cjson);
		cjson = NULL;

		if(retry<0)
		{
			syslog(Logger::ERROR, "create computer instance fail because http post");
			goto end;
		}

		/////////////////////////////////////////////////////////
		// get install status from node 
		get_status = "{\"ver\":\"0.1\",\"job_id\":\"" + uuid_job_id + "\",\"job_type\":\"get_status\"}";
		
		retry = 60;
		while(retry-->0 && !Job::do_exit)
		{
			sleep(1);

			if(Http_client::get_instance()->Http_client_post_para(post_url.c_str(), get_status.c_str(), result_str)==0)
			{
				cJSON *ret_root;
				cJSON *ret_item;
				std::string status;
				
				//syslog(Logger::INFO, "result_str=%s",result_str.c_str());
				ret_root = cJSON_Parse(result_str.c_str());
				if(ret_root == NULL)
				{
					syslog(Logger::ERROR, "cJSON_Parse error"); 
					goto end;
				}

				ret_item = cJSON_GetObjectItem(ret_root, "job_status");
				if(ret_item == NULL)
				{
					syslog(Logger::ERROR, "get job_status error");
					cJSON_Delete(ret_root);
					goto end;
				}
				status = ret_item->valuestring;
				cJSON_Delete(ret_root);

				if(status.find("error") != size_t(-1))
				{
					syslog(Logger::ERROR, "install fail %s", status.c_str());
					goto end;
				}
				else if(status == "install succeed")
				{
					syslog(Logger::INFO, "computer instance %s:%d install finish!", std::get<0>(ip_port_paths).c_str(), std::get<1>(ip_port_paths));
					break;
				}
			}
				
		}

		if(retry<0)
		{
			syslog(Logger::ERROR, "create computer instance fail %s", result_str.c_str());
			goto end;
		}
	}
	
	ret = true;

end:
	if(root!=NULL)
		cJSON_Delete(root);
	if(cjson!=NULL)
		free(cjson);

	return ret;
}

bool Job::job_start_cluster(std::vector <std::vector<Tpye_Ip_Port_Paths>> &vec_shard,
									std::vector<Tpye_Ip_Port_Paths> &comps, std::string &cluster_name)
{
	cJSON *root;
	char *cjson;
	cJSON *item_shards;
	cJSON *item_meta;
	cJSON *item_sub;
	cJSON *item_shard_nodes;
	cJSON *item_sub_sub;
	cJSON *item_shard_nodes_null;
	cJSON *item_sub_sub_sub;

	bool ret = false;
	int retry;
	int shard_id = 0;
	std::string name, uuid_job_id;
	std::string post_url,get_status,result_str;
	get_uuid(uuid_job_id);
	auto ip_port_paths = comps.at(0);

	/////////////////////////////////////////////////////////
	// create json parameter
	root = cJSON_CreateObject();
	cJSON_AddStringToObject(root, "ver", "0.1");
	cJSON_AddStringToObject(root, "job_id", uuid_job_id.c_str());
	cJSON_AddStringToObject(root, "job_type", "start_cluster");
	cJSON_AddStringToObject(root, "cluster_name", cluster_name.c_str());
	cJSON_AddNumberToObject(root, "port", std::get<1>(ip_port_paths));

	//create storage shards
	item_shards = cJSON_CreateArray();
	cJSON_AddItemToObject(root, "shards", item_shards);
	for(auto &vec_ip_port_paths: vec_shard)
	{
		item_sub = cJSON_CreateObject();
		cJSON_AddItemToArray(item_shards, item_sub);

		name = "shard" + std::to_string(shard_id++);
		cJSON_AddStringToObject(item_sub, "shard_name", name.c_str());

		item_sub_sub = cJSON_CreateArray();
		cJSON_AddItemToObject(item_sub, "shard_nodes", item_sub_sub);

		for(auto &ip_port_paths: vec_ip_port_paths)
		{
			item_sub_sub_sub = cJSON_CreateObject();
			cJSON_AddItemToArray(item_sub_sub, item_sub_sub_sub);

			cJSON_AddStringToObject(item_sub_sub_sub, "ip", std::get<0>(ip_port_paths).c_str());
			cJSON_AddNumberToObject(item_sub_sub_sub, "port", std::get<1>(ip_port_paths));
			cJSON_AddStringToObject(item_sub_sub_sub, "user", "pgx");
			cJSON_AddStringToObject(item_sub_sub_sub, "password", "pgx_pwd");
		}
	}

	//create meta
	std::vector<Tpye_Ip_Port_User_Pwd> meta;
	if(!get_meta_info(meta))
	{
		syslog(Logger::ERROR, "get_meta_info error");
		goto end;
	}

	item_meta = cJSON_CreateArray();
	cJSON_AddItemToObject(root, "meta", item_meta);
	for(auto &ip_port_user_pwd: meta)
	{
		item_sub = cJSON_CreateObject();
		cJSON_AddItemToArray(item_meta, item_sub);

		cJSON_AddStringToObject(item_sub, "ip", std::get<0>(ip_port_user_pwd).c_str());
		cJSON_AddNumberToObject(item_sub, "port", std::get<1>(ip_port_user_pwd));
		cJSON_AddStringToObject(item_sub, "user", std::get<2>(ip_port_user_pwd).c_str());
		cJSON_AddStringToObject(item_sub, "password", std::get<3>(ip_port_user_pwd).c_str());
	}

	cjson = cJSON_Print(root);
	//syslog(Logger::INFO, "cjson=%s",cjson);

	/////////////////////////////////////////////////////////
	// send json parameter to first computer node
	post_url = "http://" + std::get<0>(ip_port_paths) + ":" + std::to_string(node_mgr_http_port);
	
	while(retry-->0 && !Job::do_exit)
	{
		if(Http_client::get_instance()->Http_client_post_para(post_url.c_str(), cjson, result_str)==0)
			break;
	}
	free(cjson);
	cjson = NULL;

	if(retry<0)
	{
		syslog(Logger::ERROR, "start cluster fail because http post");
		goto end;
	}
	
	/////////////////////////////////////////////////////////
	// get install status from node 
	get_status = "{\"ver\":\"0.1\",\"job_id\":\"" + uuid_job_id + "\",\"job_type\":\"get_status\"}";
	
	retry = 20;
	while(retry-->0 && !Job::do_exit)
	{
		sleep(1);

		if(Http_client::get_instance()->Http_client_post_para(post_url.c_str(), get_status.c_str(), result_str)==0)
		{
			cJSON *ret_root;
			cJSON *ret_item;
			std::string status;

			//syslog(Logger::INFO, "result_str=%s",result_str.c_str());
			ret_root = cJSON_Parse(result_str.c_str());
			if(ret_root == NULL)
			{
				syslog(Logger::ERROR, "cJSON_Parse error"); 
				goto end;
			}

			ret_item = cJSON_GetObjectItem(ret_root, "job_status");
			if(ret_item == NULL)
			{
				syslog(Logger::ERROR, "get job_status error");
				cJSON_Delete(ret_root);
				goto end;
			}
			status = ret_item->valuestring;
			cJSON_Delete(ret_root);

			if(status.find("error") != size_t(-1))
			{
				syslog(Logger::ERROR, "install fail %s", status.c_str());
				goto end;
			}
			else if(status == "install succeed")
			{
				syslog(Logger::INFO, "start cluster %s:%d finish!", std::get<0>(ip_port_paths).c_str(), std::get<1>(ip_port_paths));
				break;
			}
		}
			
	}

	if(retry<0)
	{
		syslog(Logger::ERROR, "start cluster fail %s", result_str.c_str());
		goto end;
	}

	ret = true;

end:
	if(root!=NULL)
		cJSON_Delete(root);
	if(cjson!=NULL)
		free(cjson);

	return ret;
}

void Job::job_create_cluster(cJSON *root)
{
	std::unique_lock<std::mutex> lock(mutex_install_);

	int shards;
	int nodes;
	int comps;
	
	std::string job_id;
	std::string cluster_name;
	cJSON *item;
	cJSON *item_node;
	cJSON *item_sub;
	
	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	item = cJSON_GetObjectItem(root, "shards");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get shards error");
		return;
	}
	shards = item->valueint;

	item = cJSON_GetObjectItem(root, "nodes");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get nodes error");
		return;
	}
	nodes = item->valueint;

	item = cJSON_GetObjectItem(root, "comps");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get comps error");
		return;
	}
	comps = item->valueint;

	item = cJSON_GetObjectItem(root, "cluster_name");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get cluster_name error");
		return;
	}
	cluster_name = item->valuestring;

	if(!Node_info::get_instance()->update_nodes())
	{
		syslog(Logger::ERROR, "Node_info update_nodes error");
		return;
	}

	std::vector <std::vector<Tpye_Ip_Port_Paths>> vec_shard_storage_ip_port_paths;
	for(int i=0; i<shards; i++)
	{
		std::vector<Tpye_Ip_Port_Paths> vec_storage_ip_port_paths;
		if(!Node_info::get_instance()->get_storage_nodes(nodes, vec_storage_ip_port_paths))
		{
			syslog(Logger::ERROR, "Node_info get_storage_nodes error");
			return;
		}
		vec_shard_storage_ip_port_paths.push_back(vec_storage_ip_port_paths);
	}

	std::vector<Tpye_Ip_Port_Paths> vec_comps_ip_port_paths;
	if(!Node_info::get_instance()->get_computer_nodes(comps, vec_comps_ip_port_paths))
	{
		syslog(Logger::ERROR, "Node_info get_computer_nodes error");
		return;
	}

	for(auto &shard: vec_shard_storage_ip_port_paths)
	{
		syslog(Logger::INFO, "one shard start:");
		if(!job_create_shard(shard))
		{
			syslog(Logger::ERROR, "job_create_shard error");
			return;
		}
	}

	syslog(Logger::INFO, "comp start:");
	if(!job_create_comps(vec_comps_ip_port_paths))
	{
		syslog(Logger::ERROR, "job_create_comps error");
		return;
	}

	//start cluster on shards and comps
	syslog(Logger::INFO, "start cluster:");
	if(!job_start_cluster(vec_shard_storage_ip_port_paths, vec_comps_ip_port_paths, cluster_name))
	{
		syslog(Logger::ERROR, "job_create_comps error");
		return;
	}

	syslog(Logger::INFO, "create_cluster succeed");
}

void Job::job_handle(std::string &job)
{
	syslog(Logger::INFO, "job_handle job=%s",job.c_str());

	cJSON *root;
	cJSON *item;
	Job_type job_type;

	root = cJSON_Parse(job.c_str());
	if(root == NULL)
	{
		syslog(Logger::ERROR, "cJSON_Parse error");	
		return;
	}

	item = cJSON_GetObjectItem(root, "job_type");
	if(item == NULL || !get_job_type(item->valuestring, job_type))
	{
		syslog(Logger::ERROR, "job_handle get_job_type error");
		cJSON_Delete(root);
		return;
	}

	if(job_type == JOB_CREATE_CLUSTER)
	{
		job_create_cluster(root);
	}

	cJSON_Delete(root);
}

void Job::add_job(std::string &str)
{
	pthread_mutex_lock(&thread_mtx);
	que_job.push(str);
	pthread_cond_signal(&thread_cond);
	pthread_mutex_unlock(&thread_mtx);
}

void Job::job_work()
{
	while (!Job::do_exit)  
    {  
		pthread_mutex_lock(&thread_mtx);

        while (que_job.size() == 0 && !Job::do_exit)
            pthread_cond_wait(&thread_cond, &thread_mtx);

		if(Job::do_exit)
		{
			pthread_mutex_unlock(&thread_mtx); 
			break;
		}

		auto job = que_job.front();
		que_job.pop();

		pthread_mutex_unlock(&thread_mtx);

		job_handle(job);
	}
}

extern "C" void *thread_func_job_work(void*thrdarg)
{
	Job* job = (Job*)thrdarg;
	Assert(job);

	signal(SIGPIPE, SIG_IGN);
	job->job_work();
	
	return NULL;
}

