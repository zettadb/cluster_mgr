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
#include "shard.h"
#include "http_client.h"
#include "hdfs_client.h"
#include "mysql/server/private/sql_cmd.h"
#include <signal.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h> 
#include <netdb.h>
#include <net/if.h>
#include <arpa/inet.h>

Job* Job::m_inst = NULL;
int Job::do_exit = 0;

int64_t num_job_threads = 3;
std::string http_cmd_version;

extern int64_t cluster_mgr_http_port;
extern int64_t node_mgr_http_port;
extern std::string http_upload_path;
extern int64_t stmt_retries;
extern int64_t stmt_retry_interval_ms;
std::string hdfs_server_ip;
int64_t hdfs_server_port;

std::string cluster_json_path;
std::string program_binaries_path;
std::string instance_binaries_path;
std::string storage_prog_package_name;
std::string computer_prog_package_name;
int64_t storage_instance_port_start;
int64_t computer_instance_port_start;

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
	get_local_ip();

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

bool Job::job_create_machine(cJSON *root, std::string &str_ret)
{
	std::string hostaddr;
	std::string datadir;
	std::string logdir;
	std::string wal_log_dir;
	std::string comp_datadir;

	int datadir_used;
	int logdir_used;
	int wal_log_dir_used;
	int comp_datadir_used;

	std::string job_status, str_sql;
	std::string post_url, result_str;
	cJSON *item;
	char *cjson = NULL;
	cJSON *ret_root = NULL;
	int retry;

	item = cJSON_GetObjectItem(root, "hostaddr");
	if(item == NULL)
	{
		job_status = "get hostaddr error";
		goto end;
	}
	hostaddr = item->valuestring;

	item = cJSON_GetObjectItem(root, "datadir");
	if(item == NULL)
	{
		job_status = "get datadir error";
		goto end;
	}
	datadir = item->valuestring;

	item = cJSON_GetObjectItem(root, "logdir");
	if(item == NULL)
	{
		job_status = "get logdir error";
		goto end;
	}
	logdir = item->valuestring;

	item = cJSON_GetObjectItem(root, "wal_log_dir");
	if(item == NULL)
	{
		job_status = "get wal_log_dir error";
		goto end;
	}
	wal_log_dir = item->valuestring;

	item = cJSON_GetObjectItem(root, "comp_datadir");
	if(item == NULL)
	{
		job_status = "get comp_datadir error";
		goto end;
	}
	comp_datadir = item->valuestring;

	cJSON_ReplaceItemInObject(root, "job_type", cJSON_CreateString("machine_path"));

	cjson = cJSON_Print(root);
	//syslog(Logger::INFO, "cjson=%s",cjson);

	/////////////////////////////////////////////////////////
	// http post parameter to node
	post_url = "http://" + hostaddr + ":" + std::to_string(node_mgr_http_port);
	
	retry = 3;
	while(retry-->0 && !Job::do_exit)
	{
		if(Http_client::get_instance()->Http_client_post_para(post_url.c_str(), cjson, result_str)==0)
		{
			std::string result;
			
			syslog(Logger::INFO, "result_str=%s",result_str.c_str());
			ret_root = cJSON_Parse(result_str.c_str());
			if(ret_root == NULL)
			{
				job_status = "machine cJSON_Parse error";
				goto end;
			}

			item = cJSON_GetObjectItem(ret_root, "result");
			if(item == NULL)
			{
				syslog(Logger::ERROR, "get result error");
				goto end;
			}
			result = item->valuestring;

			if(result.find("error") != size_t(-1))
			{
				job_status = result;
				goto end;
			}

			item = cJSON_GetObjectItem(ret_root, "datadir_used");
			if(item == NULL)
			{
				job_status = "get datadir_used error";
				goto end;
			}
			datadir_used = item->valueint;

			item = cJSON_GetObjectItem(ret_root, "logdir_used");
			if(item == NULL)
			{
				job_status = "get logdir_used error";
				goto end;
			}
			logdir_used = item->valueint;

			item = cJSON_GetObjectItem(ret_root, "wal_log_dir_used");
			if(item == NULL)
			{
				job_status = "get wal_log_dir_used error";
				goto end;
			}
			wal_log_dir_used = item->valueint;

			item = cJSON_GetObjectItem(ret_root, "comp_datadir_used");
			if(item == NULL)
			{
				job_status = "get comp_datadir_used error";
				goto end;
			}
			comp_datadir_used = item->valueint;

			//delete old hostaddr if exist
			str_sql = "delete from server_nodes where hostaddr='" + hostaddr + "'";
			syslog(Logger::INFO, "create_machine str_sql=%s", str_sql.c_str());
			
			if(System::get_instance()->execute_metadate_opertation(SQLCOM_INSERT, str_sql))
			{
				job_status = "delete_machine error";
				goto end;
			}

			//insert hostaddr to table
			str_sql = "INSERT INTO server_nodes(hostaddr,datadir,logdir,wal_log_dir,comp_datadir,datadir_used,logdir_used,wal_log_dir_used,comp_datadir_used,when_created) VALUES('"
						+ hostaddr + "','" + datadir + "','" + logdir + "','" + wal_log_dir + "','" + comp_datadir + "',"
						+ std::to_string(datadir_used) + "," + std::to_string(logdir_used) + "," + std::to_string(wal_log_dir_used) + "," 
						+ std::to_string(comp_datadir_used) + ",NOW()";
			syslog(Logger::INFO, "create_machine str_sql=%s", str_sql.c_str());
			
			if(System::get_instance()->execute_metadate_opertation(SQLCOM_INSERT, str_sql))
			{
				job_status = "insert_machine error";
				goto end;
			}

			cJSON_Delete(ret_root);
			ret_root = NULL;

			break;
		}
	}
	free(cjson);
	cjson = NULL;

	if(retry<0)
	{
		job_status = "http post error";
		goto end;
	}

	syslog(Logger::INFO, "create_machine succeed");
	str_ret = "{\"result\":\"succeed\",\"info\":\"create_machine succeed\"}";
	return true;
end:
	if(cjson != NULL)
		free(cjson);
	if(ret_root != NULL)
		cJSON_Delete(ret_root);

	syslog(Logger::ERROR, "create_machine %s", job_status.c_str());
	str_ret = "{\"result\":\"error\",\"info\":\"" + job_status + "\"}";
	return true;
}

bool Job::job_update_machine(cJSON *root, std::string &str_ret)
{
	return true;
}

bool Job::job_delete_machine(cJSON *root, std::string &str_ret)
{
	return true;
}

bool Job::check_local_ip(std::string &ip)
{
	for(auto &local_ip: vec_local_ip)
		if(ip == local_ip)
			return true;
	
	return false;
}

void Job::get_local_ip()
{
	int fd, num;
	struct ifreq ifq[16];
	struct ifconf ifc;

	fd = socket(AF_INET, SOCK_DGRAM, 0);
	if(fd < 0)
	{
		syslog(Logger::ERROR, "socket failed");
		return ;
	}
	
	ifc.ifc_len = sizeof(ifq);
	ifc.ifc_buf = (caddr_t)ifq;
	if(ioctl(fd, SIOCGIFCONF, (char *)&ifc))
	{
		syslog(Logger::ERROR, "ioctl failed\n");
		close(fd);
		return ;
	}
	num = ifc.ifc_len / sizeof(struct ifreq);
	if(ioctl(fd, SIOCGIFADDR, (char *)&ifq[num-1]))
	{
		syslog(Logger::ERROR, "ioctl failed\n");
		close(fd);
		return ;
	}
	close(fd);

	for(int i=0; i<num; i++)
	{
		char *tmp_ip = inet_ntoa(((struct sockaddr_in*)(&ifq[i].ifr_addr))-> sin_addr);
		//syslog(Logger::INFO, "tmp_ip=%s", tmp_ip);
		//if(strcmp(tmp_ip, "127.0.0.1") != 0)
		{
			vec_local_ip.push_back(tmp_ip);
		}
	}

	//for(auto &ip: vec_local_ip)
	//	syslog(Logger::INFO, "vec_local_ip=%s", ip.c_str());
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
	size_t n = fread(buf, 1, 36, fp);
	fclose(fp);
	
	if(n != 36)
		return false;
	
	uuid = buf;
	return true;
}

bool Job::get_timestamp(std::string &timestamp)
{
	char sysTime[128];
	struct timespec ts = {0,0};
	clock_gettime(CLOCK_REALTIME, &ts);

	struct tm *tm;
	tm = localtime(&ts.tv_sec);
		 
	snprintf(sysTime, 128, "%04u-%02u-%02u %02u:%02u:%02u", 
		tm->tm_year+1900, tm->tm_mon+1,	tm->tm_mday, 
		tm->tm_hour, tm->tm_min, tm->tm_sec); 

	timestamp = sysTime;

	return true;
}

bool Job::get_job_type(char *str, Job_type &job_type)
{
	if(strcmp(str, "get_node")==0)
		job_type = JOB_GET_NODE;
	else if(strcmp(str, "get_status")==0)
		job_type = JOB_GET_STATUS;
	else if(strcmp(str, "check_port")==0)
		job_type = JOB_CHECK_PORT;
	else if(strcmp(str, "get_disk_size")==0)
		job_type = JOB_GET_DISK_SIZE;
	else if(strcmp(str, "create_machine")==0)
		job_type = JOB_CREATE_MACHINE;
	else if(strcmp(str, "update_machine")==0)
		job_type = JOB_UPDATE_MACHINE;
	else if(strcmp(str, "delete_machine")==0)
		job_type = JOB_DELETE_MACHINE;
	else if(strcmp(str, "create_cluster")==0)
		job_type = JOB_CREATE_CLUSTER;
	else if(strcmp(str, "delete_cluster")==0)
		job_type = JOB_DELETE_CLUSTER;
	else if(strcmp(str, "backup_cluster")==0)
		job_type = JOB_BACKUP_CLUSTER;
	else if(strcmp(str, "restore_cluster")==0)
		job_type = JOB_RESTORE_CLUSTER;
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

bool Job::update_jobid_status(std::string &jobid, std::string &result, std::string &info)
{
	std::unique_lock<std::mutex> lock(mutex_stauts_);

	if(list_jobid_result_info.size()>=kMaxStatus)
		list_jobid_result_info.pop_back();

	bool is_exist = false;
	for (auto it = list_jobid_result_info.begin(); it != list_jobid_result_info.end(); ++it)
	{
		if(std::get<0>(*it) == jobid)
		{
			std::get<1>(*it) = result;
			std::get<2>(*it) = info;
			is_exist = true;
			break;
		}
	}

	if(!is_exist)
		list_jobid_result_info.push_front(std::make_tuple(jobid, result, info));

	return true;
}

bool Job::get_jobid_status(std::string &jobid, std::string &result, std::string &info)
{
	std::unique_lock<std::mutex> lock(mutex_stauts_);

	bool ret = false;
	for (auto it = list_jobid_result_info.begin(); it != list_jobid_result_info.end(); ++it)
	{
		if(std::get<0>(*it) == jobid)
		{
			result = std::get<1>(*it).c_str();
			info = std::get<2>(*it).c_str();

			ret = true;
			break;
		}
	}

	return ret;
}

bool Job::update_operation_status(std::string &info)
{
	std::unique_lock<std::mutex> lock(mutex_stauts_);

	operation_info = info;
	return true;
}

bool Job::get_operation_status(std::string &info)
{
	std::unique_lock<std::mutex> lock(mutex_stauts_);

	info = operation_info;
	return true;
}

bool Job::job_insert_operation_record(cJSON *root, std::string &result, std::string &info)
{
	std::string str_sql;
	std::string job_id;
	std::string job_type;
	std::string cluster_name;

	if(result == "busy")
		update_operation_status(info);

	cJSON *item;
	char *cjson;

	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return false;
	}
	job_id = item->valuestring;

	item = cJSON_GetObjectItem(root, "job_type");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get job_type error");
		return false;
	}
	job_type = item->valuestring;

	item = cJSON_GetObjectItem(root, "cluster_name");
	if(item == NULL)
	{
		item = cJSON_GetObjectItem(root, "backup_cluster_name");
		if(item == NULL)
		{
			item = cJSON_GetObjectItem(root, "restore_cluster_name");
			if(item == NULL)
			{
				syslog(Logger::ERROR, "get cluster_name error");
				return false;
			}
		}
	}
	cluster_name = item->valuestring;

	//delete old job_id if exist
	str_sql = "delete from operation_record where job_id='" + job_id + "'";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());
	
	if(System::get_instance()->execute_metadate_opertation(SQLCOM_INSERT, str_sql))
	{
		syslog(Logger::ERROR, "delete_operation_record error");
		return false;
	}

	cjson = cJSON_Print(root);
	if(cjson == NULL)
		return false;

	str_sql = "INSERT INTO operation_record(job_id,job_type,cluster_name,when_created,operation,result,info) VALUES('"
				+ job_id + "','" + job_type + "','" + cluster_name + "',NOW(),'" + std::string(cjson) + "','" + result + "','" + info + "')";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());
	free(cjson);

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_INSERT, str_sql))
	{
		syslog(Logger::ERROR, "insert operation_record error");
		return false;
	}

	return true;
}

bool Job::job_update_operation_record(std::string &job_id, std::string &result, std::string &info)
{
	std::string str_sql;

	if(result == "error" || result == "succeed")
	{
		std::string empty = "";
		update_operation_status(empty);
	}

	str_sql = "UPDATE operation_record set result='" + result + "',info='" + info + "' where job_id='" + job_id + "'";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_UPDATE, str_sql))
	{
		syslog(Logger::ERROR, "update operation_record error");
		return false;

	}

	return true;
}

bool Job::job_system_cmd(std::string &cmd)
{
	FILE* pfd;
	char *line;
	char buf[256];

	syslog(Logger::INFO, "system cmd %s" ,cmd.c_str());

	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		syslog(Logger::ERROR, "system cmd error %s" ,cmd.c_str());
		return false;
	}
	memset(buf, 0, 256);
	line = fgets(buf, 256, pfd);
	pclose(pfd);

	if(strlen(buf))
	{
		syslog(Logger::ERROR, "system cmd error %s, %s", cmd.c_str(), buf);
		return false;
	}

	return true;
}

bool Job::job_save_json(std::string &path, char* cjson)
{
	FILE* pfd = fopen(path.c_str(), "wb");
	if(pfd == NULL)
	{
		syslog(Logger::ERROR, "Creat json file error %s", path.c_str());
		return false;
	}

	fwrite(cjson,1,strlen(cjson),pfd);
	fclose(pfd);
	
	return true;
}

bool Job::job_create_program_path()
{
	std::string cmd, cmd_path, program_path, instance_path;

	//upzip from program_binaries_path to instance_binaries_path for install cmd
	//storage
	cmd_path = instance_binaries_path + "/" + storage_prog_package_name + "/dba_tools";
	
	if(access(cmd_path.c_str(), F_OK) != 0)
	{
		syslog(Logger::INFO, "upzip %s.tgz" , storage_prog_package_name.c_str());
		//////////////////////////////
		//upzip from program_binaries_path to instance_binaries_path
		program_path = program_binaries_path + "/" + storage_prog_package_name + ".tgz";
		instance_path = instance_binaries_path;

		//////////////////////////////
		//mkdir instance_path
		cmd = "mkdir -p " + instance_path;
		if(!job_system_cmd(cmd))
			return false;

		//////////////////////////////
		//tar to instance_path
		cmd = "tar zxf " + program_path + " -C " + instance_path;
		if(!job_system_cmd(cmd))
			return false;
	}

	//computer
	cmd_path = instance_binaries_path + "/" + computer_prog_package_name + "/scripts";
	
	if(access(cmd_path.c_str(), F_OK) != 0)
	{
		syslog(Logger::INFO, "upzip %s.tgz" , computer_prog_package_name.c_str());
		//////////////////////////////
		//upzip from program_binaries_path to instance_binaries_path
		program_path = program_binaries_path + "/" + computer_prog_package_name + ".tgz";
		instance_path = instance_binaries_path;

		//////////////////////////////
		//mkdir instance_path
		cmd = "mkdir -p " + instance_path;
		if(!job_system_cmd(cmd))
			return false;

		//////////////////////////////
		//tar to instance_path
		cmd = "tar zxf " + program_path + " -C " + instance_path;
		if(!job_system_cmd(cmd))
			return false;
	}

	return true;
}

bool Job::job_create_shard(std::vector<Tpye_Ip_Port_Paths> &storages, std::string &cluster_name, std::string &ha_mode, int shards_id)
{
	cJSON *root = NULL;
	char *cjson = NULL;
	cJSON *item_node;
	cJSON *item_sub;

	bool ret = false;
	int install_id = 0;
	std::string pathdir, jsonfile_path;
	std::string uuid_shard, uuid_job_id;
	std::string post_url,get_status,result_str;
	get_uuid(uuid_shard);
	get_uuid(uuid_job_id);

	/////////////////////////////////////////////////////////
	// create json parameter
	root = cJSON_CreateObject();
	cJSON_AddStringToObject(root, "ver", http_cmd_version.c_str());
	cJSON_AddStringToObject(root, "job_id", uuid_job_id.c_str());
	cJSON_AddStringToObject(root, "job_type", "install_storage");
	cJSON_AddStringToObject(root, "ha_mode", ha_mode.c_str());
	cJSON_AddNumberToObject(root, "install_id", install_id);
	cJSON_AddStringToObject(root, "group_uuid", uuid_shard.c_str());
	item_node = cJSON_CreateArray();
	cJSON_AddItemToObject(root, "nodes", item_node);

	for(auto &ip_port_paths: storages)
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
		pathdir = std::get<2>(ip_port_paths).at(0) + "/instance_data/data_dir_path/" 
					+ std::to_string(std::get<1>(ip_port_paths));
		cJSON_AddStringToObject(item_sub, "data_dir_path", pathdir.c_str());
		pathdir = std::get<2>(ip_port_paths).at(1) + "/instance_data/log_dir_path/" 
					+ std::to_string(std::get<1>(ip_port_paths));
		cJSON_AddStringToObject(item_sub, "log_dir_path", pathdir.c_str());
		pathdir = std::get<2>(ip_port_paths).at(2) + "/instance_data/innodb_log_dir_path/" 
					+ std::to_string(std::get<1>(ip_port_paths));
		cJSON_AddStringToObject(item_sub, "innodb_log_dir_path", pathdir.c_str());
		cJSON_AddStringToObject(item_sub, "innodb_buffer_pool_size", "64MB");
		cJSON_AddStringToObject(item_sub, "user", "kunlun");
		cJSON_AddNumberToObject(item_sub, "election_weight", 50);
	}

	/////////////////////////////////////////////////////////
	// save json file to cluster_json_path
	jsonfile_path = cluster_json_path + "/" + cluster_name + "/mysql_shard_" + std::to_string(shards_id) + ".json";
	cjson = cJSON_Print(root);
	if(cjson != NULL)
	{
		job_save_json(jsonfile_path, cjson);
		free(cjson);
		cjson = NULL;
	}

	/////////////////////////////////////////////////////////
	// send json parameter to every node
	install_id = 0;
	for(auto &ip_port_paths: storages)
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
		// get status from node 
		get_status = "{\"ver\":\"" + http_cmd_version + "\",\"job_id\":\"" + uuid_job_id + "\",\"job_type\":\"get_status\"}";
		
		retry = 60;
		while(retry-->0 && !Job::do_exit)
		{
			sleep(1);

			if(Http_client::get_instance()->Http_client_post_para(post_url.c_str(), get_status.c_str(), result_str)==0)
			{
				cJSON *ret_root;
				cJSON *ret_item;
				std::string result,info;

				//syslog(Logger::INFO, "result_str=%s",result_str.c_str());
				ret_root = cJSON_Parse(result_str.c_str());
				if(ret_root == NULL)
				{
					syslog(Logger::ERROR, "cJSON_Parse error");	
					goto end;
				}

				ret_item = cJSON_GetObjectItem(ret_root, "result");
				if(ret_item == NULL)
				{
					syslog(Logger::ERROR, "get result error");
					cJSON_Delete(ret_root);
					goto end;
				}
				result = ret_item->valuestring;

				ret_item = cJSON_GetObjectItem(ret_root, "info");
				if(ret_item == NULL)
				{
					syslog(Logger::ERROR, "get info error");
					cJSON_Delete(ret_root);
					goto end;
				}
				info = ret_item->valuestring;
				cJSON_Delete(ret_root);

				if(result == "error")
				{
					syslog(Logger::ERROR, "install fail %s", info.c_str());
					goto end;
				}
				else if(result == "succeed")
				{
					syslog(Logger::INFO, "storage instance %s:%d install finish!", std::get<0>(ip_port_paths).c_str(), std::get<1>(ip_port_paths));
					break;
				}
			}
				
		}

		if(retry<0)
		{
			syslog(Logger::ERROR, "create storage instance timeout %s", result_str.c_str());
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

bool Job::job_create_comps(std::vector<Tpye_Ip_Port_Paths> &comps, std::string &cluster_name, int comps_id)
{
	cJSON *root = NULL;
	char *cjson = NULL;
	cJSON *item_node;
	cJSON *item_sub;

	FILE* pfd;
	char buf[256];

	bool ret = false;
	int install_id = 0;
	std::string name, pathdir, jsonfile_path;
	std::string uuid_job_id;
	std::string post_url,get_status,result_str;
	get_uuid(uuid_job_id);
	
	/////////////////////////////////////////////////////////
	// create json parameter
	root = cJSON_CreateObject();
	cJSON_AddStringToObject(root, "ver", http_cmd_version.c_str());
	cJSON_AddStringToObject(root, "job_id", uuid_job_id.c_str());
	cJSON_AddStringToObject(root, "job_type", "install_computer");
	cJSON_AddNumberToObject(root, "install_id", install_id);
	item_node = cJSON_CreateArray();
	cJSON_AddItemToObject(root, "nodes", item_node);

	for(auto &ip_port_paths: comps)
	{
		item_sub = cJSON_CreateObject();
		cJSON_AddItemToArray(item_node, item_sub);

		cJSON_AddNumberToObject(item_sub, "id", comps_id+install_id++);
		name = "comp" + std::to_string(install_id);
		cJSON_AddStringToObject(item_sub, "name", name.c_str());
		cJSON_AddStringToObject(item_sub, "ip", std::get<0>(ip_port_paths).c_str());
		cJSON_AddNumberToObject(item_sub, "port", std::get<1>(ip_port_paths));
		cJSON_AddStringToObject(item_sub, "user", "abc");
		cJSON_AddStringToObject(item_sub, "password", "abc");
		pathdir = std::get<2>(ip_port_paths).at(0) + "/instance_data/comp_datadir/" 
					+ std::to_string(std::get<1>(ip_port_paths));
		cJSON_AddStringToObject(item_sub, "datadir", pathdir.c_str());
	}

	/////////////////////////////////////////////////////////
	// save json file to cluster_json_path and cmd_path
	jsonfile_path = cluster_json_path + "/" + cluster_name + "/pgsql_comps_1_" + std::to_string(install_id) + ".json";
	cjson = cJSON_Print(item_node);
	if(cjson != NULL)
	{
		job_save_json(jsonfile_path, cjson);

		jsonfile_path = instance_binaries_path + "/" + computer_prog_package_name + "/scripts/pgsql_comps.json";
		job_save_json(jsonfile_path, cjson);

		free(cjson);
		cjson = NULL;
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
		// get status from node 
		get_status = "{\"ver\":\"" + http_cmd_version + "\",\"job_id\":\"" + uuid_job_id + "\",\"job_type\":\"get_status\"}";
		
		retry = 60;
		while(retry-->0 && !Job::do_exit)
		{
			sleep(1);

			if(Http_client::get_instance()->Http_client_post_para(post_url.c_str(), get_status.c_str(), result_str)==0)
			{
				cJSON *ret_root;
				cJSON *ret_item;
				std::string result,info;
				
				//syslog(Logger::INFO, "result_str=%s",result_str.c_str());
				ret_root = cJSON_Parse(result_str.c_str());
				if(ret_root == NULL)
				{
					syslog(Logger::ERROR, "cJSON_Parse error"); 
					goto end;
				}

				ret_item = cJSON_GetObjectItem(ret_root, "result");
				if(ret_item == NULL)
				{
					syslog(Logger::ERROR, "get result error");
					cJSON_Delete(ret_root);
					goto end;
				}
				result = ret_item->valuestring;

				ret_item = cJSON_GetObjectItem(ret_root, "info");
				if(ret_item == NULL)
				{
					syslog(Logger::ERROR, "get info error");
					cJSON_Delete(ret_root);
					goto end;
				}
				info = ret_item->valuestring;
				cJSON_Delete(ret_root);

				if(result == "error")
				{
					syslog(Logger::ERROR, "install fail %s", info.c_str());
					goto end;
				}
				else if(result == "succeed")
				{
					syslog(Logger::INFO, "computer instance %s:%d install finish!", std::get<0>(ip_port_paths).c_str(), std::get<1>(ip_port_paths));
					break;
				}
			}
				
		}

		if(retry<0)
		{
			syslog(Logger::ERROR, "create computer instance timeout %s", result_str.c_str());
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

bool Job::job_start_cluster(std::vector <std::vector<Tpye_Ip_Port_Paths>> &vec_shard, std::string &cluster_name, std::string &ha_mode)
{
	cJSON *root = NULL;
	char *cjson = NULL;
	cJSON *item_sub;
	cJSON *item_sub_sub;
	cJSON *item_sub_sub_sub;

	FILE* pfd;
	char buf[256];

	int retry;
	int shard_id = 1;
	std::string name, cmd, cmd_path, program_path, instance_path, jsonfile_path;

	/////////////////////////////////////////////////////////
	//create storage shards json
	root = cJSON_CreateArray();
	for(auto &vec_ip_port_paths: vec_shard)
	{
		item_sub = cJSON_CreateObject();
		cJSON_AddItemToArray(root, item_sub);

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

	/////////////////////////////////////////////////////////
	// save json file to cluster_json_path and cmd_path
	jsonfile_path = instance_binaries_path + "/" + computer_prog_package_name + "/scripts/pgsql_shards.json";
	cjson = cJSON_Print(root);
	if(cjson != NULL)
	{
		job_save_json(jsonfile_path, cjson);
		free(cjson);
	}
	cJSON_Delete(root);

	/////////////////////////////////////////////////////////
	//create meta json
	std::vector<Tpye_Ip_Port_User_Pwd> meta;
	if(!System::get_instance()->get_meta_info(meta))
	{
		syslog(Logger::ERROR, "get_meta_info error");
		return false;
	}

	root = cJSON_CreateArray();
	for(auto &ip_port_user_pwd: meta)
	{
		item_sub = cJSON_CreateObject();
		cJSON_AddItemToArray(root, item_sub);

		cJSON_AddStringToObject(item_sub, "ip", std::get<0>(ip_port_user_pwd).c_str());
		cJSON_AddNumberToObject(item_sub, "port", std::get<1>(ip_port_user_pwd));
		cJSON_AddStringToObject(item_sub, "user", std::get<2>(ip_port_user_pwd).c_str());
		cJSON_AddStringToObject(item_sub, "password", std::get<3>(ip_port_user_pwd).c_str());
	}

	/////////////////////////////////////////////////////////
	// save json file to cluster_json_path
	jsonfile_path = instance_binaries_path + "/" + computer_prog_package_name + "/scripts/pgsql_meta.json";
	cjson = cJSON_Print(root);
	if(cjson != NULL)
	{
		job_save_json(jsonfile_path, cjson);
		free(cjson);
	}
	cJSON_Delete(root);

	/////////////////////////////////////////////////////////
	// start cluster cmd
	cmd = "cd " + instance_binaries_path + "/" + computer_prog_package_name + "/scripts/;";
	cmd += "python2 create_cluster.py --shards_config ./pgsql_shards.json --comps_config ./pgsql_comps.json --meta_config ./pgsql_meta.json --cluster_name ";
	cmd += cluster_name + " --cluster_owner abc --cluster_biz kunlun --ha_mode " + ha_mode;
	syslog(Logger::INFO, "job_start_cluster cmd %s", cmd.c_str());

	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		syslog(Logger::ERROR, "install error %s", cmd.c_str());
		return false;
	}
	while(fgets(buf, 256, pfd)!=NULL)
	{
		if(strcasestr(buf, "error") != NULL)
			syslog(Logger::ERROR, "install %s", buf);
	}
	pclose(pfd);

	/////////////////////////////////////////////////////////////
	// check instance succeed by connect to instance
	retry = 6;
	while(retry-->0 && !Job::do_exit)
	{
		sleep(1);
		if(System::get_instance()->check_cluster_name(cluster_name))
			break;
	}

	if(retry<0)
	{
		syslog(Logger::ERROR, "cluster start error");
		return false;
	}

	return true;
}

void Job::job_create_cluster(cJSON *root)
{
	std::unique_lock<std::mutex> lock(mutex_operation_);

	std::string job_id;
	std::string job_result;
	std::string job_info;
	std::string cluster_name;
	std::string ha_mode;
	std::string cmd;
	cJSON *item;

	int shards;
	int nodes;
	int comps;
	int shards_id = 0;
	int comps_id = 0;
	std::vector <std::vector<Tpye_Ip_Port_Paths>> vec_shard_storage_ip_port_paths;
	std::vector<Tpye_Ip_Port_Paths> vec_comps_ip_port_paths;

	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	job_result = "busy";
	job_info = "create cluster start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_insert_operation_record(root, job_result, job_info);

	item = cJSON_GetObjectItem(root, "ha_mode");
	if(item == NULL)
	{
		job_info = "get ha_mode error";
		goto end;
	}
	ha_mode = item->valuestring;

	item = cJSON_GetObjectItem(root, "shards");
	if(item == NULL)
	{
		job_info = "get shards error";
		goto end;
	}
	shards = item->valueint;
	if(shards<1 || shards>10)
	{
		job_info = "shards(1-10) error";
		goto end;
	}

	item = cJSON_GetObjectItem(root, "nodes");
	if(item == NULL)
	{
		job_info = "get nodes error";
		goto end;
	}
	nodes = item->valueint;
	if(ha_mode == "mgr")
	{
		if(nodes<3 || nodes>10)
		{
			job_info = "error, nodes>=3 && nodes<=10 in mgr mode";
			goto end;
		}
	}
	else if(ha_mode == "no_rep")
	{
		if(nodes!=1)
		{
			job_info = "error, nodes=1 in no_rep mode";
			goto end;
		}
	}
	else
	{
		job_info = "it is not support " + ha_mode;
		goto end;
	}

	item = cJSON_GetObjectItem(root, "comps");
	if(item == NULL)
	{
		job_info = "get comps error";
		goto end;
	}
	comps = item->valueint;
	if(comps<1 || comps>10)
	{
		job_info = "comps(1-10) error";
		goto end;
	}

	item = cJSON_GetObjectItem(root, "cluster_name");
	if(item == NULL)
	{
		job_info = "get cluster_name error";
		goto end;
	}
	cluster_name = item->valuestring;
	if(System::get_instance()->check_cluster_name(cluster_name))
	{
		job_info = "cluster_name is exist, error";
		goto end;
	}

	if(!Machine_info::get_instance()->update_machine_info())
	{
		job_info = "Machine_info update_machine_info() error";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// for install cluster cmd
	if(!job_create_program_path())
	{
		job_info = "create_cmd_path error";
		goto end;
	}

	// for save cluster json file
	cmd = "mkdir -p " + cluster_json_path + "/" + cluster_name;
	if(!job_system_cmd(cmd))
	{
		job_info = "system_cmd error";
		goto end;
	}

	///////////////////////////////////////////////////////////////////////////////
	// get storage of shard 
	for(int i=0; i<shards; i++)
	{
		std::vector<Tpye_Ip_Port_Paths> vec_storage_ip_port_paths;
		if(!Machine_info::get_instance()->get_storage_nodes(nodes, vec_storage_ip_port_paths))
		{
			job_info = "Machine_info get_storage_nodes error";
			goto end;
		}
		vec_shard_storage_ip_port_paths.push_back(vec_storage_ip_port_paths);
	}

	///////////////////////////////////////////////////////////////////////////////
	// get computer 
	if(!Machine_info::get_instance()->get_computer_nodes(comps, vec_comps_ip_port_paths))
	{
		job_info = "Machine_info get_computer_nodes error";
		goto end;
	}

	///////////////////////////////////////////////////////////////////////////////
	// get start index for shards and comps 
	System::get_instance()->get_comp_nodes_id_seq(comps_id);
	//syslog(Logger::INFO, "comps_id=%d", comps_id);
	shards_id = 1;
	comps_id += 1;

	///////////////////////////////////////////////////////////////////////////////
	// create storage of shard
	for(auto &storages: vec_shard_storage_ip_port_paths)
	{
		job_info = "create shard " + std::to_string(shards_id) + " start";
		update_jobid_status(job_id, job_result, job_info);
		syslog(Logger::INFO, "%s", job_info.c_str());
		if(!job_create_shard(storages, cluster_name, ha_mode, shards_id++))
		{
			job_info = "create_shard error";
			goto end;
		}
	}

	///////////////////////////////////////////////////////////////////////////////
	// create computer
	job_info = "create comps start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	if(!job_create_comps(vec_comps_ip_port_paths, cluster_name, comps_id))
	{
		job_info = "create_comps error";
		goto end;
	}

	///////////////////////////////////////////////////////////////////////////////
	//start cluster on shards and comps
	job_info = "start cluster cmd";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	if(!job_start_cluster(vec_shard_storage_ip_port_paths, cluster_name, ha_mode))
	{
		job_info = "start_cluster error";
		goto end;
	}

	job_result = "succeed";
	job_info = "create cluster succeed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_update_operation_record(job_id, job_result, job_info);
	return;

end:
	job_result = "error";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::ERROR, "%s", job_info.c_str());
	job_update_operation_record(job_id, job_result, job_info);
}

bool Job::job_delete_shard(std::vector<Tpye_Ip_Port> &storages, std::string &cluster_name)
{
	cJSON *root = NULL;
	char *cjson = NULL;

	bool ret = false;
	std::string uuid_job_id;
	std::string post_url,get_status,result_str;
	get_uuid(uuid_job_id);

	/////////////////////////////////////////////////////////
	// create json parameter
	root = cJSON_CreateObject();
	cJSON_AddStringToObject(root, "ver", http_cmd_version.c_str());
	cJSON_AddStringToObject(root, "job_id", uuid_job_id.c_str());
	cJSON_AddStringToObject(root, "job_type", "delete_storage");
	cJSON_AddStringToObject(root, "ip", "");
	cJSON_AddNumberToObject(root, "port", 0);

	for(auto &ip_port: storages)
	{
		cJSON_ReplaceItemInObject(root, "ip", cJSON_CreateString(ip_port.first.c_str()));
		cJSON_ReplaceItemInObject(root, "port", cJSON_CreateNumber(ip_port.second));

		cjson = cJSON_Print(root);
		//syslog(Logger::INFO, "cjson=%s",cjson);

		/////////////////////////////////////////////////////////
		// http post parameter to node
		post_url = "http://" + ip_port.first + ":" + std::to_string(node_mgr_http_port);
		
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
			syslog(Logger::ERROR, "delete storage instance fail because http post");
			goto end;
		}
		
		/////////////////////////////////////////////////////////
		// get status from node 
		get_status = "{\"ver\":\"" + http_cmd_version + "\",\"job_id\":\"" + uuid_job_id + "\",\"job_type\":\"get_status\"}";
		
		retry = 60;
		while(retry-->0 && !Job::do_exit)
		{
			sleep(1);

			if(Http_client::get_instance()->Http_client_post_para(post_url.c_str(), get_status.c_str(), result_str)==0)
			{
				cJSON *ret_root;
				cJSON *ret_item;
				std::string result,info;

				//syslog(Logger::INFO, "result_str=%s",result_str.c_str());
				ret_root = cJSON_Parse(result_str.c_str());
				if(ret_root == NULL)
				{
					syslog(Logger::ERROR, "cJSON_Parse error");	
					goto end;
				}

				ret_item = cJSON_GetObjectItem(ret_root, "result");
				if(ret_item == NULL)
				{
					syslog(Logger::ERROR, "get result error");
					cJSON_Delete(ret_root);
					goto end;
				}
				result = ret_item->valuestring;

				ret_item = cJSON_GetObjectItem(ret_root, "info");
				if(ret_item == NULL)
				{
					syslog(Logger::ERROR, "get info error");
					cJSON_Delete(ret_root);
					goto end;
				}
				info = ret_item->valuestring;
				cJSON_Delete(ret_root);

				if(result == "error")
				{
					syslog(Logger::ERROR, "delete fail %s", info.c_str());
					goto end;
				}
				else if(result == "succeed")
				{
					syslog(Logger::INFO, "storage instance %s:%d delete finish!", ip_port.first.c_str(), ip_port.second);
					break;
				}
			}
				
		}

		if(retry<0)
		{
			syslog(Logger::ERROR, "delete storage instance timeout %s", result_str.c_str());
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

bool Job::job_delete_comps(std::vector<Tpye_Ip_Port> &comps, std::string &cluster_name)
{
	cJSON *root = NULL;
	char *cjson = NULL;

	bool ret = false;
	std::string uuid_job_id;
	std::string post_url,get_status,result_str;
	get_uuid(uuid_job_id);

	/////////////////////////////////////////////////////////
	// create json parameter
	root = cJSON_CreateObject();
	cJSON_AddStringToObject(root, "ver", http_cmd_version.c_str());
	cJSON_AddStringToObject(root, "job_id", uuid_job_id.c_str());
	cJSON_AddStringToObject(root, "job_type", "delete_computer");
	cJSON_AddStringToObject(root, "ip", "");
	cJSON_AddNumberToObject(root, "port", 0);

	for(auto &ip_port: comps)
	{
		cJSON_ReplaceItemInObject(root, "ip", cJSON_CreateString(ip_port.first.c_str()));
		cJSON_ReplaceItemInObject(root, "port", cJSON_CreateNumber(ip_port.second));

		cjson = cJSON_Print(root);
		//syslog(Logger::INFO, "cjson=%s",cjson);

		/////////////////////////////////////////////////////////
		// http post parameter to node
		post_url = "http://" + ip_port.first + ":" + std::to_string(node_mgr_http_port);
		
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
			syslog(Logger::ERROR, "delete computer instance fail because http post");
			goto end;
		}
		
		/////////////////////////////////////////////////////////
		// get status from node 
		get_status = "{\"ver\":\"" + http_cmd_version + "\",\"job_id\":\"" + uuid_job_id + "\",\"job_type\":\"get_status\"}";
		
		retry = 60;
		while(retry-->0 && !Job::do_exit)
		{
			sleep(1);

			if(Http_client::get_instance()->Http_client_post_para(post_url.c_str(), get_status.c_str(), result_str)==0)
			{
				cJSON *ret_root;
				cJSON *ret_item;
				std::string result,info;

				//syslog(Logger::INFO, "result_str=%s",result_str.c_str());
				ret_root = cJSON_Parse(result_str.c_str());
				if(ret_root == NULL)
				{
					syslog(Logger::ERROR, "cJSON_Parse error");	
					goto end;
				}

				ret_item = cJSON_GetObjectItem(ret_root, "result");
				if(ret_item == NULL)
				{
					syslog(Logger::ERROR, "get result error");
					cJSON_Delete(ret_root);
					goto end;
				}
				result = ret_item->valuestring;

				ret_item = cJSON_GetObjectItem(ret_root, "info");
				if(ret_item == NULL)
				{
					syslog(Logger::ERROR, "get info error");
					cJSON_Delete(ret_root);
					goto end;
				}
				info = ret_item->valuestring;
				cJSON_Delete(ret_root);

				if(result == "error")
				{
					syslog(Logger::ERROR, "delete fail %s", info.c_str());
					goto end;
				}
				else if(result == "succeed")
				{
					syslog(Logger::INFO, "computer instance %s:%d delete finish!", ip_port.first.c_str(), ip_port.second);
					break;
				}
			}
				
		}

		if(retry<0)
		{
			syslog(Logger::ERROR, "delete computer instance timeout %s", result_str.c_str());
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

void Job::job_delete_cluster(cJSON *root)
{
	std::unique_lock<std::mutex> lock(mutex_operation_);

	std::string job_id;
	std::string job_result;
	std::string job_info;
	std::string cluster_name;
	std::string cmd;
	cJSON *item;

	std::vector <std::vector<Tpye_Ip_Port>> vec_shard_storage_ip_port;
	std::vector<Tpye_Ip_Port> vec_comps_ip_port;

	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	job_result = "busy";
	job_info = "delete cluster start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_insert_operation_record(root, job_result, job_info);

	item = cJSON_GetObjectItem(root, "cluster_name");
	if(item == NULL)
	{
		job_info = "get cluster_name error";
		goto end;
	}
	cluster_name = item->valuestring;
	if(!System::get_instance()->check_cluster_name(cluster_name))
	{
		job_info = "cluster_name is no exist, error";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// delete cluster info from meta talbes
	job_info = "stop cluster start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	if(!System::get_instance()->stop_cluster(vec_shard_storage_ip_port, vec_comps_ip_port, cluster_name))
	{
		job_info = "end_cluster error";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// delete comps from every node
	job_info = "delete comps start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	if(!job_delete_comps(vec_comps_ip_port, cluster_name))
	{
		job_info = "delete_comps error";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// delete storages from every node
	job_info = "delete shards start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	for(auto &storages: vec_shard_storage_ip_port)
	{
		if(!job_delete_shard(storages, cluster_name))
		{
			job_info = "delete_shard error";
			goto end;
		}
	}

	/////////////////////////////////////////////////////////
	// delete cluster json file
	cmd = "rm -rf " + cluster_json_path + "/" + cluster_name;
	if(!job_system_cmd(cmd))
	{
		job_info = "system_cmd error";
		goto end;
	}

	job_result = "succeed";
	job_info = "delete cluster succeed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_update_operation_record(job_id, job_result, job_info);
	return;

end:
	job_result = "error";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::ERROR, "%s", job_info.c_str());
	job_update_operation_record(job_id, job_result, job_info);
}

bool Job::job_backup_shard(std::string &cluster_name, Tpye_Ip_Port &ip_port, int shard_id)
{
	cJSON *root = NULL;
	char *cjson = NULL;

	bool ret = false;
	std::string shard_name,uuid_job_id;
	std::string post_url,get_status,result_str;
	get_uuid(uuid_job_id);
	shard_name = "shard" + std::to_string(shard_id);

	/////////////////////////////////////////////////////////
	// create json parameter
	root = cJSON_CreateObject();
	cJSON_AddStringToObject(root, "ver", http_cmd_version.c_str());
	cJSON_AddStringToObject(root, "job_id", uuid_job_id.c_str());
	cJSON_AddStringToObject(root, "job_type", "backup_shard");
	cJSON_AddStringToObject(root, "ip", ip_port.first.c_str());
	cJSON_AddNumberToObject(root, "port", ip_port.second);
	cJSON_AddStringToObject(root, "cluster_name", cluster_name.c_str());
	cJSON_AddStringToObject(root, "shard_name", shard_name.c_str());
	cJSON_AddStringToObject(root, "hdfs_ip", hdfs_server_ip.c_str());
	cJSON_AddNumberToObject(root, "hdfs_port", hdfs_server_port);

	/////////////////////////////////////////////////////////
	// send json parameter to node

	cjson = cJSON_Print(root);
	cJSON_Delete(root);
	root = NULL;
	//syslog(Logger::INFO, "cjson=%s",cjson);

	/////////////////////////////////////////////////////////
	// http post parameter to node
	post_url = "http://" + ip_port.first + ":" + std::to_string(node_mgr_http_port);
	
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
		syslog(Logger::ERROR, "backup shard fail because http post");
		goto end;
	}

	/////////////////////////////////////////////////////////
	// get status from node 
	get_status = "{\"ver\":\"" + http_cmd_version + "\",\"job_id\":\"" + uuid_job_id + "\",\"job_type\":\"get_status\"}";
	
	retry = 90;
	while(retry-->0 && !Job::do_exit)
	{
		sleep(1);

		if(Http_client::get_instance()->Http_client_post_para(post_url.c_str(), get_status.c_str(), result_str)==0)
		{
			cJSON *ret_root;
			cJSON *ret_item;
			std::string result,info;
			
			//syslog(Logger::INFO, "result_str=%s",result_str.c_str());
			ret_root = cJSON_Parse(result_str.c_str());
			if(ret_root == NULL)
			{
				syslog(Logger::ERROR, "cJSON_Parse error"); 
				goto end;
			}

			ret_item = cJSON_GetObjectItem(ret_root, "result");
			if(ret_item == NULL)
			{
				syslog(Logger::ERROR, "get result error");
				cJSON_Delete(ret_root);
				goto end;
			}
			result = ret_item->valuestring;

			ret_item = cJSON_GetObjectItem(ret_root, "info");
			if(ret_item == NULL)
			{
				syslog(Logger::ERROR, "get info error");
				cJSON_Delete(ret_root);
				goto end;
			}
			info = ret_item->valuestring;
			cJSON_Delete(ret_root);

			if(result == "error")
			{
				syslog(Logger::ERROR, "backup fail %s", info.c_str());
				goto end;
			}
			else if(result == "succeed")
			{
				syslog(Logger::INFO, "backup shard %d:%s:%d finish!", shard_id, ip_port.first.c_str(), ip_port.second);
				break;
			}
		}
	}

	if(retry<0)
	{
		syslog(Logger::ERROR, "backup shard timeout %s", result_str.c_str());
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

void Job::job_backup_cluster(cJSON *root)
{
	std::unique_lock<std::mutex> lock(mutex_operation_);

	std::string job_id;
	std::string job_result;
	std::string job_info;
	std::string backup_cluster_name,backup_info;
	std::string str_sql,timestamp,shards_name;
	int shards_id = 0;
	cJSON *item;

	std::vector<Tpye_Ip_Port> vec_ip_port;

	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	job_result = "busy";
	job_info = "backup cluster start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_insert_operation_record(root, job_result, job_info);

	System::get_instance()->set_cluster_mgr_working(false);

	item = cJSON_GetObjectItem(root, "backup_info");
	if(item == NULL)
	{
		job_info = "get backup_info error";
		goto end;
	}
	backup_info = item->valuestring;

	item = cJSON_GetObjectItem(root, "backup_cluster_name");
	if(item == NULL)
	{
		job_info = "get backup_cluster_name error";
		goto end;
	}
	backup_cluster_name = item->valuestring;

	/////////////////////////////////////////////////////////
	// get master node of erver shard
	if(!System::get_instance()->get_shard_ip_port_backup(backup_cluster_name, vec_ip_port))
	{
		job_info = "get cluster_shard_ip_port error";
		goto end;
	}

	///////////////////////////////////////////////////////////////////////////////
	// backup every shard
	for(auto &ip_port: vec_ip_port)
	{
		shards_id++;
		job_info = "backup shard " + std::to_string(shards_id) + " start";
		update_jobid_status(job_id, job_result, job_info);
		syslog(Logger::INFO, "%s", job_info.c_str());
		if(!job_backup_shard(backup_cluster_name, ip_port, shards_id))
		{
			job_info = "job_backup_shard error";
			goto end;
		}
	}

	///////////////////////////////////////////////////////////////////////////////
	// save to metadata table
	get_timestamp(timestamp);
	str_sql = "INSERT INTO cluster_backups(backup_info,cluster_name,shards,when_created) VALUES('"
				+ backup_info + "','" + backup_cluster_name + "'," + std::to_string(shards_id) + ",'" + timestamp + "')";
	syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_INSERT, str_sql))
	{
		job_info = "insert cluster_backups error";
		goto end;
	}

	job_result = "succeed";
	job_info = "backup cluster succeed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_update_operation_record(job_id, job_result, job_info);
	System::get_instance()->set_cluster_mgr_working(true);
	return;

end:
	job_result = "error";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_update_operation_record(job_id, job_result, job_info);
	System::get_instance()->set_cluster_mgr_working(true);
}

bool Job::job_restore_shard(std::string &cluster_name, std::string &shard_name, std::string &timestamp, Tpye_Ip_Port &ip_port)
{
	cJSON *root = NULL;
	char *cjson = NULL;

	bool ret = false;
	std::string uuid_job_id;
	std::string post_url,get_status,result_str;
	get_uuid(uuid_job_id);
	
	/////////////////////////////////////////////////////////
	// create json parameter
	root = cJSON_CreateObject();
	cJSON_AddStringToObject(root, "ver", http_cmd_version.c_str());
	cJSON_AddStringToObject(root, "job_id", uuid_job_id.c_str());
	cJSON_AddStringToObject(root, "job_type", "restore_shard");
	cJSON_AddStringToObject(root, "ip", ip_port.first.c_str());
	cJSON_AddNumberToObject(root, "port", ip_port.second);
	cJSON_AddStringToObject(root, "cluster_name", cluster_name.c_str());
	cJSON_AddStringToObject(root, "shard_name", shard_name.c_str());
	cJSON_AddStringToObject(root, "timestamp", timestamp.c_str());
	cJSON_AddStringToObject(root, "hdfs_ip", hdfs_server_ip.c_str());
	cJSON_AddNumberToObject(root, "hdfs_port", hdfs_server_port);

	/////////////////////////////////////////////////////////
	// send json parameter to node

	cjson = cJSON_Print(root);
	cJSON_Delete(root);
	root = NULL;
	//syslog(Logger::INFO, "cjson=%s",cjson);

	/////////////////////////////////////////////////////////
	// http post parameter to node
	post_url = "http://" + ip_port.first + ":" + std::to_string(node_mgr_http_port);
	
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
		syslog(Logger::ERROR, "retore shard fail because http post");
		goto end;
	}

	/////////////////////////////////////////////////////////
	// get status from node 
	get_status = "{\"ver\":\"" + http_cmd_version + "\",\"job_id\":\"" + uuid_job_id + "\",\"job_type\":\"get_status\"}";
	
	retry = 90;
	while(retry-->0 && !Job::do_exit)
	{
		sleep(1);

		if(Http_client::get_instance()->Http_client_post_para(post_url.c_str(), get_status.c_str(), result_str)==0)
		{
			cJSON *ret_root;
			cJSON *ret_item;
			std::string result,info;
			
			//syslog(Logger::INFO, "result_str=%s",result_str.c_str());
			ret_root = cJSON_Parse(result_str.c_str());
			if(ret_root == NULL)
			{
				syslog(Logger::ERROR, "cJSON_Parse error"); 
				goto end;
			}

			ret_item = cJSON_GetObjectItem(ret_root, "result");
			if(ret_item == NULL)
			{
				syslog(Logger::ERROR, "get result error");
				cJSON_Delete(ret_root);
				goto end;
			}
			result = ret_item->valuestring;
			
			ret_item = cJSON_GetObjectItem(ret_root, "info");
			if(ret_item == NULL)
			{
				syslog(Logger::ERROR, "get info error");
				cJSON_Delete(ret_root);
				goto end;
			}
			info = ret_item->valuestring;
			cJSON_Delete(ret_root);

			if(result == "error")
			{
				syslog(Logger::ERROR, "restore fail %s", info.c_str());
				goto end;
			}
			else if(result == "succeed")
			{
				syslog(Logger::INFO, "retore %s:%s:%d finish!", shard_name.c_str(), ip_port.first.c_str(), ip_port.second);
				break;
			}
		}
	}

	if(retry<0)
	{
		syslog(Logger::ERROR, "retore shard timeout %s", result_str.c_str());
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

void Job::job_restore_cluster(cJSON *root)
{
	std::unique_lock<std::mutex> lock(mutex_operation_);

	std::string job_id;
	std::string job_result;
	std::string job_info;
	std::string backup_id;
	std::string backup_cluster_name,backup_ha_mode;
	std::string restore_cluster_name,restore_ha_mode;
	std::string shard_name,timestamp;
	int shards_id = 0;
	int shards;
	cJSON *item;

	std::vector<std::vector<Tpye_Ip_Port>> vec_vec_ip_port;

	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	job_result = "busy";
	job_info = "restore cluster start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_insert_operation_record(root, job_result, job_info);

	System::get_instance()->set_cluster_mgr_working(false);

	item = cJSON_GetObjectItem(root, "id");
	if(item == NULL)
	{
		job_info = "get backup_id error";
		goto end;
	}
	backup_id = item->valuestring;

	item = cJSON_GetObjectItem(root, "restore_cluster_name");
	if(item == NULL)
	{
		job_info = "get restore_cluster_name error";
		goto end;
	}
	restore_cluster_name = item->valuestring;

	/////////////////////////////////////////////////////////
	// get backup recored info from metadata table
	if(System::get_instance()->get_backup_info_from_metadata(backup_id, backup_cluster_name, timestamp, shards))
	{
		job_info = "get_backup_info_from_metadata error";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// get all node from erver shard
	if(!System::get_instance()->get_shard_ip_port_restore(restore_cluster_name, vec_vec_ip_port))
	{
		job_info = "get cluster_shard_ip_port error";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// check shards number
	if(shards != vec_vec_ip_port.size())
	{
		job_info = "shards of backup and restore is different";
		goto end;
	}

	///////////////////////////////////////////////////////////////////////////////
	// restore every shard
	for(auto &vec_ip_port: vec_vec_ip_port)
	{
		shards_id++;
		job_info = "restore shard " + std::to_string(shards_id) + " start";
		update_jobid_status(job_id, job_result, job_info);
		syslog(Logger::INFO, "%s", job_info.c_str());

		shard_name = "shard" +std::to_string(shards_id);
		for(auto &ip_port: vec_ip_port)
		{
			syslog(Logger::INFO, "restore shard node start");
			if(!job_restore_shard(backup_cluster_name, shard_name, timestamp, ip_port))
			{
				job_info = "job_restore_shard error";
				goto end;
			}
		}
	}

	job_result = "succeed";
	job_info = "restore cluster succeed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_update_operation_record(job_id, job_result, job_info);
	System::get_instance()->set_cluster_mgr_working(true);
	return;

end:
	job_result = "error";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_update_operation_record(job_id, job_result, job_info);
	System::get_instance()->set_cluster_mgr_working(true);
}

bool Job::job_get_status(cJSON *root, std::string &str_ret)
{
	std::string job_id, result, info;

	cJSON *item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return false;
	}
	job_id = item->valuestring;
	
	if(!Job::get_instance()->get_jobid_status(job_id, result, info))
		str_ret = "{\"result\":\"error\",\"info\":\"job id no find\"}";
	else
	{
		cJSON *ret_root;
		char *ret_cjson;

		ret_root = cJSON_CreateObject();
		cJSON_AddStringToObject(ret_root, "result", result.c_str());
		cJSON_AddStringToObject(ret_root, "info", info.c_str());

		ret_cjson = cJSON_Print(ret_root);
		str_ret = ret_cjson;
		cJSON_Delete(ret_root);
		free(ret_cjson);
	}

	return true;
}

bool Job::job_handle_ahead(const std::string &para, std::string &str_ret)
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
		syslog(Logger::ERROR, "job_handle_ahead get_job_type error");
		goto end;
	}

	if(job_type == JOB_GET_STATUS)
	{
		ret = job_get_status(root, str_ret);
	}
	else if(job_type == JOB_GET_NODE)
	{
		ret = System::get_instance()->get_node_instance(root, str_ret);
	}
	else if(job_type == JOB_GET_DISK_SIZE)
	{
		ret = get_disk_size(root, str_ret);
	}
	else if(job_type == JOB_CREATE_MACHINE)
	{
		ret = job_create_machine(root, str_ret);
	}
	else if(job_type == JOB_UPDATE_MACHINE)
	{
		ret = job_update_machine(root, str_ret);
	}
	else if(job_type == JOB_DELETE_MACHINE)
	{
		ret = job_delete_machine(root, str_ret);
	}
	else
	{
		if(job_type == JOB_CREATE_CLUSTER
			|| job_type == JOB_DELETE_CLUSTER
			|| job_type == JOB_BACKUP_CLUSTER
			|| job_type == JOB_RESTORE_CLUSTER)
		{
			std::string info;
			get_operation_status(info);
			if(info.length() > 0)
			{
				str_ret = "{\"result\":\"busy\",\"info\":\"" + info + "\"}";
				return true;
			}
		}
		else
			ret = false;
	}

end:
	if(root != NULL)
		cJSON_Delete(root);

	return ret;
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
	else if(job_type == JOB_DELETE_CLUSTER)
	{
		job_delete_cluster(root);
	}
	else if(job_type == JOB_BACKUP_CLUSTER)
	{
		job_backup_cluster(root);
	}
	else if(job_type == JOB_RESTORE_CLUSTER)
	{
		job_restore_cluster(root);
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

