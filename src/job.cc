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
extern int64_t thread_work_interval;
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
	get_user_name();
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
		vec_pthread.emplace_back(hdl);
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
	for(auto &node_ip: alterant_node_ip)
	{
		cJSON *root;
		char *cjson;
		
		root = cJSON_CreateObject();
		cJSON_AddStringToObject(root, "job_type", "update_instance");
		if(type == 0)
			cJSON_AddStringToObject(root, "instance_type", "meta_instance");
		else if(type == 1)
			cJSON_AddStringToObject(root, "instance_type", "storage_instance");
		else if(type == 2)
			cJSON_AddStringToObject(root, "instance_type", "computer_instance");
		
		cjson = cJSON_Print(root);
		cJSON_Delete(root);
		
		std::string post_url = "http://" + node_ip + ":" + std::to_string(node_mgr_http_port);
		//syslog(Logger::INFO, "post_url=%s",post_url.c_str());
		
		std::string result_str;
		int ret = Http_client::get_instance()->Http_client_post_para(post_url.c_str(), cjson, result_str);
		free(cjson);
	}
}

bool Job::check_timestamp(cJSON *root, std::string &str_ret)
{
	cJSON *ret_root = NULL;
	char *ret_cjson = NULL;
	cJSON *item;

	uint64_t cluster_timestamp,node_timestamp;
	std::string timestamp;
	get_timestamp(timestamp);

	item = cJSON_GetObjectItem(root, "timestamp");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get timestamp error");
		return false;
	}

	cluster_timestamp = atol(timestamp.c_str());
	node_timestamp = atol(item->valuestring);

	ret_root = cJSON_CreateObject();
	if(ABS(cluster_timestamp,node_timestamp)<3)
		cJSON_AddStringToObject(ret_root, "result", "true");
	else
		cJSON_AddStringToObject(ret_root, "result", "false");

	ret_cjson = cJSON_Print(ret_root);
	str_ret = ret_cjson;

	if(ret_root != NULL)
		cJSON_Delete(ret_root);
	if(ret_cjson != NULL)
		free(ret_cjson);

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
			vec_local_ip.emplace_back(tmp_ip);
		}
	}

	//for(auto &ip: vec_local_ip)
	//	syslog(Logger::INFO, "vec_local_ip=%s", ip.c_str());
}

void Job::get_user_name()
{
	FILE* pfd;

	char *p;
	char buf[256];
	std::string str_cmd;

	str_cmd = "who am i";
	//syslog(Logger::INFO, "get_user_name str_cmd : %s",str_cmd.c_str());

	pfd = popen(str_cmd.c_str(), "r");
	if(!pfd)
		goto end;

	if(fgets(buf, 256, pfd) == NULL)
		goto end;
	
	p = strchr(buf, 0x20);
	if(p == NULL)
		goto end;

	user_name = std::string(buf, p-buf);
	syslog(Logger::INFO, "current user=%s", user_name.c_str());

end:
	if(pfd != NULL)
		pclose(pfd);
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
		 
	snprintf(sysTime, 128, "%lu", ts.tv_sec); 

	timestamp = sysTime;

	return true;
}

bool Job::get_datatime(std::string &datatime)
{
	char sysTime[128];
	struct timespec ts = {0,0};
	clock_gettime(CLOCK_REALTIME, &ts);

	struct tm *tm;
	tm = localtime(&ts.tv_sec);
		 
	snprintf(sysTime, 128, "%04u-%02u-%02u %02u:%02u:%02u", 
		tm->tm_year+1900, tm->tm_mon+1,	tm->tm_mday, 
		tm->tm_hour, tm->tm_min, tm->tm_sec); 

	datatime = sysTime;

	return true;
}

bool Job::update_operation_status(std::string &info)
{
	std::lock_guard<std::mutex> lock(mutex_stauts_);

	operation_info = info;
	return true;
}

bool Job::get_operation_status(std::string &info)
{
	std::lock_guard<std::mutex> lock(mutex_stauts_);

	info = operation_info;
	return true;
}

bool Job::job_insert_operation_record(cJSON *root, std::string &result, std::string &info, std::string &info_other)
{
	std::string str_sql;
	std::string job_id;
	std::string job_type;

	if(result == "ongoing")
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

	//delete old job_id if exist
	str_sql = "delete from cluster_general_job_log where job_id='" + job_id + "'";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());
	
	if(System::get_instance()->execute_metadate_opertation(SQLCOM_DELETE, str_sql))
	{
		syslog(Logger::ERROR, "delete_operation_record error");
		return false;
	}

	cjson = cJSON_Print(root);
	if(cjson == NULL)
		return false;

	str_sql = "INSERT INTO cluster_general_job_log(job_id,job_type,status,operation,info,info_other) VALUES('"
				+ job_id + "','" + job_type + "','" + result + "','" + std::string(cjson) + "','" + info + "','" + info_other + "')";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());
	free(cjson);

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_INSERT, str_sql))
	{
		syslog(Logger::ERROR, "insert operation_record error");
		return false;
	}

	return true;
}

bool Job::job_update_operation_record(std::string &job_id, std::string &result, std::string &info, std::string &info_other)
{
	std::string str_sql;

	if(result == "failed" || result == "done")
	{
		std::string empty = "";
		update_operation_status(empty);
	}

	str_sql = "UPDATE cluster_general_job_log set status='" + result + "',info='" + info + "',info_other='" + info_other;
	str_sql += "',when_ended=current_timestamp(6) where job_id='" + job_id + "'";
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
	char* line;
	char buf[256];

	syslog(Logger::INFO, "system cmd: %s" ,cmd.c_str());

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

bool Job::job_save_file(std::string &path, char* buf)
{
	FILE* pfd = fopen(path.c_str(), "wb");
	if(pfd == NULL)
	{
		syslog(Logger::ERROR, "Creat json file error %s", path.c_str());
		return false;
	}

	fwrite(buf,1,strlen(buf),pfd);
	fclose(pfd);
	
	return true;
}

bool Job::job_read_file(std::string &path, std::string &str)
{
	FILE* pfd = fopen(path.c_str(), "rb");
	if(pfd == NULL)
	{
		syslog(Logger::ERROR, "read json file error %s", path.c_str());
		return false;
	}

	int len = 0;
	char buf[1024];

	do
	{
		memset(buf, 0, 1024);
		len = fread(buf,1,1024-1,pfd);
		str += buf;
	} while (len > 0);

	fclose(pfd);
	
	return true;
}

void Job::job_create_machine(cJSON *root)
{
	std::string job_id;
	std::string job_result;
	std::string job_info;

	std::string hostaddr,rack_id,total_mem,total_cpu_cores;
	std::vector<std::string> vec_paths;
	Tpye_string3 t_string3;

	cJSON *item;
	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	item = cJSON_GetObjectItem(root, "hostaddr");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get hostaddr error";
		goto end;
	}
	hostaddr = item->valuestring;

	item = cJSON_GetObjectItem(root, "rack_id");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get rack_id error";
		goto end;
	}
	rack_id = item->valuestring;

	item = cJSON_GetObjectItem(root, "datadir");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get datadir error";
		goto end;
	}
	vec_paths.emplace_back(item->valuestring);

	item = cJSON_GetObjectItem(root, "logdir");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get logdir error";
		goto end;
	}
	vec_paths.emplace_back(item->valuestring);

	item = cJSON_GetObjectItem(root, "wal_log_dir");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get wal_log_dir error";
		goto end;
	}
	vec_paths.emplace_back(item->valuestring);

	item = cJSON_GetObjectItem(root, "comp_datadir");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get comp_datadir error";
		goto end;
	}
	vec_paths.emplace_back(item->valuestring);

	item = cJSON_GetObjectItem(root, "total_mem");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get total_mem error";
		goto end;
	}
	total_mem = item->valuestring;

	item = cJSON_GetObjectItem(root, "total_cpu_cores");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get total_cpu_cores error";
		goto end;
	}
	total_cpu_cores = item->valuestring;

	t_string3 = std::make_tuple(rack_id,total_mem,total_cpu_cores);

	job_result = "not_started";
	job_info = "create machine start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_insert_operation_record(root, job_result, job_info, hostaddr);

	//////////////////////////////////////////////////////////
	if(System::get_instance()->check_machine_hostaddr(hostaddr))
	{
		job_info = "error, machine_hostaddr is exist";
		goto end;
	}
	
	//////////////////////////////////////////////////////////
	// create new machines
	if(!Machine_info::get_instance()->create_machine(hostaddr,vec_paths,t_string3,job_info))
		goto end;

	job_result = "done";
	job_info = "create machine succeed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_update_operation_record(job_id, job_result, job_info, hostaddr);
	return;

end:
	job_result = "failed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::ERROR, "%s", job_info.c_str());
	job_update_operation_record(job_id, job_result, job_info, hostaddr);
}

void Job::job_update_machine(cJSON *root)
{
	std::string job_id;
	std::string job_result;
	std::string job_info;

	std::string hostaddr,rack_id,total_mem,total_cpu_cores;
	std::vector<std::string> vec_paths;
	Tpye_string3 t_string3;

	cJSON *item;
	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	item = cJSON_GetObjectItem(root, "hostaddr");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get hostaddr error";
		goto end;
	}
	hostaddr = item->valuestring;

	item = cJSON_GetObjectItem(root, "rack_id");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get rack_id error";
		goto end;
	}
	rack_id = item->valuestring;

	item = cJSON_GetObjectItem(root, "datadir");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get datadir error";
		goto end;
	}
	vec_paths.emplace_back(item->valuestring);

	item = cJSON_GetObjectItem(root, "logdir");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get logdir error";
		goto end;
	}
	vec_paths.emplace_back(item->valuestring);

	item = cJSON_GetObjectItem(root, "wal_log_dir");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get wal_log_dir error";
		goto end;
	}
	vec_paths.emplace_back(item->valuestring);

	item = cJSON_GetObjectItem(root, "comp_datadir");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get comp_datadir error";
		goto end;
	}
	vec_paths.emplace_back(item->valuestring);

	item = cJSON_GetObjectItem(root, "total_mem");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get total_mem error";
		goto end;
	}
	total_mem = item->valuestring;

	item = cJSON_GetObjectItem(root, "total_cpu_cores");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get total_cpu_cores error";
		goto end;
	}
	total_cpu_cores = item->valuestring;

	t_string3 = std::make_tuple(rack_id,total_mem,total_cpu_cores);

	job_result = "not_started";
	job_info = "update machine start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_insert_operation_record(root, job_result, job_info, hostaddr);

	//////////////////////////////////////////////////////////
	if(!System::get_instance()->check_machine_hostaddr(hostaddr))
	{
		job_info = "error, machine_hostaddr is no exist";
		goto end;
	}

	//////////////////////////////////////////////////////////
	// update ip,port,path
	if(!Machine_info::get_instance()->update_machine(hostaddr,vec_paths,t_string3,job_info))
		goto end;

	job_result = "done";
	job_info = "update machine succeed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_update_operation_record(job_id, job_result, job_info, hostaddr);
	return;

end:
	job_result = "failed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::ERROR, "%s", job_info.c_str());
	job_update_operation_record(job_id, job_result, job_info, hostaddr);
}

void Job::job_delete_machine(cJSON *root)
{
	std::string job_id;
	std::string job_result;
	std::string job_info;

	std::string hostaddr;

	cJSON *item;
	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	item = cJSON_GetObjectItem(root, "hostaddr");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get hostaddr error";
		goto end;
	}
	hostaddr = item->valuestring;

	job_result = "not_started";
	job_info = "delete machine start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_insert_operation_record(root, job_result, job_info, hostaddr);

	//////////////////////////////////////////////////////////
	// delete machine by ip
	if(!Machine_info::get_instance()->delete_machine(hostaddr, job_info))
		goto end;

	job_result = "done";
	job_info = "delete machine succeed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_update_operation_record(job_id, job_result, job_info, hostaddr);
	return;

end:
	job_result = "failed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::ERROR, "%s", job_info.c_str());
	job_update_operation_record(job_id, job_result, job_info, hostaddr);
}

bool Job::job_control_instance(Tpye_Ip_Port &ip_port, std::string type, std::string control)
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
	cJSON_AddStringToObject(root, "job_type", "control_instance");
	cJSON_AddStringToObject(root, "type", type.c_str());
	cJSON_AddStringToObject(root, "control", control.c_str());
	cJSON_AddStringToObject(root, "ip", ip_port.first.c_str());
	cJSON_AddNumberToObject(root, "port", ip_port.second);

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
		syslog(Logger::ERROR, "control instance fail because http post");
		goto end;
	}

	/////////////////////////////////////////////////////////
	// get status from node 
	get_status = "{\"ver\":\"" + http_cmd_version + "\",\"job_id\":\"" + uuid_job_id + "\",\"job_type\":\"get_status\"}";
	
	retry = 30;
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
			if(ret_item == NULL || ret_item->valuestring == NULL)
			{
				syslog(Logger::ERROR, "get result error");
				cJSON_Delete(ret_root);
				goto end;
			}
			result = ret_item->valuestring;
			
			ret_item = cJSON_GetObjectItem(ret_root, "info");
			if(ret_item == NULL || ret_item->valuestring == NULL)
			{
				syslog(Logger::ERROR, "get info error");
				cJSON_Delete(ret_root);
				goto end;
			}
			info = ret_item->valuestring;
			cJSON_Delete(ret_root);

			if(result == "error")
			{
				syslog(Logger::ERROR, "control fail %s", info.c_str());
				goto end;
			}
			else if(result == "succeed")
			{
				syslog(Logger::INFO, "control %s %s:%d finish!", control.c_str(), ip_port.first.c_str(), ip_port.second);
				break;
			}
		}
	}

	if(retry<0)
	{
		syslog(Logger::ERROR, "control instance timeout %s", result_str.c_str());
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

void Job::job_control_instance(cJSON *root)
{
	std::string job_id;
	std::string job_result;
	std::string job_info;

	std::string ip, instance_status, info_other, type, control;
	int port;
	int instance_type = 0;
	Tpye_Ip_Port ip_port;

	cJSON *item;
	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	item = cJSON_GetObjectItem(root, "control");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get control error";
		goto end;
	}
	control = item->valuestring;

	item = cJSON_GetObjectItem(root, "ip");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get ip error";
		goto end;
	}
	ip = item->valuestring;

	item = cJSON_GetObjectItem(root, "port");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get port error";
		goto end;
	}
	port = atoi(item->valuestring);

	job_result = "not_started";
	job_info = "control instance working";
	info_other = ip + std::to_string(port);
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_insert_operation_record(root, job_result, job_info, info_other);

	System::get_instance()->set_cluster_mgr_working(false);

	//////////////////////////////////////////////////////////
	// update meta table status by ip and port
	if(control == "stop")
		instance_status = "inactive";
	else if(control == "start" || control == "restart")
		instance_status = "active";
	else
	{
		job_info = "control type error";
		goto end;
	}

	ip_port = std::make_pair(ip, port);
	if(!System::get_instance()->update_instance_status(ip_port, instance_status, instance_type))
	{
		job_info = "update_instance_status error";
		goto end;
	}

	//////////////////////////////////////////////////////////
	// stop instance by ip and port
	if(instance_type == 1)
	{
		type = "storage";
	}
	else if(instance_type == 2)
	{
		type = "computer";
	}
	else
	{
		job_info = "instance ip_port no find";
		goto end;
	}

	if(!job_control_instance(ip_port, type, control))
	{
		job_info = "job_control_instance error";
		goto end;
	}

	job_result = "done";
	job_info = "control instance succeed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_update_operation_record(job_id, job_result, job_info, info_other);
	System::get_instance()->set_cluster_mgr_working(true);
	return;

end:
	job_result = "failed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::ERROR, "%s", job_info.c_str());
	job_update_operation_record(job_id, job_result, job_info, info_other);
	System::get_instance()->set_cluster_mgr_working(true);
}

bool Job::job_generate_cluster_name(std::string &cluster_name)
{
	int cluster_id = 0;
	System::get_instance()->get_max_cluster_id(cluster_id);
	cluster_id += 1;
	cluster_id %= 1000000;

	while(true)
	{
		char buf[10];
		snprintf(buf, 10, "_%06d", cluster_id);
		get_timestamp(cluster_name);
		cluster_name = "cluster_" + cluster_name + buf;
		//check for no repeat
		if(!System::get_instance()->check_cluster_name(cluster_name) || Job::do_exit)
		{
			syslog(Logger::INFO, "cluster_name:%s", cluster_name.c_str());
			break;
		}
		sleep(1);
	}

	return true;
}

bool Job::job_create_program_path()
{
	std::string cmd, cmd_path, program_path;

	//upzip to program_binaries_path for install cmd
	//storage
	cmd_path = program_binaries_path + "/" + storage_prog_package_name + "/dba_tools";
	if(access(cmd_path.c_str(), F_OK) != 0)
	{
		syslog(Logger::INFO, "upzip %s.tgz" , storage_prog_package_name.c_str());
		program_path = program_binaries_path + "/" + storage_prog_package_name + ".tgz";

		cmd = "tar zxf " + program_path + " -C " + program_binaries_path;
		if(!job_system_cmd(cmd))
			return false;
	}

	//computer
	cmd_path = program_binaries_path + "/" + computer_prog_package_name + "/scripts";
	if(access(cmd_path.c_str(), F_OK) != 0)
	{
		syslog(Logger::INFO, "upzip %s.tgz" , computer_prog_package_name.c_str());
		program_path = program_binaries_path + "/" + computer_prog_package_name + ".tgz";

		cmd = "tar zxf " + program_path + " -C " + program_binaries_path;
		if(!job_system_cmd(cmd))
			return false;
	}

	return true;
}

bool Job::job_create_meta_jsonfile()
{
	cJSON *root = NULL;
	char *cjson = NULL;
	cJSON *item_sub;

	std::string jsonfile_path;
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
	jsonfile_path = program_binaries_path + "/" + computer_prog_package_name + "/scripts/pgsql_meta.json";
	cjson = cJSON_Print(root);
	if(cjson != NULL)
	{
		job_save_file(jsonfile_path, cjson);
		free(cjson);
	}
	cJSON_Delete(root);

	return true;
}

bool Job::job_create_shards_jsonfile(std::vector <std::vector<Tpye_Ip_Port_Paths>> &vec_shard, std::vector<std::string> &vec_shard_name)
{
	cJSON *root = NULL;
	char *cjson = NULL;
	cJSON *item_sub;
	cJSON *item_sub_sub;
	cJSON *item_sub_sub_sub;

	std::string jsonfile_path;

	/////////////////////////////////////////////////////////
	//create storage shards json
	root = cJSON_CreateArray();
	for(int i=0; i<vec_shard.size(); i++)
	{
		item_sub = cJSON_CreateObject();
		cJSON_AddItemToArray(root, item_sub);
		cJSON_AddStringToObject(item_sub, "shard_name", vec_shard_name[i].c_str());

		item_sub_sub = cJSON_CreateArray();
		cJSON_AddItemToObject(item_sub, "shard_nodes", item_sub_sub);

		for(auto &ip_port_paths: vec_shard[i])
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
	jsonfile_path = program_binaries_path + "/" + computer_prog_package_name + "/scripts/pgsql_shards.json";
	cjson = cJSON_Print(root);
	if(cjson != NULL)
	{
		job_save_file(jsonfile_path, cjson);
		free(cjson);
	}
	cJSON_Delete(root);

	return true;
}

bool Job::job_create_storage(Tpye_Ip_Port_Paths &storage, cJSON *root, int install_id)
{
	bool ret = false;
	char* cjson;
	int retry_total = 3;
	int retry;
	std::string post_url,get_status,result_str;
	std::string uuid_job_id;

	/////////////////////////////////////////////////////////
	// http post parameter to node
	post_url = "http://" + std::get<0>(storage) + ":" + std::to_string(node_mgr_http_port);
	cJSON_ReplaceItemInObject(root, "install_id", cJSON_CreateNumber(install_id));

start:
	retry = 3;
	get_uuid(uuid_job_id);
	cJSON_ReplaceItemInObject(root, "job_id", cJSON_CreateString(uuid_job_id.c_str()));

	cjson = cJSON_Print(root);
	//syslog(Logger::INFO, "cjson=%s",cjson);
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
		return false;
	}
	
	/////////////////////////////////////////////////////////
	// get status from node 
	get_status = "{\"ver\":\"" + http_cmd_version + "\",\"job_id\":\"" + uuid_job_id + "\",\"job_type\":\"get_status\"}";
	
	retry = 120;
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
			if(ret_item == NULL || ret_item->valuestring == NULL)
			{
				syslog(Logger::ERROR, "get result error");
				cJSON_Delete(ret_root);
				goto end;
			}
			result = ret_item->valuestring;

			ret_item = cJSON_GetObjectItem(ret_root, "info");
			if(ret_item == NULL || ret_item->valuestring == NULL)
			{
				syslog(Logger::ERROR, "get info error");
				cJSON_Delete(ret_root);
				goto end;
			}
			info = ret_item->valuestring;
			cJSON_Delete(ret_root);

			if(result == "error")
			{
				syslog(Logger::ERROR, "storage install fail %s", info.c_str());
				goto end;
			}
			else if(result == "succeed")
			{
				syslog(Logger::INFO, "storage instance %s:%d install finish!", std::get<0>(storage).c_str(), std::get<1>(storage));
				break;
			}
		}
	}

	if(retry<0)
	{
		syslog(Logger::ERROR, "create storage instance timeout %s", result_str.c_str());
		goto end;
	}

	ret = true;

end:
	if(!ret && retry_total-->0)
	{
		syslog(Logger::ERROR, "create storage error retry_total = %d", retry_total);
		goto start;
	}

	return ret;
}

bool Job::job_create_nodes(std::vector<Tpye_Ip_Port_Paths> &storages, std::string &cluster_name, std::string &shard_name)
{
	cJSON *root = NULL;
	char *cjson = NULL;
	cJSON *item;
	cJSON *item_node;
	cJSON *item_sub;
	
	bool ret = false;
	std::string strtmp, jsonfile_path, jsonfile_buf;
	int innodb_size;
	int nodes_num;

	jsonfile_path = cluster_json_path + "/" + cluster_name + "/mysql_" + shard_name + ".json";
	if(!job_read_file(jsonfile_path, jsonfile_buf))
	{
		syslog(Logger::ERROR, "job_read_file error");
		return false;
	}

	root = cJSON_Parse(jsonfile_buf.c_str());
	if(root == NULL)
	{
		syslog(Logger::ERROR, "file cJSON_Parse error");
		return false;
	}

	item = cJSON_GetObjectItem(root, "innodb_size");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get innodb_size error");
		goto end;
	}
	innodb_size = atoi(item->valuestring);

	item_node = cJSON_GetObjectItem(root, "nodes");
	if(item_node == NULL)
	{
		syslog(Logger::ERROR, "get nodes error");
		goto end;
	}

	nodes_num = cJSON_GetArraySize(item_node);

	for(int i=0; i<storages.size(); i++)
	{
		item_sub = cJSON_CreateObject();
		cJSON_AddItemToArray(item_node, item_sub);
		
		cJSON_AddFalseToObject(item_sub, "is_primary");
		cJSON_AddStringToObject(item_sub, "ip", std::get<0>(storages[i]).c_str());
		cJSON_AddNumberToObject(item_sub, "port", std::get<1>(storages[i]));
		cJSON_AddNumberToObject(item_sub, "xport", std::get<1>(storages[i])+1);
		cJSON_AddNumberToObject(item_sub, "mgr_port", std::get<1>(storages[i])+2);
		strtmp = std::get<2>(storages[i])[0] + "/instance_data/data_dir_path/" 
					+ std::to_string(std::get<1>(storages[i]));
		cJSON_AddStringToObject(item_sub, "data_dir_path", strtmp.c_str());
		strtmp = std::get<2>(storages[i])[1] + "/instance_data/log_dir_path/" 
					+ std::to_string(std::get<1>(storages[i]));
		cJSON_AddStringToObject(item_sub, "log_dir_path", strtmp.c_str());
		strtmp = std::get<2>(storages[i])[2] + "/instance_data/innodb_log_dir_path/" 
					+ std::to_string(std::get<1>(storages[i]));
		cJSON_AddStringToObject(item_sub, "innodb_log_dir_path", strtmp.c_str());
		strtmp = std::to_string(innodb_size) + "GB";
		//strtmp = "64MB";
		cJSON_AddStringToObject(item_sub, "innodb_buffer_pool_size", strtmp.c_str());
		cJSON_AddStringToObject(item_sub, "user", user_name.c_str());
		cJSON_AddNumberToObject(item_sub, "election_weight", 50);
	}

	/////////////////////////////////////////////////////////
	// send json parameter to every node
	for(int i=0; i<storages.size(); i++)
	{
		if(!job_create_storage(storages[i], root, nodes_num+i))
		{
			syslog(Logger::ERROR, "job_create_storage %s:%d, error", std::get<0>(storages[i]).c_str(), std::get<1>(storages[i]));
			goto end;
		}
	}

	/////////////////////////////////////////////////////////
	// save json file to cluster_json_path
	cjson = cJSON_Print(root);
	if(cjson != NULL)
	{
		job_save_file(jsonfile_path, cjson);
		free(cjson);
		cjson = NULL;
	}

	ret = true;

end:
	if(root!=NULL)
		cJSON_Delete(root);
	if(cjson!=NULL)
		free(cjson);

	return ret;
}

bool Job::job_create_shard(std::vector<Tpye_Ip_Port_Paths> &storages, std::string &cluster_name, std::string &shard_name, std::string &ha_mode, int innodb_size)
{
	cJSON *root = NULL;
	char *cjson = NULL;
	cJSON *item_node;
	cJSON *item_sub;

	bool ret = false;
	std::string strtmp, jsonfile_path;
	std::string uuid_shard;
	get_uuid(uuid_shard);

	/////////////////////////////////////////////////////////
	// create json parameter
	root = cJSON_CreateObject();
	cJSON_AddStringToObject(root, "ver", http_cmd_version.c_str());
	cJSON_AddStringToObject(root, "job_id", "");
	cJSON_AddStringToObject(root, "job_type", "install_storage");
	cJSON_AddStringToObject(root, "cluster_name", cluster_name.c_str());
	cJSON_AddStringToObject(root, "shard_name", shard_name.c_str());
	cJSON_AddStringToObject(root, "ha_mode", ha_mode.c_str());
	cJSON_AddNumberToObject(root, "install_id", 0);
	cJSON_AddStringToObject(root, "innodb_size", std::to_string(innodb_size).c_str());
	cJSON_AddStringToObject(root, "group_uuid", uuid_shard.c_str());
	item_node = cJSON_CreateArray();
	cJSON_AddItemToObject(root, "nodes", item_node);

	for(int i=0; i<storages.size(); i++)
	{
		item_sub = cJSON_CreateObject();
		cJSON_AddItemToArray(item_node, item_sub);
		
		if(i == 0)
			cJSON_AddTrueToObject(item_sub, "is_primary");
		else
			cJSON_AddFalseToObject(item_sub, "is_primary");
		cJSON_AddStringToObject(item_sub, "ip", std::get<0>(storages[i]).c_str());
		cJSON_AddNumberToObject(item_sub, "port", std::get<1>(storages[i]));
		cJSON_AddNumberToObject(item_sub, "xport", std::get<1>(storages[i])+1);
		cJSON_AddNumberToObject(item_sub, "mgr_port", std::get<1>(storages[i])+2);
		strtmp = std::get<2>(storages[i])[0] + "/instance_data/data_dir_path/" 
					+ std::to_string(std::get<1>(storages[i]));
		cJSON_AddStringToObject(item_sub, "data_dir_path", strtmp.c_str());
		strtmp = std::get<2>(storages[i])[1] + "/instance_data/log_dir_path/" 
					+ std::to_string(std::get<1>(storages[i]));
		cJSON_AddStringToObject(item_sub, "log_dir_path", strtmp.c_str());
		strtmp = std::get<2>(storages[i])[2] + "/instance_data/innodb_log_dir_path/" 
					+ std::to_string(std::get<1>(storages[i]));
		cJSON_AddStringToObject(item_sub, "innodb_log_dir_path", strtmp.c_str());
		strtmp = std::to_string(innodb_size) + "GB";
		//strtmp = "64MB";
		cJSON_AddStringToObject(item_sub, "innodb_buffer_pool_size", strtmp.c_str());
		cJSON_AddStringToObject(item_sub, "user", user_name.c_str());
		cJSON_AddNumberToObject(item_sub, "election_weight", 50);
	}

	/////////////////////////////////////////////////////////
	// send json parameter to every node
	for(int i=0; i<storages.size(); i++)
	{
		if(!job_create_storage(storages[i], root, i))
		{
			syslog(Logger::ERROR, "job_create_storage %s:%d, error", std::get<0>(storages[i]).c_str(), std::get<1>(storages[i]));
			goto end;
		}
	}

	/////////////////////////////////////////////////////////
	// save json file to cluster_json_path
	jsonfile_path = cluster_json_path + "/" + cluster_name + "/mysql_" + shard_name + ".json";
	cjson = cJSON_Print(root);
	if(cjson != NULL)
	{
		job_save_file(jsonfile_path, cjson);
		free(cjson);
		cjson = NULL;
	}

	ret = true;

end:
	if(root!=NULL)
		cJSON_Delete(root);
	if(cjson!=NULL)
		free(cjson);

	return ret;
}

bool Job::job_create_computer(Tpye_Ip_Port_Paths &computer, cJSON *root, int install_id)
{
	bool ret = false;
	char* cjson;
	int retry_total = 3;
	int retry;
	std::string post_url,get_status,result_str;
	std::string uuid_job_id;

	/////////////////////////////////////////////////////////
	// http post parameter to node
	post_url = "http://" + std::get<0>(computer) + ":" + std::to_string(node_mgr_http_port);
	cJSON_ReplaceItemInObject(root, "install_id", cJSON_CreateNumber(install_id));

start:
	retry = 3;
	get_uuid(uuid_job_id);
	cJSON_ReplaceItemInObject(root, "job_id", cJSON_CreateString(uuid_job_id.c_str()));

	cjson = cJSON_Print(root);
	//syslog(Logger::INFO, "cjson=%s",cjson);
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
		return false;
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
			if(ret_item == NULL || ret_item->valuestring == NULL)
			{
				syslog(Logger::ERROR, "get result error");
				cJSON_Delete(ret_root);
				goto end;
			}
			result = ret_item->valuestring;

			ret_item = cJSON_GetObjectItem(ret_root, "info");
			if(ret_item == NULL || ret_item->valuestring == NULL)
			{
				syslog(Logger::ERROR, "get info error");
				cJSON_Delete(ret_root);
				goto end;
			}
			info = ret_item->valuestring;
			cJSON_Delete(ret_root);

			if(result == "error")
			{
				syslog(Logger::ERROR, "computer install fail %s", info.c_str());
				goto end;
			}
			else if(result == "succeed")
			{
				syslog(Logger::INFO, "computer instance %s:%d install finish!", std::get<0>(computer).c_str(), std::get<1>(computer));
				break;
			}
		}
	}

	if(retry<0)
	{
		syslog(Logger::ERROR, "create computer instance timeout %s", result_str.c_str());
		goto end;
	}

	ret = true;

end:
	if(!ret && retry_total-->0)
	{
		syslog(Logger::ERROR, "create computer error retry_total = %d", retry_total);
		goto start;
	}

	return ret;
}

bool Job::job_create_comps(std::vector<Tpye_Ip_Port_Paths> &comps, std::string &cluster_name, std::vector<std::string> vec_comp_name, int comps_id)
{
	cJSON *root = NULL;
	char *cjson = NULL;
	cJSON *item_node;
	cJSON *item_sub;

	bool ret = false;
	std::string strtmp, jsonfile_path;
	std::string post_url,get_status,result_str;
	
	/////////////////////////////////////////////////////////
	// create json parameter
	root = cJSON_CreateObject();
	cJSON_AddStringToObject(root, "ver", http_cmd_version.c_str());
	cJSON_AddStringToObject(root, "job_id", "");
	cJSON_AddStringToObject(root, "job_type", "install_computer");
	cJSON_AddNumberToObject(root, "install_id", 0);
	item_node = cJSON_CreateArray();
	cJSON_AddItemToObject(root, "nodes", item_node);

	for(int i=0; i<comps.size(); i++)
	{
		item_sub = cJSON_CreateObject();
		cJSON_AddItemToArray(item_node, item_sub);

		cJSON_AddNumberToObject(item_sub, "id", comps_id+i);
		cJSON_AddStringToObject(item_sub, "name", vec_comp_name[i].c_str());
		cJSON_AddStringToObject(item_sub, "ip", std::get<0>(comps[i]).c_str());
		cJSON_AddNumberToObject(item_sub, "port", std::get<1>(comps[i]));
		cJSON_AddStringToObject(item_sub, "user", "abc");
		cJSON_AddStringToObject(item_sub, "password", "abc");
		strtmp = std::get<2>(comps[i])[0] + "/instance_data/comp_datadir/" 
					+ std::to_string(std::get<1>(comps[i]));
		cJSON_AddStringToObject(item_sub, "datadir", strtmp.c_str());
	}

	/////////////////////////////////////////////////////////
	// save json file to cluster_json_path and cmd_path
	jsonfile_path = cluster_json_path + "/" + cluster_name + "/pgsql_comps_" + std::to_string(comps_id) + ".json";
	cjson = cJSON_Print(item_node);
	if(cjson != NULL)
	{
		job_save_file(jsonfile_path, cjson);

		jsonfile_path = program_binaries_path + "/" + computer_prog_package_name + "/scripts/pgsql_comps.json";
		job_save_file(jsonfile_path, cjson);

		free(cjson);
		cjson = NULL;
	}

	/////////////////////////////////////////////////////////
	// send json parameter to every node
	for(int i=0; i<comps.size(); i++)
	{
		if(!job_create_computer(comps[i], root, i))
		{
			syslog(Logger::ERROR, "job_create_computer %s:%d, error", std::get<0>(comps[i]).c_str(), std::get<1>(comps[i]));
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

bool Job::job_start_cluster(std::string &cluster_name, std::string &ha_mode)
{
	FILE* pfd;
	char buf[256];

	int retry;
	std::string cmd;

	/////////////////////////////////////////////////////////
	// start cluster cmd
	cmd = "cd " + program_binaries_path + "/" + computer_prog_package_name + "/scripts/;";
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
		//if(strcasestr(buf, "error") != NULL)
			syslog(Logger::INFO, "%s", buf);
	}
	pclose(pfd);

	/////////////////////////////////////////////////////////////
	// check instance succeed by connect to instance
	retry = thread_work_interval * 30;
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

	syslog(Logger::INFO, "cluster start succeed");
	return true;
}

bool Job::job_create_cluster(Tpye_cluster_info &cluster_info, std::string &cluster_name)
{
	cJSON *root = NULL;
	char *cjson = NULL;
	
	int shards_id = 0;
	int comps_id = 0;
	int comps_id_seq = 0;
	int shards;
	int nodes;
	int comps;
	int innodb_size;
	bool roll_back = false;

	std::string cmd, ha_mode, jsonfile_path;
	std::vector <std::vector<Tpye_Ip_Port_Paths>> vec_shard_storage_ip_port_paths;
	std::vector<Tpye_Ip_Port_Paths> vec_comps_ip_port_paths;
	std::vector<std::string> vec_shard_name;
	std::vector<std::string> vec_comp_name;

	ha_mode = std::get<0>(cluster_info);
	shards = std::get<1>(cluster_info);
	nodes = std::get<2>(cluster_info);
	comps = std::get<3>(cluster_info);
	innodb_size = std::get<7>(cluster_info);

	/////////////////////////////////////////////////////////
	// update all ip,port,path of machines
	if(!Machine_info::get_instance()->update_machines_info())
	{
		syslog(Logger::ERROR, "error, no available machine");
		return false;
	}

	/////////////////////////////////////////////////////////////////////////////// 
	System::get_instance()->get_comp_nodes_id_seq(comps_id_seq);
	//syslog(Logger::INFO, "comps_id_seq=%d", comps_id_seq);
	shards_id = 1;
	comps_id = 1;
	comps_id_seq += 1;

	/////////////////////////////////////////////////////////
	// for install cluster cmd
	if(!job_create_program_path())
	{
		syslog(Logger::ERROR, "create_cmd_path error");
		return false;
	}

	// for save cluster json file
	cmd = "mkdir -p " + cluster_json_path + "/" + cluster_name;
	if(!job_system_cmd(cmd))
	{
		syslog(Logger::ERROR, "system_cmd error");
		return false;
	}

	// save json file to cluster_json_path
	jsonfile_path = cluster_json_path + "/" + cluster_name + "/create_cluster.json";
	root = cJSON_CreateObject();
	cJSON_AddStringToObject(root, "ver", http_cmd_version.c_str());
	cJSON_AddStringToObject(root, "ha_mode", std::get<0>(cluster_info).c_str());
	cJSON_AddStringToObject(root, "shards", std::to_string(std::get<1>(cluster_info)).c_str());
	cJSON_AddStringToObject(root, "nodes", std::to_string(std::get<2>(cluster_info)).c_str());
	cJSON_AddStringToObject(root, "comps", std::to_string(std::get<3>(cluster_info)).c_str());
	cJSON_AddStringToObject(root, "max_storage_size", std::to_string(std::get<4>(cluster_info)).c_str());
	cJSON_AddStringToObject(root, "max_connections", std::to_string(std::get<5>(cluster_info)).c_str());
	cJSON_AddStringToObject(root, "cpu_cores", std::to_string(std::get<6>(cluster_info)).c_str());
	cJSON_AddStringToObject(root, "innodb_size", std::to_string(std::get<7>(cluster_info)).c_str());
	cjson = cJSON_Print(root);
	if(cjson != NULL)
	{
		job_save_file(jsonfile_path, cjson);
		free(cjson);
		cjson = NULL;
	}

	///////////////////////////////////////////////////////////////////////////////
	// get storage of shard 
	for(int i=0; i<shards; i++)
	{
		std::vector<Tpye_Ip_Port_Paths> vec_storage_ip_port_paths;
		if(!Machine_info::get_instance()->get_storage_nodes(nodes, vec_storage_ip_port_paths))
		{
			syslog(Logger::ERROR, "Machine_info get_storage_nodes error");
			return false;
		}
		vec_shard_storage_ip_port_paths.emplace_back(vec_storage_ip_port_paths);
		vec_shard_name.emplace_back("shard"+std::to_string(shards_id + i));
	}

	///////////////////////////////////////////////////////////////////////////////
	// get computer 
	if(!Machine_info::get_instance()->get_computer_nodes(comps, vec_comps_ip_port_paths))
	{
		syslog(Logger::ERROR, "Machine_info get_computer_nodes error");
		return false;
	}
	for(int i=0; i<comps; i++)
		vec_comp_name.emplace_back("comp"+std::to_string(comps_id + i));

	/////////////////////////////////////////////////////////
	//create storage shards json
	job_create_shards_jsonfile(vec_shard_storage_ip_port_paths, vec_shard_name);

	/////////////////////////////////////////////////////////
	//create meta json
	job_create_meta_jsonfile();

	///////////////////////////////////////////////////////////////////////////////
	// create storage of shard
	for(int i=0; i<shards; i++)
	{
		syslog(Logger::INFO, "create shard %d working", shards_id + i);
		if(!job_create_shard(vec_shard_storage_ip_port_paths[i], cluster_name, vec_shard_name[i], ha_mode, innodb_size))
		{
			syslog(Logger::ERROR, "create shard error");
			roll_back = true;
			break;
		}
	}

	if(roll_back)
	{
		syslog(Logger::INFO, "roll back shards");
		for(auto &storages: vec_shard_storage_ip_port_paths)
			for(auto &storage: storages)
			{
				Tpye_Ip_Port ip_port = std::make_pair(std::get<0>(storage), std::get<1>(storage));
				job_delete_storage(ip_port);
			}
		return false;
	}

	///////////////////////////////////////////////////////////////////////////////
	// create computer
	syslog(Logger::INFO, "create comps working");
	if(!job_create_comps(vec_comps_ip_port_paths, cluster_name, vec_comp_name, comps_id_seq))
	{
		syslog(Logger::ERROR, "create comps error");
		roll_back = true;
	}

	if(roll_back)
	{
		syslog(Logger::INFO, "roll back shards");
		for(auto &storages: vec_shard_storage_ip_port_paths)
			for(auto &storage: storages)
			{
				Tpye_Ip_Port ip_port = std::make_pair(std::get<0>(storage), std::get<1>(storage));
				job_delete_storage(ip_port);
			}
		syslog(Logger::INFO, "roll back comps");
		for(auto &computer: vec_comps_ip_port_paths)
		{
			Tpye_Ip_Port ip_port = std::make_pair(std::get<0>(computer), std::get<1>(computer));
			job_delete_computer(ip_port);
		}
		return false;
	}

	///////////////////////////////////////////////////////////////////////////////
	//start cluster on shards and comps
	syslog(Logger::INFO, "start cluster cmd");
	if(!job_start_cluster(cluster_name, ha_mode))
	{
		syslog(Logger::INFO, "roll back cluster");
		System::get_instance()->stop_cluster(cluster_name);
		syslog(Logger::INFO, "roll back shards");
		for(auto &storages: vec_shard_storage_ip_port_paths)
			for(auto &storage: storages)
			{
				Tpye_Ip_Port ip_port = std::make_pair(std::get<0>(storage), std::get<1>(storage));
				job_delete_storage(ip_port);
			}
		syslog(Logger::INFO, "roll back comps");
		for(auto &computer: vec_comps_ip_port_paths)
		{
			Tpye_Ip_Port ip_port = std::make_pair(std::get<0>(computer), std::get<1>(computer));
			job_delete_computer(ip_port);
		}

		syslog(Logger::ERROR, "start_cluster error");
		return false;
	}

	syslog(Logger::INFO, "create cluster finish");
	return true;
}

void Job::job_create_cluster(cJSON *root)
{
	std::lock_guard<std::mutex> lock(mutex_operation_);

	std::string job_id;
	std::string job_result;
	std::string job_info;
	std::string cluster_name;
	cJSON *item;

	Tpye_cluster_info cluster_info;

	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	/////////////////////////////////////////////////////////
	// generate cluster name
	job_generate_cluster_name(cluster_name);

	job_result = "ongoing";
	job_info = "create cluster start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_insert_operation_record(root, job_result, job_info, cluster_name);

	item = cJSON_GetObjectItem(root, "ha_mode");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get ha_mode error";
		goto end;
	}
	std::get<0>(cluster_info) = item->valuestring;

	item = cJSON_GetObjectItem(root, "shards");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get shards error";
		goto end;
	}
	std::get<1>(cluster_info) = atoi(item->valuestring);
	if(std::get<1>(cluster_info)<1 || std::get<1>(cluster_info)>10)
	{
		job_info = "shards error(must in 1-10)";
		goto end;
	}

	item = cJSON_GetObjectItem(root, "nodes");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get nodes error";
		goto end;
	}
	std::get<2>(cluster_info) = atoi(item->valuestring);
	if(std::get<0>(cluster_info) == "mgr")
	{
		if(std::get<2>(cluster_info)<3 || std::get<2>(cluster_info)>10)
		{
			job_info = "error, nodes>=3 && nodes<=10 in mgr mode";
			goto end;
		}
	}
	else if(std::get<0>(cluster_info) == "no_rep")
	{
		if(std::get<2>(cluster_info)!=1)
		{
			job_info = "error, nodes=1 in no_rep mode";
			goto end;
		}
	}
	else
	{
		job_info = "it is not support " + std::get<0>(cluster_info);
		goto end;
	}

	item = cJSON_GetObjectItem(root, "comps");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get comps error";
		goto end;
	}
	std::get<3>(cluster_info) = atoi(item->valuestring);
	if(std::get<3>(cluster_info)<1 || std::get<3>(cluster_info)>10)
	{
		job_info = "comps error(must in 1-10)";
		goto end;
	}

	item = cJSON_GetObjectItem(root, "max_storage_size");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get max_storage_size error";
		goto end;
	}
	std::get<4>(cluster_info) = atoi(item->valuestring);

	item = cJSON_GetObjectItem(root, "max_connections");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get max_connections error";
		goto end;
	}
	std::get<5>(cluster_info) = atoi(item->valuestring);

	item = cJSON_GetObjectItem(root, "cpu_cores");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get cpu_cores error";
		goto end;
	}
	std::get<6>(cluster_info) = atoi(item->valuestring);

	item = cJSON_GetObjectItem(root, "innodb_size");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get innodb_size error";
		goto end;
	}
	std::get<7>(cluster_info) = atoi(item->valuestring);
	if(std::get<7>(cluster_info)<1 || std::get<7>(cluster_info)>16)
	{
		job_info = "innodb_size error(must in 1-16)";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// create cluster
	job_info = "create cluster working";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	if(!job_create_cluster(cluster_info, cluster_name))
	{
		job_info = "create cluster error";
		goto end;
	}

	job_result = "done";
	job_info = "create cluster succeed";
	update_jobid_status(job_id, job_result, cluster_name);
	syslog(Logger::INFO, "%s: %s", job_info.c_str(), cluster_name.c_str());
	job_update_operation_record(job_id, job_result, job_info, cluster_name);
	return;

end:
	job_result = "failed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::ERROR, "%s", job_info.c_str());
	job_update_operation_record(job_id, job_result, job_info, cluster_name);
}

bool Job::job_delete_storage(Tpye_Ip_Port &storage)
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
	cJSON_AddStringToObject(root, "ip", storage.first.c_str());
	cJSON_AddNumberToObject(root, "port", storage.second);

	cjson = cJSON_Print(root);
	//syslog(Logger::INFO, "cjson=%s",cjson);

	/////////////////////////////////////////////////////////
	// http post parameter to node
	post_url = "http://" + storage.first + ":" + std::to_string(node_mgr_http_port);
	
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
			if(ret_item == NULL || ret_item->valuestring == NULL)
			{
				syslog(Logger::ERROR, "get result error");
				cJSON_Delete(ret_root);
				goto end;
			}
			result = ret_item->valuestring;

			ret_item = cJSON_GetObjectItem(ret_root, "info");
			if(ret_item == NULL || ret_item->valuestring == NULL)
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
				syslog(Logger::INFO, "storage instance %s:%d delete finish!", storage.first.c_str(), storage.second);
				break;
			}
		}
			
	}

	if(retry<0)
	{
		syslog(Logger::ERROR, "delete storage instance timeout %s", result_str.c_str());
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

bool Job::job_delete_computer(Tpye_Ip_Port &computer)
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
	cJSON_AddStringToObject(root, "ip", computer.first.c_str());
	cJSON_AddNumberToObject(root, "port", computer.second);

	cjson = cJSON_Print(root);
	//syslog(Logger::INFO, "cjson=%s",cjson);

	/////////////////////////////////////////////////////////
	// http post parameter to node
	post_url = "http://" + computer.first + ":" + std::to_string(node_mgr_http_port);
	
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
			if(ret_item == NULL || ret_item->valuestring == NULL)
			{
				syslog(Logger::ERROR, "get result error");
				cJSON_Delete(ret_root);
				goto end;
			}
			result = ret_item->valuestring;

			ret_item = cJSON_GetObjectItem(ret_root, "info");
			if(ret_item == NULL || ret_item->valuestring == NULL)
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
				syslog(Logger::INFO, "computer instance %s:%d delete finish!", computer.first.c_str(), computer.second);
				break;
			}
		}
	}

	if(retry<0)
	{
		syslog(Logger::ERROR, "delete computer instance timeout %s", result_str.c_str());
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

void Job::job_delete_cluster(cJSON *root)
{
	std::lock_guard<std::mutex> lock(mutex_operation_);

	std::string job_id;
	std::string job_result;
	std::string job_info;
	std::string cluster_name;
	std::string cmd;
	cJSON *item;

	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	item = cJSON_GetObjectItem(root, "cluster_name");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get cluster_name error";
		goto end;
	}
	cluster_name = item->valuestring;

	job_result = "ongoing";
	job_info = "delete cluster start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_insert_operation_record(root, job_result, job_info, cluster_name);

	System::get_instance()->set_cluster_mgr_working(false);
	
	if(!System::get_instance()->check_cluster_name(cluster_name))
	{
		job_info = "error, cluster_name is no exist";
		goto end;
	}

	job_delete_cluster(cluster_name);

	job_result = "done";
	job_info = "delete cluster succeed";
	update_jobid_status(job_id, job_result, cluster_name);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_update_operation_record(job_id, job_result, job_info, cluster_name);
	System::get_instance()->set_cluster_mgr_working(true);
	return;

end:
	job_result = "failed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::ERROR, "%s", job_info.c_str());
	job_update_operation_record(job_id, job_result, job_info, cluster_name);
	System::get_instance()->set_cluster_mgr_working(true);
}

void Job::job_delete_cluster(std::string &cluster_name)
{
	std::string cmd;

	std::vector <std::vector<Tpye_Ip_Port>> vec_shard_storage_ip_port;
	std::vector<Tpye_Ip_Port> vec_comps_ip_port;

	/////////////////////////////////////////////////////////
	// get shards_ip_port by cluster_name
	if(!System::get_instance()->get_shards_ip_port(cluster_name, vec_shard_storage_ip_port))
	{
		syslog(Logger::ERROR, "get_shards_ip_port error");
	}

	/////////////////////////////////////////////////////////
	// get comps_ip_port by cluster_name
	if(!System::get_instance()->get_comps_ip_port(cluster_name, vec_comps_ip_port))
	{
		syslog(Logger::ERROR, "get_comps_ip_port error");
	}

	/////////////////////////////////////////////////////////
	// delete cluster info from meta talbes
	if(!System::get_instance()->stop_cluster(cluster_name))
	{
		syslog(Logger::ERROR, "stop_cluster error");
	}

	/////////////////////////////////////////////////////////
	// delete comps from every node
	for(auto &computer: vec_comps_ip_port)
	{
		if(!job_delete_computer(computer))
			syslog(Logger::ERROR, "delete computer error");
	}

	/////////////////////////////////////////////////////////
	// delete storages from every node
	for(auto &storages: vec_shard_storage_ip_port)
	{
		for(auto &storage: storages)
		{
			if(!job_delete_storage(storage))
				syslog(Logger::ERROR, "delete storage error");
		}
	}

	/////////////////////////////////////////////////////////
	// delete cluster json file
	cmd = "rm -rf " + cluster_json_path + "/" + cluster_name;
	if(!job_system_cmd(cmd))
	{
		syslog(Logger::ERROR, "system_cmd error");
	}

	syslog(Logger::INFO, "delete cluster finish");
}

bool Job::job_start_shards(std::string &cluster_name, std::vector<std::string> &vec_shard_name)
{
	FILE* pfd;
	char buf[256];

	int retry;
	std::string cmd;

	/////////////////////////////////////////////////////////
	// start cluster cmd
	cmd = "cd " + program_binaries_path + "/" + computer_prog_package_name + "/scripts/;";
	cmd += "python2 add_shards.py --config ./pgsql_shards.json --meta_config ./pgsql_meta.json --cluster_name " + cluster_name;
	syslog(Logger::INFO, "job_start_shards cmd %s", cmd.c_str());

	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		syslog(Logger::ERROR, "install error %s", cmd.c_str());
		return false;
	}
	while(fgets(buf, 256, pfd)!=NULL)
	{
		//if(strcasestr(buf, "error") != NULL)
			syslog(Logger::INFO, "%s", buf);
	}
	pclose(pfd);

	/////////////////////////////////////////////////////////////
	// check instance succeed by connect to instance
	retry = thread_work_interval * 30;
	while(retry-->0 && !Job::do_exit)
	{
		sleep(1);
		bool all_shard_start = true;
		for(auto &shard_name:vec_shard_name)
		{
			if(!System::get_instance()->check_cluster_shard_name(cluster_name, shard_name))
			{
				all_shard_start = false;
				break;
			}
		}

		if(all_shard_start)
			break;
	}

	if(retry<0)
	{
		syslog(Logger::ERROR, "shard start error");
		return false;
	}

	syslog(Logger::INFO, "shard start succeed");
	return true;
}

void Job::job_add_shards(cJSON *root)
{
	std::lock_guard<std::mutex> lock(mutex_operation_);

	std::string job_id;
	std::string job_result;
	std::string job_info;
	std::string cluster_name;
	cJSON *item;
	char *cjson;

	int shards;
	int shards_id = 0;
	bool roll_back = false;
	std::vector <std::vector<Tpye_Ip_Port_Paths>> vec_shard_storage_ip_port_paths;
	std::vector<std::string> vec_shard_name;
	Tpye_cluster_info cluster_info;

	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	item = cJSON_GetObjectItem(root, "cluster_name");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get cluster_name error");
		return;
	}
	cluster_name = item->valuestring;

	item = cJSON_GetObjectItem(root, "shards");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get shards error";
		goto end;
	}
	shards = atoi(item->valuestring);
	if(shards<1 || shards>10)
	{
		job_info = "shards error(must in 1-10)";
		goto end;
	}

	job_result = "ongoing";
	job_info = "add shards start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_insert_operation_record(root, job_result, job_info, cluster_name);

	if(!System::get_instance()->check_cluster_name(cluster_name))
	{
		job_info = "error, cluster_name is no exist";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// get cluster info
	if(!job_get_cluster_info(cluster_name, cluster_info))
	{
		job_info = "get cluster_info error";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// update all ip,port,path of machines
	if(!Machine_info::get_instance()->update_machines_info())
	{
		job_info = "error, no available machine";
		goto end;
	}

	///////////////////////////////////////////////////////////////////////////////
	// get max index for add
	System::get_instance()->get_max_shard_name_id(cluster_name, shards_id);
	syslog(Logger::INFO, "shards_id=%d", shards_id);
	shards_id += 1;

	/////////////////////////////////////////////////////////
	// for install cluster cmd
	if(!job_create_program_path())
	{
		job_info = "create_cmd_path error";
		goto end;
	}

	///////////////////////////////////////////////////////////////////////////////
	// get storage of shard 
	for(int i=0; i<shards; i++)
	{
		std::vector<Tpye_Ip_Port_Paths> vec_storage_ip_port_paths;
		if(!Machine_info::get_instance()->get_storage_nodes(std::get<2>(cluster_info), vec_storage_ip_port_paths))
		{
			job_info = "Machine_info get_storage_nodes error";
			goto end;
		}
		vec_shard_storage_ip_port_paths.emplace_back(vec_storage_ip_port_paths);
		vec_shard_name.emplace_back("shard"+std::to_string(shards_id + i));
	}

	/////////////////////////////////////////////////////////
	//create storage shards json
	job_create_shards_jsonfile(vec_shard_storage_ip_port_paths, vec_shard_name);

	/////////////////////////////////////////////////////////
	//create meta json
	job_create_meta_jsonfile();

	///////////////////////////////////////////////////////////////////////////////
	// create storage of shard
	for(int i=0; i<shards; i++)
	{
		job_info = "add shard " + std::to_string(shards_id + i) + " working";
		update_jobid_status(job_id, job_result, job_info);
		syslog(Logger::INFO, "%s", job_info.c_str());
		if(!job_create_shard(vec_shard_storage_ip_port_paths[i], cluster_name, vec_shard_name[i], std::get<0>(cluster_info), std::get<7>(cluster_info)))
		{
			job_info = "add shard error";
			roll_back = true;
			break;
		}
	}

	if(roll_back)
	{
		syslog(Logger::INFO, "roll back shards");
		for(auto &storages: vec_shard_storage_ip_port_paths)
			for(auto &storage: storages)
			{
				Tpye_Ip_Port ip_port = std::make_pair(std::get<0>(storage), std::get<1>(storage));
				job_delete_storage(ip_port);
			}
		goto end;
	}

	///////////////////////////////////////////////////////////////////////////////
	//start cluster on shards and comps
	job_info = "add shard cmd";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	if(!job_start_shards(cluster_name, vec_shard_name))
	{
		syslog(Logger::INFO, "roll back cluster shards");
		for(auto &shard_name: vec_shard_name)
			System::get_instance()->stop_cluster_shard(cluster_name, shard_name);
		syslog(Logger::INFO, "roll back shards");
		for(auto &storages: vec_shard_storage_ip_port_paths)
			for(auto &storage: storages)
			{
				Tpye_Ip_Port ip_port = std::make_pair(std::get<0>(storage), std::get<1>(storage));
				job_delete_storage(ip_port);
			}

		job_info = "start_shards error";
		goto end;
	}

	job_result = "done";
	job_info = "";
	for(auto &shard_name: vec_shard_name)
	{
		if(job_info.length()>0)
			job_info += ";";
		job_info += shard_name;
	}
	update_jobid_status(job_id, job_result, job_info);
	cluster_name += "(" + job_info + ")";
	job_info = "add shards succeed";
	syslog(Logger::INFO, "%s: %s", job_info.c_str(), cluster_name.c_str());
	job_update_operation_record(job_id, job_result, job_info, cluster_name);
	return;

end:
	job_result = "failed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::ERROR, "%s", job_info.c_str());
	job_update_operation_record(job_id, job_result, job_info, cluster_name);
}

void Job::job_delete_shard(cJSON *root)
{
	std::lock_guard<std::mutex> lock(mutex_operation_);

	std::string job_id;
	std::string job_result;
	std::string job_info;
	std::string cluster_name;
	std::string shard_name;
	cJSON *item;

	std::vector<Tpye_Ip_Port> vec_storage_ip_port;

	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	item = cJSON_GetObjectItem(root, "cluster_name");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get cluster_name error";
		goto end;
	}
	cluster_name = item->valuestring;

	item = cJSON_GetObjectItem(root, "shard_name");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get shard_name error";
		goto end;
	}
	shard_name = item->valuestring;

	job_result = "ongoing";
	job_info = "delete shard start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_insert_operation_record(root, job_result, job_info, cluster_name);

	System::get_instance()->set_cluster_mgr_working(false);
	
	if(!System::get_instance()->check_cluster_name(cluster_name))
	{
		job_info = "error, cluster_name is no exist";
		goto end;
	}

	if(!System::get_instance()->check_cluster_shard_more(cluster_name))
	{
		job_info = "error, shard <= 1";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// get shards_ip_port by cluster_name and shard_name
	if(!System::get_instance()->get_shards_ip_port(cluster_name, shard_name, vec_storage_ip_port))
	{
		job_info = "get_shards_ip_port error";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// delete shard from meta talbes
	job_info = "stop shard start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	if(!System::get_instance()->stop_cluster_shard(cluster_name, shard_name))
	{
		job_info = "stop_cluster_shard error";
		syslog(Logger::INFO, "%s", job_info.c_str());
		//goto end;
	}

	/////////////////////////////////////////////////////////
	// delete storages from shard
	job_info = "delete shard working";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	for(auto &storage: vec_storage_ip_port)
	{
		if(!job_delete_storage(storage))
			syslog(Logger::ERROR, "delete storage error");
	}

	job_result = "done";
	update_jobid_status(job_id, job_result, shard_name);
	cluster_name += "(" + shard_name + ")";
	job_info = "delete shard succeed";
	syslog(Logger::INFO, "%s: %s", job_info.c_str(),cluster_name.c_str());
	job_update_operation_record(job_id, job_result, job_info, cluster_name);
	System::get_instance()->set_cluster_mgr_working(true);
	return;

end:
	job_result = "failed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::ERROR, "%s", job_info.c_str());
	job_update_operation_record(job_id, job_result, job_info, cluster_name);
	System::get_instance()->set_cluster_mgr_working(true);
}

bool Job::job_start_comps(std::string &cluster_name)
{
	FILE* pfd;
	char buf[256];

	int retry;
	std::string cmd;

	/////////////////////////////////////////////////////////
	// start cluster cmd
	cmd = "cd " + program_binaries_path + "/" + computer_prog_package_name + "/scripts/;";
	cmd += "python2 add_comp_nodes.py --config ./pgsql_comps.json --meta_config ./pgsql_meta.json --cluster_name " + cluster_name;
	syslog(Logger::INFO, "job_start_copms cmd %s", cmd.c_str());

	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		syslog(Logger::ERROR, "install error %s", cmd.c_str());
		return false;
	}
	while(fgets(buf, 256, pfd)!=NULL)
	{
		//if(strcasestr(buf, "error") != NULL)
			syslog(Logger::INFO, "%s", buf);
	}
	pclose(pfd);

	/////////////////////////////////////////////////////////////
	// check instance succeed by connect to instance
	retry = thread_work_interval * 30;
	while(retry-->0 && !Job::do_exit)
	{
		sleep(1);
		//if(System::get_instance()->check_cluster_shard_copms(cluster_name, vec_shard_name))
			break;
	}

	if(retry<0)
	{
		syslog(Logger::ERROR, "comps start error");
		return false;
	}

	syslog(Logger::INFO, "comps start succeed");
	return true;
}

void Job::job_add_comps(cJSON *root)
{
	std::lock_guard<std::mutex> lock(mutex_operation_);

	std::string job_id;
	std::string job_result;
	std::string job_info;
	std::string cluster_name;
	cJSON *item;
	char *cjson;

	int comps;
	int comps_id = 0;
	int comps_id_seq = 0;
	bool roll_back = false;
	std::vector<Tpye_Ip_Port_Paths> vec_comps_ip_port_paths;
	std::vector<std::string> vec_comp_name;

	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	item = cJSON_GetObjectItem(root, "cluster_name");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get cluster_name error");
		return;
	}
	cluster_name = item->valuestring;

	item = cJSON_GetObjectItem(root, "comps");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get comps error";
		goto end;
	}
	comps = atoi(item->valuestring);
	if(comps<1 || comps>10)
	{
		job_info = "comps error(must in 1-10)";
		goto end;
	}

	job_result = "ongoing";
	job_info = "add comps start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_insert_operation_record(root, job_result, job_info, cluster_name);

	if(!System::get_instance()->check_cluster_name(cluster_name))
	{
		job_info = "error, cluster_name is no exist";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// update all ip,port,path of machines
	if(!Machine_info::get_instance()->update_machines_info())
	{
		job_info = "error, no available machine";
		goto end;
	}

	/////////////////////////////////////////////////////////////////////////////// 
	System::get_instance()->get_comp_nodes_id_seq(comps_id_seq);
	//syslog(Logger::INFO, "comps_id_seq=%d", comps_id_seq);
	comps_id_seq += 1;

	///////////////////////////////////////////////////////////////////////////////
	// get max index for add
	System::get_instance()->get_max_comp_name_id(cluster_name, comps_id);
	syslog(Logger::INFO, "comps_id=%d", comps_id);
	comps_id += 1;

	/////////////////////////////////////////////////////////
	// for install cluster cmd
	if(!job_create_program_path())
	{
		job_info = "create_cmd_path error";
		goto end;
	}

	///////////////////////////////////////////////////////////////////////////////
	// get computer 
	if(!Machine_info::get_instance()->get_computer_nodes(comps, vec_comps_ip_port_paths))
	{
		job_info = "Machine_info get_computer_nodes error";
		goto end;
	}
	for(int i=0; i<comps; i++)
		vec_comp_name.emplace_back("comp"+std::to_string(comps_id + i));

	/////////////////////////////////////////////////////////
	//create meta json
	job_create_meta_jsonfile();

	///////////////////////////////////////////////////////////////////////////////
	// create computer
	job_info = "add comps working";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	if(!job_create_comps(vec_comps_ip_port_paths, cluster_name, vec_comp_name, comps_id_seq))
	{
		job_info = "add comps error";
		roll_back = true;
	}

	if(roll_back)
	{
		syslog(Logger::INFO, "roll back comps");
		for(auto &computer: vec_comps_ip_port_paths)
		{
			Tpye_Ip_Port ip_port = std::make_pair(std::get<0>(computer), std::get<1>(computer));
			job_delete_computer(ip_port);
		}
		goto end;
	}

	///////////////////////////////////////////////////////////////////////////////
	//start cluster on shards and comps
	job_info = "add comps cmd";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	if(!job_start_comps(cluster_name))
	{
		syslog(Logger::INFO, "roll back cluster comps");
		for(auto &comp_name: vec_comp_name)
			System::get_instance()->stop_cluster_comp(cluster_name, comp_name);
		syslog(Logger::INFO, "roll back comps");
		for(auto &computer: vec_comps_ip_port_paths)
		{
			Tpye_Ip_Port ip_port = std::make_pair(std::get<0>(computer), std::get<1>(computer));
			job_delete_computer(ip_port);
		}

		job_info = "start comps error";
		goto end;
	}

	job_result = "done";
	job_info = "";
	for(auto &comp_name: vec_comp_name)
	{
		if(job_info.length()>0)
			job_info += ";";
		job_info += comp_name;
	}
	update_jobid_status(job_id, job_result, job_info);
	cluster_name += "(" + job_info + ")";
	job_info = "add comps succeed";
	syslog(Logger::INFO, "%s: %s", job_info.c_str(), cluster_name.c_str());
	job_update_operation_record(job_id, job_result, job_info, cluster_name);
	return;

end:
	job_result = "failed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::ERROR, "%s", job_info.c_str());
	job_update_operation_record(job_id, job_result, job_info, cluster_name);
}

void Job::job_delete_comp(cJSON *root)
{
	std::lock_guard<std::mutex> lock(mutex_operation_);

	std::string job_id;
	std::string job_result;
	std::string job_info;
	std::string cluster_name;
	std::string comp_name;
	cJSON *item;

	std::vector<Tpye_Ip_Port> vec_comps_ip_port;

	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	item = cJSON_GetObjectItem(root, "cluster_name");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get cluster_name error";
		goto end;
	}
	cluster_name = item->valuestring;

	item = cJSON_GetObjectItem(root, "comp_name");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get comp_name error";
		goto end;
	}
	comp_name = item->valuestring;

	job_result = "ongoing";
	job_info = "delete comp start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_insert_operation_record(root, job_result, job_info, cluster_name);

	System::get_instance()->set_cluster_mgr_working(false);
	
	if(!System::get_instance()->check_cluster_name(cluster_name))
	{
		job_info = "error, cluster_name is no exist";
		goto end;
	}

	if(!System::get_instance()->check_cluster_comp_more(cluster_name))
	{
		job_info = "error, comp <= 1";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// get comps_ip_port by cluster_name  and comp_name
	if(!System::get_instance()->get_comps_ip_port(cluster_name, comp_name, vec_comps_ip_port))
	{
		job_info = "get_comps_ip_port error";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// delete cluster info from meta talbes
	job_info = "stop comp start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	if(!System::get_instance()->stop_cluster_comp(cluster_name, comp_name))
	{
		job_info = "stop_cluster_comp error";
		syslog(Logger::INFO, "%s", job_info.c_str());
		//goto end;
	}

	/////////////////////////////////////////////////////////
	// delete comp
	job_info = "delete comp working";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	for(auto &computer: vec_comps_ip_port)
	{
		if(!job_delete_computer(computer))
			syslog(Logger::ERROR, "delete computer error");
	}

	job_result = "done";
	update_jobid_status(job_id, job_result, comp_name);
	cluster_name += "(" + comp_name + ")";
	job_info = "delete comp succeed";
	syslog(Logger::INFO, "%s: %s", job_info.c_str(),cluster_name.c_str());
	job_update_operation_record(job_id, job_result, job_info, cluster_name);
	System::get_instance()->set_cluster_mgr_working(true);
	return;

end:
	job_result = "failed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::ERROR, "%s", job_info.c_str());
	job_update_operation_record(job_id, job_result, job_info, cluster_name);
	System::get_instance()->set_cluster_mgr_working(true);
}

bool Job::job_update_shard_nodes(std::string &cluster_name, std::string &shard_name)
{
	cJSON *root = NULL;
	cJSON *item;
	cJSON *item_node;
	cJSON *item_sub;
	
	bool ret = false;
	std::string jsonfile_path, jsonfile_buf, group_seeds;
	int nodes_num;

	std::vector<Tpye_Ip_Port> vec_storage_ip_port;
	Tpye_string2 t_string2;

	/////////////////////////////////////////////////////////
	// get shards_ip_port by cluster_name and shard_name
	if(!System::get_instance()->get_shards_ip_port(cluster_name, shard_name, vec_storage_ip_port))
	{
		syslog(Logger::ERROR, "get_shards_ip_port error");
		return false;
	}

	/////////////////////////////////////////////////////////
	//get ip and port from every node
	jsonfile_path = cluster_json_path + "/" + cluster_name + "/mysql_" + shard_name + ".json";
	if(!job_read_file(jsonfile_path, jsonfile_buf))
	{
		syslog(Logger::ERROR, "job_read_file error");
		return false;
	}

	root = cJSON_Parse(jsonfile_buf.c_str());
	if(root == NULL)
	{
		syslog(Logger::ERROR, "file cJSON_Parse error");
		return false;
	}

	item_node = cJSON_GetObjectItem(root, "nodes");
	if(item_node == NULL)
	{
		syslog(Logger::ERROR, "get nodes error");
		goto end;
	}

	nodes_num = cJSON_GetArraySize(item_node);
	for(int i=0; i<nodes_num; i++)
	{
		int port_sub;
		std::string ip_sub;

		item_sub = cJSON_GetArrayItem(item_node,i);

		item = cJSON_GetObjectItem(item_sub, "ip");
		if(item == NULL)
		{
			syslog(Logger::ERROR, "get sub node ip error");
			goto end;
		}
		ip_sub = item->valuestring;
		
		item = cJSON_GetObjectItem(item_sub, "mgr_port");
		if(item == NULL)
		{
			syslog(Logger::ERROR, "get sub node port error");
			goto end;
		}
		port_sub = item->valueint;

		if(group_seeds.length()>0)
			group_seeds += "," + ip_sub + ":" + std::to_string(port_sub);
		else
			group_seeds = ip_sub + ":" + std::to_string(port_sub);
	}

	t_string2 = std::make_tuple("group_replication_group_seeds", group_seeds);

	/////////////////////////////////////////////////////////
	//update group_seeds to every node 
	for(auto &ip_port: vec_storage_ip_port)
	{
		if(!System::get_instance()->update_variables(cluster_name, shard_name, ip_port, t_string2))
		{
			syslog(Logger::ERROR, "update_group_seeds error");
			goto end;
		}
	}

	ret = true;

end:
	if(root!=NULL)
		cJSON_Delete(root);

	return ret;
}

void Job::job_add_nodes(cJSON *root)
{
	std::lock_guard<std::mutex> lock(mutex_operation_);

	std::string job_id;
	std::string job_result;
	std::string job_info;
	std::string cluster_name, shard_name, restore_shard_name, datatime;
	int nodes = 0;
	bool roll_back = false;
	bool all_shard = false;
	cJSON *item;

	std::vector <std::vector<Tpye_Ip_Port_Paths>> vec_shard_storage_ip_port_paths;
	std::vector<std::string> vec_shard_name;

	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	item = cJSON_GetObjectItem(root, "cluster_name");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get cluster_name error";
		goto end;
	}
	cluster_name = item->valuestring;

	item = cJSON_GetObjectItem(root, "nodes");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get nodes error";
		goto end;
	}
	nodes = atoi(item->valuestring);

	job_result = "ongoing";
	job_info = "add nodes start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_insert_operation_record(root, job_result, job_info, cluster_name);

	if(!System::get_instance()->check_cluster_name(cluster_name))
	{
		job_info = "error, cluster_name is no exist";
		goto end;
	}

	if(!System::get_instance()->get_cluster_mgr_mode(cluster_name))
	{
		job_info = "error, add nodes must in mgr mode";
		goto end;
	}

	item = cJSON_GetObjectItem(root, "shard_name");
	if(item == NULL || item->valuestring == NULL)
	{
		// get every shard_name
		if(!System::get_instance()->get_cluster_shard_name(cluster_name, vec_shard_name))
		{
			job_info = "get_cluster_shard_name error";
			goto end;
		}
		all_shard = true;
	}
	else
	{
		shard_name = item->valuestring;
		if(!System::get_instance()->check_cluster_shard_name(cluster_name, shard_name))
		{
			job_info = "error, shard_name is no exist";
			goto end;
		}
		vec_shard_name.emplace_back(shard_name);
	}

	/////////////////////////////////////////////////////////
	// update all ip,port,path of machines
	if(!Machine_info::get_instance()->update_machines_info())
	{
		job_info = "error, no available machine";
		goto end;
	}

	////////////////////////////////////////////////////////////////////////
	// backup cluster
	job_info = "backup cluster working";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	if(!job_backup_cluster(cluster_name, datatime))
	{
		job_info = "job_backup_cluster error";
		goto end;
	}

	////////////////////////////////////////////////////////////////////////
	// create nodes and add to mgr
	job_info = "add node working";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	for(int i=0; i<vec_shard_name.size(); i++)
	{
		std::vector<Tpye_Ip_Port_Paths> vec_storage_ip_port_paths;
		std::vector<Tpye_Ip_Port_User_Pwd> vec_ip_port_user_pwd;

		//get storage ip and port
		if(!Machine_info::get_instance()->get_storage_nodes(nodes, vec_storage_ip_port_paths))
		{
			job_info = "Machine_info get_storage_nodes error";
			goto end;
		}
		vec_shard_storage_ip_port_paths.emplace_back(vec_storage_ip_port_paths);

		//create nodes
		syslog(Logger::INFO, "create shard node start");
		if(!job_create_nodes(vec_storage_ip_port_paths, cluster_name, vec_shard_name[i]))
		{
			for(auto &storages: vec_shard_storage_ip_port_paths)
				for(auto &storage: storages)
				{
					Tpye_Ip_Port ip_port = std::make_pair(std::get<0>(storage), std::get<1>(storage));
					job_delete_storage(ip_port);
				}
			job_info = "job_create_nodes error";
			goto end;
		}

		//restore to new node
		syslog(Logger::INFO, "restore shard node working");
		if(all_shard)
		{
			restore_shard_name = "shard" + std::to_string(i+1);
		}
		else
		{
			int order = 0;
			System::get_instance()->get_cluster_shard_order(cluster_name, shard_name, order);
			restore_shard_name = "shard" + std::to_string(order+1);
		}

		for(auto &ip_port_paths: vec_storage_ip_port_paths)
		{
			Tpye_Ip_Port ip_port = std::make_pair(std::get<0>(ip_port_paths), std::get<1>(ip_port_paths));
			vec_ip_port_user_pwd.emplace_back(std::make_tuple(std::get<0>(ip_port_paths),std::get<1>(ip_port_paths), "pgx", "pgx_pwd"));
			if(!job_restore_storage(cluster_name, restore_shard_name, datatime, ip_port))
			{
				roll_back = true;
				syslog(Logger::INFO, "job_restore_storage error");
				//goto end;
			}
		}

		if(roll_back)
		{
			for(auto &storages: vec_shard_storage_ip_port_paths)
				for(auto &storage: storages)
				{
					Tpye_Ip_Port ip_port = std::make_pair(std::get<0>(storage), std::get<1>(storage));
					job_delete_storage(ip_port);
				}
			job_info = "job_restore_storage error";
			goto end;
		}

		//update old node
		if(!job_update_shard_nodes(cluster_name, vec_shard_name[i]))
		{
			job_info = "job_update_shard_nodes error";
			goto end;
		}

		//add new node to shard
		if(!System::get_instance()->add_shard_nodes(cluster_name, vec_shard_name[i], vec_ip_port_user_pwd))
		{
			job_info = "job_add_shard_nodes error";
			goto end;
		}
	}

	job_result = "done";
	job_info = "";
	for(auto &storages: vec_shard_storage_ip_port_paths)
		for(auto &ip_port_paths: storages)
		{
			if(job_info.length()>0)
				job_info += ";";
			job_info += std::get<0>(ip_port_paths) + ":" + std::to_string(std::get<1>(ip_port_paths));
		}
	update_jobid_status(job_id, job_result, job_info);
	cluster_name += "(" + job_info + ")";
	job_info = "add nodes succeed";
	syslog(Logger::INFO, "%s:%s", job_info.c_str(),cluster_name.c_str());
	job_update_operation_record(job_id, job_result, job_info, cluster_name);
	return;

end:
	job_result = "failed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_update_operation_record(job_id, job_result, job_info, cluster_name);
}

bool Job::job_delete_shard_json(std::string &cluster_name, std::string &shard_name, Tpye_Ip_Port &ip_port)
{
	cJSON *root = NULL;
	char *cjson = NULL;
	cJSON *item;
	cJSON *item_node;
	cJSON *item_sub;
	
	bool ret = false;
	std::string jsonfile_path, jsonfile_buf;
	int nodes_num;

	jsonfile_path = cluster_json_path + "/" + cluster_name + "/mysql_" + shard_name + ".json";
	if(!job_read_file(jsonfile_path, jsonfile_buf))
	{
		syslog(Logger::ERROR, "job_read_file error");
		return false;
	}

	root = cJSON_Parse(jsonfile_buf.c_str());
	if(root == NULL)
	{
		syslog(Logger::ERROR, "file cJSON_Parse error");
		return false;
	}

	item_node = cJSON_GetObjectItem(root, "nodes");
	if(item_node == NULL)
	{
		syslog(Logger::ERROR, "get nodes error");
		goto end;
	}

	nodes_num = cJSON_GetArraySize(item_node);

	for(int i=0; i<nodes_num; i++)
	{
		int port_sub;
		std::string ip_sub;

		item_sub = cJSON_GetArrayItem(item_node,i);
		if(item_sub == NULL)
		{
			syslog(Logger::ERROR, "get sub node error");
			goto end;
		}

		item = cJSON_GetObjectItem(item_sub, "ip");
		if(item == NULL)
		{
			syslog(Logger::ERROR, "get sub node ip error");
			goto end;
		}
		ip_sub = item->valuestring;
		
		item = cJSON_GetObjectItem(item_sub, "port");
		if(item == NULL)
		{
			syslog(Logger::ERROR, "get sub node port error");
			goto end;
		}
		port_sub = item->valueint;

		if(ip_sub != ip_port.first || port_sub != ip_port.second)
			continue;

		cJSON_DeleteItemFromArray(item_node, i);
		break;
	}

	/////////////////////////////////////////////////////////
	// save json file to cluster_json_path
	cjson = cJSON_Print(root);
	if(cjson != NULL)
	{
		job_save_file(jsonfile_path, cjson);
		free(cjson);
		cjson = NULL;
	}

	ret = true;

end:
	if(root!=NULL)
		cJSON_Delete(root);
	if(cjson!=NULL)
		free(cjson);

	return ret;
}

void Job::job_delete_node(cJSON *root)
{
	std::lock_guard<std::mutex> lock(mutex_operation_);

	std::string job_id;
	std::string job_result;
	std::string job_info;
	std::string cluster_name, shard_name, ip;
	int port;
	cJSON *item;
	Tpye_Ip_Port ip_port;

	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	item = cJSON_GetObjectItem(root, "cluster_name");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get cluster_name error";
		goto end;
	}
	cluster_name = item->valuestring;

	item = cJSON_GetObjectItem(root, "shard_name");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get shard_name error";
		goto end;
	}
	shard_name = item->valuestring;

	item = cJSON_GetObjectItem(root, "ip");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get ip error";
		goto end;
	}
	ip = item->valuestring;

	item = cJSON_GetObjectItem(root, "port");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get port error";
		goto end;
	}
	port = atoi(item->valuestring);

	ip_port = std::make_pair(ip, port);

	job_result = "ongoing";
	job_info = "delete node start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_insert_operation_record(root, job_result, job_info, cluster_name);

	System::get_instance()->set_cluster_mgr_working(false);

	if(!System::get_instance()->check_cluster_name(cluster_name))
	{
		job_info = "error, cluster_name is no exist";
		goto end;
	}

	if(!System::get_instance()->get_cluster_mgr_mode(cluster_name))
	{
		job_info = "error, delete node must in mgr mode";
		goto end;
	}

	if(!System::get_instance()->check_cluster_shard_name(cluster_name, shard_name))
	{
		job_info = "error, shard_name is no exist";
		goto end;
	}

	if(!System::get_instance()->check_cluster_shard_node_more(cluster_name, shard_name))
	{
		job_info = "error, node <= 1";
		goto end;
	}

	////////////////////////////////////////////////////////////////////////
	// delete nodes from meta talbes
	job_info = "delete node working";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	if(!System::get_instance()->stop_cluster_shard_node(cluster_name, shard_name, ip_port))
	{
		job_info = "stop_cluster_shard_node error";
		syslog(Logger::INFO, "%s", job_info.c_str());
		//goto end;
	}

	////////////////////////////////////////////////////////////////////////
	// remove nodes from json file
	if(!job_delete_shard_json(cluster_name, shard_name, ip_port))
	{
		job_info = "job_delete_shard_json error";
		syslog(Logger::INFO, "%s", job_info.c_str());
		//goto end;
	}
	
	////////////////////////////////////////////////////////////////////////
	//update left node
	syslog(Logger::INFO, "update shard nodes working");
	if(!job_update_shard_nodes(cluster_name, shard_name))
	{
		job_info = "job_update_shard_nodes error";
		goto end;
	}

	////////////////////////////////////////////////////////////////////////
	//delete storage
	syslog(Logger::INFO, "delete storage working");
	if(!job_delete_storage(ip_port))
	{
		job_info = "job_delete_storage error";
		goto end;
	}

	job_result = "done";
	job_info = ip + ":" + std::to_string(port);
	update_jobid_status(job_id, job_result, job_info);
	cluster_name += "(" + job_info + ")";
	job_info = "delete nodes succeed";
	syslog(Logger::INFO, "%s:%s", job_info.c_str(),cluster_name.c_str());
	job_update_operation_record(job_id, job_result, job_info, cluster_name);
	System::get_instance()->set_cluster_mgr_working(true);
	return;

end:
	job_result = "failed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_update_operation_record(job_id, job_result, job_info, cluster_name);
	System::get_instance()->set_cluster_mgr_working(true);
}

bool Job::job_get_cluster_info(std::string &cluster_name, Tpye_cluster_info &cluster_info)
{
	bool ret = false;
	std::string jsonfile_path, jsonfile_buf;
	int shards, nodes, comps;

	cJSON *root = NULL;
	cJSON *item;

	jsonfile_path = cluster_json_path + "/" + cluster_name + "/create_cluster.json";
	if(!job_read_file(jsonfile_path, jsonfile_buf))
	{
		syslog(Logger::ERROR, "job_read_file error");
		return false;
	}

	root = cJSON_Parse(jsonfile_buf.c_str());
	if(root == NULL)
	{
		syslog(Logger::ERROR, "file cJSON_Parse error");
		return false;
	}

	item = cJSON_GetObjectItem(root, "ha_mode");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get ha_mode error");
		goto end;
	}
	std::get<0>(cluster_info) = item->valuestring;

	item = cJSON_GetObjectItem(root, "max_storage_size");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get max_storage_size error");
		goto end;
	}
	std::get<4>(cluster_info) = atoi(item->valuestring);

	item = cJSON_GetObjectItem(root, "max_connections");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get max_connections error");
		goto end;
	}
	std::get<5>(cluster_info) = atoi(item->valuestring);

	item = cJSON_GetObjectItem(root, "cpu_cores");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get cpu_cores error");
		goto end;
	}
	std::get<6>(cluster_info) = atoi(item->valuestring);

	item = cJSON_GetObjectItem(root, "innodb_size");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get innodb_size error");
		goto end;
	}
	std::get<7>(cluster_info) = atoi(item->valuestring);

	//shards, nodes, comps, maybe add or remove after create
	//must get from current cluster
	if(!System::get_instance()->get_cluster_shards_nodes_comps(cluster_name, shards, nodes, comps))
	{
		syslog(Logger::ERROR, "get_cluster_shards_nodes_comps error");
		goto end;
	}

	std::get<1>(cluster_info) = shards;
	std::get<2>(cluster_info) = nodes;
	std::get<3>(cluster_info) = comps;

	ret = true;

end:
	if(root!=NULL)
		cJSON_Delete(root);

	return ret;
}

bool Job::job_backup_shard(std::string &cluster_name, Tpye_Ip_Port &ip_port, int shards_id)
{
	cJSON *root = NULL;
	char *cjson = NULL;

	bool ret = false;
	std::string shard_name,uuid_job_id;
	std::string post_url,get_status,result_str;
	get_uuid(uuid_job_id);
	shard_name = "shard" + std::to_string(shards_id);

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
	
	retry = 60*60*3;	//3 hours
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
			if(ret_item == NULL || ret_item->valuestring == NULL)
			{
				syslog(Logger::ERROR, "get result error");
				cJSON_Delete(ret_root);
				goto end;
			}
			result = ret_item->valuestring;

			ret_item = cJSON_GetObjectItem(ret_root, "info");
			if(ret_item == NULL || ret_item->valuestring == NULL)
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
				syslog(Logger::INFO, "backup shard %d:%s:%d finish!", shards_id, ip_port.first.c_str(), ip_port.second);
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

bool Job::job_backup_cluster(std::string &cluster_name, std::string &datatime)
{
	std::string str_sql;
	int shards_id = 0;
	cJSON *item;

	std::vector<Tpye_Ip_Port> vec_ip_port;
	Tpye_cluster_info cluster_info;

	/////////////////////////////////////////////////////////
	// get master node of erver shard
	if(!System::get_instance()->get_shard_ip_port_backup(cluster_name, vec_ip_port))
	{
		syslog(Logger::ERROR, "get cluster_shard_ip_port error");
		return false;
	}

	///////////////////////////////////////////////////////////////////////////////
	// backup every shard
	for(auto &ip_port: vec_ip_port)
	{
		shards_id++;
		syslog(Logger::INFO, "backup shard %d working", shards_id);
		if(!job_backup_shard(cluster_name, ip_port, shards_id))
		{
			syslog(Logger::ERROR, "job_backup_shard error");
			return false;
		}
	}

	///////////////////////////////////////////////////////////////////////////////
	// get backup cluster info for save cluster_backups
	if(!job_get_cluster_info(cluster_name, cluster_info))
	{
		syslog(Logger::ERROR, "get cluster_info error");
		return false;
	}

	///////////////////////////////////////////////////////////////////////////////
	// save to metadata table
	get_datatime(datatime);
	str_sql = "INSERT INTO cluster_backups(cluster_name,ha_mode,shards,nodes,comps,max_storage_size,max_connections,cpu_cores,innodb_size,when_created) VALUES('"
				+ cluster_name + "','" + std::get<0>(cluster_info) + "'," + std::to_string(std::get<1>(cluster_info))
				+ "," + std::to_string(std::get<2>(cluster_info)) + "," + std::to_string(std::get<3>(cluster_info))
				+ "," + std::to_string(std::get<4>(cluster_info)) + "," + std::to_string(std::get<5>(cluster_info))
				+ "," + std::to_string(std::get<6>(cluster_info)) + "," + std::to_string(std::get<7>(cluster_info))
				+ ",'" + datatime + "')";
	syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_INSERT, str_sql))
	{
		syslog(Logger::ERROR, "insert cluster_backups error");
		return false;
	}

	return true;
}

void Job::job_backup_cluster(cJSON *root)
{
	std::lock_guard<std::mutex> lock(mutex_operation_);

	std::string job_id;
	std::string job_result;
	std::string job_info;
	std::string backup_cluster_name;
	std::string datatime;
	int shards_id = 0;
	cJSON *item;

	std::vector<Tpye_Ip_Port> vec_ip_port;
	Tpye_cluster_info backup_cluster_info;

	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	item = cJSON_GetObjectItem(root, "backup_cluster_name");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get backup_cluster_name error";
		goto end;
	}
	backup_cluster_name = item->valuestring;

	job_result = "ongoing";
	job_info = "backup cluster start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_insert_operation_record(root, job_result, job_info, backup_cluster_name);

	if(!System::get_instance()->check_cluster_name(backup_cluster_name))
	{
		job_info = "error, backup_cluster_name is no exist";
		goto end;
	}

	////////////////////////////////////////////////////////////////////////
	// backup cluster
	job_info = "backup cluster working";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	if(!job_backup_cluster(backup_cluster_name, datatime))
	{
		job_info = "job_backup_cluster error";
		goto end;
	}

	job_result = "done";
	update_jobid_status(job_id, job_result, datatime);
	job_info = "backup cluster succeed";
	syslog(Logger::INFO, "%s: %s", job_info.c_str(), datatime.c_str());
	job_update_operation_record(job_id, job_result, job_info, backup_cluster_name);
	return;

end:
	job_result = "failed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_update_operation_record(job_id, job_result, job_info, backup_cluster_name);
}

bool Job::job_restore_storage(std::string &cluster_name, std::string &shard_name, std::string &timestamp, Tpye_Ip_Port &ip_port)
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
	cJSON_AddStringToObject(root, "job_type", "restore_storage");
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
	
	retry = 60*60*3;	//3 hours
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
			if(ret_item == NULL || ret_item->valuestring == NULL)
			{
				syslog(Logger::ERROR, "get result error");
				cJSON_Delete(ret_root);
				goto end;
			}
			result = ret_item->valuestring;
			
			ret_item = cJSON_GetObjectItem(ret_root, "info");
			if(ret_item == NULL || ret_item->valuestring == NULL)
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

bool Job::job_restore_computer(std::string &cluster_name, Tpye_Ip_Port &ip_port)
{
	cJSON *root = NULL;
	char *cjson = NULL;

	bool ret = false;
	std::string strtmp, uuid_job_id;
	std::string post_url,get_status,result_str;
	get_uuid(uuid_job_id);
	
	/////////////////////////////////////////////////////////
	// create json parameter
	root = cJSON_CreateObject();
	cJSON_AddStringToObject(root, "ver", http_cmd_version.c_str());
	cJSON_AddStringToObject(root, "job_id", uuid_job_id.c_str());
	cJSON_AddStringToObject(root, "job_type", "restore_computer");
	cJSON_AddStringToObject(root, "ip", ip_port.first.c_str());
	cJSON_AddNumberToObject(root, "port", ip_port.second);
	cJSON_AddStringToObject(root, "cluster_name", cluster_name.c_str());
	strtmp = "pgx:pgx_pwd@(" + meta_svr_ip + ":" + std::to_string(meta_svr_port) + ")/mysql";
	cJSON_AddStringToObject(root, "meta_str", strtmp.c_str());

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
		syslog(Logger::ERROR, "retore computer fail because http post");
		goto end;
	}

	/////////////////////////////////////////////////////////
	// get status from node 
	get_status = "{\"ver\":\"" + http_cmd_version + "\",\"job_id\":\"" + uuid_job_id + "\",\"job_type\":\"get_status\"}";
	
	retry = 60*30;	//30 minutes
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
			if(ret_item == NULL || ret_item->valuestring == NULL)
			{
				syslog(Logger::ERROR, "get result error");
				cJSON_Delete(ret_root);
				goto end;
			}
			result = ret_item->valuestring;
			
			ret_item = cJSON_GetObjectItem(ret_root, "info");
			if(ret_item == NULL || ret_item->valuestring == NULL)
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
				syslog(Logger::INFO, "retore %s:%d finish!", ip_port.first.c_str(), ip_port.second);
				break;
			}
		}
	}

	if(retry<0)
	{
		syslog(Logger::ERROR, "retore computer timeout %s", result_str.c_str());
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
	std::lock_guard<std::mutex> lock(mutex_operation_);

	std::string job_id;
	std::string job_result;
	std::string job_info;
	std::string backup_cluster_name;
	std::string restore_cluster_name;
	std::string shard_name,timestamp;
	int shards_id = 0;
	int shards;
	int retry;
	cJSON *item;

	std::vector<std::vector<Tpye_Ip_Port>> vec_vec_storage_ip_port;
	std::vector<Tpye_Ip_Port> vec_computer_ip_port;
	Tpye_cluster_info backup_cluster_info;

	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	item = cJSON_GetObjectItem(root, "timestamp");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get timestamp error";
		goto end;
	}
	timestamp = item->valuestring;

	item = cJSON_GetObjectItem(root, "backup_cluster_name");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get backup_cluster_name error";
		goto end;
	}
	backup_cluster_name = item->valuestring;

	item = cJSON_GetObjectItem(root, "restore_cluster_name");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get restore_cluster_name error";
		goto end;
	}
	restore_cluster_name = item->valuestring;

	job_result = "ongoing";
	job_info = "restore cluster start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_insert_operation_record(root, job_result, job_info, restore_cluster_name);

	System::get_instance()->set_cluster_mgr_working(false);

	if(!System::get_instance()->check_cluster_name(restore_cluster_name))
	{
		job_info = "error, restore_cluster_name is no exist";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// get backup recored info from metadata table
	if(System::get_instance()->get_backup_info_from_metadata(backup_cluster_name, timestamp, backup_cluster_info))
	{
		job_info = "get backup_record error";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// get all node from erver shard
	if(!System::get_instance()->get_shard_ip_port_restore(restore_cluster_name, vec_vec_storage_ip_port))
	{
		job_info = "get cluster_shard_ip_port error";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// check shards number
	if(std::get<1>(backup_cluster_info) != vec_vec_storage_ip_port.size())
	{
		job_info = "shards of backup and restore is different";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// clear cluster shard master for restore
	if(System::get_instance()->get_cluster_mgr_mode(restore_cluster_name))
	{
		if(!System::get_instance()->clear_cluster_shard_master(restore_cluster_name))
		{
			job_info = "clear_cluster_shard_master error";
			goto end;
		}
	}
	
	///////////////////////////////////////////////////////////////////////////////
	// restore every shard
	for(auto &vec_ip_port: vec_vec_storage_ip_port)
	{
		shards_id++;
		job_info = "restore shard " + std::to_string(shards_id) + " working";
		update_jobid_status(job_id, job_result, job_info);
		syslog(Logger::INFO, "%s", job_info.c_str());

		shard_name = "shard" +std::to_string(shards_id);
		for(auto &ip_port: vec_ip_port)
		{
			syslog(Logger::INFO, "restore shard node working");
			if(!job_restore_storage(backup_cluster_name, shard_name, timestamp, ip_port))
			{
				job_info = "job_restore_storage error";
				goto end;
			}
		}
	}

	///////////////////////////////////////////////////////////////////////////////
	// restore every computer
	for(auto &ip_port: vec_computer_ip_port)
	{
		syslog(Logger::INFO, "restore computer node working");
		if(!job_restore_computer(backup_cluster_name, ip_port))
		{
			job_info = "job_restore_computer error";
			goto end;
		}
	}

	/////////////////////////////////////////////////////////////
	// update every instance cluster info
	System::get_instance()->set_cluster_mgr_working(true);
	retry = thread_work_interval * 30;
	while(retry-->0 && !Job::do_exit)
	{
		sleep(1);
		if(System::get_instance()->update_instance_cluster_info(restore_cluster_name))
			break;
	}

	if(retry<0)
	{
		job_info = "update_instance_cluster_info timeout";
		goto end;
	}

	job_result = "done";
	update_jobid_status(job_id, job_result, restore_cluster_name);
	job_info = "restore cluster succeed";
	syslog(Logger::INFO, "%s: %s", job_info.c_str(), restore_cluster_name.c_str());
	job_update_operation_record(job_id, job_result, job_info, restore_cluster_name);
	return;

end:
	job_result = "failed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_update_operation_record(job_id, job_result, job_info, restore_cluster_name);
	System::get_instance()->set_cluster_mgr_working(true);
}

void Job::job_restore_new_cluster(cJSON *root)
{
	std::lock_guard<std::mutex> lock(mutex_operation_);

	std::string job_id;
	std::string job_result;
	std::string job_info;
	std::string backup_cluster_name;
	std::string restore_cluster_name;
	std::string shard_name,timestamp;
	int shards_id = 0;
	int retry;
	cJSON *item;
	bool roll_back = false;

	std::vector<std::vector<Tpye_Ip_Port>> vec_vec_storage_ip_port;
	std::vector<Tpye_Ip_Port> vec_computer_ip_port;
	Tpye_cluster_info backup_cluster_info;

	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	item = cJSON_GetObjectItem(root, "timestamp");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get timestamp error";
		goto end;
	}
	timestamp = item->valuestring;

	item = cJSON_GetObjectItem(root, "backup_cluster_name");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get backup_cluster_name error";
		goto end;
	}
	backup_cluster_name = item->valuestring;

	/////////////////////////////////////////////////////////
	// generate cluster name
	job_generate_cluster_name(restore_cluster_name);

	job_result = "ongoing";
	job_info = "restore new cluster start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_insert_operation_record(root, job_result, job_info, restore_cluster_name);

	/////////////////////////////////////////////////////////
	// get backup_info from metadata
	if(System::get_instance()->get_backup_info_from_metadata(backup_cluster_name, timestamp, backup_cluster_info))
	{
		job_info = "get_backup_info_from_metadata error";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// create a new cluster for restore
	job_info = "create new cluster for restore working";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	if(!job_create_cluster(backup_cluster_info, restore_cluster_name))
	{
		job_info = "create cluster for restore error";
		goto end;
	}

	System::get_instance()->set_cluster_mgr_working(false);

	/////////////////////////////////////////////////////////
	// clear cluster shard master for restore
	if(std::get<0>(backup_cluster_info) == "mgr")
	{
		if(!System::get_instance()->clear_cluster_shard_master(restore_cluster_name))
		{
			job_info = "clear_cluster_shard_master error";
			goto end;
		}
	}

	/////////////////////////////////////////////////////////
	// get all storage nodes from erver shard
	if(!System::get_instance()->get_shard_ip_port_restore(restore_cluster_name, vec_vec_storage_ip_port))
	{
		job_info = "get cluster_shard_ip_port error";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// get all comps
	if(!System::get_instance()->get_comps_ip_port_restore(restore_cluster_name, vec_computer_ip_port))
	{
		job_info = "get comps_ip_port_restore error";
		goto end;
	}

	///////////////////////////////////////////////////////////////////////////////
	// restore every shard
	for(auto &vec_ip_port: vec_vec_storage_ip_port)
	{
		shards_id++;
		job_info = "restore shard " + std::to_string(shards_id) + " working";
		update_jobid_status(job_id, job_result, job_info);
		syslog(Logger::INFO, "%s", job_info.c_str());

		shard_name = "shard" +std::to_string(shards_id);
		for(auto &ip_port: vec_ip_port)
		{
			syslog(Logger::INFO, "restore shard node working");
			if(!job_restore_storage(backup_cluster_name, shard_name, timestamp, ip_port))
			{
				roll_back = true;
				job_info = "job_restore_storage error";
				//goto end;
			}
		}
	}

	if(roll_back)
	{
		syslog(Logger::INFO, "roll back cluster start");
		job_delete_cluster(restore_cluster_name);
		goto end;
	}

	///////////////////////////////////////////////////////////////////////////////
	// restore every computer
	for(auto &ip_port: vec_computer_ip_port)
	{
		syslog(Logger::INFO, "restore computer node working");
		if(!job_restore_computer(backup_cluster_name, ip_port))
		{
			roll_back = true;
			job_info = "job_restore_computer error";
			//goto end;
		}
	}

	if(roll_back)
	{
		syslog(Logger::INFO, "roll back cluster start");
		job_delete_cluster(restore_cluster_name);
		goto end;
	}

	/////////////////////////////////////////////////////////////
	// update every instance cluster info
	System::get_instance()->set_cluster_mgr_working(true);
	retry = thread_work_interval * 30;
	while(retry-->0 && !Job::do_exit)
	{
		sleep(1);
		if(System::get_instance()->update_instance_cluster_info(restore_cluster_name))
			break;
	}

	if(retry<0)
	{
		job_info = "update_instance_cluster_info timeout";
		goto end;
	}

	job_result = "done";
	update_jobid_status(job_id, job_result, restore_cluster_name);
	job_info = "restore new cluster succeed";
	syslog(Logger::INFO, "%s: %s", job_info.c_str(), restore_cluster_name.c_str());
	job_update_operation_record(job_id, job_result, job_info, restore_cluster_name);
	return;

end:
	job_result = "failed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	job_update_operation_record(job_id, job_result, job_info, restore_cluster_name);
	System::get_instance()->set_cluster_mgr_working(true);
}

bool Job::update_jobid_status(std::string &jobid, std::string &result, std::string &info)
{
	std::lock_guard<std::mutex> lock(mutex_stauts_);

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
	std::lock_guard<std::mutex> lock(mutex_stauts_);

	bool ret = false;
	for (auto it = list_jobid_result_info.begin(); it != list_jobid_result_info.end(); ++it)
	{
		if(std::get<0>(*it) == jobid)
		{
			result = std::get<1>(*it);
			info = std::get<2>(*it);
			ret = true;
			break;
		}
	}

	return ret;
}

bool Job::job_get_status(cJSON *root, std::string &str_ret)
{
	std::string job_id, result, info;

	cJSON *item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return false;
	}
	job_id = item->valuestring;
	
	if(!Job::get_instance()->get_jobid_status(job_id, result, info))
		str_ret = "{\"result\":\"ongoing\",\"info\":\"job id no find\"}";
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

bool Job::get_job_type(char *str, Job_type &job_type)
{
	if(strcmp(str, "get_status")==0)
		job_type = JOB_GET_STATUS;
	else if(strcmp(str, "get_instance")==0)
		job_type = JOB_GET_INSTANCES;
	else if(strcmp(str, "get_meta")==0)
		job_type = JOB_GET_META;
	else if(strcmp(str, "get_cluster")==0)
		job_type = JOB_GET_CLUSTER;
	else if(strcmp(str, "get_storage")==0)
		job_type = JOB_GET_STORAGE;
	else if(strcmp(str, "get_computer")==0)
		job_type = JOB_GET_COMPUTER;
	else if(strcmp(str, "check_timestamp")==0)
		job_type = JOB_CHECK_TIMESTAMP;
	else if(strcmp(str, "get_variable")==0)
		job_type = JOB_GET_VARIABLE;
	else if(strcmp(str, "set_variable")==0)
		job_type = JOB_SET_VARIABLE;
	else if(strcmp(str, "create_machine")==0)
		job_type = JOB_CREATE_MACHINE;
	else if(strcmp(str, "update_machine")==0)
		job_type = JOB_UPDATE_MACHINE;
	else if(strcmp(str, "delete_machine")==0)
		job_type = JOB_DELETE_MACHINE;
	else if(strcmp(str, "control_instance")==0)
		job_type = JOB_CONTROL_INSTANCE;
	else if(strcmp(str, "create_cluster")==0)
		job_type = JOB_CREATE_CLUSTER;
	else if(strcmp(str, "delete_cluster")==0)
		job_type = JOB_DELETE_CLUSTER;
	else if(strcmp(str, "add_shards")==0)
		job_type = JOB_ADD_SHARDS;
	else if(strcmp(str, "delete_shard")==0)
		job_type = JOB_DELETE_SHARD;
	else if(strcmp(str, "add_comps")==0)
		job_type = JOB_ADD_COMPS;
	else if(strcmp(str, "delete_comp")==0)
		job_type = JOB_DELETE_COMP;
	else if(strcmp(str, "add_nodes")==0)
		job_type = JOB_ADD_NODES;
	else if(strcmp(str, "delete_node")==0)
		job_type = JOB_DELETE_NODE;
	else if(strcmp(str, "backup_cluster")==0)
		job_type = JOB_BACKUP_CLUSTER;
	else if(strcmp(str, "restore_cluster")==0)
		job_type = JOB_RESTORE_CLUSTER;
	else if(strcmp(str, "restore_new_cluster")==0)
		job_type = JOB_RESTORE_NEW_CLUSTER;
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
	else if(job_type == JOB_GET_INSTANCES)
	{
		ret = System::get_instance()->get_node_instance(root, str_ret);
	}
	else if(job_type == JOB_GET_META)
	{
		ret = System::get_instance()->get_meta(root, str_ret);
	}
	else if(job_type == JOB_GET_CLUSTER)
	{
		ret = System::get_instance()->get_cluster(root, str_ret);
	}
	else if(job_type == JOB_GET_STORAGE)
	{
		ret = System::get_instance()->get_storage(root, str_ret);
	}
	else if(job_type == JOB_GET_COMPUTER)
	{
		ret = System::get_instance()->get_computer(root, str_ret);
	}
	else if(job_type == JOB_GET_VARIABLE)
	{
		ret = System::get_instance()->get_variable(root, str_ret);
	}
	else if(job_type == JOB_SET_VARIABLE)
	{
		ret = System::get_instance()->set_variable(root, str_ret);
	}
	else if(job_type == JOB_CHECK_TIMESTAMP)
	{
		ret = check_timestamp(root, str_ret);
	}
	else
	{
		if(job_type == JOB_CREATE_CLUSTER
			|| job_type == JOB_DELETE_CLUSTER
			|| job_type == JOB_ADD_SHARDS
			|| job_type == JOB_DELETE_SHARD
			|| job_type == JOB_ADD_COMPS
			|| job_type == JOB_DELETE_COMP
			|| job_type == JOB_ADD_NODES
			|| job_type == JOB_DELETE_NODE
			|| job_type == JOB_BACKUP_CLUSTER
			|| job_type == JOB_RESTORE_CLUSTER
			|| job_type == JOB_RESTORE_NEW_CLUSTER)
		{
			std::string info;
			get_operation_status(info);
			if(info.length() > 0)
			{
				str_ret = "{\"result\":\"ongoing\",\"info\":\"" + info + "\"}";
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

	if(job_type == JOB_CREATE_MACHINE)
	{
		job_create_machine(root);
	}
	else if(job_type == JOB_UPDATE_MACHINE)
	{
		job_update_machine(root);
	}
	else if(job_type == JOB_DELETE_MACHINE)
	{
		job_delete_machine(root);
	}
	else if(job_type == JOB_CONTROL_INSTANCE)
	{
		job_control_instance(root);
	}
	else if(job_type == JOB_CREATE_CLUSTER)
	{
		job_create_cluster(root);
	}
	else if(job_type == JOB_DELETE_CLUSTER)
	{
		job_delete_cluster(root);
	}
	else if(job_type == JOB_ADD_SHARDS)
	{
		job_add_shards(root);
	}
	else if(job_type == JOB_DELETE_SHARD)
	{
		job_delete_shard(root);
	}
	else if(job_type == JOB_ADD_COMPS)
	{
		job_add_comps(root);
	}
	else if(job_type == JOB_DELETE_COMP)
	{
		job_delete_comp(root);
	}
	else if(job_type == JOB_ADD_NODES)
	{
		job_add_nodes(root);
	}
	else if(job_type == JOB_DELETE_NODE)
	{
		job_delete_node(root);
	}
	else if(job_type == JOB_BACKUP_CLUSTER)
	{
		job_backup_cluster(root);
	}
	else if(job_type == JOB_RESTORE_CLUSTER)
	{
		//delete job_restore_cluster(root);
	}
	else if(job_type == JOB_RESTORE_NEW_CLUSTER)
	{
		job_restore_new_cluster(root);
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

