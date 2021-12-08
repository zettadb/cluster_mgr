/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef JOB_H
#define JOB_H
#include "sys_config.h"
#include <errno.h>
#include "global.h"
#include "cjson.h"
#include "node_info.h"

#include <pthread.h>
#include <mutex>
#include <map>
#include <set>
#include <list>
#include <queue>
#include <string>
#include <vector>
#include <algorithm>

enum Job_type {
JOB_NONE, 
JOB_GET_NODE,
JOB_CHECK_PORT,
JOB_GET_DISK_SIZE,
JOB_CREATE_CLUSTER, 
};
enum File_type {
FILE_NONE, 
};

class Job
{
public:

	std::queue<std::string> que_job;
	static int do_exit;
	
private:
	static Job *m_inst;

	std::vector<pthread_t> vec_pthread;
	pthread_mutex_t thread_mtx;
	pthread_cond_t thread_cond;

	static const int kMaxStatus = 60;
	std::mutex mutex_stauts_;
	std::list<std::pair<std::string, std::string>> list_jobid_status;

	std::mutex mutex_install_;
	
public:
	Job();
	~Job();
	static Job *get_instance()
	{
		if (!m_inst) m_inst = new Job();
		return m_inst;
	}
	int start_job_thread();
	void join_all();
	
	bool http_para_cmd(const std::string &para, std::string &str_ret);
	bool get_meta_info(std::vector<Tpye_Ip_Port_User_Pwd> &meta);
	bool get_node_instance(cJSON *root, std::string &str_ret);
	void notify_node_update(std::set<std::string> &alterant_node_ip, int type);
	bool check_port_used(cJSON *root, std::string &str_ret);
	bool get_disk_size(cJSON *root, std::string &str_ret);

	bool get_uuid(std::string &uuid);
	bool get_job_type(char     *str, Job_type &job_type);
	bool get_file_type(char     *str, File_type &file_type);
	
	bool update_jobid_status(std::string &jobid, std::string &status);
	bool get_jobid_status(std::string &jobid, std::string &status);

	bool job_create_shard(std::vector<Tpye_Ip_Port_Paths> &shard);
	bool job_create_comps(std::vector<Tpye_Ip_Port_Paths> &comps);
	bool job_start_cluster(std::vector <std::vector<Tpye_Ip_Port_Paths>> &vec_shard,
							std::vector<Tpye_Ip_Port_Paths> &comps, std::string &cluster_name);
	void job_create_cluster(cJSON *root);

	void job_handle(std::string &job);
	void add_job(std::string &str);
	void job_work();
};

#endif // !JOB_H
