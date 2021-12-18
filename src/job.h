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
#include "machine_info.h"

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
JOB_GET_STATUS,
JOB_CHECK_PORT,
JOB_GET_DISK_SIZE,
JOB_CREATE_MACHINE, 
JOB_UPDATE_MACHINE, 
JOB_DELETE_MACHINE, 
JOB_CREATE_CLUSTER, 
JOB_DELETE_CLUSTER, 
JOB_BACKUP_CLUSTER, 
JOB_RESTORE_CLUSTER, 
};
enum File_type {
FILE_NONE, 
};

class Job
{
public:

	std::queue<std::string> que_job;
	static int do_exit;
	std::vector<std::string> vec_local_ip;

private:
	static Job *m_inst;

	std::vector<pthread_t> vec_pthread;
	pthread_mutex_t thread_mtx;
	pthread_cond_t thread_cond;

	static const int kMaxStatus = 60;
	std::mutex mutex_stauts_;
	std::list<std::tuple<std::string, std::string, std::string>> list_jobid_result_info;
	std::string operation_info;

	std::mutex mutex_operation_;
	
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

	void notify_node_update(std::set<std::string> &alterant_node_ip, int type);
	bool get_disk_size(cJSON *root, std::string &str_ret);

	bool job_create_machine(cJSON *root, std::string &str_ret);
	bool job_update_machine(cJSON *root, std::string &str_ret);
	bool job_delete_machine(cJSON *root, std::string &str_ret);

	bool check_local_ip(std::string &ip);
	void get_local_ip();
	bool get_uuid(std::string &uuid);
	bool get_timestamp(std::string &timestamp);
	bool get_job_type(char *str, Job_type &job_type);
	bool get_file_type(char *str, File_type &file_type);
	
	bool update_jobid_status(std::string &jobid, std::string &result, std::string &info);
	bool get_jobid_status(std::string &jobid, std::string &result, std::string &info);
	bool update_operation_status(std::string &info);
	bool get_operation_status(std::string &info);
	bool job_insert_operation_record(cJSON *root, std::string &result, std::string &info);
	bool job_update_operation_record(std::string &job_id, std::string &result, std::string &info);
	bool job_system_cmd(std::string &cmd);
	bool job_save_json(std::string &path, char* cjson);

	bool job_create_program_path();
	bool job_create_shard(std::vector<Tpye_Ip_Port_Paths> &storages, std::string &cluster_name, std::string &ha_mode, int shards_id);
	bool job_create_comps(std::vector<Tpye_Ip_Port_Paths> &comps, std::string &cluster_name, int comps_id);
	bool job_start_cluster(std::vector <std::vector<Tpye_Ip_Port_Paths>> &vec_shard, std::string &cluster_name, std::string &ha_mode);
	void job_create_cluster(cJSON *root);
	bool job_delete_shard(std::vector<Tpye_Ip_Port> &storages, std::string &cluster_name);
	bool job_delete_comps(std::vector<Tpye_Ip_Port> &comps, std::string &cluster_name);
	void job_delete_cluster(cJSON *root);

	bool job_backup_shard(std::string &cluster_name, Tpye_Ip_Port &ip_port, int shard_id);
	void job_backup_cluster(cJSON *root);
	bool job_restore_shard(std::string &cluster_name, std::string &shard_name, std::string &timestamp, Tpye_Ip_Port &ip_port);
	void job_restore_cluster(cJSON *root);

	bool job_get_status(cJSON *root, std::string &str_ret);
	bool job_handle_ahead(const std::string &para, std::string &str_ret);
	void job_handle(std::string &job);
	void add_job(std::string &str);
	void job_work();
};

#endif // !JOB_H
