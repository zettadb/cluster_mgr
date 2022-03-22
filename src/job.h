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
JOB_GET_STATUS,
JOB_GET_INSTANCES,
JOB_GET_META,
JOB_GET_META_MODE,
JOB_GET_CLUSTER_SUMMARY,
JOB_GET_CLUSTER_DETAIL,
JOB_CHECK_TIMESTAMP,
JOB_GET_VARIABLE,
JOB_SET_VARIABLE,
JOB_RENAME_CLUSTER, 
JOB_MACHINE_SUMMARY, 
JOB_CREATE_MACHINE, 
JOB_UPDATE_MACHINE, 
JOB_DELETE_MACHINE, 
JOB_UPDATE_PROMETHEUS, 
JOB_POSTGRES_EXPORTER, 
JOB_MYSQLD_EXPORTER, 
JOB_CONTROL_INSTANCE, 
JOB_CREATE_CLUSTER, 
JOB_DELETE_CLUSTER, 
JOB_ADD_SHARDS, 
JOB_DELETE_SHARD, 
JOB_ADD_COMPS, 
JOB_DELETE_COMP, 
JOB_ADD_NODES,
JOB_DELETE_NODE,
JOB_BACKUP_CLUSTER, 
JOB_RESTORE_CLUSTER, 
JOB_RESTORE_NEW_CLUSTER, 
JOB_CREATE_BACKUP_STORAGE, 
JOB_UPDATE_BACKUP_STORAGE, 
JOB_DELETE_BACKUP_STORAGE, 
JOB_GET_BACKUP_STORAGE, 
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
	std::string user_name;
	std::string storage_id,backup_storage;

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
	bool check_timestamp(cJSON *root, std::string &str_ret);
	bool check_local_ip(std::string &ip);
	void get_local_ip();
	void get_user_name();
	bool get_uuid(std::string &uuid);
	bool get_timestamp(std::string &timestamp);
	bool get_datatime(std::string &datatime);
	
	bool update_operation_status(std::string &info);
	bool get_operation_status(std::string &info);
	bool job_get_cluster_info(std::string &cluster_name, Tpye_cluster_info &cluster_info);
	bool job_update_cluster_info(std::string &cluster_name, std::string &nick_name, char* cjson);
	bool job_insert_operation_record(cJSON *root, std::string &result, std::string &info);
	bool job_update_operation_record(std::string &job_id, std::string &result, std::string &info);
	bool job_insert_roll_back_record(std::string &job_id, char* cjson);
	bool job_delete_roll_back_record(std::string &job_id);
	bool job_roll_back_record(std::string &job_id);
	bool job_roll_back_check();

	bool job_system_cmd(std::string &cmd);
	bool job_save_file(std::string &path, char* buf);
	bool job_read_file(std::string &path, std::string &str);
	bool job_machine_summary(cJSON *root, std::string &str_ret);
	bool job_rename_cluster(cJSON *root, std::string &str_ret);
	bool job_create_backup_storage(cJSON *root, std::string &str_ret);
	bool job_update_backup_storage(cJSON *root, std::string &str_ret);
	bool job_delete_backup_storage(cJSON *root, std::string &str_ret);
	void job_create_machine(cJSON *root);
	void job_update_machine(cJSON *root);
	void job_delete_machine(cJSON *root);

	bool job_restart_node_exporter(std::vector<std::string> &vec_node);
	bool job_restart_postgres_exporter(std::string &ip, int port);
	bool job_restart_mysql_exporter(std::string &ip, int port);
	bool job_restart_prometheus();
	bool job_update_prometheus();
	void job_update_prometheus(cJSON *root);
	void job_postgres_exporter(cJSON *root);
	void job_mysqld_exporter(cJSON *root);
	bool job_control_instance(Tpye_Ip_Port &ip_port, std::string type, std::string control);
	void job_control_instance(cJSON *root);

	bool job_generate_cluster_name(std::string &cluster_name);
	bool job_create_program_path();
	bool job_create_meta_jsonfile();
	bool job_create_shards_jsonfile(std::vector <std::vector<Tpye_Ip_Port_Paths>> &vec_shard, std::vector<std::string> &vec_shard_name);
	bool job_create_storage(Tpye_Ip_Port_Paths &storage, cJSON *root, int install_id);
	bool job_create_nodes(std::vector<Tpye_Ip_Port_Paths> &storages, std::string &cluster_name, std::string &shard_name, std::string &job_id);
	bool job_create_shard(std::vector<Tpye_Ip_Port_Paths> &storages, std::string &cluster_name, std::string &shard_name, std::string &job_id, Tpye_string2 &t_string2);
	bool job_create_computer(Tpye_Ip_Port_Paths &computer, cJSON *root, int install_id);
	bool job_create_comps(std::vector<Tpye_Ip_Port_Paths> &comps, std::string &cluster_name, std::vector<std::string> &vec_comp_name, std::string &job_id, int comps_id);
	bool job_start_cluster(std::string &cluster_name, std::string &job_id, std::string &ha_mode);
	bool job_create_cluster(Tpye_cluster_info &cluster_info, std::string &cluster_name, std::string &job_id, std::set<std::string> &set_machine);
	void job_create_cluster(cJSON *root);
	bool job_delete_storage(Tpye_Ip_Port &storage);
	bool job_delete_computer(Tpye_Ip_Port &computer);
	void job_delete_cluster(cJSON *root);
	void job_delete_cluster(std::string &cluster_name);

	bool job_start_shards(std::string &cluster_name, std::vector<std::string> &vec_shard_name, std::string &job_id);
	void job_add_shards(cJSON *root);
	void job_delete_shard(cJSON *root);
	bool job_start_comps(std::string &cluster_name, std::vector<std::string> &vec_comp_name, std::string &job_id);
	void job_add_comps(cJSON *root);
	void job_delete_comp(cJSON *root);
	bool job_update_shard_nodes(std::string &cluster_name, std::string &shard_name);
	void job_add_nodes(cJSON *root);
	bool job_delete_shard_json(std::string &cluster_name, std::string &shard_name, Tpye_Ip_Port &ip_port);
	void job_delete_node(cJSON *root);

	bool job_backup_shard_node(std::string &cluster_name, std::string &cluster_id, Tpye_Shard_Id_Ip_Port_Id &shard_id_ip_port_id);
	bool job_backup_shard(std::string &cluster_name, std::string &shard_name, std::string &datatime);
	bool job_backup_cluster(std::string &cluster_name, std::string &datatime);
	void job_backup_cluster(cJSON *root);
	bool job_restore_storage(std::string &cluster_name, std::string &shard_name, std::string &timestamp, Tpye_Ip_Port &ip_port);
	bool job_restore_computer(std::string &cluster_name, std::string &shard_map, Tpye_Ip_Port &ip_port);
	void job_restore_cluster(cJSON *root);
	void job_restore_new_cluster(cJSON *root);

	bool update_jobid_status(std::string &jobid, std::string &result, std::string &info);
	bool get_jobid_status(std::string &jobid, std::string &result, std::string &info);
	bool job_get_status(cJSON *root, std::string &str_ret);
	bool get_job_type(char *str, Job_type &job_type);
	bool get_file_type(char *str, File_type &file_type);
	bool job_handle_ahead(const std::string &para, std::string &str_ret);
	void job_handle(std::string &job);
	void add_job(std::string &str);
	void job_work();
};

#endif // !JOB_H
