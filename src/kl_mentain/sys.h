/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef SYS_H
#define SYS_H
#include "sys_config.h"
#include "global.h"
#include "shard.h"
#include "kl_cluster.h"
#include "json/json.h"
#include <vector>
#include <map>
#include <mutex>
#include "thread_pool.h"
#include "async_mysql.h"
#include "util_func/kl_mutex.h"
#include "util_func/object_ptr.h"

using namespace kunlun;

/*
  Singleton class for global settings and functionality.
*/
class System {
private:
	ObjectPtr<MetadataShard> meta_shard;
	std::vector<ObjectPtr<KunlunCluster> > kl_clusters;
	
	//stop working for backup/restore cluster
	std::atomic<bool> cluster_mgr_working;

	std::string config_path;
	CThreadPool* tpool_;
	CAsyncMysqlManager* amysql_mgr_;

	System(const std::string&cfg_path);

	static System *m_global_instance;
	System(const System&);
	System&operator=(const System&);
public:
	//std::mutex mtx;
	KlWrapMutex mtx;
  	std::vector<ObjectPtr<KunlunCluster> > & get_kl_clusters(){
    	return kl_clusters;
  	}	

	void meta_shard_maintenance() {
		meta_shard->maintenance();
	}
	ObjectPtr<MetadataShard> get_MetadataShard() {
		return meta_shard;
	}

	CThreadPool* get_tpool() {
		return tpool_;
	}

	CAsyncMysqlManager* get_amysql_mgr() {
		return amysql_mgr_;
	}

	void set_cluster_mgr_working(bool stop) {
		cluster_mgr_working.store(stop);
	}
	bool get_cluster_mgr_working() const {
		return cluster_mgr_working.load();
	}

	int process_recovered_prepared();
	//bool acquire_shard(Thread *thd, bool force);
	int setup_metadata_shard();
	int setup_metadata_shard_mgr(ObjectPtr<Shard_node> sn, std::string& master_ip, int& master_port);
	int setup_metadata_shard_rbr(ObjectPtr<Shard_node> sn);
	int refresh_shards_from_metadata_server();
	int refresh_computers_from_metadata_server();
	int refresh_storages_info_to_computers();
	int refresh_storages_info_to_computers_metashard();
	int truncate_commit_log_from_metadata_server();
	void dispatch_shard_process_txns();
	void scan_cn_ddl_undoing_jobs();
	
	~System();
	static int create_instance(const std::string&cfg_path);
	static System* get_instance() {
		Assert(m_global_instance != NULL);
		return m_global_instance;
	}

	const std::string&get_config_path()const {
		//Scopped_mutex sm(mtx);
		return config_path;
	}

	int execute_metadate_opertation(enum_sql_command command, const std::string & str_sql);

	int get_max_cluster_id(int &cluster_id);
	int get_max_shard_name_id(std::string &cluster_name, int &shard_id);
	int get_max_comp_name_id(std::string &cluster_name, int &comp_id);
	//bool get_server_nodes_from_metadata(std::vector<Machine*> &vec_machines);



	bool get_backup_info_from_metadata(std::string &cluster_name, std::string &timestamp, std::vector<std::string> &vec_shard);
	bool get_cluster_info_from_metadata(std::string &cluster_name, std::string &json_buf);
	bool get_machine_info_from_metadata(std::vector<std::string> &vec_machine);
	bool get_server_nodes_stats_id(std::string &hostaddr, std::string &id);
	bool check_backup_storage_name(std::string &name);
	bool check_machine_hostaddr(std::string &hostaddr);
	bool check_cluster_name(std::string &cluster_name);
	bool check_nick_name(std::string &nick_name);
	bool rename_cluster(std::string &cluster_name, std::string &nick_name);

	bool check_cluster_shard_ip_port(std::string &cluster_name, std::string &shard_name, Tpye_Ip_Port &ip_port);

	bool get_cluster_shard_name(std::string &cluster_name, std::vector<std::string> &vec_shard_name);

	bool update_instance_status(Tpye_Ip_Port &ip_port, std::string &status, int &type);
	bool get_backup_storage_string(std::string &name, std::string &backup_storage_id, std::string &backup_storage_str);
	bool get_shards_ip_port(std::string &cluster_name, std::vector <std::vector<Tpye_Ip_Port>> &vec_vec_shard);
	bool get_shards_ip_port(std::string &cluster_name, std::string &shard_name, std::vector<Tpye_Ip_Port> &vec_shard);
	bool get_comps_ip_port(std::string &cluster_name, std::vector<Tpye_Ip_Port> &vec_comp);
	bool get_comps_ip_port(std::string &cluster_name, std::string &comp_name, std::vector<Tpye_Ip_Port> &vec_comp);
	bool get_meta_ip_port(std::vector<Tpye_Ip_Port> &vec_meta);
	
	bool stop_cluster(std::string &cluster_name);

	
	bool get_shard_info_for_backup(std::string &cluster_name, std::string &cluster_id, std::vector<Tpye_Shard_Id_Ip_Port_Id> &vec_shard_id_ip_port_id);
	bool get_node_info_for_backup(std::string &cluster_name, std::string &shard_name, std::string &cluster_id, Tpye_Shard_Id_Ip_Port_Id &shard_id_ip_port_id);
	bool get_shard_ip_port_restore(std::string &cluster_name, std::vector<std::vector<Tpye_Ip_Port>> &vec_vec_ip_port);
	bool get_comps_ip_port_restore(std::string &cluster_name, std::vector<Tpye_Ip_Port> &vec_ip_port);
	bool get_shard_map_for_restore(std::string &backup_cluster_name, std::string &restore_cluster_name, std::string &shard_map);
	bool get_cluster_shards_nodes_comps(std::string &cluster_name, int &shards, int &nodes, int &comps);
	bool get_cluster_shard_variable(std::string &cluster_name, std::string &shard_name, std::string &variable, std::string &value);
	bool get_cluster_no_rep_mode(std::string &cluster_name);
	bool clear_cluster_shard_master(std::string &cluster_name);
	bool update_instance_cluster_info(std::string &cluster_name);
	bool update_shard_group_seeds(std::string &cluster_name, std::string &shard_name, std::string &group_seeds);
	
	bool get_cluster_info(std::string &cluster_name, Json::Value &json);
	bool get_cluster_shard_info(std::string &cluster_name, std::vector<std::string> &vec_shard_name, Json::Value &json);
	bool get_cluster_comp_info(std::string &cluster_name, std::vector<std::string> &vec_comp_name, Json::Value &json);
	bool get_cluster_shard_node_info(std::string &cluster_name, std::vector<std::string> &vec_shard_name, 
			std::vector <std::vector<Tpye_Ip_Port_Paths>> &vec_shard_storage_ip_port_paths, Json::Value &json);
	bool get_meta_mode(Json::Value &attachment);
	bool get_meta_summary(Json::Value &attachment);
	bool get_backup_storage(Json::Value &attachment);
	bool get_cluster_summary(Json::Value &attachment);
	bool get_cluster_detail(Json::Value &paras, Json::Value &attachment);
	bool get_variable(Json::Value &paras, Json::Value &attachment);
	bool set_variable(Json::Value &paras, Json::Value &attachment);

	 /*
    * get all storage shards in current cluster, include meta
    */
    std::vector<ObjectPtr<Shard> > get_storage_shards();
	std::vector<ObjectPtr<Shard> > get_shard_by_cluster_id(uint cluster_id);
	bool update_cluster_delete_state(uint cluster_id);
	void add_shard_cluster_memory(int cluster_id, const std::string& cluster_name, 
			const std::string& nick_name, const std::string& ha_mode,
			const std::vector<Shard_Type>& shard_params);
	void add_computer_cluster_memory(int cluster_id, const std::string& cluster_name, 
			const std::string& nick_name, const std::vector<Comps_Type>& comps_params);
	void del_shard_cluster_memory(int cluster_id, const std::vector<int>& shard_id);
	void del_computer_cluster_memory(int cluster_id, const std::vector<std::string>& comps_hosts);
	void add_node_shard_memory(int cluster_id, int shard_id, const std::vector<CNode_Type>& node_params);
	void del_node_shard_memory(int cluster_id, int shard_id, const std::string& host);
	void update_shard_master(ObjectPtr<Shard> shard, const std::string& host);
	ObjectPtr<Shard> get_shard_by_host(const std::string& host);
	ObjectPtr<Shard_node> get_node_by_host(const std::string& host);
	/*
	* [out] 0 -- master host
	*		1 -- not master host
	*		2 -- other state
	*/
	int chk_host_is_master_node(const std::string& host);
	CAsyncMysql* get_amysql_by_host(const std::string& host);
	ObjectPtr<Shard> get_shard_by_node(ObjectPtr<Shard_node> node);
};	
#endif // !SYS_H
