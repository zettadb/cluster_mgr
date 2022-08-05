#ifndef KL_CLUSTER_H
#define KL_CLUSTER_H

#include "sys_config.h"
#include "global.h"
#include "shard.h"
#include "zettalib/op_pg.h"
//#include "log.h"

#include <atomic>
#include <set>
#include <string>
#include <map>
#include <vector>
#include <tuple>
#include "util_func/meta_info.h"
#include "util_func/object_ptr.h"
#include "util_func/kl_mutex.h"
//#include "pgsql/libpq-fe.h"
using namespace kunlun;

class Computer_node : public ObjectRef {
public:
	int id;
	int cluster_id;
	std::string name;
	std::string ip;
	int port;
	std::string user;
	std::string pwd;
	std::string db;
	PGConnection *gpsql_conn;
	KlWrapMutex sql_mtx_;
  CounterTriggerTimePeriod coldbackup_timer;


	Computer_node(int id_, int cluster_id_, int port_,
		const char * name_, const char * ip_, const char * user_, const char * pwd_):
		id(id_), cluster_id(cluster_id_), name(name_), ip(ip_), port(port_),
		user(user_), pwd(pwd_) ,coldbackup_timer(1,"01:00:00-02:00:00")//,
	{
		gpsql_conn = nullptr;
		Assert(name_ && ip_ && user_ && pwd_);
		Assert(port_ > 0);
    coldbackup_timer.InitOrRefresh("01:00:00-02:00:00");
	}

	void get_ip_port(std::string&ip, int&port) const {
		ip = this->ip;
		port = this->port;
	}
	void get_user_pwd(std::string&user, std::string&pwd) const {
		user = this->user;
		pwd = this->pwd;
	}

	int get_id() const
	{
		return id;
	}

	const std::string &get_name() const
	{
		return name;
	}

	bool matches_ip_port(const std::string &ip, int port) const
	{
		return this->ip == ip && this->port == port;
	}

	bool refresh_node_configs(int port_,
		const char * name_, const char * ip_, const char * user_, const char * pwd_,std::string time_str = "01:00:00-02:00:00")
	{
		bool is_change = false;

		if(name != name_)
		{
			name = name_;
		}

		if(port != port_)
		{
			port = port_;
			is_change = true;
		}
			
		if(ip != ip_)
		{
			ip = ip_;
			is_change = true;
		}
			
		if(user != user_)
		{
			user = user_;
			is_change = true;
		}
			
		if(pwd != pwd_)
		{
			pwd = pwd_;
			is_change = true;
		}

		// close connect, it will be reconnect while next send_stmt
		if(is_change) {
			close_conn();
		}
    bool ret = coldbackup_timer.InitOrRefresh(time_str);
    if(!ret){
      KLOG_ERROR("Init or refresh compute coldback time faild: {}",coldbackup_timer.getErr());
    }
		return is_change;
	}

	bool pg_connect(const std::string& database);
	int send_stmt(const char *database, const char *stmt, PgResult *result, int nretries = 1);

	void close_conn() {
		if(gpsql_conn) {
			gpsql_conn->Close(); 
			delete gpsql_conn;
			gpsql_conn = nullptr;
		}
	}

	bool connect_status() {
		if(!gpsql_conn){
			return false;
		}
		return gpsql_conn->CheckIsConnected(); 
  	}
	//PGresult *get_result() { return gpsql_conn.result; }
	//void free_pgsql_result() { gpsql_conn.free_pgsql_result(); }
	bool get_variables(std::string &variable, std::string &value);
	bool set_variables(std::string &variable, std::string &value_int, std::string &value_str);
};

class KunlunCluster : public ObjectRef {
private:
	KlWrapMutex mtx_;
	int id;
	std::string name;
	std::string nick_name;
	std::atomic<bool> is_delete_;

public:
	std::vector<ObjectPtr<Computer_node> > computer_nodes;
	std::vector<ObjectPtr<Shard> > storage_shards;

public:
	KunlunCluster(int id_, const std::string &name_);
	~KunlunCluster();

	int get_id() 
	{
		return id;
	}

	std::string &get_name() 
	{
		return name;
	}

	std::string &get_nick_name() 
	{
		KlWrapGuard<KlWrapMutex> guard(mtx_);
		return nick_name;
	}

	void set_nick_name(const std::string &nick_name_)
	{
		KlWrapGuard<KlWrapMutex> guard(mtx_);
		nick_name = nick_name_;
	}
	void set_cluster_delete_state(bool is_delete) {
		is_delete_.store(is_delete);
	}
	bool get_cluster_delete_state() const {
		return is_delete_.load();
	}

	int refresh_storages_to_computers();
	int refresh_storages_to_computers_metashard(ObjectPtr<MetadataShard> meta_shard);
	int truncate_commit_log_from_metadata_server(std::vector<ObjectPtr<KunlunCluster> > &kl_clusters, 
					ObjectPtr<MetadataShard> meta_shard);
  	ObjectPtr<Computer_node> get_coldbackup_comp_node();
	void deal_cn_ddl_undoing_jobs(ObjectPtr<MetadataShard> meta_shard);
	bool lock_ddl_locks(ObjectPtr<Shard_node> master);
	void unlock_ddl_locks(ObjectPtr<Shard_node> master);
	int get_computer_database_namespace(ObjectPtr<Computer_node> computer,
			std::map<std::string, std::vector<std::string> >& db_map_tables);
	int get_storage_database_namespace(ObjectPtr<Shard> shard, 
					std::map<std::string, std::vector<std::string> >& db_map_tables);
	int get_node_mgr_database_namespace(ObjectPtr<Shard> shard, 
					std::map<std::string, std::vector<std::string> >& db_map_tables);
};

#endif // !KL_CLUSTER_H

