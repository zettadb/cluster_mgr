/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef SHARD_H
#define SHARD_H
#include "sys_config.h"
#include "global.h"

#include <atomic>
#include <set>
#include <string>
#include <map>
#include <vector>
#include "zettalib/op_mysql.h"
#include "zettalib/op_log.h"
#include "async_mysql.h"
#include "mysql/server/private/sql_cmd.h"
#include "util_func/meta_info.h"
#include "util_func/kl_mutex.h"
#include "util_func/object_ptr.h"

using namespace kunlun;

extern int64_t mysql_connect_timeout;
extern int64_t mysql_read_timeout;
extern int64_t mysql_write_timeout;
extern int64_t mysql_max_packet_size;
extern int64_t prepared_transaction_ttl;
extern int64_t check_shard_interval;
extern int64_t meta_svr_port;
extern int64_t stmt_retries;
extern int64_t stmt_retry_interval_ms;
extern int64_t commit_log_retention_hours;

extern std::string meta_svr_ip;
extern std::string meta_group_seeds;
extern std::string meta_svr_user;
extern std::string meta_svr_pwd;
extern std::vector<Tpye_Ip_Port> vec_meta_ip_port;

//class Thread;
class Shard;
class Shard_node;
class Computer_node;
class KunlunCluster;

typedef enum Rbr_sync_status {
	FSYNC = 1,
	ASYNC,
	UNSYNC,
} Rbr_SyncStatus;

class Shard_node : public ObjectRef {
public:
	// keep consistent with mysql
	enum Group_member_status {
		MEMBER_ONLINE = 1, 
		MEMBER_OFFLINE,
		MEMBER_IN_RECOVERY,
		MEMBER_ERROR,
		MEMBER_UNREACHABLE,
		MEMBER_END  // the end of the enum
	};
	// keep consistent with above enum Group_member_status.
	static const char *Group_member_status_strs[6];
private:
	bool _is_master;
	Group_member_status mgr_status;

	uint id;
	uint64_t latest_mgr_pos;
	ObjectPtr<Shard> owner;
	MysqlConnection* mysql_conn;
	CAsyncMysql* amysql_conn;

	int port;
	std::string ip, user, pwd;
	//rbr configuration
	int master_priority;
	// true -- normal
	// false -- abnormal
	std::atomic<bool> aconn_state_;
	// 0 -- normal
	// 1 -- rebuild host
	// ...
	std::atomic<int> work_state_;
	std::string server_uuid_;
	std::atomic<int> leader_term_;
    std::atomic<uint64_t> slave_io_ts_;
    std::atomic<int> sql_delay_;
    std::atomic<bool> io_state_;	
	Rbr_SyncStatus rbr_sync;
	KlWrapMutex sql_mux;
	
public:
	void get_ip_port(std::string&ip, int&port) const {
		ip = this->ip;
		port = this->port;
	}
	void get_user_pwd(std::string&user, std::string&pwd) const {
		user = this->user;
		pwd = this->pwd;
	}

	void set_master(bool b) { _is_master = b; }
	bool is_master() const { return _is_master;}
	
	ObjectPtr<Shard> get_owner() { return owner; }
	int get_master_priority() const { return master_priority; }
	void set_master_priority(int priority) { master_priority = priority; } 
	void set_rbr_sync_status(Rbr_SyncStatus sync) { rbr_sync = sync; }
	Rbr_SyncStatus get_rbr_sync_status() { return rbr_sync; }
	MysqlConnection* get_mysql_conn() { return mysql_conn; }
	CAsyncMysql* get_async_mysql() { return amysql_conn; }
	//void add_ignore_error(int ec) { mysql_conn.ignore_errs.insert(ec); }
	//void clear_ignore_errors() { mysql_conn.ignore_errs.clear(); }
	char* get_mysql_err() { return mysql_conn->getErr(); }
	int get_node_work_state() {
		return work_state_.load();
	}
	void set_node_work_state(int work_state) {
		work_state_.store(work_state);
	}
	bool get_node_aconn_state() {
		return aconn_state_.load();
	}
	void set_node_aconn_state(bool aconn_state) {
		aconn_state_.store(aconn_state);
	}
	int get_leader_term() {
		return leader_term_.load();
	}
	void set_leader_term(int leader_term) {
		leader_term_.store(leader_term);
	}
	uint64_t get_slave_io_ts() {
		return slave_io_ts_.load();
	}
	void set_slave_io_ts(uint64_t slave_io_ts) {
		slave_io_ts_.store(slave_io_ts);
	}
	int get_sql_delay() {
		return sql_delay_.load();
	}
	void set_sql_delay(int sql_delay) {
		sql_delay_.store(sql_delay);
	}
	bool get_io_state() {
		return io_state_.load();
	}
	void set_io_state(bool io_state) {
		io_state_.store(io_state);
	}

	int get_id() const { return id; }
	void set_id(int id) { this->id = id; }

	Shard_node(int id_, ObjectPtr<Shard> owner_, const char * ip_, int port_,
			const char * user_, const char * pwd_, int priority):
			_is_master(false), id(id_), latest_mgr_pos(0), owner(owner_), amysql_conn(nullptr),
			ip(ip_), port(port_), user(user_), pwd(pwd_), master_priority(priority), aconn_state_(false),
			work_state_(0), leader_term_(0), slave_io_ts_(0), sql_delay_(2147483647), io_state_(false) {
		connect();
		Assert(ip_ && user_ && pwd_);
		Assert(port_ > 0);
	}

	bool update_conn_params(const char * ip_, int port_, const char * user_,
		const char * pwd_);
	int check_mgr_state();
	bool send_stmt(const char *stmt, size_t len, MysqlResult* result, int nretries = 1);
	bool send_stmt(const std::string& stmt, MysqlResult* result, int nretries = 1);
	void reload_server_uuid();
	int connect();

	bool matches_ip_port(const std::string &ip, int port) const {
		return this->ip == ip && this->port == port;
	}
	bool match_host(const std::string& host) {
		return (get_host() == host);
	}
	
	const std::string& get_server_uuid() const {
		return server_uuid_;
	}
	void set_server_uuid(const std::string& server_uuid) {
		server_uuid_ = server_uuid;
	}
	bool update_variables(Tpye_string2 &t_string2);
	bool get_variables(std::string &variable, std::string &value);
	bool set_variables(std::string &variable, std::string &value_int, std::string &value_str);
	bool update_instance_cluster_info();
	int start_mgr(Group_member_status st, bool as_master);
	
	std::string get_host() const {
		return (ip +"_"+ std::to_string(port));
	}
	
	bool fetch_mgr_progress();
	int get_mgr_master_ip_port(std::string&ip, int&port);
	
	uint64_t get_latest_mgr_pos() const { return latest_mgr_pos; }
	void close_conn() { mysql_conn->Close(); } //{ mysql_conn.close_conn(); }
	bool connect_status() { return mysql_conn->IsConnected(); }
};

class Txn_ts {
public:
	Txn_ts(time_t st, time_t nt) : st_(st), nt_(nt) {}
	Txn_ts(time_t st) : st_(st), nt_(0) {}
	Txn_ts() : st_(0), nt_(0) {}

	void set_st(time_t st) {
		st_ = st;
	}
	void set_nt(time_t nt) {
		nt_ = nt;
	}
	bool const operator==(const Txn_ts& ts) const {
		return (this->st_ == ts.st_ && this->nt_ == ts.nt_);
	}
	bool const operator!=(const Txn_ts& ts) const {
		if(this->st_ == ts.st_ && this->nt_ == ts.nt_)
			return false;
		else
			return true;
	}
	bool const operator>(const Txn_ts& ts) const {
		if(this->st_ < ts.st_)
			return false;
		else if(this->st_ == ts.st_) {
			return (this->nt_ > ts.nt_);
		}
		return true;
	}

	bool const operator<(const Txn_ts& ts) const {
		if(this->st_ > ts.st_)
			return false;
		else if(this->st_ == ts.st_) {
			return (this->nt_ < ts.nt_);
		} 
		return true;
	}
	Txn_ts& operator=(const Txn_ts& ts) {
		this->st_ = ts.st_;
		this->nt_ = ts.nt_;
		return *this;
	}

	Txn_ts(const Txn_ts& ts) {
		this->st_ = ts.st_;
		this->nt_ = ts.nt_;
	}

	time_t st_;
	time_t nt_;
};

/*
  The logic in this class and its child classes assumes that an
  object(Shard, Shard_node)'s ID
  uniquely identifies the object permanently, it never changes after the
  object is registered into the metadata shard's tables.
*/
class Shard : public ObjectRef {
public:
	enum Shard_type {NONE, STORAGE, METADATA};
	enum HAVL_mode {HA_no_rep, HA_mgr, HA_rbr};
  kunlun::CounterTriggerTimePeriod coldback_time_trigger_;
protected:
	//std::atomic<bool> thrd_hdlr_assigned;
	ObjectPtr<Shard_node> cur_master;
	Shard_type shard_type;
	HAVL_mode ha_mode;
	int id;
	int cluster_id; // cluster identifier
	/*
	  Starting a node as master may not succeed in one shot, and we must start
	  exactly the same one if we have to do it again after a failure attempt
	  otherwise we could cause a brainsplit.
	*/
	uint pending_master_node_id;
	time_t last_time_check;
	std::string name;
	std::string cluster_name;
	friend class System;
	std::vector<ObjectPtr<Shard_node> > nodes;
	uint innodb_page_size;
	//for rbr
	std::atomic<int> fullsync_level_;
	std::atomic<int> running_fullsync_level_;
	std::atomic<bool> enable_fullsync_;
	bool degrade_conf_state;
	int degrade_conf_time;
	bool degrade_run_state;
	std::atomic<bool> is_delete;
  	std::string backup_node_ip_port;
	
public:
	/*
	  @retval true if master changed; false otherwise.
	*/
	bool set_master(ObjectPtr<Shard_node> node) {
		if(cur_master.Invalid()) {
			if (cur_master->get_id() == node->get_id())
				return false;

			cur_master->set_master(false);
		}
		
		if (node.Invalid())
			node->set_master(true);
		cur_master = node;
		return true;
	}

	ObjectPtr<Shard_node> get_node_by_id(int id);
protected:
	/*
	  A shard's all nodes are always handled in the same thread.
	*/
	friend class KunlunCluster;
	KlWrapMutex mtx;
	KlWrapMutex mtx_txninfo;

public:
	struct Txn_key {
		Txn_key() { memset(this, 0, sizeof(*this)); }
		//time_t start_ts;
		Txn_ts start_ts;
		uint32_t local_txnid;
		uint32_t comp_nodeid;
		bool operator==(const Txn_key&tk) const {
			return (start_ts == tk.start_ts &&
					local_txnid == tk.local_txnid &&
					comp_nodeid == tk.comp_nodeid);
		}
		bool operator<(const Txn_key&tk) const {
			if (start_ts != tk.start_ts)
				return start_ts < tk.start_ts;
			if (local_txnid != tk.local_txnid)
				return local_txnid < tk.local_txnid;
			return comp_nodeid < tk.comp_nodeid;
		}
		bool operator>(const Txn_key&tk) const {
			if (start_ts != tk.start_ts)
				return start_ts > tk.start_ts;
			if (local_txnid != tk.local_txnid)
				return local_txnid > tk.local_txnid;
			return comp_nodeid > tk.comp_nodeid;
		}
	};

	enum Txn_decision_enum {TXN_DECISION_NONE, COMMIT, ABORT};
	struct Txn_decision {
		Txn_decision() : decision(TXN_DECISION_NONE), prepare_ts(0) {}
		Txn_decision(const Txn_key &tk_, Txn_decision_enum d, Txn_ts p) :
			tk(tk_), decision(d), prepare_ts(p){}
		Txn_key tk;
		Txn_decision_enum decision;
		//time_t prepare_ts;
		Txn_ts prepare_ts;
	};

	typedef std::vector<Txn_decision> Txn_end_decisions_t;
	typedef std::vector<Txn_key> Prep_recvrd_txns_t;

protected:
	//access to the 2 members must be sync'ed by mtx_txninfo
	Txn_end_decisions_t txn_end_decisions;
	Prep_recvrd_txns_t prep_recvrd_txns;

public:
	Shard(int id_, const std::string &name_, Shard_type type, HAVL_mode mode) :
    coldback_time_trigger_(1,"01:00:00-02:00:00"),
			cur_master(nullptr), shard_type(type), ha_mode(mode),
			id(id_), cluster_id(0), pending_master_node_id(0), last_time_check(0),
			name(name_), innodb_page_size(0), fullsync_level_(1), running_fullsync_level_(0), 
			enable_fullsync_(true), is_delete(false) {
    	coldback_time_trigger_.InitOrRefresh("01:00:00-02:00:00");
	}

	~Shard() {
		//for (auto &i:nodes)
		//	delete i;
	}

	// do db ops without mutex held
	void take_prep_recvrd_txns(Prep_recvrd_txns_t &prt) {
		KlWrapGuard<KlWrapMutex> guard(mtx_txninfo);
		prt = prep_recvrd_txns;
		prep_recvrd_txns.clear();
	}

	// do db ops without mutex held
	void take_txn_end_decisions(Txn_end_decisions_t&ted) {
		KlWrapGuard<KlWrapMutex> guard(mtx_txninfo);
		ted = txn_end_decisions;
		txn_end_decisions.clear();
	}

	void set_txn_end_decisions(Txn_end_decisions_t&ted) {
		KlWrapGuard<KlWrapMutex> guard(mtx_txninfo);
		txn_end_decisions.insert(txn_end_decisions.end(), ted.begin(), ted.end());
	}

	/*Thread *get_thread_handler() {
		Scopped_mutex sm(mtx);
		return m_thrd_hdlr;
	}*/

	// called by worker threads to maintain shard working state.
	void maintenance();

	/*
	  Set h to be the thread handler, or remove current thread handler(h is 0).
	  if force is true, ignore shard's last_time_check and always assign it
	  to the thread.
	  @retval true if set OK; false if not set.
	*/
	//bool set_thread_handler(Thread *h, bool force = false);

	time_t get_last_time_check() {
		KlWrapGuard<KlWrapMutex> guard(mtx);
		return last_time_check;
	}

	std::vector<ObjectPtr<Shard_node> > &get_nodes() {
		KlWrapGuard<KlWrapMutex> guard(mtx);
		return nodes;
	}

	int get_cluster_id() {
		KlWrapGuard<KlWrapMutex> guard(mtx);
		return cluster_id;
	}

	int get_fullsync_level()  {
		return fullsync_level_.load();
	}
	void set_fullsync_level(int fullsync) {
		fullsync_level_.store(fullsync);
	}
	int get_running_fullsync_level() {
		return running_fullsync_level_.load();
	}
	void set_running_fullsync_level(int running_level) {
		running_fullsync_level_.store(running_level);
	}
	bool get_enable_fullsync() {
		return enable_fullsync_.load();
	}
	void set_enable_fullsync(bool enable_fullsync) {
		enable_fullsync_.store(enable_fullsync);
	}

	bool get_degrade_conf_state()  {
		KlWrapGuard<KlWrapMutex> guard(mtx);
		return degrade_conf_state;
	}
	void set_degrade_conf_state(bool conf_state) {
		KlWrapGuard<KlWrapMutex> guard(mtx);
		degrade_conf_state = conf_state;
	}
	bool get_degrade_run_state()  {
		KlWrapGuard<KlWrapMutex> guard(mtx);
		return degrade_run_state;
	}
	void set_degrade_run_state(bool run_state) {
		KlWrapGuard<KlWrapMutex> guard(mtx);
		degrade_run_state = run_state;
	}
	void set_is_delete(bool is_delete) {
		this->is_delete.store(is_delete);
	}
	bool get_is_delete() const {
		return is_delete.load();
	}
	int get_degrade_conf_time()  {
		KlWrapGuard<KlWrapMutex> guard(mtx);
		return degrade_conf_time;
	}
	void set_degrade_conf_time(int conf_time) {
		KlWrapGuard<KlWrapMutex> guard(mtx);
		degrade_conf_time = conf_time;
	}

  	std::string get_bacupnode_ip_port()  {
		KlWrapGuard<KlWrapMutex> guard(mtx);
		return backup_node_ip_port;
	}
	void set_backupnode_ip_port(std::string ip,std::string port) {
		KlWrapGuard<KlWrapMutex> guard(mtx);
		backup_node_ip_port = ip +"_" + port;
	}

	const std::string &get_cluster_name() const  {
		return cluster_name;
	}

	void update_cluster_name(const std::string &name, uint cid) {
		KlWrapGuard<KlWrapMutex> guard(mtx);
		Assert(cluster_id == cid);
		cluster_name = name;
	}

	void set_cluster_info(const std::string &name, int cid) {
		KlWrapGuard<KlWrapMutex> guard(mtx);
		cluster_name = name;
		cluster_id = cid;
	}

	bool contains_node(const std::string&ip, int port) {
		KlWrapGuard<KlWrapMutex> guard(mtx);
		for (auto &i:nodes)
			if (i->matches_ip_port(ip, port))
				return true;
		return false;
	}

	bool contains_node(int nodeid) {
		KlWrapGuard<KlWrapMutex> guard(mtx);
		for (auto &i:nodes)
			if (i->get_id() == nodeid)
				return true;
		return false;
	}

	Shard_type get_type()  {
		KlWrapGuard<KlWrapMutex> guard(mtx);
		return shard_type;
	}

	HAVL_mode get_mode()  {
		return ha_mode;
	}

	void set_mode(HAVL_mode mode) {
		ha_mode = mode;
	}

	int get_id() const {
		return id;
	}

	const std::string &get_name() const {
		return name;
	}

	void add_node(ObjectPtr<Shard_node> node) {
		nodes.emplace_back(node);
		std::string ip;
		int port;
		node->get_ip_port(ip, port);
		KLOG_INFO("Added shard({}.{}, {}) node ({}:{}, {}) into protection.",
			cluster_name, name,
			id, ip, port, node->get_id());
	}

	void remove_nodes_not_in(const std::set<int>& ids) {
		KlWrapGuard<KlWrapMutex> guard(mtx);
		for (auto i = nodes.begin(); i != nodes.end(); ++i) {
			if (ids.find((*i)->get_id()) == ids.end()) {
				if ((*i)->get_id() == cur_master->get_id()) 
					cur_master.SetTRaw(nullptr);
				//delete *i;
				auto j = i;
				nodes.erase(j);
			}
		}
	}

	void remove_node(int id) {
		KlWrapGuard<KlWrapMutex> guard(mtx);
		for (auto i = nodes.begin(); i != nodes.end(); ++i) {
			if ((*i)->get_id() == id) {
				if ((*i)->get_id() == cur_master->get_id()) 
					cur_master.SetTRaw(nullptr);
				//delete *i;
				//*i =  nullptr;
				nodes.erase(i);
				break;
			}
		}
	}

	/*
	  Here we assume a node's ID is never changed, its ip/port/user/password can be modified.
	  If a node is not found by its ID, we create a new one.
	  @param [out] changed : set to true if the node existed and has been updated by this call.
	*/
	ObjectPtr<Shard_node> refresh_node_configs(int id, const char * ip_, int port_,
		const char * user_, const char * pwd_, uint master_priority, bool&changed) {
		ObjectPtr<Shard_node> n = get_node_by_id(id);

		changed = false;

		if (n.Invalid()) {
			n->set_master_priority(master_priority);
			changed = n->update_conn_params(ip_, port_, user_, pwd_);
		} else {
			ObjectPtr<Shard_node> sn(new Shard_node(id, this, ip_, port_, user_, pwd_, master_priority));
			n = sn;
			add_node(n);
		}
		return n;
	}

	Shard_type get_shard_type() {
		KlWrapGuard<KlWrapMutex> guard(mtx);
		return shard_type;
	}

	ObjectPtr<Shard_node> get_master() { 
		KlWrapGuard<KlWrapMutex> guard(mtx);
		return cur_master; 
	}

	ObjectPtr<Shard_node> get_node_by_ip_port(const std::string &ip, int port) {
		std::string ip1;
		int port1 = 0;
		ObjectPtr<Shard_node> sn;

		for (auto&n:nodes) {
			if(!n.Invalid())
				continue;
			n->get_ip_port(ip1, port1);
			if (ip == ip1 && port == port1) {
				sn = n;
				break;
			}
		}
		return sn;
	}

	int check_mgr_cluster();
	int end_recovered_prepared_txns();
	int get_xa_prepared();
	bool WriteCommitLog(const Txn_decision& td);
	uint get_innodb_page_size();
	std::string get_clusterid(const std::string& cluster_name);
	std::string get_shardid(const std::string& cluster_id, const std::string& shard_name);
	std::vector<std::string> get_compid(const std::string& cluster_id);
};

typedef std::tuple<int, std::string, int, int> CNode_Type;
typedef std::tuple<int, std::string, std::string, std::string, int, std::vector<CNode_Type> > Shard_Type;
typedef std::tuple<std::string, int, std::string, std::string> Comps_Type;

class MetadataShard : public Shard {
public:
	struct txn_info {
		txn_info() : processed(false) {}
		bool processed;
		std::vector<ObjectPtr<Shard> > branches;
	};
	
	struct cluster_txninfo {
		cluster_txninfo(int id, const std::string & name) :
			min_start_ts(0xffffffff), max_start_ts(0), cid(id), cname(name)
		{}
	
		//time_t min_start_ts, max_start_ts;
		Txn_ts min_start_ts, max_start_ts;
		int cid; // cluster id
		std::string cname; // cluster name
		std::map<Shard::Txn_key, txn_info> tkis;
	};

	// Keep this same as in computing node impl(METADATA_SHARDID).
	const static uint32_t METADATA_SHARD_ID = 0xFFFFFFFF;

	MetadataShard() : Shard(METADATA_SHARD_ID, "MetadataShard", METADATA, HA_no_rep) {
		// Need to assign the pair for consistent generic processing.
		cluster_id = 0xffffffff;
		cluster_name = "MetadataShardVirtualCluster";
	}

	int compute_txn_decisions(std::map<uint, cluster_txninfo> &cluster_txns);

	int fetch_meta_shard_nodes(ObjectPtr<Shard_node> sn, bool is_master,
		const char *master_ip = NULL, int master_port = 0);

  	int refresh_shard_backupnode_info(ObjectPtr<Shard> );
	int refresh_shards(std::vector<ObjectPtr<KunlunCluster> > &kl_clusters);
	int refresh_computers(std::vector<ObjectPtr<KunlunCluster> > &kl_clusters);

	int get_max_cluster_id(int &cluster_id);
	int execute_metadate_opertation(enum_sql_command command, const std::string & str_sql);
	int delete_cluster_from_metadata(std::string &cluster_name);

	int update_instance_status(Tpye_Ip_Port &ip_port, std::string &status, int &type);
	int get_backup_storage_string(std::string &name, std::string &backup_storage_id, std::string &backup_storage_str);
	int get_backup_storage_list(std::vector<Tpye_string4> &vec_t_string4);

	int get_backup_info_from_metadata(std::string &cluster_name, std::string &timestamp, std::vector<std::string> &vec_shard);
	int get_cluster_info_from_metadata(std::string &cluster_name, std::string &json_buf);
	int get_machine_info_from_metadata(std::vector<std::string> &vec_machine);
	int get_server_nodes_stats_id(std::string &hostaddr, std::string &id);
	int check_backup_storage_name(std::string &name);
	int check_machine_hostaddr(std::string &hostaddr);
	int check_cluster_name(std::string &cluster_name);
	int check_nick_name(std::string &nick_name);


	void update_shard_delete_state(uint cluster_id, const std::vector<int>& del_shards);
	void adjust_shard_master(ObjectPtr<Shard> shard);
	void update_metadb_shard_master(const std::string nip, int nport,
									const std::string oip, int oport);

	void add_shard_cluster_memory(std::vector<ObjectPtr<KunlunCluster> > &kl_clusters, 
			int cluster_id, const std::string& cluster_name, const std::string& nick_name, const std::string& ha_mode,
			const std::vector<Shard_Type>& shard_params);
	void add_computer_cluster_memory(std::vector<ObjectPtr<KunlunCluster> > &kl_clusters, 
			int cluster_id, const std::string& cluster_name, const std::string& nick_name, 
			const std::vector<Comps_Type>& comps_params);
	void add_node_shard_memory(std::vector<ObjectPtr<KunlunCluster> > &kl_clusters, int cluster_id,
				int shard_id, const std::vector<CNode_Type>& node_params);
};

#endif // !SHARD_H
