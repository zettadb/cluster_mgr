/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef SHARD_H
#define SHARD_H
#include "sys_config.h"
#include "global.h"
#include "shard.h"
#include "log.h"
#include "machine_info.h"

#include <atomic>
#include <set>
#include <string>
#include <map>
#include <vector>
#include "mysql/mysql.h"
#include "mysql/errmsg.h"
#include "mysql/mysqld_error.h"
#include "mysql/server/private/sql_cmd.h"

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
extern std::string meta_svr_user;
extern std::string meta_svr_pwd;

class Thread;
class Shard;
class Shard_node;
class Computer_node;
class KunlunCluster;

class MYSQL_CONN
{
private:
    bool connected;
	enum_sql_command sqlcmd;
	int port;
	std::string ip, user, pwd;
    MYSQL_RES *result;
    int nrows_affected;
    int nwarnings;
	MYSQL conn;
	Shard_node *owner;
	std::set<int> ignore_errs;
	bool mysql_get_next_result();
	int handle_mysql_error(const char *stmt_ptr = NULL, size_t stmt_len = 0);
	bool handle_mysql_result();
	void close_conn();
	friend class Shard_node;
	void free_mysql_result();
	int verify_version();
public:
	MYSQL_CONN(const char * ip_, int port_, const char * user_,
		const char * pwd_, Shard_node *owner_):
		connected(false),sqlcmd(SQLCOM_END),
		port(port_), ip(ip_), user(user_), pwd(pwd_), owner(owner_)
	{
		result = NULL;
		nrows_affected = 0;
		nwarnings = 0;
	}

	~MYSQL_CONN() { close_conn(); }

	bool send_stmt(enum_sql_command sqlcom_, const char *stmt, size_t len);
	bool send_stmt(enum_sql_command sqlcom_, const std::string &stmt);

	Shard_node *get_owner() { return owner; }

	int connect();
};

class Shard_node
{
public:
	// keep consistent with mysql
	enum Group_member_status
	{
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
	friend class MYSQL_CONN;
	uint id;
	uint64_t latest_mgr_pos;
	Shard *owner;
	MYSQL_CONN mysql_conn;

public:
	void get_ip_port(std::string&ip, int&port) const
	{
		ip = mysql_conn.ip;
		port = mysql_conn.port;
	}
	void get_user_pwd(std::string&user, std::string&pwd) const
	{
		user = mysql_conn.user;
		pwd = mysql_conn.pwd;
	}

	void set_master(bool b) { _is_master = b; }
	bool is_master() const { return _is_master;}
	
	Shard*get_owner() { return owner; }
	
	void add_ignore_error(int ec) { mysql_conn.ignore_errs.insert(ec); }
	void clear_ignore_errors() { mysql_conn.ignore_errs.clear(); }

	uint get_id() const { return id; }
	void set_id(uint id) { this->id = id; }

	Shard_node(uint id_, Shard *owner_, const char * ip_, int port_,
		const char * user_, const char * pwd_):
		_is_master(false), id(id_), latest_mgr_pos(0), owner(owner_),
		mysql_conn(ip_, port_, user_, pwd_, this)
	{
		Assert(owner && ip_ && user_ && pwd_);
		Assert(port_ > 0);
	}

	bool update_conn_params(const char * ip_, int port_, const char * user_,
		const char * pwd_);
	int check_mgr_state();
	bool send_stmt(enum_sql_command sqlcom_, const char *stmt, size_t len, int nretries = 1);
	bool send_stmt(enum_sql_command sqlcom_, const std::string &stmt, int nretries = 1);
	int connect();

	void free_mysql_result() { mysql_conn.free_mysql_result(); }

	bool matches_ip_port(const std::string &ip, int port) const
	{
		return mysql_conn.ip == ip && mysql_conn.port == port;
	}

	bool update_variables(Tpye_string2 &t_string2);
	bool get_variables(std::string &variable, std::string &value);
	bool set_variables(std::string &variable, std::string &value_int, std::string &value_str);
	bool update_instance_cluster_info();
	int start_mgr(Group_member_status st, bool as_master);
	
	MYSQL_RES *get_result() { return mysql_conn.result; }
	
	bool fetch_mgr_progress();
	int get_mgr_master_ip_port(std::string&ip, int&port);
	
	uint64_t get_latest_mgr_pos() const { return latest_mgr_pos; }
	void close_conn() { mysql_conn.close_conn(); }
};


/*
  The logic in this class and its child classes assumes that an
  object(Shard, Shard_node)'s ID
  uniquely identifies the object permanently, it never changes after the
  object is registered into the metadata shard's tables.
*/
class Shard
{
public:
	enum Shard_type {NONE, STORAGE, METADATA};
	enum HAVL_mode {HA_no_rep, HA_mgr, HA_rbr};
protected:
	std::atomic<bool> thrd_hdlr_assigned;
	Shard_node *cur_master;
	Shard_type shard_type;
	HAVL_mode ha_mode;
	uint id;
	uint cluster_id; // cluster identifier
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
	std::vector<Shard_node*>nodes;
	uint innodb_page_size;
public:
	/*
	  @retval true if master changed; false otherwise.
	*/
	bool set_master(Shard_node *node)
	{
		Scopped_mutex sm(mtx);
		if (cur_master == node)
			return false;

		if (cur_master)
			cur_master->set_master(false);
		if (node)
			node->set_master(true);
		cur_master = node;
		return true;
	}

	Shard_node *get_node_by_id(uint id);
protected:
	/*
	  A shard's all nodes are always handled in the same thread.
	*/
	friend class KunlunCluster;
	mutable pthread_mutex_t mtx;
	mutable pthread_mutex_t mtx_txninfo;
	mutable pthread_mutexattr_t mtx_attr;

	Thread *m_thrd_hdlr;

public:
	struct Txn_key
	{
		Txn_key() { memset(this, 0, sizeof(*this)); }
		time_t start_ts;
		uint32_t local_txnid;
		uint32_t comp_nodeid;
		bool operator==(const Txn_key&tk) const
		{
			return (start_ts == tk.start_ts &&
					local_txnid == tk.local_txnid &&
					comp_nodeid == tk.comp_nodeid);
		}
		bool operator<(const Txn_key&tk) const
		{
			if (start_ts != tk.start_ts)
				return start_ts < tk.start_ts;
			if (local_txnid != tk.local_txnid)
				return local_txnid < tk.local_txnid;
			return comp_nodeid < tk.comp_nodeid;
		}
		bool operator>(const Txn_key&tk) const
		{
			if (start_ts != tk.start_ts)
				return start_ts > tk.start_ts;
			if (local_txnid != tk.local_txnid)
				return local_txnid > tk.local_txnid;
			return comp_nodeid > tk.comp_nodeid;
		}
	};

	enum Txn_decision_enum {TXN_DECISION_NONE, COMMIT, ABORT};
	struct Txn_decision
	{
		Txn_decision() : decision(TXN_DECISION_NONE), prepare_ts(0) {}
		Txn_decision(const Txn_key &tk_, Txn_decision_enum d, time_t p) :
			tk(tk_), decision(d), prepare_ts(p){}
		Txn_key tk;
		Txn_decision_enum decision;
		time_t prepare_ts;
	};

	typedef std::vector<Txn_decision> Txn_end_decisions_t;
	typedef std::vector<Txn_key> Prep_recvrd_txns_t;

protected:
	//access to the 2 members must be sync'ed by mtx_txninfo
	Txn_end_decisions_t txn_end_decisions;
	Prep_recvrd_txns_t prep_recvrd_txns;

public:
	Shard(uint id_, const std::string &name_, Shard_type type, HAVL_mode mode) :
		thrd_hdlr_assigned(false), cur_master(NULL), shard_type(type), ha_mode(mode),
		id(id_), cluster_id(0), pending_master_node_id(0), last_time_check(0),
		name(name_), m_thrd_hdlr(NULL), innodb_page_size(0)
	{
		pthread_mutexattr_init(&mtx_attr);
		pthread_mutexattr_settype(&mtx_attr, PTHREAD_MUTEX_RECURSIVE);
		pthread_mutex_init(&mtx, &mtx_attr);
		pthread_mutex_init(&mtx_txninfo, &mtx_attr);
	}

	~Shard()
	{
		for (auto &i:nodes)
			delete i;
		pthread_mutex_destroy(&mtx);
		pthread_mutex_destroy(&mtx_txninfo);
		pthread_mutexattr_destroy(&mtx_attr);
	}

	// do db ops without mutex held
	void take_prep_recvrd_txns(Prep_recvrd_txns_t &prt)
	{
		Scopped_mutex sm(mtx_txninfo);
		prt = prep_recvrd_txns;
		prep_recvrd_txns.clear();
	}

	// do db ops without mutex held
	void take_txn_end_decisions(Txn_end_decisions_t&ted)
	{
		Scopped_mutex sm(mtx_txninfo);
		ted = txn_end_decisions;
		txn_end_decisions.clear();
	}

	void set_txn_end_decisions(Txn_end_decisions_t&ted)
	{
		Scopped_mutex sm(mtx_txninfo);
		txn_end_decisions.insert(txn_end_decisions.end(), ted.begin(), ted.end());
	}

	Thread *get_thread_handler()
	{
		Scopped_mutex sm(mtx);
		return m_thrd_hdlr;
	}

	// called by worker threads to maintain shard working state.
	void maintenance();

	/*
	  Set h to be the thread handler, or remove current thread handler(h is 0).
	  if force is true, ignore shard's last_time_check and always assign it
	  to the thread.
	  @retval true if set OK; false if not set.
	*/
	bool set_thread_handler(Thread *h, bool force = false);

	time_t get_last_time_check() const
	{
		Scopped_mutex sm(mtx);
		return last_time_check;
	}

	std::vector<Shard_node*>&get_nodes()
	{
		Scopped_mutex sm(mtx);
		return nodes;
	}

	uint get_cluster_id() const
	{
		Scopped_mutex sm(mtx);
		return cluster_id;
	}

	const std::string &get_cluster_name() const
	{
		Scopped_mutex sm(mtx);
		return cluster_name;
	}

	void update_cluster_name(const std::string &name, uint cid)
	{
		Scopped_mutex sm(mtx);
		Assert(cluster_id == cid);
		cluster_name = name;
	}

	void set_cluster_info(const std::string &name, uint cid)
	{
		Scopped_mutex sm(mtx);
		cluster_name = name;
		cluster_id = cid;
	}

	bool contains_node(const std::string&ip, int port) const
	{
		Scopped_mutex sm(mtx);
		for (auto &i:nodes)
			if (i->matches_ip_port(ip, port))
				return true;
		return false;
	}

	bool contains_node(uint nodeid) const
	{
		Scopped_mutex sm(mtx);
		for (auto &i:nodes)
			if (i->get_id() == nodeid)
				return true;
		return false;
	}

	Shard_type get_type() const
	{
		Scopped_mutex sm(mtx);
		return shard_type;
	}

	HAVL_mode get_mode() const
	{
		Scopped_mutex sm(mtx);
		return ha_mode;
	}

	void set_mode(HAVL_mode mode)
	{
		Scopped_mutex sm(mtx);
		ha_mode = mode;
	}

	uint get_id() const
	{
		Scopped_mutex sm(mtx);
		return id;
	}

	const std::string &get_name() const
	{
		Scopped_mutex sm(mtx);
		return name;
	}

	void add_node(Shard_node *node)
	{
		Scopped_mutex sm(mtx);
		nodes.emplace_back(node);
		std::string ip;
		int port;
		node->get_ip_port(ip, port);
		syslog(Logger::INFO, "Added shard(%s.%s, %u) node (%s:%d, %u) into protection.",
			cluster_name.c_str(), name.c_str(),
			id, ip.c_str(), port, node->get_id());
	}

	void remove_nodes_not_in(const std::set<uint>& ids)
	{
		Scopped_mutex sm(mtx);
		for (auto i = nodes.begin(); i != nodes.end(); ++i)
		{
			if (ids.find((*i)->get_id()) == ids.end())
			{
				if (*i == cur_master) cur_master = NULL;
				delete *i;
				auto j = i;
				nodes.erase(j);
			}
		}
	}

	void remove_node(uint id)
	{
		Scopped_mutex sm(mtx);
		for (auto i = nodes.begin(); i != nodes.end(); ++i)
		{
			if ((*i)->get_id() == id)
			{
				if (*i == cur_master) cur_master = NULL;
				delete *i;
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
	Shard_node* refresh_node_configs(uint id, const char * ip_, int port_,
		const char * user_, const char * pwd_, bool&changed)
	{
		Scopped_mutex sm(mtx);
		Shard_node *n = get_node_by_id(id);
		changed = false;

		if (n)
			changed = n->update_conn_params(ip_, port_, user_, pwd_);
		else
			add_node(n = new Shard_node(id, this, ip_, port_, user_, pwd_));
		return n;
	}

	Shard_type get_shard_type() const
	{
		Scopped_mutex sm(mtx);
		return shard_type;
	}

	Shard_node*get_master() { return cur_master; }

	Shard_node *get_node_by_ip_port(const std::string &ip, int port)
	{
		std::string ip1;
		int port1 = 0;

		for (auto&n:nodes)
		{
			n->get_ip_port(ip1, port1);
			if (ip == ip1 && port == port1)
				return n;
		}
		return NULL;
	}

	int check_mgr_cluster();
	int end_recovered_prepared_txns();
	int get_xa_prepared();
	uint get_innodb_page_size();
};


class MetadataShard : public Shard
{
public:
	struct txn_info
	{
		txn_info() : processed(false) {}
		bool processed;
		std::vector<Shard *>branches;
	};
	
	struct cluster_txninfo
	{
		cluster_txninfo(uint id, const std::string & name) :
			min_start_ts(0xffffffff), max_start_ts(0), cid(id), cname(name)
		{}
	
		time_t min_start_ts, max_start_ts;
		uint cid; // cluster id
		std::string cname; // cluster name
		std::map<Shard::Txn_key, txn_info> tkis;
	};

	// Keep this same as in computing node impl(METADATA_SHARDID).
	const static uint32_t METADATA_SHARD_ID = 0xFFFFFFFF;

	MetadataShard() : Shard(METADATA_SHARD_ID, "MetadataShard", METADATA, HA_mgr)
	{
		// Need to assign the pair for consistent generic processing.
		cluster_id = 0xffffffff;
		cluster_name = "MetadataShardVirtualCluster";
	}

	int compute_txn_decisions(std::map<uint, cluster_txninfo> &cluster_txns);

	int fetch_meta_shard_nodes(Shard_node *sn, bool is_master,
		const char *master_ip = NULL, int master_port = 0);

	int refresh_shards(std::vector<KunlunCluster *> &kl_clusters);
	int refresh_computers(std::vector<KunlunCluster *> &kl_clusters);
	int check_port_used(std::string &ip, int port);
	int get_comp_nodes_id_seq(int &comps_id);
	int get_max_cluster_id(int &cluster_id);
	int execute_metadate_opertation(enum_sql_command command, const std::string & str_sql);
	int delete_cluster_from_metadata(std::string &cluster_name);
	int delete_cluster_shard_from_metadata(std::string &cluster_name, std::string &shard_name);
	int delete_cluster_shard_node_from_metadata(std::string &cluster_name, std::string &shard_name, Tpye_Ip_Port &ip_port);
	int delete_cluster_comp_from_metadata(std::string &cluster_name, std::string &comp_name);
	int get_server_nodes_from_metadata(std::vector<Machine*> &vec_machines);
	int get_meta_instance(Machine* machine);
	int get_storage_instance_port(Machine* machine);
	int get_computer_instance_port(Machine* machine);
	int update_instance_status(Tpye_Ip_Port &ip_port, std::string &status, int &type);
	int get_backup_storage(std::string &backup_storage);
	int add_shard_nodes(std::string &cluster_name, std::string &shard_name, std::vector<Tpye_Ip_Port_User_Pwd> vec_ip_port_user_pwd);
	int get_roll_info_from_metadata(std::string &job_id, std::vector<std::string> &vec_roll_info);
	int get_ongoing_job_id_from_metadata(std::vector<std::string> &vec_job_id);
	int get_ongoing_job_json_from_metadata(std::vector<std::string> &vec_job_json);
	int get_backup_info_from_metadata(std::string &cluster_name, std::string &timestamp, std::vector<std::string> &vec_shard);
	int get_cluster_info_from_metadata(std::string &cluster_name, std::string &json_buf);
	int get_prometheus_info_from_metadata(std::vector<std::string> &vec_machine);
	int check_machine_hostaddr(std::string &hostaddr);
	int check_cluster_name(std::string &cluster_name);
	int check_cluster_shard_name(std::string &cluster_name, std::string &shard_name);
	int check_cluster_comp_name(std::string &cluster_name, std::string &comp_name);
	int check_cluster_shard_more(std::string &cluster_name);
	int check_cluster_shard_node_more(std::string &cluster_name, std::string &shard_name);
	int check_cluster_comp_more(std::string &cluster_name);
};

#endif // !SHARD_H
