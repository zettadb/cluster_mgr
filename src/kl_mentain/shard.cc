/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "sys_config.h"
#include "global.h"
#include "config.h"
#include "sys.h"
#include "shard.h"
#include "os.h"
#include "kl_cluster.h"
#include <unistd.h>
#include <utility>
#include <time.h>
#include <sys/time.h>
#include "zettalib/tool_func.h"
#include "util_func/object_ptr.h"

using namespace kunlun;

// config variables
int64_t mysql_connect_timeout = 50;
int64_t mysql_read_timeout = 50;
int64_t mysql_write_timeout = 50;
int64_t mysql_max_packet_size = 1024*1024*1024;
int64_t prepared_transaction_ttl = 3;
int64_t meta_svr_port = 0;
int64_t check_shard_interval = 3;
int64_t stmt_retries = 3;
int64_t stmt_retry_interval_ms = 500;
int64_t global_txn_commit_log_wait_max_sec = 10;

std::string meta_svr_ip;
std::string meta_group_seeds;
std::string meta_svr_user;
std::string meta_svr_pwd;

std::vector<Tpye_Ip_Port> vec_meta_ip_port;

// not configurable for now
bool mysql_transmit_compress = false;

//#define IS_MYSQL_CLIENT_ERROR(err) (((err) >= CR_MIN_ERROR && (err) <= CR_MAX_ERROR) || ((err) >= CER_MIN_ERROR && (err) <= CER_MAX_ERROR))
using namespace kunlun;

static void convert_preps2ti(ObjectPtr<Shard> ps, const Shard::Prep_recvrd_txns_t &preps,
		MetadataShard::cluster_txninfo &cti, bool is_meta_shard);
static void process_prep_txns(const Shard::Txn_decision &txn_dsn,
	MetadataShard::txn_info &ti,
	std::vector<std::tuple<ObjectPtr<Shard>, Shard::Txn_end_decisions_t> >& shard_txn_decisions);
	//std::map<ObjectPtr<Shard>, Shard::Txn_end_decisions_t>&shard_txn_decisions);

bool Shard_node::update_conn_params(const char * ip_, int port_, const char * user_,
	const char * pwd_)
{
	bool changed = false;
	std::string old_ip = ip; //mysql_conn.ip;
	int old_port = port; //mysql_conn.port;

	if (ip != ip_)
	{
		changed = true;
		ip = ip_;
	}

	if (port != port_)
	{
		changed = true;
		port = port_;
	}

	if (user != user_)
	{
		changed = true;
		user = user_;
	}

	if (pwd != pwd_)
	{
		changed = true;
		pwd = pwd_;
	}

	if (changed && mysql_conn->IsConnected())
	{
		/*syslog(Logger::INFO, "Connection parameters for shard (%s.%s %u) node (%u, %s:%d) changed to (%s:%d, %s, ***), reconnected with new params",
			   owner->get_cluster_name().c_str(), owner->get_name().c_str(),
			   owner->get_id(), id, old_ip.c_str(),
			   old_port, ip_, port_, user_);*/
		KLOG_INFO("Connection parameters for shard ({}.{} {}) node ({}, {}:{}) changed to ({}:{}, {}, ***), reconnected with new params",
			   owner->get_cluster_name(), owner->get_name(),
			   owner->get_id(), id, old_ip, old_port, ip_, port_, user_);
		//mysql_conn.close_conn(); can't do it here because this node may have a valid 'result' being iterated.
		mysql_conn->Connect();
	}
	return changed;
}

bool Shard_node::
send_stmt(const char *stmt, size_t len, MysqlResult* result, int nretries) {
	bool ret = true;
	for(int i=0; i<nretries; i++) {
		//KLOG_INFO("mysql conn execute sql: {}", stmt);
		if(mysql_conn->ExcuteQuery(stmt, result, false) != -1) {
			ret = false;
			break;
		} else 
			KLOG_ERROR("mysql execute sql {} failed: {}", stmt, mysql_conn->getErr());

		if (!System::get_instance()->get_cluster_mgr_working())
			return ret;

		usleep(stmt_retry_interval_ms * 1000);
	}
	return ret;
}

bool Shard_node::
send_stmt(const std::string& stmt, MysqlResult* result, int nretries) {
	KlWrapGuard<KlWrapMutex> guard(sql_mux);
	return send_stmt(stmt.c_str(), stmt.length(), result, nretries);
}

int Shard_node::connect() {
	//sync mysql connection
	MysqlConnectionOption op;
    op.ip = ip;
    op.port_str = std::to_string(port);
    op.port_num = port;
	op.user = user;
	op.password = pwd;
    op.timeout_sec = mysql_read_timeout;
    op.connect_timeout_sec = mysql_connect_timeout;
	op.database = (owner->get_id() == MetadataShard::METADATA_SHARD_ID ? KUNLUN_METADATA_DBNAME  : "");
	mysql_conn = new MysqlConnection(op);
	if(!mysql_conn->Connect()) {
		KLOG_ERROR("sync mysql connect failed, {}", mysql_conn->getErr());
	}

	MysqlResult result;
	bool ret = send_stmt("show global variables like 'server_uuid'", &result, stmt_retries);
	if(!ret) {
		if(result.GetResultLinesNum() == 1) {
			server_uuid_ = result[0]["Value"];
		}
	}

	//async mysql connection
	op.timeout_sec = 3;
    op.connect_timeout_sec = 3;
    amysql_conn = new CAsyncMysql(System::get_instance()->get_amysql_mgr(), op);
    if(!amysql_conn->Connect()) {
        KLOG_ERROR("async mysql connect failed, {}", amysql_conn->getErr());
    }

    //set session thread
    amysql_conn->ExecuteQuery("set session my_thread_handling=1", nullptr, nullptr, false);
	return 0;
}

bool Shard_node::update_variables(Tpye_string2 &t_string2)
{
	std::string str_sql = "set persist " + std::get<0>(t_string2) + "='" + std::get<1>(t_string2) + "'";
	//return send_stmt(SQLCOM_SET_OPTION, str_sql.c_str(), str_sql.length(), stmt_retries);
	MysqlResult res;
	return send_stmt(str_sql, &res, stmt_retries);
}

void Shard_node::reload_server_uuid() {
	MysqlResult result;
	bool ret = send_stmt("show global variables like 'server_uuid'", &result, stmt_retries);
	if(!ret) {
		if(result.GetResultLinesNum() == 1) {
			server_uuid_ = result[0]["Value"];
		}
	}
}

bool Shard_node::get_variables(std::string &variable, std::string &value)
{
	std::string str_sql = "select @@" + variable;
	MysqlResult res;
	int ret = send_stmt(str_sql, &res, stmt_retries);
	int zero = 0;
	if(res.GetResultLinesNum())
		value = res[0][zero];
	else
		ret = -1;
	
	return ret;
}

bool Shard_node::set_variables(std::string &variable, std::string &value_int, std::string &value_str)
{
	std::string str_sql;

	if(value_int.length())
		str_sql = "set persist " + variable + "=" + value_int;
	else
		str_sql = "set persist " + variable + "='" + value_str + "'";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());
	KLOG_INFO("str_sql={}", str_sql);
	MysqlResult res;
	return send_stmt(str_sql, &res, stmt_retries);

	//return send_stmt(SQLCOM_SET_OPTION, str_sql.c_str(), str_sql.length(), stmt_retries);
}

bool Shard_node::update_instance_cluster_info()
{
	std::string cluster_name = owner->get_cluster_name();
	std::string shard_name = owner->get_name();
	std::string str_sql;

	str_sql = "update kunlun_sysdb.cluster_info set cluster_name='" + cluster_name + "',shard_name='" + shard_name + "'";
	MysqlResult res;
	return send_stmt(str_sql, &res, stmt_retries);
	//return send_stmt(SQLCOM_UPDATE, str_sql.c_str(), str_sql.length(), stmt_retries);
}

/*
  do START GROUP_repliplication, and optionally after stop GROUP_repliplication
  if its status is ERROR rather than OFFLINE.
  @retval -1 if connection broken or stmt exec failure
*/
int Shard_node::start_mgr(Group_member_status curstat, bool as_master)
{
	const char *extra_info = "";
	if (as_master) extra_info = "as primary";

	static const char stmt1[] = "stop group_replication; start group_replication";
	static const char stmt2[] = "start group_replication";
	static const char stmt3[] = "stop group_replication; SET GLOBAL group_replication_bootstrap_group=ON; START GROUP_REPLICATION; SET GLOBAL group_replication_bootstrap_group=OFF";
	static const char stmt4[] = "SET GLOBAL group_replication_bootstrap_group=ON; START GROUP_REPLICATION; SET GLOBAL group_replication_bootstrap_group=OFF";
	Assert(curstat == Shard_node::MEMBER_ERROR ||
		   curstat == Shard_node::MEMBER_OFFLINE);

	int ret = 0;
	/*
	  We are using MariaDB's client lib, so there is no
	  SQLCOM_START_GROUP_REPLICATION, and actually we simply can use any
	  command other than SQLCOM_SELECT here.
	*/
	MysqlResult res;
	if (as_master)
	{
		if (curstat == Shard_node::MEMBER_ERROR)
			ret = send_stmt(stmt3, &res, stmt_retries);
			//send_stmt(SQLCOM_SLAVE_START, CONST_STR_PTR_LEN(stmt3), stmt_retries);
		else
			ret = send_stmt(stmt4, &res, stmt_retries);
			//send_stmt(SQLCOM_SLAVE_START, CONST_STR_PTR_LEN(stmt4), stmt_retries);
	}
	else
	{
		if (curstat == Shard_node::MEMBER_ERROR)
			ret = send_stmt(stmt1, &res, stmt_retries);
			//send_stmt(SQLCOM_SLAVE_START, CONST_STR_PTR_LEN(stmt1), stmt_retries);
		else
			ret = send_stmt(stmt2, &res, stmt_retries);
			//send_stmt(SQLCOM_SLAVE_START, CONST_STR_PTR_LEN(stmt2), stmt_retries);
	}

	//mysql_conn.free_mysql_result();

    if (ret)
	{
		/*syslog(Logger::ERROR, "Failed to join shard (%s.%s, %u) node (%u, %s:%d) of status %s back to the MGR cluster%s.",
			   owner->get_cluster_name().c_str(), owner->get_name().c_str(),
			   owner->get_id(), this->id, ip.c_str(),
			   port, Group_member_status_strs[curstat - 1],
			   extra_info);*/
		KLOG_ERROR("Failed to join shard ({}.{}, {}) node ({}, {}:{}) of status {} back to the MGR cluster {}.",
			   owner->get_cluster_name(), owner->get_name(), owner->get_id(), this->id, ip,
			   port, Group_member_status_strs[curstat - 1], extra_info);
        return -1;
	}


	/*syslog(Logger::INFO, "Added shard (%s.%s, %u) node (%u, %s:%d) of status %s back to the MGR cluster%s.",
		   owner->get_cluster_name().c_str(), owner->get_name().c_str(),
		   owner->get_id(), this->id, ip.c_str(), port,
		   Group_member_status_strs[curstat - 1], extra_info);*/
	KLOG_INFO("Added shard ({}.{}, {}) node ({}, {}:{}) of status %s back to the MGR cluster {}.",
		   owner->get_cluster_name(), owner->get_name(),
		   owner->get_id(), this->id, ip, port, Group_member_status_strs[curstat - 1], extra_info);
	return 0;
}


/*
  Fetch from the shard node the latest executed gtid of the
  group_replication_applier channel, to be used to decide which node to start
  as master node. Only to be called when no nodes of the MGR cluster is in
  MEMBER_ONLINE state, because:
  1. otherwise the progress keeps changing and can't be cached in the
  	 cluster_manager for further decisions
  2. only in this case do we really need the positions.
  @retval true on error; false on success
*/
bool Shard_node::fetch_mgr_progress() {
	MysqlResult res;
	char *endptr = NULL;
	bool ret = send_stmt(
        "select interval_end from mysql.gtid_executed where source_uuid=@@group_replication_group_name", 
				&res, stmt_retries);
	if(ret)
		return ret;

	uint32_t n_rows = res.GetResultLinesNum();
	if(n_rows) {
		latest_mgr_pos = strtoull(res[n_rows-1]["interval_end"], &endptr, 10);
		Assert(endptr == NULL || *endptr == '\0');
	}

	KLOG_INFO("Found shard ({}.{}, {}) node({}, {}:{}) latest MGR position: {}",
		   owner->get_cluster_name(), owner->get_name(),
		   owner->get_id(), this->id, ip, port, latest_mgr_pos);
	return false;
}

/*
  @retval -1: connection broken or stmt exec error;
  		-2: multiple or no primary nodes found
*/
int Shard_node::get_mgr_master_ip_port(std::string&ip, int&port) {
	MysqlResult res;
	char *endptr = NULL;
	int ret = send_stmt(
		"select MEMBER_HOST, MEMBER_PORT from performance_schema.replication_group_members where MEMBER_ROLE = 'PRIMARY' and MEMBER_STATE = 'ONLINE'", &res, stmt_retries);
	if(ret)
		return -1;
	
	uint64_t nrows = res.GetResultLinesNum();
	if(nrows != 1) {
		/*syslog(Logger::WARNING,
			"Wrong NO.(%lu) of primary nodes found in shard (%s.%s, %u) node(%u, %s:%d), it could be joining the MGR cluster.",
		   nrows, owner->get_cluster_name().c_str(), owner->get_name().c_str(),
		   owner->get_id(), this->id, ip.c_str(), port);*/
		KLOG_WARN("Wrong NO.({}) of primary nodes found in shard ({}.{}, {}) node({}, {}:{}), it could be joining the MGR cluster.",
		   nrows, owner->get_cluster_name(), owner->get_name(),
		   owner->get_id(), this->id, ip, port);
		ret = -2;
		goto end;
	}

	ret = 0;

	for(uint32_t i=0; i < nrows; i++) {
		ip = res[i]["MEMBER_HOST"];
		port = strtol(res[i]["MEMBER_PORT"], &endptr, 10);
		Assert(endptr == NULL || *endptr == '\0');
	}
end:
	return ret;
}


/*
  Query node mgr status.
  @retval -1: communication error; -2: node not initialized with MGR
  -3: invalid GR status returned from node;
  -4: unrecognized node status returned from node.
  -5: MGR cluster doesn't have this node itself, it returned a bunch of other nodes.
  -6: invalid row field values in returned results.
  positive: Group_member_status enums;
*/
int Shard_node::check_mgr_state()
{
	MysqlResult res;
	const char *the_stmt = NULL;
    int ret = send_stmt(the_stmt =
		"select MEMBER_HOST, MEMBER_PORT, MEMBER_STATE, MEMBER_ROLE from performance_schema.replication_group_members", &res, stmt_retries);
	if (ret)
        return -1;
	ret = 0;

	uint64_t nrows = res.GetResultLinesNum(), n_myrows = 0; 
	std::string node_stat;
	char *endptr = NULL;
	int port1 = 0;

    //while ((row = mysql_fetch_row(result)))
	for(uint32_t i=0; i<nrows; i++) {
		if (strlen(res[i]["MEMBER_HOST"]) == 0 && strcmp(res[i]["MEMBER_PORT"], "NULL") == 0 && 
				strlen(res[i]["MEMBER_ROLE"]) == 0 && nrows == 1) {
			Assert(strcmp(res[i]["MEMBER_STATE"], "OFFLINE") == 0 || strcmp(res[i]["MEMBER_STATE"], "ERROR") == 0);
			goto got_my_row;
		}

		if (strcmp(res[i]["MEMBER_PORT"], "NULL") == 0) {
			KLOG_ERROR("Invalid SQL query ({}) result({} rows) returned from shard({}.{}, {}) node({}:{}, {}): ({}, NULL, {}, {})",
				the_stmt, nrows, owner->get_cluster_name(), owner->get_name(),
				owner->get_id(), ip, port, this->id, res[i]["MEMBER_HOST"], res[i]["MEMBER_STATE"], res[i]["MEMBER_ROLE"]);
			ret = -6;
			goto end;
		}

		// all fields of the row must now have valid values.
		//Assert(res[i][zero][0] && res[i][1] && res[i][2][0]); // && row[3][0]); could be "" when state is OFFLINE.
		port1 = strtol(res[i]["MEMBER_PORT"], &endptr, 10);
		Assert(endptr == NULL || *endptr == '\0');
		if (strcmp(ip.c_str(), res[i]["MEMBER_HOST"]) || port != port1)
			continue;
got_my_row:
		n_myrows++;
		node_stat = res[i]["MEMBER_STATE"];
		if (strcmp(res[i]["MEMBER_ROLE"], "PRIMARY") == 0 && owner->set_master(this)) {
			KLOG_INFO("Found primary node of shard({}.{}, {}) changed to ({}:{}, {})",
		   		owner->get_cluster_name(), owner->get_name(),
		   		owner->get_id(), res[i]["MEMBER_HOST"], port1, get_id());

		}

		for (int j = 0; j < sizeof(Group_member_status_strs)/sizeof(char*); j++) {
			if (strcmp(res[i]["MEMBER_STATE"], Group_member_status_strs[j]) == 0) {
				ret = j + 1; // ONLINE is set to 1.
				break;
			}
		}
	}
end:
	if (ret == -6)
		return ret;

	if (nrows == 0) {
		KLOG_ERROR("Group replication not initialized in shard ({}.{}, {}) node ({}, {}:{}), this node is invalid as a shard node for Kunlun DDC.",
			   owner->get_cluster_name(), owner->get_name(), owner->get_id(), this->id, ip, port);
		ret = -2;
	} else if (n_myrows == 0) {
		KLOG_ERROR(
		"Invalid group replication cluster status in shard ({}.{}, {}) node ({}, {}:{}): no valid rows found representing this node itself.",
			   owner->get_cluster_name(), owner->get_name(),
			   owner->get_id(), this->id, ip,
			   port);
		ret = -5;
	} else if (n_myrows > 1) {
		KLOG_ERROR(
		"Invalid group replication cluster status in shard ({}.{}, {}) node ({}, {}:{}): {} rows found for this node",
			   owner->get_cluster_name(), owner->get_name(),
			   owner->get_id(), this->id, ip,
			   port, n_myrows);
		ret = -3;
	} else if (ret == 0) {
		KLOG_ERROR(
		"Invalid group replication node status in shard ({}.{}, {}) node ({}, {}:{}): unrecognized node status {}",
			   owner->get_cluster_name(), owner->get_name(),
			   owner->get_id(), this->id, ip,
			   port,node_stat);
		ret = -4;
	}

	/*KLOG_INFO("Found shard ({}.{}, {}) node({}, {}:{}) status: {}",
		   owner->get_cluster_name(), owner->get_name(),
		   owner->get_id(), this->id,
		   ip, port, node_stat);*/
	Assert(ret != 0);
	return ret;
}


const char *Shard_node::Group_member_status_strs[] = {"ONLINE", "OFFLINE", "RECOVERING", "ERROR", "UNREACHABLE", "INVALID"};

/*
  If all nodes connect with no other nodes, the cluster is down altogether.
  Choose the one with latest changes as master and start it first, then
  do START GROUP_repliplication to the rest nodes.

  For each node, do stop GROUP_repliplication first
  if its status is ERROR rather than OFFLINE.
  
  Finally, find out which one is current master and set cur_master.

  @retval 0 on success;
  -1 if majority nodes not reachable and the cluster can't be
  started up.
  -2 if multiple master nodes found thus unable to set cur_master for this
  run.
  -3 if no master node found thus unable to set cur_master for this run.
  -4 master node confirmed by non-majority(quorum) nodes.
  -5 latest master node not registered into cluster manager yet. later main
  thread will be able to find it from metadata server and register it.
  -6 if new master startup failure
*/
int Shard::check_mgr_cluster()
{
	KlWrapGuard<KlWrapMutex> guard(mtx);
	std::vector<std::pair<ObjectPtr<Shard_node>, Shard_node::Group_member_status> >
		down_reachables;

	int nodes_down = 0, reachables = 0;
	std::vector<ObjectPtr<Shard_node> >unreachables;
	for (auto &i:nodes) {
		if (!System::get_instance()->get_cluster_mgr_working())
			break;
		int stat = i->check_mgr_state();
		if (stat < -1)
			return stat;
		Assert(stat == -1 || (stat > 0 && stat < Shard_node::MEMBER_END));
		// if stat == -1 or MEMBER_UNREACHABLE, node i can't be connected for now, it probably will
		// be started up and come back again later, so leave it alone for now.
		// for MEMBER_IN_RECOVERY, node i will very soon be usable with no further action needed.
		if (stat == Shard_node::MEMBER_ERROR || stat == Shard_node::MEMBER_OFFLINE) {
			down_reachables.emplace_back(
				std::make_pair(i, (Shard_node::Group_member_status)stat));
		}

		// take MEMBER_UNREACHABLE as down, if a cluster has 1 MEMBER_UNREACHABLE
		// node and 2 ERROR/OFFLINE nodes, we still need to bring the cluster up entirely.
		if (stat != Shard_node::MEMBER_IN_RECOVERY &&
			stat != Shard_node::MEMBER_ONLINE)
			nodes_down++;
		if (stat != -1 && stat != Shard_node::MEMBER_UNREACHABLE)
			reachables++;
		else {
			int exist_flag = 0;
			for(auto us : unreachables) {
				if(us->get_id() == i->get_id()) {
					exist_flag = 1;
					break;
				}
			}

			if(!exist_flag)
				unreachables.push_back(i);
		}
	}

	if (KL_LIKELY(nodes_down == 0)) return 0; // most common case, we trust MGR will not brainsplit.

	if (nodes_down < nodes.size()) {
		// Some nodes in the MGR cluster are running, one of them must be a
		// master node, no need to choose a master for the cluster.
		for (auto i=down_reachables.begin(); i != down_reachables.end(); ++i) {
			if (!System::get_instance()->get_cluster_mgr_working())
				break;
			(*i).first->start_mgr((*i).second, false);
		}
	} else {
		Assert(reachables == down_reachables.size());

		/*
		  All nodes down, need to find the one with latest changes and make it
		  the master and start it up first, then start up the rest nodes.
		  Doing so requires simple majority nodes to be connectable/reachable, otherwise we
		  might lose some changes if the unreachable nodes happen to have changes not
		  in the connectable/reachable nodes.
		*/
		if (reachables > nodes.size() / 2) {
			uint64_t max_pos = 0;
			ObjectPtr<Shard_node> max_sn;
			Shard_node::Group_member_status max_stat = Shard_node::MEMBER_END;
			std::string top_ip;
			int top_port;

			if (this->pending_master_node_id) {
				max_sn = get_node_by_id(this->pending_master_node_id);
				goto got_new_master;
			}

			// find the node with most binlogs and start it as master first, then
			// start up the rest down&reachable nodes, i.e. those in down_reachables.
			for (auto itr = down_reachables.begin();
				 itr != down_reachables.end(); ++itr) {
				if (!System::get_instance()->get_cluster_mgr_working())
					break;
				if(!itr->first.Invalid())
					continue;

				if (itr->first->fetch_mgr_progress()) {
					auto jtr = itr;
					down_reachables.erase(jtr);
					reachables--;
				}
			}

			if (reachables <= nodes.size() / 2)
				goto out1;

			if(down_reachables.size()>0)	//maybe first node is primary
			{
				max_sn = down_reachables[0].first;
				max_stat = down_reachables[0].second;
			}

			for (auto &n:down_reachables)
				if (n.first->get_latest_mgr_pos() > max_pos)
				{
					max_pos = n.first->get_latest_mgr_pos();
					max_sn = n.first;
					max_stat = n.second;
				}

			Assert((max_sn.Invalid() && max_pos > 0 && max_stat != Shard_node::MEMBER_END) ||
				   (!max_sn.Invalid() && max_pos == 0 && max_stat == Shard_node::MEMBER_END));
			if (!max_sn.Invalid()) goto out1; // it's likely that MGR isn't activated in the cluster.

			max_sn->get_ip_port(top_ip, top_port);
			// max_sn found, start it as master, start the rest as slaves.
			KLOG_INFO("Found shard ({}.{}, {}) node({}, {}:{}) has max gtid position: {} and will be chosen as primary node.",
				   get_cluster_name(), get_name(),
				   get_id(), max_sn->get_id(),
				   top_ip, top_port, max_pos);
			set_master(max_sn);
got_new_master:
			/*
			  when retrying max_stat is END and max_sn's real state isn't
			  known here, could be any. simply set ERROR as 'stop gr'
			  is simply no-op if not ERROR.
			*/
			if (max_sn->start_mgr(max_stat == Shard_node::MEMBER_END ?
					Shard_node::MEMBER_ERROR : max_stat, true))
			{
				Assert(this->pending_master_node_id == 0 ||
					   this->pending_master_node_id == max_sn->get_id());

				// 3093: The START GROUP_REPLICATION command failed since the group is already running.
				// this error code isn't in mariadb's client header, so no macro for it.
				//max_sn->add_ignore_error(3093);

				this->pending_master_node_id = max_sn->get_id();
				return -6;
			}

			if (this->pending_master_node_id != 0)
			{
				this->pending_master_node_id = 0;
				//max_sn->clear_ignore_errors();
			}

			for (auto &n:down_reachables) {
				if (!System::get_instance()->get_cluster_mgr_working())
					break;
				if (n.first->get_id() != max_sn->get_id() && n.first->start_mgr(n.second, false))
					reachables--;
			}

			// if we end up started no more than half of all nodes, we still get an error
			if (reachables <= nodes.size() / 2)
				goto out1;
			// we know the master now.
			return 0;
		}
		else
		{
out1:
			// majority nodes unreachable, we don't know which to start as master,
			// so we can not start up the cluster otherwise we may lose committed txns.
			KLOG_ERROR("More than half({}/{}) nodes in the MGR cluster {} are unreachable, unable to reliably startup the MGR cluster with the right primary node.",
				   reachables, nodes.size() / 2, get_cluster_name());
			return -1;
		}
	}
find_master:
	/*
	  Find which is current master node, set it as master.
	  Above we've tried to bring the entire MGR cluster's all reachable nodes
	  up and join them into the cluster, though we may fail for any number of
	  these nodes. So below we should be prepared for any NO. of nodes to be
	  unreachable or knowing no primary info. Especially, it takes a while after
	  START GROUP_REPLICATION is executed for the node to know its master.
	*/
	std::string master_ip;
	int master_port = 0, ndone = 0;

	for (auto &i:nodes) {
		if (!System::get_instance()->get_cluster_mgr_working())
			break;

		int exist_flag = 0;
		for(auto us : unreachables) {
			if(us->get_id() == i->get_id()) {
				exist_flag = 1;
				break;
			}
		}

		if (!exist_flag) {
			std::string new_master_ip;
			int new_master_port = 0, nretries = 0, retx = 0;

			while ((retx = i->get_mgr_master_ip_port(new_master_ip, new_master_port)) != 0 &&
				   nretries++ < stmt_retries)
			{
				if (!System::get_instance()->get_cluster_mgr_working())
					break;
				usleep(stmt_retry_interval_ms * 1000);
			}
			if (retx) continue;

			ndone++;
			if (master_port == 0)
			{
				master_port = new_master_port;
				master_ip = new_master_ip;
			}
			else if (new_master_port != 0 &&
					 (master_port != new_master_port || master_ip != new_master_ip))
			{
				/*
				  Might because a master switch is going on. report error and skip
				  the rest work for this run.
				*/
				std::string iip;
				int iport = 0;
				i->get_ip_port(iip, iport);
				KLOG_ERROR("Found in shard ({}.{}, {}) node({}, {}:{}) a different primary node {}:{} than the one already found: {}:{}. A primary switch might be going on.",
				   get_cluster_name(), get_name(), get_id(), i->get_id(),
				   iip, iport, new_master_ip,
				   new_master_port, master_ip, master_port);
				return -2;
			}
		}
	}

	if (!master_port) {
		KLOG_ERROR("Found in shard ({}.{}, {}) no MGR primary node when we've attemted to join all reachable nodes to the MGR cluster.",
		   get_cluster_name(), get_name(), get_id());
		return -3;
	}

	if (ndone < nodes.size() / 2) {
		KLOG_ERROR("Found in shard ({}.{}, {}) of {} nodes the primary node confirmed to be {}:{} by only {} nodes, not majority quorum.",
		   get_cluster_name(), get_name(), get_id(),
		   nodes.size(), master_ip, master_port, ndone);
		return -4;
	}

	// now we have found the latest master ip&port, find it from Shard::nodes.
	// main thread is solely responsible for refreshing shard nodes, so here we
	// may not be able to find the new master node, that's OK, we simply skip
	// the rest work of this run and do it again later.
	bool found_master = false;
	ObjectPtr<Shard_node> the_master;
	for (auto &i:nodes) {
		if (i->matches_ip_port(master_ip, master_port)) {
			set_master(i);
			the_master = i;
			found_master = true;
			break;
		}
	}

	if (!found_master) {
		KLOG_ERROR("Latest primary node ({}:{}) in shard ({}.{}, {}) not registered into cluster manager yet",
		   master_ip, master_port, get_cluster_name(),
		   get_name(), get_id());
		return -5;
	}

	KLOG_INFO("Found current primary node ({}:{}, {}) in shard ({}.{}, {}) of {} nodes confirmed by {} nodes.",
		master_ip, master_port, the_master->get_id(),
	   get_cluster_name(), get_name(), get_id(),
	   nodes.size(), ndone);
	return 0;
}

void System::dispatch_shard_process_txns() {
	std::vector<ObjectPtr<Shard> > storage_shards;
  	for (auto &cluster : kl_clusters) {
    	if(cluster->get_cluster_delete_state())
			continue;
		storage_shards.insert(storage_shards.end(), cluster->storage_shards.begin(),
                      cluster->storage_shards.end());
	}
	
	ObjectPtr<Shard> shard(dynamic_cast<Shard*>(meta_shard.GetTRaw()));
  	storage_shards.emplace_back(shard);
	
	for(auto ss : storage_shards) {
		if(ss->get_is_delete())
			continue;

		tpool_->commit([ss] {
			ss->maintenance();
		});
	}
}

/*
  Combine all recovered prepared txns branches which were fetched by worker
  threads for each shard, into per-global-txn form, i.e. each global txn is an
  entry of (Shard::Txn_key, txn_info) which contains the Shard objects
  containing the global txn's all txn branches.
  and go through their txn-ids to get trx id
  range to fetch commit_logs from metadata server's commit_log table.
*/

int System::process_recovered_prepared() {
	std::map<uint, MetadataShard::cluster_txninfo> cluster_txns;

	MetadataShard::cluster_txninfo
		meta_tki(meta_shard->get_cluster_id(),meta_shard->get_cluster_name());

	cluster_txns.insert(std::make_pair(meta_shard->get_cluster_id(), meta_tki));
	std::vector<ObjectPtr<Shard> > all_shards;
	ObjectPtr<Shard> shd(dynamic_cast<Shard*>(meta_shard.GetTRaw()));
	all_shards.emplace_back(shd);
	for (auto &cluster:kl_clusters){
		if(cluster->get_cluster_delete_state())
			continue;

		for (auto &shard:cluster->storage_shards){
			all_shards.emplace_back(shard);
		}
  	} 

	for (auto &sd:all_shards) {
		auto itr = cluster_txns.find(sd->get_cluster_id());
		if (itr == cluster_txns.end()) {
			MetadataShard::cluster_txninfo
				cti(sd->get_cluster_id(), sd->get_cluster_name());
			cluster_txns.insert(std::make_pair(sd->get_cluster_id(), cti));
			itr = cluster_txns.find(sd->get_cluster_id());
			Assert(itr != cluster_txns.end());
		}

		Shard::Prep_recvrd_txns_t preps;
		sd->take_prep_recvrd_txns(preps);
		convert_preps2ti(sd, preps, itr->second, sd->get_id() == meta_shard->get_id());
	}

	int ret = 0;
	if ((ret = meta_shard->compute_txn_decisions(cluster_txns)))
		return ret;
	return 0;
}

/*
  Fetch commit_logs from metadata server's commit_log table.
  For each fetched commit log entry, if the txnid is found in the global txns
  map, it's committed/aborted accordingly. Finally those left unprocessed global
  txns are all aborted because they have no entry in commit log.

  Why not decide in each storage shard for each prepared XA txn branch?
  To minimize access/load to metadata cluster. Our current approach always
  only execute one select stmt for each Kunlun DDC cluster no matter how
  many prepared recovered XA txns there are.
*/
int MetadataShard::
compute_txn_decisions(std::map<uint, cluster_txninfo> &cluster_txns) {
	char qstr_buf[256];

	if(!cur_master.Invalid())
		return -1;

	KlWrapGuard<KlWrapMutex> guard(mtx);
	for (auto &clstr:cluster_txns) {
		//std::map<ObjectPtr<Shard> , Txn_end_decisions_t> shard_txn_decisions;
		std::vector<std::tuple<ObjectPtr<Shard>, Txn_end_decisions_t> > shard_txn_decisions;
		/*
		  If no recovered prepared txns we have nothing to do for this cluster.
		*/
		if (clstr.second.tkis.size() == 0)
			continue;
		/*
		  The metadata shard has no commit log, simply abort all
		  prepared recovered xa txns.
		*/
		if (clstr.second.cid == MetadataShard::METADATA_SHARD_ID)
			goto abort_rest;

		{
			uint64_t min_trxid = 0, max_trxid = 0;
			min_trxid = (clstr.second.min_start_ts.st_ << 32);
			max_trxid = ((clstr.second.max_start_ts.st_ << 32) | 0xffffffff);
			
			int slen = snprintf(qstr_buf, sizeof(qstr_buf),
				"select txn_id, comp_node_id, next_txn_cmd, unix_timestamp(prepare_ts) from commit_log_%s where txn_id >= %lu and txn_id <= %lu order by txn_id",
				clstr.second.cname.c_str(), min_trxid, max_trxid);
			Assert(slen < sizeof(qstr_buf));
			
			MysqlResult res;
			if(cur_master->send_stmt(qstr_buf, &res, stmt_retries))  {
				/*
				Remaining clusters will fail almost definitely, so error out.
				*/
				return -1;
			}

			time_t now = time(NULL);
			
			char *endptr = NULL;
			auto tk_itr = clstr.second.tkis.begin();
			uint32_t nrows = res.GetResultLinesNum();
			for(uint32_t i=0; i<nrows; i++) {
				uint64_t trxid = strtoull(res[i]["txn_id"], &endptr, 10);
				Assert(endptr == NULL || *endptr == '\0');
				Txn_key ti;
				ti.start_ts = (trxid >> 32);
				ti.local_txnid = (trxid & 0xffffffff);
				ti.comp_nodeid = strtoul(res[i]["comp_node_id"], &endptr, 10);
				Assert(endptr == NULL || *endptr == '\0');

				/*
				The SQL query result is in Txn_key increasing order (for free),
				and the clstr.second.tkis is also in the same order, so we can do a
				merge to find those with identical Txn_key values.
				It's likely that a recovered prepared txn was not scanned in last
				Shard::get_xa_prepared() call, and it's OK, it will be found
				next time.
				*/
				if (tk_itr->first < ti) {
					while (tk_itr != clstr.second.tkis.end() && tk_itr->first < ti)
						++tk_itr;
					if (tk_itr == clstr.second.tkis.end())
						break;
				}

				if (tk_itr->first > ti)
					continue;

				Assert(tk_itr->first == ti);

				Txn_decision_enum txndcs = TXN_DECISION_NONE;
				if (strcasecmp(res[i]["next_txn_cmd"], "commit") == 0)
				{
					txndcs = COMMIT;
				}
				else if (strcasecmp(res[i]["next_txn_cmd"], "abort") == 0)
				{
					txndcs = ABORT;
				}
				else
					Assert(false);

				std::string str_ts = res[i]["unix_timestamp(prepare_ts)"];
				std::string str_st = str_ts.substr(0, str_ts.rfind("."));
				std::string str_nt = str_ts.substr(str_ts.rfind(".")+1);
				Txn_ts prepts;
				if(!str_st.empty()) {
					time_t st = strtol(str_st.c_str(), &endptr, 10);
					Assert(endptr == NULL || *endptr == '\0');
					prepts.set_st(st);
				}
				if(!str_nt.empty()) {
					time_t nt = strtol(str_nt.c_str(), &endptr, 10);
					Assert(endptr == NULL || *endptr == '\0');
					prepts.set_nt(nt);
				}
				//time_t prepts = strtol(res[i][3], &endptr, 10);

				Txn_decision txn_dsn(ti, txndcs, prepts);
				process_prep_txns(txn_dsn, tk_itr->second, shard_txn_decisions);
			}
		}

abort_rest:
		for (auto &ti:clstr.second.tkis) {
			/*
			  The remaining not processed are those *recovered prepared* txns
			  whose global txn were not fully prepared (some branches not
			  prepared yet) and before their commit logs are written,the 2PC process
			  quit because the computing node crashed or a shard node crashed
			  or connection between client to computing node or computing node
			  to storage shard master were broken.
			  Such txn branches should be unconditionally aborted.
			*/
			if (ti.second.processed == false) {
				Txn_decision td(ti.first, ABORT, 0);
				process_prep_txns(td, ti.second, shard_txn_decisions);
			}
		}

		
		/*
		  set txn decisions to shard for worker thread to execute.
		*/
		for (auto entry : shard_txn_decisions)
		{
			std::get<0>(entry)->set_txn_end_decisions(std::get<1>(entry));
			//entry.first->set_txn_end_decisions(entry.second);
		}
	}

	return 0;
}

/*
  Add 'txn_dsn' into the Shard objects in ti.branches, such shard objects'
  txn-decisions are stored in shard_txn_decisions.
  With this function we can produce for each shard a list of txn branches and
  how to end it.
*/
static void process_prep_txns(const Shard::Txn_decision &txn_dsn,
	MetadataShard::txn_info &ti,
	std::vector<std::tuple<ObjectPtr<Shard>, Shard::Txn_end_decisions_t> >& shard_txn_decisions)
	//std::map<ObjectPtr<Shard>, Shard::Txn_end_decisions_t>&shard_txn_decisions)
{
	for (auto&ps:ti.branches) {
		//auto itr2 = shard_txn_decisions.find(ps);
		int exist_flag = 0;
		std::vector<std::tuple<ObjectPtr<Shard>, Shard::Txn_end_decisions_t> >::iterator itr2;
		for(std::vector<std::tuple<ObjectPtr<Shard>, Shard::Txn_end_decisions_t> >::iterator itr = shard_txn_decisions.begin();
						 itr != shard_txn_decisions.end(); itr++) {
			if(std::get<0>(*itr)->get_id() == ps->get_id()) {
				exist_flag = 1;
				itr2 = itr;
				break;
			}
		}

		if (!exist_flag) {
			Shard::Txn_end_decisions_t txn_decision;
			txn_decision.emplace_back(txn_dsn);
			std::tuple<ObjectPtr<Shard>, Shard::Txn_end_decisions_t> sxd{ps, txn_decision};
			shard_txn_decisions.emplace_back(sxd);
			//shard_txn_decisions.insert(std::make_pair(ps, txn_decision));
			//itr2 = shard_txn_decisions.find(ps);
			//Assert(itr2 != shard_txn_decisions.end());
		} else
			std::get<1>(*itr2).emplace_back(txn_dsn);
	}
	ti.processed = true;
}

bool Shard::WriteCommitLog(const Txn_decision& td) {
	if(id == -1)
		return true;

	ObjectPtr<MetadataShard> metashard = System::get_instance()->get_MetadataShard();
	if(!metashard.Invalid())
		return false;

	ObjectPtr<Shard_node> master = metashard->get_master();
	if(!master.Invalid())
		return false;

	std::string decision = (td.decision == COMMIT ? "commit":"abort");
    timespec time;
    clock_gettime(CLOCK_REALTIME, &time);
	uint64_t deadline = time.tv_sec + global_txn_commit_log_wait_max_sec;

	std::string sql_stmt = string_sprintf("insert into %s.commit_log_%s (comp_node_id, txn_id, next_txn_cmd) values (%u, %lu, (if (unix_timestamp() > %ld), NULL, '%s'))",
				KUNLUN_METADATA_DB_NAME, cluster_name.c_str(), td.tk.comp_nodeid, td.tk.local_txnid, deadline, decision.c_str());
	MysqlResult res;
	if(master->send_stmt(sql_stmt, &res, 1)) {
		KLOG_ERROR("write commit log to meta db failed, so quit to commit xa");
		return false;
	}
	return true;
}

int Shard::end_recovered_prepared_txns()
{
	KlWrapGuard<KlWrapMutex> guard(mtx);
	Txn_end_decisions_t txn_dcsns;
	take_txn_end_decisions(txn_dcsns);
	char txnid_buf[64];

	for (auto &td:txn_dcsns) {
		if(!WriteCommitLog(td))
			continue;

		int slen = snprintf(txnid_buf, sizeof(txnid_buf), "XA %s '%u-%ld-%u'",
			td.decision == COMMIT ? "COMMIT":"ROLLBACK",
			td.tk.comp_nodeid, td.tk.start_ts.st_, td.tk.local_txnid);
		Assert(slen < sizeof(txnid_buf));
		MysqlResult res;
		if(!cur_master.Invalid())
			continue;

		if(cur_master->send_stmt(txnid_buf, &res, stmt_retries)) {
			// current master gone, simply abandon remaining work, they can
			// be picked up again later on new master
			return -1;
		}
		KLOG_INFO("Ended prepared txn on shard({}.{} {}): {}",
			   cluster_name, name, id, txnid_buf);
	}

	return 0;
}

static void convert_preps2ti(ObjectPtr<Shard> ps, const Shard::Prep_recvrd_txns_t &preps,
	MetadataShard::cluster_txninfo &cti, bool is_meta_shard) {
	for (auto&tk:preps) {
		//time_t start_ts = tk.start_ts;
		Txn_ts start_ts = tk.start_ts;
		auto itr = cti.tkis.find(tk);
		if (itr != cti.tkis.end()) {
			itr->second.branches.emplace_back(ps);
		} else {
			MetadataShard::txn_info txninfo;
			txninfo.branches.emplace_back(ps);
			cti.tkis.insert(std::make_pair(tk, txninfo));
		}

		// there is no commit log for DDL txns, don't enlarge the scanned range in vain.
		if (is_meta_shard) continue;

		if (cti.min_start_ts > start_ts) cti.min_start_ts = start_ts;
		if (cti.max_start_ts < start_ts) cti.max_start_ts = start_ts;
	}
}


int Shard::get_xa_prepared() {
	Prep_recvrd_txns_t txns;
	std::string mip;
	int mport;

	{
		KlWrapGuard<KlWrapMutex> guard(mtx);
		if(!cur_master.Invalid())
			return 0;
		cur_master->get_ip_port(mip, mport);

		// we can only operate on recovered XA txns. if the connection still holds
		// the prepared txn, we can't operate on it in another connection.
		MysqlResult res;
		int ret = cur_master->send_stmt("select trx_xid from information_schema.innodb_trx where trx_xa_type='external_recvrd'", 
						&res, stmt_retries);
		if (ret)
			return ret;
	
		char *endptr = NULL;
	
		uint32_t nrows = res.GetResultLinesNum();
		for(uint32_t i=0; i<nrows; i++) {
			char* result = res[i]["trx_xid"];
			if(strcmp(result, "NULL") == 0)
				continue;

			//Assert(res[i][zero][0] == '\'' && res[i][zero][strlen(res[i][zero]) - 1] == '\'');
			char *xaks = res[i]["trx_xid"] + 1; // format e.g.: '1-1598596846-967098'
			char *sp1 = strchr(xaks, '-');
			char *sp2 = NULL;
			char *sp3 = NULL;
			if (sp1) sp2 = strchr(sp1 + 1, '-');
			if (sp2) sp3 = strchr(sp2 + 1, '-'); // sp3 should be NULL

			if (!(res[i]["trx_xid"][0] == '\'' && res[i]["trx_xid"][strlen(res[i]["trx_xid"]) - 1] == '\'') ||
				sp1 == NULL || sp2 == NULL || sp3) {
				KLOG_WARN("Got XA transaction ID {} from shard ({}.{}, {}) primary node({}, {}:{}), not under Kunlun DRDBMS control, and it's skipped.",
					res[i]["trx_xid"], this->cluster_name, this->name, this->id,
					cur_master->get_id(), mip, mport);
				continue;
			}
			Assert(sp1 && sp2 && !sp3);
			*sp1 = '\0';
			*sp2 = '\0';
			uint comp_nodeid = strtoul(xaks, &endptr, 10);
			Assert(endptr == NULL || *endptr == '\0');
			time_t start_ts = strtoul(sp1 + 1, &endptr, 10);
			Assert(endptr == NULL || *endptr == '\0');
			uint32_t local_txnid = strtoul(sp2 + 1, &endptr, 10);
			Assert(*endptr == '\'');// the trailing ', ie. the last char.
			Txn_key tk;
			tk.comp_nodeid = comp_nodeid;
			tk.start_ts = start_ts;
			tk.local_txnid = local_txnid;
			txns.emplace_back(tk);
			// restore content in case it's used elsewhere.
			*sp1 = '-';
			*sp2 = '-';
		}
	
		if (txns.size() == 0)
			goto end;
	}

	{
		KlWrapGuard<KlWrapMutex> guard(mtx_txninfo);
		prep_recvrd_txns.insert(prep_recvrd_txns.end(), txns.begin(), txns.end());
	}

	KLOG_INFO("Got {} prepared txns in shard ({}.{}, {}) primary node({}, {}:{}).",
		   txns.size(), this->cluster_name, this->name,
		   this->id, cur_master->get_id(), mip, mport);
end:
	return 0;
}


ObjectPtr<Shard_node> Shard::get_node_by_id(int id)
{
	ObjectPtr<Shard_node> sn;
	for (auto &n : nodes) {
		if (n->get_id() == id) {
			sn = n;
			break;
		}
	}
	return sn;
}

uint Shard::get_innodb_page_size() {
	if(innodb_page_size == 0) {
		KlWrapGuard<KlWrapMutex> guard(mtx);
		if(!cur_master.Invalid())
			return 0;

		MysqlResult res;
		int ret = cur_master->send_stmt(
						"show variables like 'innodb_page_size'", &res, stmt_retries);
		if (ret)
		   return 0;
		
		char *endptr = NULL;
		if(res.GetResultLinesNum() > 0) {
			innodb_page_size = strtol(res[0][1], &endptr, 10);
			Assert(endptr == NULL || *endptr == '\0');
		}
	}
	
	return innodb_page_size;
}

int MetadataShard::refresh_shard_backupnode_info(ObjectPtr<Shard> shard){
  char sql[2048] = {'\0'};
  snprintf(sql,2048,"select hostaddr as ip,port from kunlun_metadata_db.shard_nodes where shard_id = %d and backup_node = 'on'",shard->get_id());
  kunlun::MysqlResult result;
  int ret = cur_master->send_stmt(sql, &result);
  if(ret){
    KLOG_ERROR("refresh shard backup node info failed: {}",cur_master->get_mysql_err());
    return ret;
  }
  if(result.GetResultLinesNum() != 1){
    KLOG_ERROR("refresh shard backup node info failed, lines number is not 1 : {}",cur_master->get_mysql_err());
    return ret;
  }
  shard->set_backupnode_ip_port(result[0]["ip"], result[0]["port"]);
  return 0;
}

void MetadataShard::add_computer_cluster_memory(std::vector<ObjectPtr<KunlunCluster> > &kl_clusters, 
		int cluster_id, const std::string& cluster_name, const std::string& nick_name, 
		const std::vector<Comps_Type>& comps_params) {
	KlWrapGuard<KlWrapMutex> guard(mtx);
	ObjectPtr<KunlunCluster> pcluster;
	for (auto &kl : kl_clusters) {
		if(kl->get_id() == cluster_id) {
			pcluster = kl;
			break;
		}
	}

	int exist_flag = 0;
	if(!pcluster.Invalid()) {
		ObjectPtr<KunlunCluster> kc(new KunlunCluster(cluster_id, cluster_name));
		pcluster = kc;
		if(!nick_name.empty())
			pcluster->set_nick_name(nick_name);
	} else 
		exist_flag = 1;

	for(size_t i=0; i<comps_params.size(); i++) {
		std::string comp_name = "comp_"+std::to_string(i+1);
		ObjectPtr<Computer_node> pcomputer(new Computer_node(i+1, cluster_id, 
					std::get<1>(comps_params[i]), comp_name.c_str(), std::get<0>(comps_params[i]).c_str(), 
					std::get<2>(comps_params[i]).c_str(), std::get<3>(comps_params[i]).c_str()));
		pcluster->computer_nodes.emplace_back(pcomputer);
		KLOG_INFO("Added Computer({}, {}, {}) into protection.",
							pcluster->get_name(), pcomputer->id, pcomputer->name);
	}

	if(!exist_flag) {
		kl_clusters.emplace_back(pcluster);
		KLOG_INFO("Added KunlunCluster({}.{}) into protection.", cluster_name, cluster_id);
	}
}

void MetadataShard::add_shard_cluster_memory(std::vector<ObjectPtr<KunlunCluster> > &kl_clusters, 
			int cluster_id, const std::string& cluster_name, const std::string& nick_name, const std::string& ha_mode,
			const std::vector<Shard_Type>& shard_params) {
	KlWrapGuard<KlWrapMutex> guard(mtx);
	ObjectPtr<KunlunCluster> pcluster;
	for(auto kl : kl_clusters) {
		if(kl->get_id() == cluster_id) {
			pcluster = kl;
			break;
		}
	}
	
	int exist_flag = 0;
	if(!pcluster.Invalid()) {
		ObjectPtr<KunlunCluster> kc(new KunlunCluster(cluster_id, cluster_name));
		pcluster = kc;
		if(!nick_name.empty())
			pcluster->set_nick_name(nick_name);
	} else {
		exist_flag =1;
	}  

	HAVL_mode mode = HA_mgr;
	if(!ha_mode.empty()) {
		if(ha_mode == "no_rep")
			mode = HA_no_rep;
		else if(ha_mode == "mgr")
			mode = HA_mgr;
		else if(ha_mode == "rbr")
			mode = HA_rbr;
	}

	for(auto sp : shard_params) {
		int shardid = std::get<0>(sp);
		std::string shard_name = std::get<1>(sp);
		std::string user = std::get<2>(sp);
		std::string pwd = std::get<3>(sp);
		auto node_params = std::get<5>(sp);

		ObjectPtr<Shard> pshard(new Shard(shardid, shard_name, STORAGE, mode));
		pshard->set_fullsync_level(std::get<4>(sp));
		pshard->set_cluster_info(cluster_name, cluster_id);
		
		bool change = false;
		for(size_t i=0; i<node_params.size(); i++) {
			ObjectPtr<Shard_node> sn = pshard->refresh_node_configs(std::get<0>(node_params[i]), std::get<1>(node_params[i]).c_str(),
							std::get<2>(node_params[i]), user.c_str(), pwd.c_str(), 1, change);
			if(i == 0) {
				pshard->set_master(sn);
			}
			if(std::get<3>(node_params[i]))
				sn->set_rbr_sync_status(Rbr_SyncStatus::FSYNC);
			else
				sn->set_rbr_sync_status(Rbr_SyncStatus::ASYNC);
		}
		pcluster->storage_shards.emplace_back(pshard);
		KLOG_INFO("Added shard({}.{}, {}) into protection.",
					pshard->get_cluster_name(), pshard->get_name(),
					pshard->get_id());
	}

	if(!exist_flag) {
		kl_clusters.emplace_back(pcluster);
		KLOG_INFO("Added KunlunCluster({}.{}) into protection.", cluster_name, cluster_id);
	}
}

void MetadataShard::add_node_shard_memory(std::vector<ObjectPtr<KunlunCluster> > &kl_clusters, int cluster_id,
				int shard_id, const std::vector<CNode_Type>& node_params) {
	KlWrapGuard<KlWrapMutex> guard(mtx);
	ObjectPtr<KunlunCluster> pcluster;
	for(auto kl : kl_clusters) {
		if(kl->get_id() == cluster_id) {
			pcluster = kl;
			break;
		}
	}

	ObjectPtr<Shard> pshard;
	if(pcluster.Invalid()) {
		for(auto ss : pcluster->storage_shards) {
			if(ss->get_id() == shard_id) {
				pshard = ss;
				break;
			}
		}
	}

	if(pshard.Invalid()) {
		std::vector<ObjectPtr<Shard_node> > snode = pshard->get_nodes();
		std::string user = "pgx";
		std::string pwd = "pgx_pwd";
		if(snode.size() > 0) {
			snode[0]->get_user_pwd(user, pwd);
		}

		bool change = false;
		for(size_t i=0; i<node_params.size(); i++) {
			ObjectPtr<Shard_node> sn = pshard->refresh_node_configs(std::get<0>(node_params[i]), std::get<1>(node_params[i]).c_str(),
							std::get<2>(node_params[i]), user.c_str(), pwd.c_str(), 1, change);
							
			if(std::get<3>(node_params[i]))
				sn->set_rbr_sync_status(Rbr_SyncStatus::FSYNC);
			else
				sn->set_rbr_sync_status(Rbr_SyncStatus::ASYNC);
		}
	}
}

void MetadataShard::update_metadb_shard_master(const std::string nip, int nport,
									const std::string oip, int oport) {
	MysqlResult result;
	std::string sql;
	if(!nip.empty() && nport !=0)
		sql = string_sprintf("update %s.shard_nodes set member_state='source' where hostaddr='%s' and port=%d",
					KUNLUN_METADATA_DB_NAME, nip.c_str(), nport);
	
	if(!oip.empty() && oport !=0) 
		sql += string_sprintf("; update %s.shard_nodes set member_state='replica' where hostaddr='%s' and port=%d",
					KUNLUN_METADATA_DB_NAME, oip.c_str(), oport);
	KLOG_INFO("update shard_node member_state sql: {}", sql);
	if(!cur_master.Invalid()) {
		KLOG_ERROR("meta shard don't have master shard node");
		return;
	}

	bool ret = cur_master->send_stmt(sql, &result, stmt_retries);
	if(ret) 
		KLOG_ERROR("update shard_node member_state sql{} failed", sql);
}

void MetadataShard::adjust_shard_master(ObjectPtr<Shard> shard) {
	bool ret;
	MysqlResult result;

	int master_exist_flag = 0;
	std::string sql = "select host, port from performance_schema.replication_connection_configuration where channel_name='kunlun_repl'";
	ObjectPtr<Shard_node> master = shard->get_master();
	std::string m_ip;
	int m_port=0;
	if(master.Invalid()) {
		ret = master->send_stmt(sql, &result, stmt_retries);
		if(!ret) {
			if(result.GetResultLinesNum() == 0) {
				master->get_ip_port(m_ip, m_port);
			}
		}
	}

	for(auto sn : shard->get_nodes()) {
		if(!sn.Invalid())
			continue;

		std::string ip;
		int port=0;
		sn->get_ip_port(ip, port);
		if(ip == m_ip && port == m_port)
			continue;
		ret = sn->send_stmt(sql, &result, stmt_retries);
		if(ret)
			continue;
		
		if(result.GetResultLinesNum() == 1) {
			ip = result[0]["host"];
			port = atoi(result[0]["port"]);

			if(ip == m_ip && m_port == port) {
				master_exist_flag = 1;
				break;
			} else {
				ObjectPtr<Shard_node> new_master = shard->get_node_by_ip_port(ip, port);
				if(new_master.Invalid()) { 
					master_exist_flag = 1;
					shard->set_master(new_master);
					update_metadb_shard_master(ip, port, m_ip, m_port);
					m_ip = ip;
					m_port = port;
					break;
				}
			}
		}
	}

	if(!master_exist_flag)
		return;

	for(auto sn : shard->get_nodes()) {
		if(!sn.Invalid())
			continue;
		
		std::string ip;
		int port;
		sn->get_ip_port(ip, port);
		if(ip == m_ip && port == m_port)
			continue;

		if(!cur_master.Invalid())
			continue;

		sql = string_sprintf("select member_state from %s.shard_nodes where hostaddr='%s' and port=%d",
					KUNLUN_METADATA_DB_NAME, ip.c_str(), port);
		ret = cur_master->send_stmt(sql, &result, stmt_retries);
		if(ret)
			continue;
		if(result.GetResultLinesNum() != 1)
			continue;

		std::string member_state = result[0]["member_state"];
		if(member_state == "source") {
			sql = string_sprintf("update %s.shard_nodes set member_state='replica' where hostaddr='%s' and port=%d",
					KUNLUN_METADATA_DB_NAME, ip.c_str(), port);
			KLOG_INFO("update shard_nodes member_state sql: {}", sql);
			ret = cur_master->send_stmt(sql, &result, stmt_retries);
			if(ret)
				KLOG_ERROR("update host {}_{} member_state replica failed", ip, port);
		}
	}
}

/*
  Fetch storage shard nodes from metadata shard, and refresh Shard/Shard_info
  objects in storage_shards. Newly added shard nodes will be added into
  storage_shards and obsolete nodes that no longer registered in shard nodes
  will be destroyed. If an existing node's connection info changes, existing
  mysql connection will be closed and connected again using new info.
  Call this repeatedly to refresh storage shard topology periodically.
*/
int MetadataShard::refresh_shards(std::vector<ObjectPtr<KunlunCluster> > &kl_clusters) {
	if(!cur_master.Invalid()) {
		KLOG_ERROR("meta shard no master node");
		return -1;
	}

	MysqlResult res;
	int ret = cur_master->send_stmt(
	"select t1.id as shard_id, t1.name as shard_name, t2.id, hostaddr, port, user_name, passwd, t3.name as cluster_name, t3.nick_name, t3.id as cluster_id, t3.ha_mode, t2.master_priority, t2.member_state, t2.sync_state from shards t1, shard_nodes t2, db_clusters t3 where t2.shard_id = t1.id and t3.id=t1.db_cluster_id and t2.status!='inactive' order by t1.id", 
						&res, stmt_retries);
	if (ret) {
		KLOG_ERROR("get shards from meta failed: {}", cur_master->get_mysql_err());
		return ret;
	}

	char *endptr = NULL;
	std::map<std::tuple<int, int, int>, ObjectPtr<Shard_node> > sdns;
	
	//std::set<std::string> alterant_node_ip; //for notify node_mgr
	uint32_t nrows = res.GetResultLinesNum();
	for(uint32_t i=0 ; i<nrows; i++) {
		int shardid = strtol(res[i]["shard_id"], &endptr, 10);
		Assert(endptr == NULL || *endptr == '\0');
		int cluster_id = strtol(res[i]["cluster_id"], &endptr, 10);
		Assert(endptr == NULL || *endptr == '\0');
		uint master_priority = strtol(res[i]["master_priority"], &endptr, 10);
    	Assert(endptr == NULL || *endptr == '\0');
		int nodeid = strtol(res[i]["id"], &endptr, 10);
		Assert(endptr == NULL || *endptr == '\0');
		int port = strtol(res[i]["port"], &endptr, 10);
		Assert(endptr == NULL || *endptr == '\0');

		ObjectPtr<KunlunCluster> pcluster;
		for (auto &cluster: kl_clusters) {
			if (cluster->get_id() == cluster_id) {
				pcluster = cluster;
				break;
			}
		}

		if (!pcluster.Invalid()) {
			ObjectPtr<KunlunCluster> kc(new KunlunCluster(cluster_id, res[i]["cluster_name"]));
			pcluster = kc;
			if(res[i]["nick_name"] != NULL)
				pcluster->set_nick_name(res[i]["nick_name"]);
			kl_clusters.emplace_back(pcluster);
			KLOG_INFO("Added KunlunCluster({}.{}) into protection.", res[i]["cluster_name"], cluster_id);
		}

		ObjectPtr<Shard> pshard;
		for (auto &ssd : pcluster->storage_shards) {
			if (ssd->get_id() == shardid) {
				pshard = ssd;
				break;
			}
		}
		
		if (!pshard.Invalid()) {
			//set ha_mode for maintenance
			HAVL_mode ha_mode = HA_mgr;
			if(res[i]["ha_mode"]!=NULL) {
				if(strcmp("no_rep", res[i]["ha_mode"]) == 0)
					ha_mode = HA_no_rep;
				else if(strcmp("mgr", res[i]["ha_mode"]) == 0)
					ha_mode = HA_mgr;
				else if(strcmp("rbr", res[i]["ha_mode"]) == 0)
					ha_mode = HA_rbr;
			}
			
			ObjectPtr<Shard> sd(new Shard(shardid, res[i]["shard_name"], STORAGE, ha_mode));
			pshard = sd;
			pshard->set_cluster_info(res[i]["cluster_name"], cluster_id);
			pcluster->storage_shards.emplace_back(pshard);
			KLOG_INFO("Added shard({}.{}, {}) into protection.",
				pshard->get_cluster_name(), pshard->get_name(),
				pshard->get_id());
		}
		else if (pshard->get_cluster_name() != std::string(res[i]["cluster_name"]))
			pshard->update_cluster_name(res[i]["cluster_name"], cluster_id);

		/*
		  Iterating a storage shard's rows and a metashard's result, so every
		  node's conn can be closed if params changed.
		*/
		bool changed = false;
		//Shard_node *n = pshard->get_node_by_id(nodeid);
		ObjectPtr<Shard_node> sn = pshard->refresh_node_configs(nodeid, 
					res[i]["hostaddr"], port, res[i]["user_name"], res[i]["passwd"], 
					master_priority, changed);
		
		if (changed) pshard->get_node_by_id(nodeid)->close_conn();

		//if(n == NULL || changed)
		//	alterant_node_ip.insert(res[i][3]);
		
		ObjectPtr<Shard_node> up_node = pshard->get_node_by_id(nodeid);
		if(up_node.Invalid()) {
			std::string member_state = res[i]["member_state"];
			if(member_state == "source")
				pshard->set_master(up_node);

			std::string rbr_status = res[i]["sync_state"];
			Rbr_SyncStatus sync_status = Rbr_SyncStatus::UNSYNC;
			if(rbr_status == "fsync")
				sync_status = Rbr_SyncStatus::FSYNC;
			else
				sync_status = Rbr_SyncStatus::ASYNC;

			up_node->set_rbr_sync_status(sync_status);
		}

		if(pshard->get_mode() == Shard::HA_no_rep) {
			if(pshard->get_nodes().size()>0)
				pshard->set_master(pshard->get_nodes()[0]);
		}

		// remove nodes that still exist.
		sdns.erase(std::make_tuple(cluster_id, shardid, nodeid));
	}

	for(auto cl : kl_clusters) {
		for(auto sd : cl->storage_shards) {
			if(sd->get_mode() == HA_rbr) {
				adjust_shard_master(sd);
			}
		}
	}

	// Remove shard nodes that are no longer in the shard, they are all left in sdns.
	for (auto &i:sdns) {
		ObjectPtr<Shard> pshard = i.second->get_owner();
		std::string rip;
		int rport;
		i.second->get_ip_port(rip, rport);

		//alterant_node_ip.insert(rip);

		KLOG_INFO("Removed shard({}.{}, {}) node ({}:{}, {}) from protection since it's not in cluster registration anymore.",
			pshard->get_cluster_name(), pshard->get_name(),
			pshard->get_id(), rip, rport, i.second->get_id());

		pshard->remove_node(i.second->get_id());
	}

	//if(alterant_node_ip.size() != 0)
	//	Machine_info::get_instance()->notify_node_update(alterant_node_ip, 1);

	//update shard fullsync_level and degrade_state
	ret = cur_master->send_stmt("select * from shards", 
					&res, stmt_retries);
	if(ret) {
		KLOG_ERROR("get shards from meta failed: {}", cur_master->get_mysql_err());
		return ret;
	}
	nrows = res.GetResultLinesNum();
	for(uint32_t i=0 ; i<nrows; i++) {
		std::string id = res[i]["id"];
		std::string cluster_id = res[i]["db_cluster_id"];
		for (auto &cluster:kl_clusters) {
			if(cluster->get_id() == atoi(cluster_id.c_str())) {
				for (auto &ssd : cluster->storage_shards) {
					if(ssd->get_id() == atoi(id.c_str())) {
						std::string sync_num = res[i]["sync_num"];
						ssd->set_fullsync_level(atoi(sync_num.c_str()));
						bool conf_state = (strcmp(res[i]["degrade_conf_state"],"ON") == 0 ? true : false);
						ssd->set_degrade_conf_state(conf_state);
						std::string conf_time = res[i]["degrade_conf_time"];
						ssd->set_degrade_conf_time(atoi(conf_time.c_str()));
						bool run_state = (strcmp(res[i]["degrade_run_state"],"ON") == 0 ? true : false);
						ssd->set_degrade_run_state(run_state);

						std::string coldbackup_period = res[i]["coldbackup_period"];
						bool ret1 = ssd->coldback_time_trigger_.InitOrRefresh(coldbackup_period);
						if(!ret1){
						KLOG_INFO("coldback_time_trigger init failed {}",ssd->coldback_time_trigger_.getErr());
						}
						else{
						KLOG_INFO("coldback_time_trigger init success ");
						}
						refresh_shard_backupnode_info(ssd);
						KLOG_INFO("shard backupnode info is {}",ssd->get_bacupnode_ip_port());
					}
				}
			}
		}
	}

	return 0;
}

/*
  Fetch computer nodes from metadata shard, and refresh computer_nodes. 
  Newly added computer nodes will be added into computer_nodes and 
  obsolete nodes that no longer registered in computer nodes will be destroyed. 
  Call this repeatedly to refresh computer_nodes topology periodically.
*/
int MetadataShard::refresh_computers(std::vector<ObjectPtr<KunlunCluster> > &kl_clusters) {
	//std::set<std::string> alterant_node_ip;	//for notify node_mgr

	int ret;
	MysqlResult res;
	char *endptr = NULL;
	
	std::string str_sql;

	for (auto &cluster:kl_clusters) {
		if(cluster->get_cluster_delete_state())
			continue;

		str_sql = "select id,name,hostaddr,port,user_name,passwd ,coldbackup_period from comp_nodes where status!='inactive' and db_cluster_id=" 
					+ std::to_string(cluster->get_id());

		ret = cur_master->send_stmt(str_sql, &res, stmt_retries);
		if (ret)
			return ret;
		
		uint32_t nrows = res.GetResultLinesNum();

		std::map<uint, ObjectPtr<Computer_node> > sdns;
		for (auto &i:cluster->computer_nodes)
			sdns[i->id] = i;
		
		for(uint32_t i=0; i<nrows; i++) {
			uint compid = strtol(res[i]["id"], &endptr, 10);
			Assert(endptr == NULL || *endptr == '\0');
			int port = strtol(res[i]["port"], &endptr, 10);
			Assert(endptr == NULL || *endptr == '\0');

			ObjectPtr<Computer_node> pcomputer;

			for (auto &computer:cluster->computer_nodes) {
				if (computer->id == compid) {
					pcomputer = computer;
					break;
				}
			}
			
			if (!pcomputer.Invalid()) {
				ObjectPtr<Computer_node> cn(new Computer_node(compid, cluster->get_id(), port, res[i][1], res[i][2], res[i][4], res[i][5]));
				pcomputer = cn;
        		pcomputer->coldbackup_timer.InitOrRefresh(res[i]["coldbackup_period"]);
				cluster->computer_nodes.emplace_back(pcomputer);
				KLOG_INFO("Added Computer({}, {}, {}) into protection.",
							cluster->get_name(), pcomputer->id, pcomputer->name);

				//alterant_node_ip.insert(res[i][2]);
			} else {
				if(pcomputer->refresh_node_configs(port, res[i]["name"], res[i]["hostaddr"], 
								res[i]["user_name"], res[i]["passwd"],res[i]["coldbackup_period"]));
					//alterant_node_ip.insert(res[i]["hostaddr"]);
			}

			// remove nodes that still exist.
			sdns.erase(compid);
		}

		// Remove computer nodes that are no longer in the computer_nodes, they are all left in sdns.
		for (auto &i:sdns) {
			std::string ip;
			int port;

			i.second->get_ip_port(ip, port);
			//alterant_node_ip.insert(ip);
			
			for(auto it=cluster->computer_nodes.begin(); it!=cluster->computer_nodes.end(); ) {
				if ((*it)->id == i.first) {
					//delete *it;
					it = cluster->computer_nodes.erase(it);
					break;
				}
				else
					it++;
			}
		}
	}

	//if(alterant_node_ip.size() != 0)
	//	Machine_info::get_instance()->notify_node_update(alterant_node_ip, 2);

	return 0;
}

/*
  get max cluster id from metadata tables
  @retval 0 succeed;
  		  1 fail;
*/
int MetadataShard::get_max_cluster_id(int &cluster_id)
{
	KlWrapGuard<KlWrapMutex> guard(mtx);

	if(!cur_master.Invalid())
		return 1;

	MysqlResult res;
	int ret = cur_master->send_stmt("select max(id) from db_clusters", &res, stmt_retries);
	//int ret = cur_master->send_stmt(SQLCOM_SELECT, CONST_STR_PTR_LEN("select max(id) from db_clusters"), stmt_retries);	
	if (ret==0)
	{
		if(res.GetResultLinesNum() > 0) {
			if(res[0]["max(id)"] != NULL)
				cluster_id = atoi(res[0]["max(id)"]);
		}
	}

	return ret;
}

/*
  execute metadate opertation
  @retval 0 succeed;
  		  1 fail;
*/
int MetadataShard::execute_metadate_opertation(enum_sql_command command, const std::string & str_sql)
{
	KlWrapGuard<KlWrapMutex> guard(mtx);

	if(!cur_master.Invalid())
		return 1;

	MysqlResult res;
	int ret = cur_master->send_stmt(str_sql, &res, stmt_retries);
	
	return ret;
}

/*
  delete cluster from metadata
  @retval 0 succeed;
  		  1 fail;
*/
int MetadataShard::delete_cluster_from_metadata(std::string & cluster_name)
{
	KlWrapGuard<KlWrapMutex> guard(mtx);

	if(!cur_master.Invalid())
		return 1;

	//get cluster_id
	std::string cluster_id = get_clusterid(cluster_name);
	if(cluster_id.length()==0)
		return 1;

	//get comp_id
	std::vector<std::string> vec_comp_id = get_compid(cluster_id);

	MysqlResult res;
	//remove cluster_backups
	std::string str_sql = "delete from cluster_backups where cluster_id=" + cluster_id;
	cur_master->send_stmt(str_sql, &res, stmt_retries);
	//cur_master->send_stmt(SQLCOM_DELETE, str_sql.c_str(), str_sql.length(), stmt_retries);

	//remove cluster_shard_backup_restore_log
	str_sql = "delete from cluster_shard_backup_restore_log where cluster_id=" + cluster_id;
	cur_master->send_stmt(str_sql, &res, stmt_retries);
	//cur_master->send_stmt(SQLCOM_DELETE, str_sql.c_str(), str_sql.length(), stmt_retries);

	//drop table commit_log_cluster_name
	str_sql = "drop table commit_log_" + cluster_name;
	cur_master->send_stmt(str_sql, &res, stmt_retries);
	//cur_master->send_stmt(SQLCOM_DROP_TABLE, str_sql.c_str(), str_sql.length(), stmt_retries);

	//drop table ddl_ops_log_cluster_name
	str_sql = "drop table ddl_ops_log_" + cluster_name;
	cur_master->send_stmt(str_sql, &res, stmt_retries);
	//cur_master->send_stmt(SQLCOM_DROP_TABLE, str_sql.c_str(), str_sql.length(), stmt_retries);

	//remove comp_nodes
	str_sql = "delete from comp_nodes where db_cluster_id=" + cluster_id;
	cur_master->send_stmt(str_sql, &res, stmt_retries);
	//cur_master->send_stmt(SQLCOM_DELETE, str_sql.c_str(), str_sql.length(), stmt_retries);

	//remove comp_nodes_id_seq
	for(auto &comp_id: vec_comp_id)
	{
		str_sql  = "delete from comp_nodes_id_seq where id=" + comp_id;
		cur_master->send_stmt(str_sql, &res, stmt_retries);
		//cur_master->send_stmt(SQLCOM_DELETE, str_sql.c_str(), str_sql.length(), stmt_retries);
	}

	//remove shard_nodes
	str_sql = "delete from shard_nodes where db_cluster_id=" + cluster_id;
	cur_master->send_stmt(str_sql, &res, stmt_retries);
	//cur_master->send_stmt(SQLCOM_DELETE, str_sql.c_str(), str_sql.length(), stmt_retries);

	//remove shards
	str_sql = "delete from shards where db_cluster_id=" + cluster_id;
	cur_master->send_stmt(str_sql, &res, stmt_retries);
	//cur_master->send_stmt(SQLCOM_DELETE, str_sql.c_str(), str_sql.length(), stmt_retries);

	//remove db_clusters
	str_sql = "delete from db_clusters where id=" + cluster_id;
	cur_master->send_stmt(str_sql, &res, stmt_retries);
	//cur_master->send_stmt(SQLCOM_DELETE, str_sql.c_str(), str_sql.length(), stmt_retries);

	return 0;
}



/*
  update_instance_status to metadata table
  @retval 0 succeed;
  		  1 fail;
*/
int MetadataShard::update_instance_status(Tpye_Ip_Port &ip_port, std::string &status, int &type)
{
	KlWrapGuard<KlWrapMutex> guard(mtx);

	if(!cur_master.Invalid())
		return 1;

	std::string str_sql;
	int ret;

	str_sql = "select status from shard_nodes where hostaddr='" + ip_port.first + "' and port=" + std::to_string(ip_port.second);
	MysqlResult res;
	ret = cur_master->send_stmt(str_sql, &res, stmt_retries);
	//ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		//MYSQL_RES *result = cur_master->get_result();
		int num_rows = res.GetResultLinesNum(); //(int)mysql_num_rows(result);
		//cur_master->free_mysql_result();

		if(num_rows==1)
		{
			type = 1;

			str_sql = "update shard_nodes set status='" + status + "' where hostaddr='" + ip_port.first + "' and port=" + std::to_string(ip_port.second);
			return cur_master->send_stmt(str_sql, &res, stmt_retries);
			//return cur_master->send_stmt(SQLCOM_UPDATE, str_sql.c_str(), str_sql.length(), stmt_retries);
		}
	}

	str_sql = "select status from comp_nodes where hostaddr='" + ip_port.first + "' and port=" + std::to_string(ip_port.second);
	ret = cur_master->send_stmt(str_sql, &res, stmt_retries);
	//ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		//MYSQL_RES *result = cur_master->get_result();
		int num_rows = res.GetResultLinesNum();//(int)mysql_num_rows(result);
		//cur_master->free_mysql_result();

		if(num_rows==1)
		{
			type = 2;

			str_sql = "update comp_nodes set status='" + status + "' where hostaddr='" + ip_port.first + "' and port=" + std::to_string(ip_port.second);
			return cur_master->send_stmt(str_sql, &res, stmt_retries);
			//return cur_master->send_stmt(SQLCOM_UPDATE, str_sql.c_str(), str_sql.length(), stmt_retries);
		}
	}

	return ret;
}

/*
  get_backup_storage_string from metadata table
  @retval 0 succeed;
  		  1 fail;
*/
int MetadataShard::get_backup_storage_string(std::string &name, std::string &backup_storage_id, std::string &backup_storage_str)
{
	KlWrapGuard<KlWrapMutex> guard(mtx);

	if(!cur_master.Invalid())
		return 1;

	int ret = 1;

	//get backup_storage from first record, but no name
	std::string str_sql = "select id,conn_str from backup_storage";
	MysqlResult res;
	ret = cur_master->send_stmt(str_sql, &res, stmt_retries);
	//ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		ret = 1;
		if(res.GetResultLinesNum() > 0) {
			ret = 0;
			backup_storage_id = res[0]["id"];
			backup_storage_str = res[0]["conn_str"];
		}
	}

	return ret;
}

/*
  get_backup_storage_list from metadata table
  @retval 0 succeed;
  		  1 fail;
*/
int MetadataShard::get_backup_storage_list(std::vector<Tpye_string4> &vec_t_string4)
{
	KlWrapGuard<KlWrapMutex> guard(mtx);

	if(!cur_master.Invalid())
		return 1;

	int ret = 1;

	//get cluster_id
	std::string str_sql = "select name,stype,hostaddr,port from backup_storage";
	MysqlResult res;
	ret = cur_master->send_stmt(str_sql, &res, stmt_retries);
	//ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		uint32_t nrows = res.GetResultLinesNum();
		for(uint32_t i=0; i<nrows; i++) {
			Tpye_string4 t_string4;
			std::get<0>(t_string4) = res[i]["name"];
			std::get<1>(t_string4) = res[i]["stype"];
			std::get<2>(t_string4) = res[i]["hostaddr"];
			std::get<3>(t_string4) = res[i]["port"];
			vec_t_string4.emplace_back(t_string4);
		}
	}

	return ret;
}

/*
  get cluster_backups from metadata table 
  @retval 0 succeed;
  		  1 fail;
*/
int MetadataShard::get_backup_info_from_metadata(std::string &cluster_name, std::string &timestamp, std::vector<std::string> &vec_shard)
{
	KlWrapGuard<KlWrapMutex> guard(mtx);

	if(!cur_master.Invalid())
		return 1;

	int ret;
	std::string str_sql;

	//get cluster_id
	std::string cluster_id = get_clusterid(cluster_name);
	if(cluster_id.length()==0)
		return 1;

	str_sql = "select name from cluster_backups where cluster_id=" + cluster_id;
	str_sql += " and end_ts<='" + timestamp + "' order by end_ts desc limit 1";
	MysqlResult res;
	ret = cur_master->send_stmt(str_sql, &res, stmt_retries);
	//ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		uint32_t nrows = res.GetResultLinesNum();

		//MYSQL_RES *result = cur_master->get_result();
		//MYSQL_ROW row;

		//if ((row = mysql_fetch_row(result)))
		if(nrows > 0)
		{
			const char *cStart, *cEnd;
			std::string shard_name;

			cStart = res[0]["name"];
			while(*cStart!='\0')
			{
				cEnd = strchr(cStart, ';');
				if(cEnd == NULL)
				{
					shard_name = std::string(cStart, strlen(cStart));
					if(shard_name.length())
						vec_shard.emplace_back(shard_name);
					break;
				}
				else
				{
					shard_name = std::string(cStart, cEnd-cStart);
					if(shard_name.length())
						vec_shard.emplace_back(shard_name);
					cStart = cEnd+1;
				}
			}

		}
		//cur_master->free_mysql_result();
	}

	if(vec_shard.size()==0)
		return 1;

	return 0;
}

/*
  get cluster_info from metadata table 
  @retval 0 succeed;
  		  1 fail;
*/
int MetadataShard::get_cluster_info_from_metadata(std::string &cluster_name, std::string &json_buf)
{
	KlWrapGuard<KlWrapMutex> guard(mtx);

	if(!cur_master.Invalid())
		return 1;

	int ret;
	std::string str_sql;

	str_sql = "select memo from db_clusters where name='" + cluster_name + "'";
	MysqlResult res;
	ret = cur_master->send_stmt(str_sql, &res, stmt_retries);
	//ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		if(res.GetResultLinesNum() > 0) {
			if(res[0]["memo"] != NULL)
				json_buf = res[0]["memo"];
		}
	}

	if(json_buf.length()==0)
		return 1;

	return 0;
}

/*
  get machine_info from metadata table 
  @retval 0 succeed;
  		  1 fail;
*/
int MetadataShard::get_machine_info_from_metadata(std::vector<std::string> &vec_machine)
{
	KlWrapGuard<KlWrapMutex> guard(mtx);

	if(!cur_master.Invalid())
		return 1;

	int ret;
	std::string str_sql;

	str_sql = "select hostaddr from server_nodes";
	MysqlResult res;
	ret = cur_master->send_stmt(str_sql, &res, stmt_retries);
	//ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		uint32_t nrows = res.GetResultLinesNum();
		for(uint32_t i=0; i<nrows; i++) {
			if(strcmp(res[i]["hostaddr"], "pseudo_server_useless") == 0)
				continue;
			vec_machine.emplace_back(res[i]["hostaddr"]);
		}
		
	}

	return 0;
}

/*
  get server_nodes_stats_id from metadata table 
  @retval 0 succeed;
  		  1 fail;
*/
int MetadataShard::get_server_nodes_stats_id(std::string &hostaddr, std::string &id){
	KlWrapGuard<KlWrapMutex> guard(mtx);

	if(!cur_master.Invalid())
		return 1;

	int ret;
	std::string str_sql;

	str_sql = "select id from server_nodes where hostaddr='" + hostaddr + "'";
	MysqlResult res;
	ret = cur_master->send_stmt(str_sql, &res, stmt_retries);
	//ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		if(res.GetResultLinesNum() == 1)
			id = res[0]["id"];
		else
			ret = 1;
	}

	return ret;
}

/*
  check backup storage name from metadata table 
  @retval 0 succeed;
  		  1 fail;
*/
int MetadataShard::check_backup_storage_name(std::string &name)
{
	KlWrapGuard<KlWrapMutex> guard(mtx);

	if(!cur_master.Invalid())
		return 1;

	std::string str_sql = "select id from backup_storage where name='" + name + "'";
	MysqlResult res;
	int ret = cur_master->send_stmt(str_sql, &res, stmt_retries);
	//int ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		ret = 1;
		if(res.GetResultLinesNum() > 0)
			ret = 0;
	}

	return ret;
}

/*
  check machine hostaddr from metadata table 
  @retval 0 succeed;
  		  1 fail;
*/
int MetadataShard::check_machine_hostaddr(std::string &hostaddr)
{
	KlWrapGuard<KlWrapMutex> guard(mtx);

	if(!cur_master.Invalid())
		return 1;

	std::string str_sql = "select id from server_nodes where hostaddr='" + hostaddr + "'";
	MysqlResult res;
	int ret = cur_master->send_stmt(str_sql, &res, stmt_retries);
	//int ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		ret = 1;
		if(res.GetResultLinesNum() > 0)
			ret = 0;
	}

	return ret;
}

/*
  check cluster name from metadata table 
  @retval 0 succeed;
  		  1 fail;
*/
int MetadataShard::check_cluster_name(std::string &cluster_name)
{
	KlWrapGuard<KlWrapMutex> guard(mtx);

	if(!cur_master.Invalid())
		return 1;

	std::string str_sql = "select id from db_clusters where name='" + cluster_name + "'";
	MysqlResult res;
	int ret = cur_master->send_stmt(str_sql, &res, stmt_retries);
	//int ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		ret = 1;
		if(res.GetResultLinesNum() > 0)
			ret = 0;
	}

	return ret;
}

/*
  check cluster name from metadata table 
  @retval 0 succeed;
  		  1 fail;
*/
int MetadataShard::check_nick_name(std::string &nick_name)
{
	KlWrapGuard<KlWrapMutex> guard(mtx);

	if(!cur_master.Invalid())
		return 1;

	std::string str_sql = "select id from db_clusters where nick_name='" + nick_name + "'";
	MysqlResult res;
	int ret = cur_master->send_stmt(str_sql, &res, stmt_retries);
	//int ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		ret = 1;
		if(res.GetResultLinesNum() > 0)
			ret = 0;
	}

	return ret;
}


std::vector<std::string> Shard::get_compid(const std::string& cluster_id) {
	std::vector<std::string> comp_id;
	if(!cur_master.Invalid())
		return comp_id;

	std::string str_sql = "select id from comp_nodes where db_cluster_id=" + cluster_id;
	MysqlResult res;
	int ret = cur_master->send_stmt(str_sql, &res, stmt_retries);
	//int ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		uint32_t nrows = res.GetResultLinesNum();
		for(uint32_t i=0; i<nrows; i++) {
			if(res[i]["id"] != NULL)
				comp_id.emplace_back(res[i]["id"]);
		}
	}
	return comp_id;
}

/*
* get cluster id by cluster_name;
*/
std::string Shard::get_clusterid(const std::string& cluster_name) {
	std::string cluster_id;
	if(!cur_master.Invalid())
		return cluster_id;
	
	std::string str_sql = "select id from db_clusters where name='" + cluster_name + "'";
	MysqlResult res;
	int ret = cur_master->send_stmt(str_sql, &res, stmt_retries);
	//int ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		uint32_t nrows = res.GetResultLinesNum();
		for(uint32_t i=0; i<nrows; i++) {
			if(res[i]["id"] != NULL)
				cluster_id = res[i]["id"];
		}
	
	}
	return cluster_id;
}

std::string Shard::get_shardid(const std::string& cluster_id, const std::string& shard_name) {
	std::string shard_id;
	if(!cur_master.Invalid())
		return shard_id;

	std::string str_sql = "select id from shards where db_cluster_id=" + cluster_id + " and name='" + shard_name + "'";
	MysqlResult res;
	int ret = cur_master->send_stmt(str_sql, &res, stmt_retries);
	//int ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		uint32_t nrows = res.GetResultLinesNum();
		for(uint32_t i=0; i<nrows; i++) {
			if(res[i]["id"] != NULL)
				shard_id = res[i]["is"];
		}
	}
	return shard_id;
}


void MetadataShard::update_shard_delete_state(uint cluster_id, const std::vector<int>& del_shards) {
	KlWrapGuard<KlWrapMutex> guard(mtx);
	if(!cur_master.Invalid())
		return;

	MysqlResult res;	
	std::string sql = string_sprintf("delete from node_map_master where cluster_id=%d", cluster_id);
	cur_master->send_stmt(sql, &res, stmt_retries);

	std::vector<ObjectPtr<Shard> > cluster_shards = System::get_instance()->get_shard_by_cluster_id(cluster_id);
	for(auto shard : cluster_shards) {
		if(!shard.Invalid())
			continue;
		
		int shardid = shard->get_id();
		int exist_flag = 0;
		for(size_t i=0; i<del_shards.size(); i++) {
			if(del_shards[i] == shardid) {
				exist_flag = 1;
				break;
			}
		}
		
		if(exist_flag)
			shard->set_is_delete(true);
	}
}


/*
  Query meta shard node sn to fetch all meta shard nodes from its
  meta_db_nodes table, and refresh the shard nodes contained in this object.
  This method is can be called repeatedly to refresh metashard nodes periodically.
  @param is_master whether 'sn' is current master node,
    if so 'master_ip' and 'master_port' are 0;
	otherwise, 'master_ip' and 'master_port' are the current master's conn info.
*/
int MetadataShard::fetch_meta_shard_nodes(ObjectPtr<Shard_node> sn, bool is_master,
	const char *master_ip, int master_port) {
	KlWrapGuard<KlWrapMutex> guard(mtx);
	Assert(nodes.size() > 0); // sn should have been added already.

	MysqlResult res;
	int ret = sn->send_stmt(
		"select id, hostaddr, port, user_name, passwd, master_priority, member_state, sync_state from meta_db_nodes", &res, stmt_retries);
	if(ret) {
		KLOG_ERROR("select meta_db_nodes failed: {}", sn->get_mysql_err());
		return ret;
	}

	//std::set<std::string> alterant_node_ip;	//for notify node_mgr

	char *endptr = NULL;
	bool close_snconn = false;

	std::map<uint, ObjectPtr<Shard_node> > snodes;
	for (auto &i:nodes) {
		snodes.insert(std::make_pair(i->get_id(), i));
	} 

	uint32_t nrows = res.GetResultLinesNum();
	for(uint32_t i=0; i<nrows; i++) {
		int port = strtol(res[i]["port"], &endptr, 10);
		Assert(endptr == NULL || *endptr == '\0');
		uint nodeid = strtol(res[i]["id"], &endptr, 10);
		Assert(endptr == NULL || *endptr == '\0');
		uint master_priority = strtol(res[i]["master_priority"], &endptr, 10);
    	Assert(endptr == NULL || *endptr == '\0');
		/*
		  config file has no meta svr node id, need to set it before below
		  erase.
		*/

		std::string sync_state = res[i]["sync_state"];
		if (meta_svr_ip == res[i]["hostaddr"] && meta_svr_port == port &&
			sn->matches_ip_port(meta_svr_ip, meta_svr_port))
		{
			snodes.erase(sn->get_id());
			Rbr_SyncStatus sync_status = Rbr_SyncStatus::UNSYNC;
			if(sync_state == "ON")
				sync_status = Rbr_SyncStatus::FSYNC;
			else 
				sync_status = Rbr_SyncStatus::ASYNC;
			sn->set_rbr_sync_status(sync_status);
			sn->set_id(nodeid);
		}
		
		snodes.erase(nodeid);

		bool changed = false;
		
		ObjectPtr<Shard_node> node =
			refresh_node_configs(nodeid, res[i]["hostaddr"], port, 
							res[i]["user_name"], res[i]["passwd"], master_priority, changed);
		
		Rbr_SyncStatus sync_status = Rbr_SyncStatus::UNSYNC;
		if(sync_state == "ON")
			sync_status = Rbr_SyncStatus::FSYNC;
		else 
			sync_status = Rbr_SyncStatus::ASYNC;
		node->set_rbr_sync_status(sync_status);
		// need to close sn's conn, but not now for sn since we are iterating them.
		if (node->get_id() == sn->get_id() && changed)
			close_snconn = true;
		else if (changed)
			node->close_conn();

		//if(n == NULL || changed)
		//	alterant_node_ip.insert(res[i]["hostaddr"]);

		if (!is_master && master_ip && strcmp(master_ip, res[i]["hostaddr"]) == 0 &&
			master_port == port && set_master(node)) {
			/*
			  sn isn't the master, here we have the master, create it and
			  we will connect to it to create the rest nodes.
			*/
			KLOG_INFO("Found primary node of shard({}.{}, {}) to be ({}:{}, {})",
			   get_cluster_name(), get_name(),
			   get_id(), master_ip, master_port, node->get_id());
		}
	}

	if (close_snconn) sn->close_conn();

	/*
	  Remove nodes that are no longer registered in the metadata shard.
	*/
	for (auto &i:snodes) {
		ObjectPtr<Shard> ps = i.second->get_owner();
		ObjectPtr<Shard_node> pn = i.second;

		std::string snip;
		int snport;
		pn->get_ip_port(snip, snport);

		//alterant_node_ip.insert(snip);

		KLOG_INFO("Removed shard({}.{}, {}) node({}:{}, {}) which no longer belong to the meta shard.",
			   ps->get_cluster_name(), ps->get_name(),
			   ps->get_id(), snip, snport, pn->get_id());
		remove_node(pn->get_id());
	}

	//if(alterant_node_ip.size() != 0)
	//	Machine_info::get_instance()->notify_node_update(alterant_node_ip, 0);

	return 0;
}

void Shard::maintenance() {
	int ret = 0;
	HAVL_mode mode = get_mode();
	if(mode == HAVL_mode::HA_mgr)
		ret = check_mgr_cluster();
	
	// if ret not 0, master node isn't uniquely resolved or running.
	if (ret == 0) {
		end_recovered_prepared_txns();
		get_xa_prepared();
	} else
		KLOG_WARN("Got error {} from check_mgr_cluster() in shard ({}.{}, {}), skipping prepared txns.",
			   ret, get_cluster_name(), get_name(), get_id());
	//set_thread_handler(NULL);
}
