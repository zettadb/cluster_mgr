/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "sys_config.h"
#include "global.h"
#include "config.h"
#include "log.h"
#include "sys.h"
#include "shard.h"
#include "os.h"
#include "job.h"
#include "kl_cluster.h"
#include "thread_manager.h"
#include <unistd.h>
#include <utility>
#include <time.h>
#include <sys/time.h>

// config variables
int64_t mysql_connect_timeout = 3;
int64_t mysql_read_timeout = 3;
int64_t mysql_write_timeout = 3;
int64_t mysql_max_packet_size = 1024*1024*1024;
int64_t prepared_transaction_ttl = 3;
int64_t meta_svr_port = 0;
int64_t check_shard_interval = 3;
int64_t stmt_retries = 3;
int64_t stmt_retry_interval_ms = 500;

std::string meta_svr_ip;
std::string meta_svr_user;
std::string meta_svr_pwd;

// not configurable for now
bool mysql_transmit_compress = false;

#define IS_MYSQL_CLIENT_ERROR(err) (((err) >= CR_MIN_ERROR && (err) <= CR_MAX_ERROR) || ((err) >= CER_MIN_ERROR && (err) <= CER_MAX_ERROR))

static void convert_preps2ti(Shard *ps, const Shard::Prep_recvrd_txns_t &preps,
		MetadataShard::cluster_txninfo &cti, bool is_meta_shard);
static void process_prep_txns(const Shard::Txn_decision &txn_dsn,
	MetadataShard::txn_info &ti,
	std::map<Shard *, Shard::Txn_end_decisions_t>&shard_txn_decisions);


int MYSQL_CONN::connect()
{
	if (connected) return 0;

    nrows_affected = 0;
    nwarnings = 0;
    result = NULL;
    connected = false;

    mysql_init(&conn);
    //mysql_options(mysql, MYSQL_OPT_NONBLOCK, 0); always do sync send
    mysql_options(&conn, MYSQL_OPT_CONNECT_TIMEOUT, &mysql_connect_timeout);
    mysql_options(&conn, MYSQL_OPT_READ_TIMEOUT, &mysql_read_timeout);
    mysql_options(&conn, MYSQL_OPT_WRITE_TIMEOUT, &mysql_write_timeout);
    mysql_options(&conn, MYSQL_OPT_MAX_ALLOWED_PACKET, &mysql_max_packet_size);

    if (mysql_transmit_compress)
        mysql_options(&conn, MYSQL_OPT_COMPRESS, NULL);

    // Never reconnect, because that messes up txnal status.
    my_bool reconnect = 0;
    mysql_options(&conn, MYSQL_OPT_RECONNECT, &reconnect);

    /* Returns 0 when done, else flag for what to wait for if need to block. */
    MYSQL *ret =
		mysql_real_connect(&conn, ip.c_str(), user.c_str(), pwd.c_str(),
			owner->owner->get_id() == MetadataShard::METADATA_SHARD_ID ? KUNLUN_METADATA_DBNAME  : NULL,
			port, NULL, CLIENT_MULTI_STATEMENTS |
			(mysql_transmit_compress ? MYSQL_OPT_COMPRESS : 0));
    if (!ret)
    {
        handle_mysql_error();
        return -1;
    }

    connected = true; // check_mysql_instance_status() needs this set to true here.

	int vers;
	if ((vers = verify_version()))
	{
		close_conn();
		return vers;
	}

	Shard *oowner = owner->owner;
	syslog(Logger::LOG, "Connected to shard(%s.%s %u) node(%s:%d %u)",
		   oowner->get_cluster_name().c_str(), oowner->get_name().c_str(),
		   oowner->get_id(), this->ip.c_str(), this->port, owner->get_id());
    return 0;
}

/*
  Verify mysql version is supported by this software, i.e. it's kunlun-storage.
*/
int MYSQL_CONN::verify_version()
{
	if (send_stmt(SQLCOM_SELECT, CONST_STR_PTR_LEN("select version()")))
		return -1;

    MYSQL_ROW row;
	int ret = -1;

    while ((row = mysql_fetch_row(result)))
    {
		const char *verstr = row[0];
		if (strcasestr(verstr, "kunlun-storage"))
			ret = 0;
		else
		{
			syslog(Logger::ERROR, "Unsupported mysql version %s, must use kunlun-storage-8.0.x", verstr);
			ret = -2;
		}
		break;
	}

	if (ret == -1)
		syslog(Logger::ERROR, "Version information unknown, can't handle mysql instance(%s:%p).",
				this->ip.c_str(), this->port);
end:
	free_mysql_result();
	return ret;
}


void MYSQL_CONN::close_conn()
{
	if (!connected) return;

    Assert(!result);
    mysql_close(&conn);
    connected = false;
}

TLS_VAR char errmsg_buf[512];

int MYSQL_CONN::handle_mysql_error(const char *stmt_ptr, size_t stmt_len)
{
    int ret = mysql_errno(&conn);

    errmsg_buf[0] = '\0';
    strncat(errmsg_buf, mysql_error(&conn), sizeof(errmsg_buf) - 1);

	if (ignore_errs.find(ret) != ignore_errs.end())
		return 0;
	if (result) free_mysql_result();

	const char *extra_msg = "";
    /*
     * Only break the connection for client errors. Errors returned by server
	 * are not caused by the connection.
     * */
	const bool is_mysql_client_error = IS_MYSQL_CLIENT_ERROR(ret);
    if (is_mysql_client_error)
    {
		close_conn();
		extra_msg = ", and disconnected from the node";
    }
	syslog(Logger::ERROR, "Got error executing '%s' from MySQL server (%s:%d) of shard (%s.%s, %u) node(%u): {%u: %s}%s.",
		   stmt_ptr ? stmt_ptr : "<none>", ip.c_str(), port, owner->owner->get_cluster_name().c_str(),
		   owner->owner->get_name().c_str(),
		   owner->owner->get_id(), owner->get_id(), ret, errmsg_buf, extra_msg);

    return ret;
}

bool Shard_node::update_conn_params(const char * ip_, int port_, const char * user_,
	const char * pwd_)
{
	bool changed = false;
	std::string old_ip = mysql_conn.ip;
	int old_port = mysql_conn.port;

	if (mysql_conn.ip != ip_)
	{
		changed = true;
		mysql_conn.ip = ip_;
	}

	if (mysql_conn.port != port_)
	{
		changed = true;
		mysql_conn.port = port_;
	}

	if (mysql_conn.user != user_)
	{
		changed = true;
		mysql_conn.user = user_;
	}

	if (mysql_conn.pwd != pwd_)
	{
		changed = true;
		mysql_conn.pwd = pwd_;
	}

	if (changed && mysql_conn.connected)
	{
		syslog(Logger::INFO, "Connection parameters for shard (%s.%s %u) node (%u, %s:%d) changed to (%s:%d, %s, ***), reconnected with new params",
			   owner->get_cluster_name().c_str(), owner->get_name().c_str(),
			   owner->get_id(), id, old_ip.c_str(),
			   old_port, ip_, port_, user_);
		//mysql_conn.close_conn(); can't do it here because this node may have a valid 'result' being iterated.
		mysql_conn.connect();
	}
	return changed;
}

/*
 * Receive mysql result from mysql server.
 * For SELECT stmt, make MYSQL_RES result ready to this->result; For others,
 * update affected rows.
 *
 * @retval true on error, false on success.
 * */
bool MYSQL_CONN::handle_mysql_result()
{
    int status = 1;

    if (sqlcmd == SQLCOM_SELECT)
    {
        /*
         * Iff the cmd isn't a SELECT stmt, mysql_use_result() returns NULL and
         * mysql_errno() is 0.
         * */
        MYSQL_RES *mysql_res = mysql_store_result(&conn);
        if (mysql_res)
        {
            if (result)
			{
                syslog(Logger::ERROR, "MySQL result not consumed/freed before sending a SELECT statement.");
				return true;
			}
            else
            {
                nwarnings += mysql_warning_count(&conn);
                result = mysql_res;
                goto end;
            }
        }
        else if (mysql_errno(&conn))
        {
            handle_mysql_error();
			return true;
        }
        else
            Assert(mysql_field_count(&conn) == 0);

        /*
         * The 1st result isn't SELECT result, fetch more for it.
         * */
        if (!mysql_get_next_result() && !result)
        {
        	syslog(Logger::ERROR, "A SELECT statement returned no results.");
			return true;
        }
    }
    else
    {
        do {
	        uint64_t n = mysql_affected_rows(&conn);
	        nwarnings = mysql_warning_count(&conn);
	        if (n == (uint64_t)-1)
            {
	           handle_mysql_error();
               return true;
            }
	        nrows_affected += n;
	        // TODO: handle RETURNING result later, and below Assert will need be removed.
	        Assert(mysql_field_count(&conn) == 0);

			/*
             * mysql_next_result() return value:
			 * 		more results? -1 = no, >0 = error, 0 = yes (keep looping)
             * Note that mariadb's client library doesn't return -1 to indicate
             * no more results, we have to call mysql_more_results() to see if
             * there are more results.
             * */
            if (mysql_more_results(&conn))
            {
			    if ((status = mysql_next_result(&conn)) > 0)
                {
	                handle_mysql_error();
                    return true;
                }
            }
            else
                break;
		} while (status == 0);
    }
end:
    return false;
}

/*
 * Send SQL statement [stmt, stmt_len) to mysql node in sync
 * @retval true on error, false on success.
 * */
bool MYSQL_CONN::send_stmt(enum_sql_command sqlcom_, const char *stmt, size_t len)
{
	if (!connected)
	{
		syslog(Logger::ERROR, "Connection to shard (%s.%s, %u) node(%u, %s:%d) broken.",
				owner->owner->get_cluster_name().c_str(),
				owner->owner->get_name().c_str(), owner->owner->get_id(),
				owner->id, ip.c_str(), port);
		return true;
	}

    // previous result must have been freed.
    Assert(result == NULL);
    nrows_affected = 0;
    nwarnings = 0;
    sqlcmd = sqlcom_;
    int ret = mysql_real_query(&conn, stmt, len);
    if (ret != 0)
    {
        handle_mysql_error(stmt, len);
        return true;
    }
    if (handle_mysql_result())
        return true;
    return false;
}

void MYSQL_CONN::free_mysql_result()
{
    if (result)
    {
        mysql_free_result(result);
        result = NULL;
    }

    // consume remaining results if any to keep mysql conn state clean.
    while (mysql_get_next_result())
        ;

    sqlcmd = SQLCOM_END;
    nrows_affected = 0;
    nwarnings = 0;
}

/*
 * @retval: whether there are more results of any stmt type.
 * */
bool MYSQL_CONN::mysql_get_next_result()
{
    int status = 0;

    if (result) {
        mysql_free_result(result);
        result = NULL;
    }

    while (true)
    {
        if (mysql_more_results(&conn))
        {
            if ((status = mysql_next_result(&conn)) > 0)
            {
                handle_mysql_error();
                return false;
            }

			if (status == -1)
				return false;
			Assert(status == 0);
        }
        else
            return false;

        nwarnings += mysql_warning_count(&conn);
        MYSQL_RES *mysql_res = mysql_store_result(&conn);
        if (mysql_res)
        {
            result = mysql_res;
            break;
        }
        else if (mysql_errno(&conn))
        {
            handle_mysql_error();
			return false;
        }
        else
            Assert(mysql_field_count(&conn) == 0);

    }

    return true;
}

bool MYSQL_CONN::send_stmt(enum_sql_command sqlcom_, const std::string &stmt)
{
	return send_stmt(sqlcom_, stmt.c_str(), stmt.length());
}

/*
  If send stmt fails because connection broken, reconnect and
  retry sending the stmt. Retry mysql_stmt_conn_retries times.
  @retval true on error, false if successful.
*/
bool Shard_node::
send_stmt(enum_sql_command sqlcom_, const char *stmt, size_t len, int nretries)
{
	bool ret = true;
	for (int i = 0; i < nretries; i++)
	{
		if (!mysql_conn.connected) connect();
		if (!mysql_conn.send_stmt(sqlcom_, stmt, len))
		{
			ret = false;
			break;
		}

		if (Thread_manager::do_exit)
			return ret;

		usleep(stmt_retry_interval_ms * 1000);
	}
	return ret;
}


bool Shard_node::
send_stmt(enum_sql_command sqlcom_, const std::string &stmt, int nretries)
{
	return send_stmt(sqlcom_, stmt.c_str(), stmt.length(), nretries);
}


int Shard_node::connect()
{
	return mysql_conn.connect();
}

bool Shard_node::update_variables(Tpye_string2 &t_string2)
{
	std::string str_sql = "set persist " + std::get<0>(t_string2) + "='" + std::get<1>(t_string2) + "'";
	return send_stmt(SQLCOM_SET_OPTION, str_sql.c_str(), str_sql.length(), stmt_retries);
}

bool Shard_node::get_variables(std::string &variable, std::string &value)
{
	std::string str_sql = "select @@" + variable;
	int ret = send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret)
		return -1;

    MYSQL_RES *result = get_result();
    MYSQL_ROW row;
    if ((row = mysql_fetch_row(result)))
    {
		value = row[0];
	}
	else
	{
		ret = -1;
	}

	free_mysql_result();
	return ret;
}

bool Shard_node::set_variables(std::string &variable, std::string &value_int, std::string &value_str)
{
	std::string str_sql;

	if(value_int.length())
		str_sql = "set persist " + variable + "=" + value_int;
	else
		str_sql = "set persist " + variable + "='" + value_str + "'";
	syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	return send_stmt(SQLCOM_SET_OPTION, str_sql.c_str(), str_sql.length(), stmt_retries);
}

bool Shard_node::update_instance_cluster_info()
{
	std::string cluster_name = owner->get_cluster_name();
	std::string shard_name = owner->get_name();
	std::string str_sql;

	str_sql = "update kunlun_sysdb.cluster_info set cluster_name='" + cluster_name + "',shard_name='" + shard_name + "'";
	return send_stmt(SQLCOM_UPDATE, str_sql.c_str(), str_sql.length(), stmt_retries);
}

/*
  do START GROUP_repliplication, and optionally after stop GROUP_repliplication
  if its status is ERROR rather than OFFLINE.
  @retval -1 if connection broken or stmt exec failure
*/
int Shard_node::start_mgr(Group_member_status curstat, bool as_master)
{
	const char *extra_info = "";
	if (as_master) extra_info = " as primary";

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
	if (as_master)
	{
		if (curstat == Shard_node::MEMBER_ERROR)
			ret = send_stmt(SQLCOM_SLAVE_START, CONST_STR_PTR_LEN(stmt3), stmt_retries);
		else
			ret = send_stmt(SQLCOM_SLAVE_START, CONST_STR_PTR_LEN(stmt4), stmt_retries);
	}
	else
	{
		if (curstat == Shard_node::MEMBER_ERROR)
			ret = send_stmt(SQLCOM_SLAVE_START, CONST_STR_PTR_LEN(stmt1), stmt_retries);
		else
			ret = send_stmt(SQLCOM_SLAVE_START, CONST_STR_PTR_LEN(stmt2), stmt_retries);
	}

	mysql_conn.free_mysql_result();

    if (ret)
	{
		syslog(Logger::ERROR, "Failed to join shard (%s.%s, %u) node (%u, %s:%d) of status %s back to the MGR cluster%s.",
			   owner->get_cluster_name().c_str(), owner->get_name().c_str(),
			   owner->get_id(), this->id, mysql_conn.ip.c_str(),
			   mysql_conn.port, Group_member_status_strs[curstat - 1],
			   extra_info);
        return -1;
	}


	syslog(Logger::INFO, "Added shard (%s.%s, %u) node (%u, %s:%d) of status %s back to the MGR cluster%s.",
		   owner->get_cluster_name().c_str(), owner->get_name().c_str(),
		   owner->get_id(), this->id, mysql_conn.ip.c_str(), mysql_conn.port,
		   Group_member_status_strs[curstat - 1], extra_info);
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
bool Shard_node::fetch_mgr_progress()
{
	bool ret = send_stmt(SQLCOM_SELECT, CONST_STR_PTR_LEN(
        "select interval_end from mysql.gtid_executed where source_uuid=@@group_replication_group_name"), stmt_retries);
    if (ret)
        return ret;

    MYSQL_RES *result = get_result();
    MYSQL_ROW row;
	uint64_t nrows = mysql_num_rows(result);
	int i = 0;
	char *endptr = NULL;

    while ((row = mysql_fetch_row(result)))
    {
		if (i++ < nrows - 1)
			continue;
		// Only want the last row which contains the latest executed gtid
		// of the group_replication_applier channel.
		latest_mgr_pos = strtoull(row[0], &endptr, 10);
		Assert(endptr == NULL || *endptr == '\0');
	}
end:
	free_mysql_result();
	syslog(Logger::LOG,
		   "Found shard (%s.%s, %u) node(%u, %s:%d) latest MGR position: %llu",
		   owner->get_cluster_name().c_str(), owner->get_name().c_str(),
		   owner->get_id(), this->id,
		   mysql_conn.ip.c_str(), mysql_conn.port, latest_mgr_pos);
	return false;
}

/*
  @retval -1: connection broken or stmt exec error;
  		-2: multiple or no primary nodes found
*/
int Shard_node::get_mgr_master_ip_port(std::string&ip, int&port)
{
	int ret = send_stmt(SQLCOM_SELECT, CONST_STR_PTR_LEN(
		"select MEMBER_HOST, MEMBER_PORT from performance_schema.replication_group_members where MEMBER_ROLE = 'PRIMARY' and MEMBER_STATE = 'ONLINE'"), stmt_retries);
	if (ret)
		return -1;

	char *endptr = NULL;
    MYSQL_RES *result = get_result();
    MYSQL_ROW row;
	uint64_t nrows = mysql_num_rows(result);
	if (nrows != 1)
	{
		syslog(Logger::WARNING,
			"Wrong NO.(%lu) of primary nodes found in shard (%s.%s, %u) node(%u, %s:%d), it could be joining the MGR cluster.",
		   nrows, owner->get_cluster_name().c_str(), owner->get_name().c_str(),
		   owner->get_id(), this->id,
		   mysql_conn.ip.c_str(), mysql_conn.port);
		ret = -2;
		goto end;
	}
	
	ret = 0;

    while ((row = mysql_fetch_row(result)))
    {
		ip = row[0];
		port = strtol(row[1], &endptr, 10);
		Assert(endptr == NULL || *endptr == '\0');
	}
end:
	free_mysql_result();
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
	const char *the_stmt = NULL;
	int ret = send_stmt(SQLCOM_SELECT, the_stmt =
		CONST_STR_PTR_LEN("select MEMBER_HOST, MEMBER_PORT, MEMBER_STATE, MEMBER_ROLE from performance_schema.replication_group_members"), stmt_retries);
    if (ret)
        return -1;
	ret = 0;
    MYSQL_RES *result = get_result();
    MYSQL_ROW row;
	uint64_t nrows = mysql_num_rows(result), n_myrows = 0;
	std::string node_stat;
	char *endptr = NULL;
	int port1 = 0;

    while ((row = mysql_fetch_row(result)))
    {
		if (row[0][0] == '\0' && row[1] == NULL && row[3][0] == '\0' && nrows == 1)
		{
			Assert(strcmp(row[2], "OFFLINE") == 0 || strcmp(row[2], "ERROR") == 0);
			goto got_my_row;
		}
		if (row[1] == NULL)
		{
			syslog(Logger::ERROR,
			"Invalid SQL query (%s) result(%lu rows) returned from shard(%s.%s, %u) node(%s:%d, %u): (%s, NULL, %s, %s)",
				the_stmt, nrows, owner->get_cluster_name().c_str(),
				owner->get_name().c_str(),
				owner->get_id(), mysql_conn.ip.c_str(), mysql_conn.port,
				this->id, row[0], row[2], row[3]);
			ret = -6;
			goto end;
		}

		// all fields of the row must now have valid values.
		Assert(row[0][0] && row[1] && row[2][0]); // && row[3][0]); could be "" when state is OFFLINE.

		port1 = strtol(row[1], &endptr, 10);
		Assert(endptr == NULL || *endptr == '\0');
		if (strcmp(mysql_conn.ip.c_str(), row[0]) || mysql_conn.port != port1)
			continue;
got_my_row:
		n_myrows++;
		node_stat = row[2];
		if (strcmp(row[3], "PRIMARY") == 0 && owner->set_master(this))
		{
			syslog(Logger::INFO,
		   		"Found primary node of shard(%s.%s, %u) changed to (%s:%d, %u)",
		   		owner->get_cluster_name().c_str(), owner->get_name().c_str(),
		   		owner->get_id(), row[0], port1, get_id());

		}

		for (int i = 0; i < sizeof(Group_member_status_strs)/sizeof(char*); i++)
			if (strcmp(row[2], Group_member_status_strs[i]) == 0)
			{
				ret = i + 1; // ONLINE is set to 1.
				break;
			}
	}
end:
	free_mysql_result();
	if (ret == -6)
		return ret;

	if (nrows == 0)
	{
		syslog(Logger::ERROR, 
		"Group replication not initialized in shard (%s.%s, %u) node (%u, %s:%d), this node is invalid as a shard node for Kunlun DDC.",
			   owner->get_cluster_name().c_str(), owner->get_name().c_str(),
			   owner->get_id(), this->id, mysql_conn.ip.c_str(), mysql_conn.port);
		ret = -2;
	}
	else if (n_myrows == 0)
	{
		// The mgr nodes returned by this node has no itself!
		// That would be a serious MGR bug!
		syslog(Logger::ERROR,
		"Invalid group replication cluster status in shard (%s.%s, %u) node (%u, %s:%d): no valid rows found representing this node itself.",
			   owner->get_cluster_name().c_str(), owner->get_name().c_str(),
			   owner->get_id(), this->id, mysql_conn.ip.c_str(),
			   mysql_conn.port);
		ret = -5;
	}
	else if (n_myrows > 1)
	{
		syslog(Logger::ERROR,
		"Invalid group replication cluster status in shard (%s.%s, %u) node (%u, %s:%d): %lu rows found for this node",
			   owner->get_cluster_name().c_str(), owner->get_name().c_str(),
			   owner->get_id(), this->id, mysql_conn.ip.c_str(),
			   mysql_conn.port, n_myrows);
		ret = -3;
	}
	else if (ret == 0)
	{
		syslog(Logger::ERROR,
		"Invalid group replication node status in shard (%s.%s, %u) node (%u, %s:%d): unrecognized node status %s",
			   owner->get_cluster_name().c_str(), owner->get_name().c_str(),
			   owner->get_id(), this->id, mysql_conn.ip.c_str(),
			   mysql_conn.port,node_stat.c_str());
		ret = -4;
	}

	syslog(Logger::LOG, "Found shard (%s.%s, %u) node(%u, %s:%d) status: %s",
		   owner->get_cluster_name().c_str(), owner->get_name().c_str(),
		   owner->get_id(), this->id,
		   mysql_conn.ip.c_str(), mysql_conn.port, node_stat.c_str());
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
	Scopped_mutex sm(mtx);
	std::vector<std::pair<Shard_node*, Shard_node::Group_member_status> >
		down_reachables;

	int nodes_down = 0, reachables = 0;
	std::set<Shard_node *>unreachables;
	for (auto &i:nodes)
	{
		if (Thread_manager::do_exit)
			break;
		int stat = i->check_mgr_state();
		if (stat < -1)
			return stat;
		Assert(stat == -1 || (stat > 0 && stat < Shard_node::MEMBER_END));
		// if stat == -1 or MEMBER_UNREACHABLE, node i can't be connected for now, it probably will
		// be started up and come back again later, so leave it alone for now.
		// for MEMBER_IN_RECOVERY, node i will very soon be usable with no further action needed.
		if (stat == Shard_node::MEMBER_ERROR || stat == Shard_node::MEMBER_OFFLINE)
		{
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
		else
			unreachables.insert(i);
	}

	if (likely(nodes_down == 0)) return 0; // most common case, we trust MGR will not brainsplit.

	if (nodes_down < nodes.size())
	{
		// Some nodes in the MGR cluster are running, one of them must be a
		// master node, no need to choose a master for the cluster.
		for (auto i=down_reachables.begin(); i != down_reachables.end(); ++i)
		{
			if (Thread_manager::do_exit)
				break;
			(*i).first->start_mgr((*i).second, false);
		}
	}
	else
	{
		Assert(reachables == down_reachables.size());

		/*
		  All nodes down, need to find the one with latest changes and make it
		  the master and start it up first, then start up the rest nodes.
		  Doing so requires simple majority nodes to be connectable/reachable, otherwise we
		  might lose some changes if the unreachable nodes happen to have changes not
		  in the connectable/reachable nodes.
		*/
		if (reachables > nodes.size() / 2)
		{
			uint64_t max_pos = 0;
			Shard_node*max_sn = NULL;
			Shard_node::Group_member_status max_stat = Shard_node::MEMBER_END;
			std::string top_ip;
			int top_port;

			if (this->pending_master_node_id)
			{
				max_sn = get_node_by_id(this->pending_master_node_id);
				goto got_new_master;
			}

			// find the node with most binlogs and start it as master first, then
			// start up the rest down&reachable nodes, i.e. those in down_reachables.
			for (auto itr = down_reachables.begin();
				 itr != down_reachables.end(); ++itr)
			{
				if (Thread_manager::do_exit)
					break;
				if (itr->first->fetch_mgr_progress())
				{
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

			Assert((max_sn != NULL && max_pos > 0 && max_stat != Shard_node::MEMBER_END) ||
				   (max_sn == NULL && max_pos == 0 && max_stat == Shard_node::MEMBER_END));
			if (!max_sn) goto out1; // it's likely that MGR isn't activated in the cluster.

			max_sn->get_ip_port(top_ip, top_port);
			// max_sn found, start it as master, start the rest as slaves.
			syslog(Logger::INFO, "Found shard (%s.%s, %u) node(%u, %s:%d) has max gtid position: %llu and will be chosen as primary node.",
				   get_cluster_name().c_str(), get_name().c_str(),
				   get_id(), max_sn->get_id(),
				   top_ip.c_str(), top_port, max_pos);
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
				max_sn->add_ignore_error(3093);

				this->pending_master_node_id = max_sn->get_id();
				return -6;
			}

			if (this->pending_master_node_id != 0)
			{
				this->pending_master_node_id = 0;
				max_sn->clear_ignore_errors();
			}

			for (auto &n:down_reachables)
			{
				if (Thread_manager::do_exit)
					break;
				if (n.first != max_sn && n.first->start_mgr(n.second, false))
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
			syslog(Logger::ERROR, "More than half(%d/%u) nodes in the MGR cluster %s are unreachable, unable to reliably startup the MGR cluster with the right primary node.",
				   reachables, nodes.size() / 2, get_cluster_name().c_str());
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

	for (auto &i:nodes)
	{
		if (Thread_manager::do_exit)
			break;

		if (unreachables.find(i) == unreachables.end())
		{
			std::string new_master_ip;
			int new_master_port = 0, nretries = 0, retx = 0;

			while ((retx = i->get_mgr_master_ip_port(new_master_ip, new_master_port)) != 0 &&
				   nretries++ < stmt_retries)
			{
				if (Thread_manager::do_exit)
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
				syslog(Logger::ERROR, "Found in shard (%s.%s, %u) node(%u, %s:%d) a different primary node %s:%d than the one already found: %s:%d. A primary switch might be going on.",
				   get_cluster_name().c_str(), get_name().c_str(), get_id(), i->get_id(),
				   iip.c_str(), iport, new_master_ip.c_str(),
				   new_master_port, master_ip.c_str(), master_port);
				return -2;
			}
		}
	}

	if (!master_port)
	{
		syslog(Logger::ERROR, "Found in shard (%s.%s, %u) no MGR primary node when we've attemted to join all reachable nodes to the MGR cluster.",
		   get_cluster_name().c_str(), get_name().c_str(), get_id());
		return -3;
	}
	if (ndone < nodes.size() / 2)
	{
		syslog(Logger::ERROR, "Found in shard (%s.%s, %u) of %u nodes the primary node confirmed to be %s:%d by only %d nodes, not majority quorum.",
		   get_cluster_name().c_str(), get_name().c_str(), get_id(),
		   nodes.size(), master_ip.c_str(), master_port, ndone);
		return -4;
	}

	// now we have found the latest master ip&port, find it from Shard::nodes.
	// main thread is solely responsible for refreshing shard nodes, so here we
	// may not be able to find the new master node, that's OK, we simply skip
	// the rest work of this run and do it again later.
	bool found_master = false;
	Shard_node *the_master = NULL;
	for (auto &i:nodes)
	{
		if (i->matches_ip_port(master_ip, master_port))
		{
			set_master(i);
			the_master = i;
			found_master = true;
			break;
		}
	}

	if (!found_master)
	{
		syslog(Logger::ERROR, "Latest primary node (%s:%d) in shard (%s.%s, %u) not registered into cluster manager yet",
		   master_ip.c_str(), master_port, get_cluster_name().c_str(),
		   get_name().c_str(), get_id());
		return -5;
	}

	syslog(Logger::INFO, "Found current primary node (%s:%d, %u) in shard (%s.%s, %u) of %u nodes confirmed by %u nodes.",
		master_ip.c_str(), master_port, the_master->get_id(),
	   get_cluster_name().c_str(), get_name().c_str(), get_id(),
	   nodes.size(), ndone);
	return 0;
}

/*
  Combine all recovered prepared txns branches which were fetched by worker
  threads for each shard, into per-global-txn form, i.e. each global txn is an
  entry of (Shard::Txn_key, txn_info) which contains the Shard objects
  containing the global txn's all txn branches.
  and go through their txn-ids to get trx id
  range to fetch commit_logs from metadata server's commit_log table.
*/

int System::process_recovered_prepared()
{
	std::map<uint, MetadataShard::cluster_txninfo> cluster_txns;

	MetadataShard::cluster_txninfo
		meta_tki(meta_shard.get_cluster_id(),meta_shard.get_cluster_name());

	cluster_txns.insert(std::make_pair(meta_shard.get_cluster_id(), meta_tki));
	std::vector<Shard *> all_shards;
	all_shards.emplace_back(&meta_shard);
	for (auto &cluster:kl_clusters)
		for (auto &shard:cluster->storage_shards)
			all_shards.emplace_back(shard);

	for (auto &sd:all_shards)
	{
		auto itr = cluster_txns.find(sd->get_cluster_id());
		if (itr == cluster_txns.end())
		{
			MetadataShard::cluster_txninfo
				cti(sd->get_cluster_id(), sd->get_cluster_name());
			cluster_txns.insert(std::make_pair(sd->get_cluster_id(), cti));
			itr = cluster_txns.find(sd->get_cluster_id());
			Assert(itr != cluster_txns.end());
		}

		Shard::Prep_recvrd_txns_t preps;
		sd->take_prep_recvrd_txns(preps);
		convert_preps2ti(sd, preps, itr->second, sd == &meta_shard);
	}

	int ret = 0;
	if ((ret = meta_shard.compute_txn_decisions(cluster_txns)))
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
compute_txn_decisions(std::map<uint, cluster_txninfo> &cluster_txns)
{
	char qstr_buf[256];

	Scopped_mutex sm(mtx);
	for (auto &clstr:cluster_txns)
	{
		std::map<Shard *, Txn_end_decisions_t> shard_txn_decisions;
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
		min_trxid = (clstr.second.min_start_ts << 32);
		max_trxid = ((clstr.second.max_start_ts << 32) | 0xffffffff);
		
		int slen = snprintf(qstr_buf, sizeof(qstr_buf),
			"select txn_id, comp_node_id, next_txn_cmd, unix_timestamp(prepare_ts) from commit_log_%s where txn_id >= %lu and txn_id <= %lu order by txn_id",
			 clstr.second.cname.c_str(), min_trxid, max_trxid);
		Assert(slen < sizeof(qstr_buf));
		
		if (cur_master->send_stmt(SQLCOM_SELECT, qstr_buf, slen, stmt_retries))
		{
			/*
			  Remaining clusters will fail almost definitely, so error out.
			*/
			return -1;
		}

		time_t now = time(NULL);
		MYSQL_RES *result = cur_master->get_result();
		MYSQL_ROW row;
		char *endptr = NULL;
		auto tk_itr = clstr.second.tkis.begin();
		while ((row = mysql_fetch_row(result)))
		{
			uint64_t trxid = strtoull(row[0], &endptr, 10);
			Assert(endptr == NULL || *endptr == '\0');
			Txn_key ti;
			ti.start_ts = (trxid >> 32);
			ti.local_txnid = (trxid & 0xffffffff);
			ti.comp_nodeid = strtoul(row[1], &endptr, 10);
			Assert(endptr == NULL || *endptr == '\0');

			/*
			  The SQL query result is in Txn_key increasing order (for free),
			  and the clstr.second.tkis is also in the same order, so we can do a
			  merge to find those with identical Txn_key values.
			  It's likely that a recovered prepared txn was not scanned in last
			  Shard::get_xa_prepared() call, and it's OK, it will be found
			  next time.
			*/
			if (tk_itr->first < ti)
			{
				while (tk_itr != clstr.second.tkis.end() && tk_itr->first < ti)
					++tk_itr;
				if (tk_itr == clstr.second.tkis.end())
					break;
			}

			if (tk_itr->first > ti)
				continue;

			Assert(tk_itr->first == ti);

			Txn_decision_enum txndcs = TXN_DECISION_NONE;
			if (strcasecmp(row[2], "commit") == 0)
			{
				txndcs = COMMIT;
			}
			else if (strcasecmp(row[2], "abort") == 0)
			{
				txndcs = ABORT;
			}
			else
				Assert(false);

			time_t prepts = strtol(row[3], &endptr, 10);
			Assert(endptr == NULL || *endptr == '\0');

			Txn_decision txn_dsn(ti, txndcs, prepts);
			process_prep_txns(txn_dsn, tk_itr->second, shard_txn_decisions);
		}

		cur_master->free_mysql_result();

		}

abort_rest:
		for (auto &ti:clstr.second.tkis)
		{
			/*
			  The remaining not processed are those *recovered prepared* txns
			  whose global txn were not fully prepared (some branches not
			  prepared yet) and before their commit logs are written,the 2PC process
			  quit because the computing node crashed or a shard node crashed
			  or connection between client to computing node or computing node
			  to storage shard master were broken.
			  Such txn branches should be unconditionally aborted.
			*/
			if (ti.second.processed == false)
			{
				Txn_decision td(ti.first, ABORT, 0);
				process_prep_txns(td, ti.second, shard_txn_decisions);
			}
		}

		
		/*
		  set txn decisions to shard for worker thread to execute.
		*/
		for (auto&entry:shard_txn_decisions)
		{
			entry.first->set_txn_end_decisions(entry.second);
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
	std::map<Shard *, Shard::Txn_end_decisions_t>&shard_txn_decisions)
{
	for (auto&ps:ti.branches)
	{
		auto itr2 = shard_txn_decisions.find(ps);
		if (itr2 == shard_txn_decisions.end())
		{
			Shard::Txn_end_decisions_t txn_decision;
			shard_txn_decisions.insert(std::make_pair(ps, txn_decision));
			itr2 = shard_txn_decisions.find(ps);
			Assert(itr2 != shard_txn_decisions.end());
		}
		itr2->second.emplace_back(txn_dsn);
	}
	ti.processed = true;
}

int Shard::end_recovered_prepared_txns()
{
	Scopped_mutex sm(mtx);
	Txn_end_decisions_t txn_dcsns;
	take_txn_end_decisions(txn_dcsns);
	char txnid_buf[64];

	for (auto &td:txn_dcsns)
	{
		int slen = snprintf(txnid_buf, sizeof(txnid_buf), "XA %s '%u-%ld-%u'",
			td.decision == COMMIT ? "COMMIT":"ROLLBACK",
			td.tk.comp_nodeid, td.tk.start_ts, td.tk.local_txnid);
		Assert(slen < sizeof(txnid_buf));
		if (cur_master->send_stmt((td.decision == COMMIT ? SQLCOM_XA_COMMIT :
				SQLCOM_XA_ROLLBACK), txnid_buf, slen, stmt_retries))
		{
			// current master gone, simply abandon remaining work, they can
			// be picked up again later on new master
			return -1;
		}
		cur_master->free_mysql_result();
		syslog(Logger::INFO, "Ended prepared txn on shard(%s.%s %u): %s",
			   cluster_name.c_str(), name.c_str(), id, txnid_buf);
	}

	return 0;
}

static void convert_preps2ti(Shard *ps, const Shard::Prep_recvrd_txns_t &preps,
	MetadataShard::cluster_txninfo &cti, bool is_meta_shard)
{
	for (auto&tk:preps)
	{
		time_t start_ts = tk.start_ts;
		auto itr = cti.tkis.find(tk);
		if (itr != cti.tkis.end())
		{
			itr->second.branches.emplace_back(ps);
		}
		else
		{
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


int Shard::get_xa_prepared()
{
	Prep_recvrd_txns_t txns;
	std::string mip;
	int mport;

	{
	Scopped_mutex sm(mtx);

	if(cur_master == NULL)
		return 0;
	cur_master->get_ip_port(mip, mport);

	// we can only operate on recovered XA txns. if the connection still holds
	// the prepared txn, we can't operate on it in another connection.
	int ret = cur_master->send_stmt(SQLCOM_SELECT, CONST_STR_PTR_LEN(
		"select trx_xid from information_schema.innodb_trx where trx_xa_type='external_recvrd'"), stmt_retries);
	if (ret)
		return ret;
	MYSQL_RES *result = cur_master->get_result();
	MYSQL_ROW row;
	char *endptr = NULL;
	
	while ((row = mysql_fetch_row(result)))
	{
		unsigned long *lengths = mysql_fetch_lengths(result);
		Assert(row[0][0] == '\'' && row[0][lengths[0] - 1] == '\'');
		char *xaks = row[0] + 1; // format e.g.: '1-1598596846-967098'
		char *sp1 = strchr(xaks, '-');
		char *sp2 = NULL;
		char *sp3 = NULL;
		if (sp1) sp2 = strchr(sp1 + 1, '-');
		if (sp2) sp3 = strchr(sp2 + 1, '-'); // sp3 should be NULL

		if (!(row[0][0] == '\'' && row[0][lengths[0] - 1] == '\'') ||
			sp1 == NULL || sp2 == NULL || sp3)
		{
			syslog(Logger::WARNING, "Got XA transaction ID %s from shard (%s.%s, %u) primary node(%u, %s:%d), not under Kunlun DRDBMS control, and it's skipped.",
		   		row[0], this->cluster_name.c_str(), this->name.c_str(), this->id,
				cur_master->get_id(), mip.c_str(), mport);
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

	cur_master->free_mysql_result();
	
	if (txns.size() == 0)
		goto end;
	}

	{
	Scopped_mutex sm1(mtx_txninfo);
	prep_recvrd_txns.insert(prep_recvrd_txns.end(), txns.begin(), txns.end());
	}

	syslog(Logger::LOG, "Got %lu prepared txns in shard (%s.%s, %u) primary node(%u, %s:%d).",
		   txns.size(), this->cluster_name.c_str(), this->name.c_str(),
		   this->id, cur_master->get_id(), mip.c_str(), mport);
end:
	return 0;
}


Shard_node *Shard::get_node_by_id(uint id)
{
	Scopped_mutex sm(mtx);
	for (auto &n:nodes)
		if (n->get_id() == id)
			return n;
	return NULL;
}

uint Shard::get_innodb_page_size()
{
	if(innodb_page_size == 0)
	{
		Scopped_mutex sm(mtx);

		if(cur_master == NULL)
			return 0;

		int ret = cur_master->send_stmt(SQLCOM_SELECT, CONST_STR_PTR_LEN(
						"show variables like 'innodb_page_size'"), stmt_retries);
		
		if (ret)
		   return 0;
		MYSQL_RES *result = cur_master->get_result();
		MYSQL_ROW row;
		char *endptr = NULL;
		
		if ((row = mysql_fetch_row(result)))
		{
			innodb_page_size = strtol(row[1], &endptr, 10);
			Assert(endptr == NULL || *endptr == '\0');
		}
		
		cur_master->free_mysql_result();
	}
	
	return innodb_page_size;
}


/*
  Fetch storage shard nodes from metadata shard, and refresh Shard/Shard_info
  objects in storage_shards. Newly added shard nodes will be added into
  storage_shards and obsolete nodes that no longer registered in shard nodes
  will be destroyed. If an existing node's connection info changes, existing
  mysql connection will be closed and connected again using new info.
  Call this repeatedly to refresh storage shard topology periodically.
*/
int MetadataShard::refresh_shards(std::vector<KunlunCluster *> &kl_clusters)
{
	Scopped_mutex sm(mtx);
	int ret = cur_master->send_stmt(SQLCOM_SELECT, CONST_STR_PTR_LEN(
	"select t1.id as shard_id, t1.name, t2.id, hostaddr, port, user_name, passwd, t3.name, t3.id as cluster_id, t3.ha_mode from \
shards t1, shard_nodes t2, db_clusters t3 where t2.shard_id = t1.id and t3.id=t1.db_cluster_id and t2.status!='inactive' order by t1.id"), stmt_retries);

	if (ret)
		return ret;
	MYSQL_RES *result = cur_master->get_result();
	MYSQL_ROW row;
	char *endptr = NULL;
	std::map<std::tuple<uint, uint, uint>, Shard_node*> sdns;
	
	for (auto &i:kl_clusters)
		for (auto &j:i->storage_shards)
			for (auto &k:j->get_nodes())
				sdns[std::make_tuple(i->get_id(), j->get_id(),k->get_id())] = k;

	std::set<std::string> alterant_node_ip; //for notify node_mgr

	while ((row = mysql_fetch_row(result)))
	{
		uint shardid = strtol(row[0], &endptr, 10);
		Assert(endptr == NULL || *endptr == '\0');
		uint cluster_id = strtol(row[8], &endptr, 10);
		Assert(endptr == NULL || *endptr == '\0');

		uint nodeid = strtol(row[2], &endptr, 10);
		Assert(endptr == NULL || *endptr == '\0');
		int port = strtol(row[4], &endptr, 10);
		Assert(endptr == NULL || *endptr == '\0');\

		KunlunCluster *pcluster = NULL;
		for (auto &cluster:kl_clusters)
		{
			if (cluster->get_id() == cluster_id)
			{
				pcluster = cluster;
				break;
			}
		}
		if (!pcluster)
		{
			pcluster = new KunlunCluster(cluster_id, row[7]);
			kl_clusters.emplace_back(pcluster);
			syslog(Logger::INFO, "Added KunlunCluster(%s.%u) into protection.", row[7], cluster_id);
		}

		Shard *pshard = NULL;

		for (auto &ssd:pcluster->storage_shards)
		{
			if (ssd->get_id() == shardid)
			{
				pshard = ssd;
				break;
			}
		}
		if (!pshard)
		{
			//set ha_mode for maintenance
			HAVL_mode ha_mode = HA_mgr;
			if(row[9]!=NULL)
			{
				if(strcmp("no_rep", row[9]) == 0)
					ha_mode = HA_no_rep;
				else if(strcmp("mgr", row[9]) == 0)
					ha_mode = HA_mgr;
				else if(strcmp("rbr", row[9]) == 0)
					ha_mode = HA_rbr;
			}
			
			pshard = new Shard(shardid, row[1], STORAGE, ha_mode);
			pshard->set_cluster_info(row[7], cluster_id);
			pcluster->storage_shards.emplace_back(pshard);
			syslog(Logger::INFO, "Added shard(%s.%s, %u) into protection.",
				pshard->get_cluster_name().c_str(), pshard->get_name().c_str(),
				pshard->get_id());
		}
		else if (pshard->get_cluster_name() != std::string(row[7]))
			pshard->update_cluster_name(row[7], cluster_id);

		/*
		  Iterating a storage shard's rows and a metashard's result, so every
		  node's conn can be closed if params changed.
		*/
		bool changed = false;
		Shard_node *n = pshard->get_node_by_id(nodeid);
		pshard->refresh_node_configs(nodeid, row[3], port, row[5], row[6], changed);
		if (changed) pshard->get_node_by_id(nodeid)->close_conn();

		if(n == NULL || changed)
			alterant_node_ip.insert(row[3]);
		
		if(pshard->get_mode() == Shard::HA_no_rep)
		{
			if(pshard->get_nodes().size()>0)
				pshard->set_master(pshard->get_nodes()[0]);
		}

		// remove nodes that still exist.
		sdns.erase(std::make_tuple(cluster_id, shardid, nodeid));
	}

	cur_master->free_mysql_result();

	// Remove shard nodes that are no longer in the shard, they are all left in sdns.
	for (auto &i:sdns)
	{
		Shard *pshard = i.second->get_owner();
		std::string rip;
		int rport;
		i.second->get_ip_port(rip, rport);

		alterant_node_ip.insert(rip);

		syslog(Logger::INFO, "Removed shard(%s.%s, %u) node (%s:%d, %u) from protection since it's not in cluster registration anymore.",
			pshard->get_cluster_name().c_str(), pshard->get_name().c_str(),
			pshard->get_id(), rip.c_str(), rport, i.second->get_id());

		pshard->remove_node(i.second->get_id());
	}

	if(alterant_node_ip.size() != 0)
		Job::get_instance()->notify_node_update(alterant_node_ip, 1);

	return 0;
}

/*
  Fetch computer nodes from metadata shard, and refresh computer_nodes. 
  Newly added computer nodes will be added into computer_nodes and 
  obsolete nodes that no longer registered in computer nodes will be destroyed. 
  Call this repeatedly to refresh computer_nodes topology periodically.
*/
int MetadataShard::refresh_computers(std::vector<KunlunCluster *> &kl_clusters)
{
	Scopped_mutex sm(mtx);

	std::set<std::string> alterant_node_ip;	//for notify node_mgr

	int ret;
	MYSQL_RES *result;
	MYSQL_ROW row;
	char *endptr = NULL;
	
	std::string str_sql;

	for (auto &cluster:kl_clusters)
	{
		str_sql = "select id,name,hostaddr,port,user_name,passwd from comp_nodes where status!='inactive' and db_cluster_id=" 
					+ std::to_string(cluster->get_id());

		//syslog(Logger::INFO, "refresh_computers str_sql = %s", str_sql.c_str());
		ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
		if (ret)
			return ret;
		result = cur_master->get_result();

		std::map<uint, Computer_node*> sdns;
		for (auto &i:cluster->computer_nodes)
			sdns[i->id] = i;
		
		while ((row = mysql_fetch_row(result)))
		{
			uint compid = strtol(row[0], &endptr, 10);
			Assert(endptr == NULL || *endptr == '\0');
			int port = strtol(row[3], &endptr, 10);
			Assert(endptr == NULL || *endptr == '\0');

			Computer_node *pcomputer = NULL;

			for (auto &computer:cluster->computer_nodes)
			{
				if (computer->id == compid)
				{
					pcomputer = computer;
					break;
				}
			}
			if (!pcomputer)
			{
				pcomputer = new Computer_node(compid, cluster->get_id(), port, row[1], row[2], row[4], row[5]);
				cluster->computer_nodes.emplace_back(pcomputer);
				syslog(Logger::INFO, "Added Computer(%s, %u, %s) into protection.",
							cluster->get_name().c_str(), pcomputer->id, pcomputer->name.c_str());

				alterant_node_ip.insert(row[2]);
			}
			else
			{
				if(pcomputer->refresh_node_configs(port, row[1], row[2], row[4], row[5]))
					alterant_node_ip.insert(row[2]);
			}

			// remove nodes that still exist.
			sdns.erase(compid);
		}

		cur_master->free_mysql_result();

		// Remove computer nodes that are no longer in the computer_nodes, they are all left in sdns.
		for (auto &i:sdns)
		{
			std::string ip;
			int port;

			i.second->get_ip_port(ip, port);
			alterant_node_ip.insert(ip);
			
			for(auto it=cluster->computer_nodes.begin(); it!=cluster->computer_nodes.end(); )
			{
				if ((*it)->id == i.first)
				{
					delete *it;
					it = cluster->computer_nodes.erase(it);
					break;
				}
				else
					it++;
			}
		}
	}

	if(alterant_node_ip.size() != 0)
		Job::get_instance()->notify_node_update(alterant_node_ip, 2);

	return 0;
}

/*
  check port if used from metadata tables
  @retval 0 unused;
  		  1 used;
*/
int MetadataShard::check_port_used(std::string &ip, int port)
{
	Scopped_mutex sm(mtx);
	
	if(cur_master == NULL)
		return 1;

	int ret = 1;

	std::string str_sql  = "select hostaddr,port from meta_db_nodes where hostaddr=\"" + ip + "\" and port=" + std::to_string(port);
	ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);

	if (ret==0)
	{
		MYSQL_RES *result = cur_master->get_result();
		MYSQL_ROW row;

		if ((row = mysql_fetch_row(result)))
			ret = 1;
		else
			ret = 0;
		
		cur_master->free_mysql_result();

		if(ret)
			return ret;
	}

	str_sql  = "select hostaddr,port from shard_nodes where hostaddr=\"" + ip + "\" and port=" + std::to_string(port);
	ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
		
	if (ret==0)
	{
		MYSQL_RES *result = cur_master->get_result();
		MYSQL_ROW row;

		if ((row = mysql_fetch_row(result)))
			ret = 1;
		else
			ret = 0;
		
		cur_master->free_mysql_result();

		if(ret)
			return ret;
	}

	str_sql  = "select hostaddr,port from comp_nodes where hostaddr=\"" + ip + "\" and port=" + std::to_string(port);
	ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
		
	if (ret==0)
	{
		MYSQL_RES *result = cur_master->get_result();
		MYSQL_ROW row;

		if ((row = mysql_fetch_row(result)))
			ret = 1;
		else
			ret = 0;
		
		cur_master->free_mysql_result();

		if(ret)
			return ret;
	}

	return 0;
}

/*
  get max index of comps from metadata tables
  @retval 0 succeed;
  		  1 fail;
*/
int MetadataShard::get_comp_nodes_id_seq(int &comps_id)
{
	Scopped_mutex sm(mtx);

	if(cur_master == NULL)
		return 1;

	int ret = cur_master->send_stmt(SQLCOM_SELECT, CONST_STR_PTR_LEN("select max(id) from comp_nodes_id_seq"), stmt_retries);	
	if (ret==0)
	{
		MYSQL_RES *result = cur_master->get_result();
		MYSQL_ROW row;
		if ((row = mysql_fetch_row(result)))
		{
			if(row[0] != NULL)
				comps_id = atoi(row[0]);
		}
		cur_master->free_mysql_result();
	}

	return ret;
}

/*
  get max cluster id from metadata tables
  @retval 0 succeed;
  		  1 fail;
*/
int MetadataShard::get_max_cluster_id(int &cluster_id)
{
	Scopped_mutex sm(mtx);

	if(cur_master == NULL)
		return 1;

	int ret = cur_master->send_stmt(SQLCOM_SELECT, CONST_STR_PTR_LEN("select max(id) from db_clusters"), stmt_retries);	
	if (ret==0)
	{
		MYSQL_RES *result = cur_master->get_result();
		MYSQL_ROW row;
		if ((row = mysql_fetch_row(result)))
		{
			if(row[0] != NULL)
				cluster_id = atoi(row[0]);
		}
		cur_master->free_mysql_result();
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
	Scopped_mutex sm(mtx);

	if(cur_master == NULL)
		return 1;

	int ret = cur_master->send_stmt(command, str_sql.c_str(), str_sql.length(), stmt_retries);

	return ret;
}

/*
  delete cluster from metadata
  @retval 0 succeed;
  		  1 fail;
*/
int MetadataShard::delete_cluster_from_metadata(std::string & cluster_name)
{
	Scopped_mutex sm(mtx);

	if(cur_master == NULL)
		return 1;

	int ret = 1;
	std::string cluster_id;
	std::vector<std::string> vec_comp_id;

	//get cluster_id
	std::string str_sql = "select id from db_clusters where name='" + cluster_name + "'";
	ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		MYSQL_RES *result = cur_master->get_result();
		MYSQL_ROW row;
		if ((row = mysql_fetch_row(result)))
		{
			if(row[0] != NULL)
				cluster_id = row[0];
		}
		cur_master->free_mysql_result();
	}

	if(cluster_id.length()==0)
		return 1;

	//get comp_id
	str_sql = "select id from comp_nodes where db_cluster_id=" + cluster_id;
	ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		MYSQL_RES *result = cur_master->get_result();
		MYSQL_ROW row;
		while ((row = mysql_fetch_row(result)))
		{
			if(row[0] != NULL)
				vec_comp_id.emplace_back(row[0]);
		}
		cur_master->free_mysql_result();
	}

	//remove comp_nodes
	str_sql = "delete from comp_nodes where db_cluster_id=" + cluster_id;
	cur_master->send_stmt(SQLCOM_DELETE, str_sql.c_str(), str_sql.length(), stmt_retries);

	//remove comp_nodes_id_seq
	for(auto &comp_id: vec_comp_id)
	{
		str_sql  = "delete from comp_nodes_id_seq where id=" + comp_id;
		cur_master->send_stmt(SQLCOM_DELETE, str_sql.c_str(), str_sql.length(), stmt_retries);
	}

	//remove shard_nodes
	str_sql = "delete from shard_nodes where db_cluster_id=" + cluster_id;
	cur_master->send_stmt(SQLCOM_DELETE, str_sql.c_str(), str_sql.length(), stmt_retries);

	//remove shards
	str_sql = "delete from shards where db_cluster_id=" + cluster_id;
	cur_master->send_stmt(SQLCOM_DELETE, str_sql.c_str(), str_sql.length(), stmt_retries);

	//remove db_clusters
	str_sql = "delete from db_clusters where id=" + cluster_id;
	cur_master->send_stmt(SQLCOM_DELETE, str_sql.c_str(), str_sql.length(), stmt_retries);

	//drop table commit_log_cluster_name
	str_sql = "drop table commit_log_" + cluster_name;
	cur_master->send_stmt(SQLCOM_DROP_TABLE, str_sql.c_str(), str_sql.length(), stmt_retries);

	//drop table ddl_ops_log_cluster_name
	str_sql = "drop table ddl_ops_log_" + cluster_name;
	cur_master->send_stmt(SQLCOM_DROP_TABLE, str_sql.c_str(), str_sql.length(), stmt_retries);

	return ret;
}

/*
  delete cluster shard from metadata
  @retval 0 succeed;
  		  1 fail;
*/
int MetadataShard::delete_cluster_shard_from_metadata(std::string & cluster_name, std::string &shard_name)
{
	Scopped_mutex sm(mtx);

	if(cur_master == NULL)
		return 1;

	int ret = 1;
	std::string cluster_id;
	std::string shard_id;

	//get cluster_id
	std::string str_sql = "select id from db_clusters where name='" + cluster_name + "'";
	ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		MYSQL_RES *result = cur_master->get_result();
		MYSQL_ROW row;
		if ((row = mysql_fetch_row(result)))
		{
			if(row[0] != NULL)
				cluster_id = row[0];
		}
		cur_master->free_mysql_result();
	}

	if(cluster_id.length()==0)
		return 1;

	//get shard_id
	str_sql = "select id from shards where db_cluster_id=" + cluster_id + " and name='" + shard_name + "'";
	ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		MYSQL_RES *result = cur_master->get_result();
		MYSQL_ROW row;
		if ((row = mysql_fetch_row(result)))
		{
			if(row[0] != NULL)
				shard_id = row[0];
		}
		cur_master->free_mysql_result();
	}

	if(shard_id.length()==0)
		return 1;

	//remove ddl_ops_log_cluster_name
	str_sql = "delete from ddl_ops_log_" + cluster_name + " where target_shard_id=" + shard_id;
	cur_master->send_stmt(SQLCOM_DELETE, str_sql.c_str(), str_sql.length(), stmt_retries);

	//remove shard_nodes
	str_sql = "delete from shard_nodes where db_cluster_id=" + cluster_id + " and shard_id=" + shard_id;
	cur_master->send_stmt(SQLCOM_DELETE, str_sql.c_str(), str_sql.length(), stmt_retries);

	//remove shards
	str_sql = "delete from shards where db_cluster_id=" + cluster_id + " and id=" + shard_id;
	cur_master->send_stmt(SQLCOM_DELETE, str_sql.c_str(), str_sql.length(), stmt_retries);

	return ret;
}

/*
  delete cluster shard node from metadata
  @retval 0 succeed;
  		  1 fail;
*/
int MetadataShard::delete_cluster_shard_node_from_metadata(std::string &cluster_name, std::string &shard_name, Tpye_Ip_Port &ip_port)
{
	Scopped_mutex sm(mtx);

	if(cur_master == NULL)
		return 1;

	int ret = 1;
	std::string cluster_id;
	std::string shard_id;

	//get cluster_id
	std::string str_sql = "select id from db_clusters where name='" + cluster_name + "'";
	ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		MYSQL_RES *result = cur_master->get_result();
		MYSQL_ROW row;
		if ((row = mysql_fetch_row(result)))
		{
			if(row[0] != NULL)
				cluster_id = row[0];
		}
		cur_master->free_mysql_result();
	}

	if(cluster_id.length()==0)
		return 1;

	//get shard_id
	str_sql = "select id from shards where db_cluster_id=" + cluster_id + " and name='" + shard_name + "'";
	ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		MYSQL_RES *result = cur_master->get_result();
		MYSQL_ROW row;
		if ((row = mysql_fetch_row(result)))
		{
			if(row[0] != NULL)
				shard_id = row[0];
		}
		cur_master->free_mysql_result();
	}

	if(shard_id.length()==0)
		return 1;

	//remove shard_nodes
	str_sql = "delete from shard_nodes where hostaddr='" + ip_port.first + "' and port=" + std::to_string(ip_port.second);
	cur_master->send_stmt(SQLCOM_DELETE, str_sql.c_str(), str_sql.length(), stmt_retries);

	//update shards
	str_sql = "update shards set num_nodes=num_nodes-1";
	str_sql += " where id=" + shard_id;
	cur_master->send_stmt(SQLCOM_UPDATE, str_sql.c_str(), str_sql.length(), stmt_retries);

	return ret;
}

/*
  delete cluster cpmp from metadata
  @retval 0 succeed;
  		  1 fail;
*/
int MetadataShard::delete_cluster_comp_from_metadata(std::string &cluster_name, std::string &comp_name)
{
	Scopped_mutex sm(mtx);

	if(cur_master == NULL)
		return 1;

	int ret = 1;
	std::string cluster_id;
	std::string comp_id;

	//get cluster_id
	std::string str_sql = "select id from db_clusters where name='" + cluster_name + "'";
	ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		MYSQL_RES *result = cur_master->get_result();
		MYSQL_ROW row;
		if ((row = mysql_fetch_row(result)))
		{
			if(row[0] != NULL)
				cluster_id = row[0];
		}
		cur_master->free_mysql_result();
	}

	if(cluster_id.length()==0)
		return 1;

	//get vec_shard_id
	str_sql = "select id from comp_nodes where db_cluster_id=" + cluster_id + " and name='" + comp_name + "'";
	ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		MYSQL_RES *result = cur_master->get_result();
		MYSQL_ROW row;
		if ((row = mysql_fetch_row(result)))
		{
			if(row[0] != NULL)
				comp_id = row[0];
		}
		cur_master->free_mysql_result();
	}

	if(comp_id.length()==0)
		return 1;

	//remove commit_log_cluster_name
	str_sql = "delete from commit_log_" + cluster_name + " where comp_node_id=" + comp_id;
	cur_master->send_stmt(SQLCOM_DELETE, str_sql.c_str(), str_sql.length(), stmt_retries);

	//remove comp_nodes_id_seq
	str_sql = "delete from comp_nodes_id_seq where id=" + comp_id;
	cur_master->send_stmt(SQLCOM_DELETE, str_sql.c_str(), str_sql.length(), stmt_retries);

	//remove comp_nodes
	str_sql = "delete from comp_nodes where id=" + comp_id;
	cur_master->send_stmt(SQLCOM_DELETE, str_sql.c_str(), str_sql.length(), stmt_retries);

	return ret;
}

/*
  get server_nodes from metadata table
  @retval 0 succeed;
  		  1 fail;
*/
int MetadataShard::get_server_nodes_from_metadata(std::vector<Machine*> &vec_machines)
{
	Scopped_mutex sm(mtx);

	if(cur_master == NULL)
		return 1;

	std::string str_sql = "select hostaddr,rack_id,datadir,logdir,wal_log_dir,comp_datadir,total_mem,total_cpu_cores from server_nodes";
	int ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		MYSQL_RES *result = cur_master->get_result();
		MYSQL_ROW row;
		while ((row = mysql_fetch_row(result)))
		{
			bool is_null = false;
			for(int i=0; i<8; i++)
			{
				if(row[i] == NULL)
				{
					is_null = true;
					break;
				}
			}

			if(!is_null)
			{
				std::string hostaddr = row[0];
				std::vector<std::string> vec_paths;
				vec_paths.emplace_back(row[2]);
				vec_paths.emplace_back(row[3]);
				vec_paths.emplace_back(row[4]);
				vec_paths.emplace_back(row[5]);
				Tpye_string3 t_string3 = std::make_tuple(row[1],row[6],row[7]);
				Machine *machine = new Machine(hostaddr, vec_paths, t_string3);
				vec_machines.emplace_back(machine);
			}
		}
		cur_master->free_mysql_result();
	}

	return ret;
}

/*
  get_meta_instance from metadata table
  @retval 0 succeed;
  		  1 fail;
*/
int MetadataShard::get_meta_instance(Machine* machine)
{
	Scopped_mutex sm(mtx);

	if(cur_master == NULL)
		return 1;

	std::string str_sql = "select port from meta_db_nodes where hostaddr='" + machine->ip + "'";
	int ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		MYSQL_RES *result = cur_master->get_result();
		machine->instance_storage += (int)mysql_num_rows(result);
		cur_master->free_mysql_result();
	}

	return ret;
}

/*
  get_storage_instance_port from metadata table
  @retval 0 succeed;
  		  1 fail;
*/
int MetadataShard::get_storage_instance_port(Machine* machine)
{
	Scopped_mutex sm(mtx);

	if(cur_master == NULL)
		return 1;

	std::string str_sql = "select port from shard_nodes where hostaddr='" + machine->ip + "'";
	int ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		MYSQL_RES *result = cur_master->get_result();
		machine->instance_storage += (int)mysql_num_rows(result);
		MYSQL_ROW row;
		while ((row = mysql_fetch_row(result)))
		{
			int port = atoi(row[0]);
			if(port > machine->port_storage)
				machine->port_storage = port;
		}
		cur_master->free_mysql_result();
	}

	return ret;
}

/*
  get_computer_instance_port from metadata table
  @retval 0 succeed;
  		  1 fail;
*/
int MetadataShard::get_computer_instance_port(Machine* machine)
{
	Scopped_mutex sm(mtx);

	if(cur_master == NULL)
		return 1;

	std::string str_sql = "select port from comp_nodes where hostaddr='" + machine->ip + "'";
	int ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		MYSQL_RES *result = cur_master->get_result();
		machine->instance_computer += (int)mysql_num_rows(result);
		MYSQL_ROW row;
		while ((row = mysql_fetch_row(result)))
		{
			int port = atoi(row[0]);
			if(port > machine->port_computer)
				machine->port_computer = port;
		}
		cur_master->free_mysql_result();
	}

	return ret;
}

/*
  update_instance_status to metadata table
  @retval 0 succeed;
  		  1 fail;
*/
int MetadataShard::update_instance_status(Tpye_Ip_Port &ip_port, std::string &status, int &type)
{
	Scopped_mutex sm(mtx);

	if(cur_master == NULL)
		return 1;

	std::string str_sql;
	int ret;

	str_sql = "select status from shard_nodes where hostaddr='" + ip_port.first + "' and port=" + std::to_string(ip_port.second);
	ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		MYSQL_RES *result = cur_master->get_result();
		int num_rows = (int)mysql_num_rows(result);
		cur_master->free_mysql_result();

		if(num_rows==1)
		{
			type = 1;

			str_sql = "update shard_nodes set status='" + status + "' where hostaddr='" + ip_port.first + "' and port=" + std::to_string(ip_port.second);
			return cur_master->send_stmt(SQLCOM_UPDATE, str_sql.c_str(), str_sql.length(), stmt_retries);
		}
	}

	str_sql = "select status from comp_nodes where hostaddr='" + ip_port.first + "' and port=" + std::to_string(ip_port.second);
	ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		MYSQL_RES *result = cur_master->get_result();
		int num_rows = (int)mysql_num_rows(result);
		cur_master->free_mysql_result();

		if(num_rows==1)
		{
			type = 2;

			str_sql = "update comp_nodes set status='" + status + "' where hostaddr='" + ip_port.first + "' and port=" + std::to_string(ip_port.second);
			return cur_master->send_stmt(SQLCOM_UPDATE, str_sql.c_str(), str_sql.length(), stmt_retries);
		}
	}

	return ret;
}

/*
  add shard nodes to metadata table
  @retval 0 succeed;
  		  1 fail;
*/
int MetadataShard::add_shard_nodes(std::string &cluster_name, std::string &shard_name, std::vector<Tpye_Ip_Port_User_Pwd> vec_ip_port_user_pwd)
{
	Scopped_mutex sm(mtx);

	if(cur_master == NULL)
		return 1;

	int ret = 1;
	std::string cluster_id;
	std::string shard_id;

	//get cluster_id
	std::string str_sql = "select id from db_clusters where name='" + cluster_name + "'";
	ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		MYSQL_RES *result = cur_master->get_result();
		MYSQL_ROW row;
		if ((row = mysql_fetch_row(result)))
		{
			if(row[0] != NULL)
				cluster_id = row[0];
		}
		cur_master->free_mysql_result();
	}

	if(cluster_id.length()==0)
		return 1;

	//get shard_id
	str_sql = "select id from shards where db_cluster_id=" + cluster_id + " and name='" + shard_name + "'";
	ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		MYSQL_RES *result = cur_master->get_result();
		MYSQL_ROW row;
		if ((row = mysql_fetch_row(result)))
		{
			if(row[0] != NULL)
				shard_id = row[0];
		}
		cur_master->free_mysql_result();
	}

	if(shard_id.length()==0)
		return 1;

	//update shards
	str_sql = "update shards set num_nodes=num_nodes+" + std::to_string(vec_ip_port_user_pwd.size());
	str_sql += " where id=" + shard_id;
	ret = cur_master->send_stmt(SQLCOM_UPDATE, str_sql.c_str(), str_sql.length(), stmt_retries);
	if(ret)
		return ret;

	//add shard_nodes
	for(auto &ip_port_user_pwd :vec_ip_port_user_pwd)
	{
		str_sql = "insert into shard_nodes(hostaddr, port, user_name, passwd, shard_id, db_cluster_id, svr_node_id, master_priority) values('";
		str_sql += std::get<0>(ip_port_user_pwd) + "',"	+ std::to_string(std::get<1>(ip_port_user_pwd)) + ",'" + std::get<2>(ip_port_user_pwd);
		str_sql += "','" + std::get<3>(ip_port_user_pwd) + "'," + shard_id + "," + cluster_id + ",1,0)";
		ret = cur_master->send_stmt(SQLCOM_INSERT, str_sql.c_str(), str_sql.length(), stmt_retries);
		if(ret)
			return ret;
	}

	return ret;
}

/*
  get cluster_backups from metadata table 
  @retval 0 succeed;
  		  1 fail;
*/
int MetadataShard::get_backup_info_from_metadata(std::string &cluster_name, std::string &timestamp, Tpye_cluster_info &cluster_info)
{
	Scopped_mutex sm(mtx);

	if(cur_master == NULL)
		return 1;

	std::string str_sql  = "select ha_mode,shards,nodes,comps,max_storage_size,max_connections,cpu_cores,innodb_size from cluster_backups";
	str_sql += " where cluster_name='" + cluster_name + "' and when_created<='" + timestamp + "' order by when_created desc";
	int ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		MYSQL_RES *result = cur_master->get_result();
		MYSQL_ROW row;

		ret = 1;
		if ((row = mysql_fetch_row(result)))
		{
			std::get<0>(cluster_info) = row[0];
			std::get<1>(cluster_info) = atoi(row[1]);
			std::get<2>(cluster_info) = atoi(row[2]);
			std::get<3>(cluster_info) = atoi(row[3]);
			std::get<4>(cluster_info) = atoi(row[4]);
			std::get<5>(cluster_info) = atoi(row[5]);
			std::get<6>(cluster_info) = atoi(row[6]);
			std::get<7>(cluster_info) = atoi(row[7]);
			ret = 0;
		}
		cur_master->free_mysql_result();
	}

	return ret;
}

/*
  check machine hostaddr from metadata table 
  @retval true succeed;
  		  false fail;
*/
bool MetadataShard::check_machine_hostaddr(std::string &hostaddr)
{
	Scopped_mutex sm(mtx);

	if(cur_master == NULL)
		return false;

	std::string str_sql  = "select hostaddr from server_nodes where hostaddr='" + hostaddr + "'";
	int ret = cur_master->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
	if (ret==0)
	{
		MYSQL_RES *result = cur_master->get_result();
		MYSQL_ROW row;

		if ((row = mysql_fetch_row(result)))
		{
			if(hostaddr == row[0])
				ret = 1;
		}
		cur_master->free_mysql_result();
	}

	return (ret == 1);
}

/*
  Query meta shard node sn to fetch all meta shard nodes from its
  meta_db_nodes table, and refresh the shard nodes contained in this object.
  This method is can be called repeatedly to refresh metashard nodes periodically.
  @param is_master whether 'sn' is current master node,
    if so 'master_ip' and 'master_port' are 0;
	otherwise, 'master_ip' and 'master_port' are the current master's conn info.
*/
int MetadataShard::fetch_meta_shard_nodes(Shard_node *sn, bool is_master,
	const char *master_ip, int master_port)
{
	Scopped_mutex sm(mtx);
	Assert(nodes.size() > 0); // sn should have been added already.

	int ret = sn->send_stmt(SQLCOM_SELECT, CONST_STR_PTR_LEN(
		"select id, hostaddr, port, user_name, passwd from meta_db_nodes"), stmt_retries);
	if (ret)
		return ret;
	
	std::set<std::string> alterant_node_ip;	//for notify node_mgr

	std::string master_usr, master_pwd;
	MYSQL_RES *result = sn->get_result();
	MYSQL_ROW row;
	char *endptr = NULL;
	bool close_snconn = false;

	std::map<uint, Shard_node*>snodes;
	for (auto &i:nodes)
	{
		snodes.insert(std::make_pair(i->get_id(), i));
	}

	while ((row = mysql_fetch_row(result)))
	{
		int port = strtol(row[2], &endptr, 10);
		Assert(endptr == NULL || *endptr == '\0');
		uint nodeid = strtol(row[0], &endptr, 10);
		Assert(endptr == NULL || *endptr == '\0');
		/*
		  config file has no meta svr node id, need to set it before below
		  erase.
		*/
		if (meta_svr_ip == row[1] && meta_svr_port == port &&
			sn->matches_ip_port(meta_svr_ip, meta_svr_port))
		{
			snodes.erase(sn->get_id());
			sn->set_id(nodeid);
		}

		snodes.erase(nodeid);

		bool changed = false;
		Shard_node *n = get_node_by_id(nodeid);
		Shard_node *node =
			refresh_node_configs(nodeid, row[1], port, row[3], row[4], changed);
		// need to close sn's conn, but not now for sn since we are iterating them.
		if (node == sn && changed)
			close_snconn = true;
		else if (changed)
			node->close_conn();

		if(n == NULL || changed)
			alterant_node_ip.insert(row[1]);

		if (!is_master && master_ip && strcmp(master_ip, row[1]) == 0 &&
			master_port == port && set_master(node))
		{
			/*
			  sn isn't the master, here we have the master, create it and
			  we will connect to it to create the rest nodes.
			*/
			syslog(Logger::INFO,
			   "Found primary node of shard(%s.%s, %u) to be (%s:%d, %u)",
			   get_cluster_name().c_str(), get_name().c_str(),
			   get_id(), master_ip, master_port, node->get_id());
		}
	}

	sn->free_mysql_result();
	if (close_snconn) sn->close_conn();

	/*
	  Remove nodes that are no longer registered in the metadata shard.
	*/
	for (auto &i:snodes)
	{
		Shard *ps = i.second->get_owner();
		Shard_node *pn = i.second;

		std::string snip;
		int snport;
		pn->get_ip_port(snip, snport);

		alterant_node_ip.insert(snip);

		syslog(Logger::INFO,
			   "Removed shard(%s.%s, %u) node(%s:%d, %u) which no longer belong to the meta shard.",
			   ps->get_cluster_name().c_str(), ps->get_name().c_str(),
			   ps->get_id(), snip.c_str(), snport, pn->get_id());

		remove_node(pn->get_id());
	}

	if(alterant_node_ip.size() != 0)
		Job::get_instance()->notify_node_update(alterant_node_ip, 0);

	return 0;
}

void Shard::maintenance()
{
	int ret = 0;

	if(get_mode() != HAVL_mode::HA_no_rep)
		ret = check_mgr_cluster();
	
	// if ret not 0, master node isn't uniquely resolved or running.
	if (ret == 0)
	{
		end_recovered_prepared_txns();
		get_xa_prepared();
	}
	else
		syslog(Logger::WARNING, "Got error %d from check_mgr_cluster() in shard (%s.%s, %u), skipping prepared txns.",
			   ret, get_cluster_name().c_str(), get_name().c_str(), get_id());

	set_thread_handler(NULL);
}

bool Shard::set_thread_handler(Thread *h, bool force)
{
	bool hdlr_assigned = false;
	if (!h ||
		(thrd_hdlr_assigned.compare_exchange_strong(hdlr_assigned, true) &&
		 !hdlr_assigned))
	{
		Scopped_mutex sm(mtx);
		if ((h && !m_thrd_hdlr && (
#ifdef ENABLE_DEBUG
/*
  so that tests could run faster. Can't enable in release build otherwise it
  could be exploited to drain system resources and causes a DoS attack.
*/
				force ||
#endif
			 (time(NULL) - last_time_check >= check_shard_interval))) ||
			(!h && m_thrd_hdlr))
		{
			m_thrd_hdlr = h;
			if (!h) // released after handled
			{
				last_time_check = time(NULL);
				thrd_hdlr_assigned.store(false);
			}
			else
				m_thrd_hdlr->set_shard(this);
			return true;
		}

		if (!m_thrd_hdlr)
			thrd_hdlr_assigned.store(false);
	}
	return false;
}
