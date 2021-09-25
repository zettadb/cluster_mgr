#ifndef KL_CLUSTER_H
#define KL_CLUSTER_H

#include "sys_config.h"
#include "global.h"
#include "shard.h"
#include "log.h"

#include <atomic>
#include <set>
#include <string>
#include <map>
#include <vector>
#include <tuple>

#include "pgsql/libpq-fe.h"

class PGSQL_CONN
{
private:
	bool connected;
	int port;
	std::string db, ip, user, pwd;
	PGconn	   *conn;
	PGresult   *result;
	Computer_node *owner;
	friend class Computer_node;
	void free_pgsql_result();
public:
	PGSQL_CONN(const char * ip_, int port_, const char * user_,		const char * pwd_, Computer_node *owner_):
		connected(false), port(port_), ip(ip_), user(user_), pwd(pwd_), owner(owner_)
	{
		result = NULL;
	}

	~PGSQL_CONN() { close_conn(); }

	int send_stmt(int pgres, const char *database, const char *stmt);

	Computer_node *get_owner() { return owner; }

	int connect(const char *database);
	void close_conn();
};

class Computer_node
{
public:
	uint id;
	uint cluster_id;
	std::string name;
	friend class PGSQL_CONN;
	PGSQL_CONN gpsql_conn;

	Computer_node(uint id_, uint cluster_id_, int port_,
		const char * name_, const char * ip_, const char * user_, const char * pwd_):
		id(id_), cluster_id(cluster_id_), name(name_),
		gpsql_conn(ip_, port_, user_, pwd_, this)
	{
		Assert(name_ && ip_ && user_ && pwd_);
		Assert(port_ > 0);
	}

	void refresh_node_configs(int port_,
		const char * name_, const char * ip_, const char * user_, const char * pwd_)
	{
		bool is_change = false;

		if(name != name_)
		{
			name = name_;
		}

		if(gpsql_conn.port != port_)
		{
			gpsql_conn.port = port_;
			is_change = true;
		}
			
		if(gpsql_conn.ip != ip_)
		{
			gpsql_conn.ip = ip_;
			is_change = true;
		}
			
		if(gpsql_conn.user != user_)
		{
			gpsql_conn.user = user_;
			is_change = true;
		}
			
		if(gpsql_conn.pwd != pwd_)
		{
			gpsql_conn.pwd = pwd_;
			is_change = true;
		}

		// close connect, it will be reconnect while next send_stmt
		if(is_change)
			close_conn();
	}

	int send_stmt(int pgres, const char *database, const char *stmt, int nretries = 1);
	void close_conn() { gpsql_conn.close_conn(); }
	PGresult *get_result() { return gpsql_conn.result; }
	void free_pgsql_result() { gpsql_conn.free_pgsql_result(); }
};

class MetadataSync
{
public:
	MetadataSync(){};

	int refresh_storages_to_computers(std::vector<Shard *> &storage_shards, std::vector<Computer_node *> &computer_nodes);
	int refresh_storages_to_computers_metashard(std::vector<Shard *> &storage_shards, std::vector<Computer_node *> &computer_nodes, MetadataShard &meta_shard);
	int truncate_commit_log_from_metadata_server(std::vector<Shard *> &storage_shards, MetadataShard &meta_shard);
};

#endif // !KL_CLUSTER_H

