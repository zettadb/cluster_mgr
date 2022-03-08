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
#include "kl_cluster.h"
#include "os.h"
#include "thread_manager.h"
#include <unistd.h>
#include <utility>
#include <time.h>
#include <sys/time.h>

int PGSQL_CONN::connect(const char *database)
{
	if(connected && db == database)
		return 0;
	else
		close_conn();
	
	char conninfo[256];
	sprintf(conninfo, "dbname=%s host=%s port=%d user=%s password=%s",
						database, ip.c_str(), port, user.c_str(), pwd.c_str());

	conn = PQconnectdb(conninfo);

	if (PQstatus(conn) != CONNECTION_OK)
	{
		syslog(Logger::ERROR, "Connected to pgsql fail: %s", PQerrorMessage(conn));
		return 1;
	}

	db = database;
	connected = true;
		
	return 0;
}

void PGSQL_CONN::close_conn()
{
	if(connected)
	{
		PQfinish(conn);
		connected = false;
	}
}

void PGSQL_CONN::free_pgsql_result()
{
    if (result)
    {
		PQclear(result);
		result = NULL;
    }
}

int PGSQL_CONN::send_stmt(int pgres, const char *database, const char *stmt)
{
	if (connect(database))
	{
		syslog(Logger::ERROR, "pgsql need to connect first");
		return 1;
	}

	int ret = 0;
	result = PQexec(conn, stmt);

	if(pgres == PG_COPYRES_TUPLES)
	{
		if (PQresultStatus(result) != PGRES_TUPLES_OK)
		{
			syslog(Logger::ERROR, "PQresultStatus error: %s", PQerrorMessage(conn));
			close_conn();
			ret = 1;
		}
	}
	else
	{
		if (PQresultStatus(result) != PGRES_COMMAND_OK)
		{
			syslog(Logger::ERROR, "PQresultStatus error: %s", PQerrorMessage(conn));
			close_conn();
			ret = 1;
		}
	}

	return ret;
}

/*
  If send stmt fails because connection broken, reconnect and
  retry sending the stmt. Retry pgsql_stmt_conn_retries times.
  @retval 1 on error, 0 if successful.
*/
int Computer_node::
send_stmt(int pgres, const char *database, const char *stmt, int nretries)
{
	int ret = 1;
	for (int i = 0; i < nretries; i++)
	{
		if (gpsql_conn.send_stmt(pgres, database, stmt) == 0)
		{
			ret = 0;
			break;
		}

		if (Thread_manager::do_exit)
			return 1;

		close_conn();
		usleep(stmt_retry_interval_ms * 1000);
	}
	return ret;
}

bool Computer_node::get_variables(std::string &variable, std::string &value)
{
	int ret;
	PGresult *presult;
	MYSQL_RES *result;
	MYSQL_ROW row;
	std::string str_sql;

	str_sql = "show " + variable;
	ret = send_stmt(PG_COPYRES_TUPLES, "postgres", str_sql.c_str(), stmt_retries);
	if(ret)
		return -1;

	presult = get_result();
	if(PQntuples(presult)==1)
	{
		value = PQgetvalue(presult,0,0);
	}
	else
	{
		ret = -1;
	}
	free_pgsql_result();

	return ret;
}

bool Computer_node::set_variables(std::string &variable, std::string &value_int, std::string &value_str)
{
	return true;
}

KunlunCluster::KunlunCluster(uint id_, const std::string &name_):
	id(id_),name(name_)
{
	pthread_mutex_init(&mtx, NULL);
}

KunlunCluster::~KunlunCluster()
{
	for (auto &i:storage_shards)
		delete i;
	for (auto &i:computer_nodes)
		delete i;
}

/*
  Connect to storage node, get tables' rows & pages, 
  and update to computer nodes.
*/
int KunlunCluster::refresh_storages_to_computers()
{
	int ret;
	PGresult *presult;
	MYSQL_RES *result;
	MYSQL_ROW row;
	char *endptr = NULL;
	std::string str_sql;
	
	std::vector<std::string> vec_database;
	std::vector<std::tuple<std::string, std::string, uint>> vec_database_namespace_oid;
	//map 				database	namespace	oid					table				page	row
	std::map<std::tuple<std::string, std::string, uint>, std::map<std::string, std::pair<uint, uint>>> map_dbnsid_table_page_row;
	
	////////////////////////////////////////////////////////
	//get TABLE_SCHEMA from one comp
	if(computer_nodes.size() == 0)
		return 0;
	Computer_node* computer = NULL;
	
	for(auto &node:computer_nodes)
	{
		if(node->send_stmt(PG_COPYRES_TUPLES, "postgres", "select datname from pg_database", stmt_retries)==0)
		{
			computer = node;
			break;
		}
	}

	if(computer == NULL)
	{
		syslog(Logger::ERROR, "none of computer node available!");
		return 1;
	}
	
	//get database
	ret = computer->send_stmt(PG_COPYRES_TUPLES, "postgres", "select datname from pg_database", stmt_retries);
	if(ret)
		return ret;

	presult = computer->get_result();
	for(int i=0;i<PQntuples(presult);i++)
	{
		//syslog(Logger::ERROR, "presult %d = %s", i, PQgetvalue(presult,i,0));
		std::string db = PQgetvalue(presult,i,0);
		if(db == "template1")
			continue;
		else if(db == "template0")
			continue;

		vec_database.emplace_back(db);
	}
	computer->free_pgsql_result();

	////////////////////////////////////////////////////////
	//get namespace from every database
	for(auto &db:vec_database)
	{
		ret = computer->send_stmt(PG_COPYRES_TUPLES, db.c_str(), "select oid,nspname from pg_namespace", stmt_retries);
		if(ret)
			return ret;

		presult = computer->get_result();
		for(int i=0;i<PQntuples(presult);i++)
		{
			//syslog(Logger::ERROR, "presult %d = %s", i, PQgetvalue(presult,i,0));
			std::string ns = PQgetvalue(presult,i,1);
			if(ns == "pg_toast")
				continue;
			else if(ns == "pg_temp_1")
				continue;
			else if(ns == "pg_toast_temp_1")
				continue;
			else if(ns == "pg_catalog")
				continue;
			else if(ns == "information_schema")
				continue;

			uint oid = strtol(PQgetvalue(presult,i,0), &endptr, 10);
			vec_database_namespace_oid.emplace_back(std::make_tuple(db, ns, oid));
		}
		computer->free_pgsql_result();
		computer->close_conn();
	}

	////////////////////////////////////////////////////////
	//get TABLE_NAME,TABLE_ROWS by TABLE_SCHEMA from storage_shards
	for(auto &shard:storage_shards)
	{
		Shard_node *master_sn = shard->get_master();
		if(shard->get_type() == Shard::METADATA || master_sn == NULL)
			continue;

		////////////////////////////////////////////////////////
		//get innodb_page_size
		uint page_size = shard->get_innodb_page_size();
		if(page_size == 0)
			continue;

		////////////////////////////////////////////////////////
		//get tables' rows&size, pages = size/page_size from every databases _$$_ namespace
		for(auto &db_ns_id:vec_database_namespace_oid)
		{
			str_sql = "select TABLE_NAME,TABLE_ROWS,DATA_LENGTH from information_schema.tables where table_type='BASE TABLE' and TABLE_SCHEMA='" + 
						std::get<0>(db_ns_id) + "_$$_" + std::get<1>(db_ns_id) + "'";
			//syslog(Logger::INFO, "str_sql11111111 = %s", str_sql.c_str());

			Scopped_mutex sm(shard->mtx);
			ret = master_sn->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);

			if (ret)
			   continue;
			result = master_sn->get_result();
			endptr = NULL;

			while ((row = mysql_fetch_row(result)))
			{
				uint rows = strtol(row[1], &endptr, 10);
				Assert(endptr == NULL || *endptr == '\0');
				uint pages = strtol(row[2], &endptr, 10);
				Assert(endptr == NULL || *endptr == '\0');
				pages = pages/page_size;

				auto it0 = map_dbnsid_table_page_row.find(db_ns_id);
				if(it0 == map_dbnsid_table_page_row.end())
				{
					std::map<std::string, std::pair<uint, uint>> map_table_page_row;
					map_table_page_row[row[0]] = std::make_pair(pages, rows);
					map_dbnsid_table_page_row[db_ns_id] = map_table_page_row;
				}
				else
				{
					auto it1 = it0->second.find(row[0]);
					if(it1 == it0->second.end())
					{
						it0->second[row[0]] = std::make_pair(pages, rows);
					}
					else	//may be a table in two shards
					{
						it1->second.first += pages;
						it1->second.second += rows;
					}
				}
			}
			
			master_sn->free_mysql_result();
		}
	}

	////////////////////////////////////////////////////////
	// refresh tables' pages&rows to computer_nodes by db,ns,ns_oid
	for(auto &comp:computer_nodes)
	{
		for(auto &dbnsid:map_dbnsid_table_page_row)
		{
			for(auto &tb_p_r:dbnsid.second)
			{
				str_sql = "update pg_class set relpages=" + std::to_string(tb_p_r.second.first) +
							",reltuples=" + std::to_string(tb_p_r.second.second) +
							" where relname='" + tb_p_r.first + 
							"' and relnamespace=" + std::to_string(std::get<2>(dbnsid.first));

				//syslog(Logger::INFO, "str_sql222222 = %s", str_sql.c_str());
				bool ret = comp->send_stmt(PG_COPYRES_EVENTS, std::get<0>(dbnsid.first).c_str(), str_sql.c_str(), stmt_retries);
				comp->free_pgsql_result();
				comp->close_conn();
			}
		}
	}

	return 0;
}

/*
  Connect to storage node, get num_tablets & space_volumn, 
  and update to computer nodes and meta shard.
*/
int KunlunCluster::refresh_storages_to_computers_metashard(MetadataShard &meta_shard)
{
	int ret;
	PGresult *presult;
	MYSQL_RES *result;
	MYSQL_ROW row;
	char *endptr = NULL;
	
	std::string str_sql;
	std::vector<std::string> vec_database;
	std::vector<std::pair<std::string, std::string>> vec_database_namespace;
	std::map<uint, std::pair<uint, uint>> map_shard_tables_space;
	
	////////////////////////////////////////////////////////
	//get TABLE_SCHEMA from one comp
	if(computer_nodes.size() == 0)
		return 0;
	Computer_node* computer = NULL;
	
	for(auto &node:computer_nodes)
	{
		if(node->send_stmt(PG_COPYRES_TUPLES, "postgres", "select datname from pg_database", stmt_retries)==0)
		{
			computer = node;
			break;
		}
	}

	if(computer == NULL)
	{
		syslog(Logger::ERROR, "none of computer node available!");
		return 1;
	}

	presult = computer->get_result();
	for(int i=0;i<PQntuples(presult);i++)
	{
		//syslog(Logger::ERROR, "presult %d = %s", i, PQgetvalue(presult,i,0));
		std::string db = PQgetvalue(presult,i,0);
		if(db == "template1")
			continue;
		else if(db == "template0")
			continue;

		vec_database.emplace_back(db);
	}
	computer->free_pgsql_result();

	////////////////////////////////////////////////////////
	//get namespace from every database
	for(auto &db:vec_database)
	{
		ret = computer->send_stmt(PG_COPYRES_TUPLES, db.c_str(), "select nspname from pg_namespace", stmt_retries);
		if(ret)
			return ret;

		presult = computer->get_result();
		for(int i=0;i<PQntuples(presult);i++)
		{
			//syslog(Logger::ERROR, "presult %d = %s", i, PQgetvalue(presult,i,0));
			std::string ns = PQgetvalue(presult,i,0);
			if(ns == "pg_toast")
				continue;
			else if(ns == "pg_temp_1")
				continue;
			else if(ns == "pg_toast_temp_1")
				continue;
			else if(ns == "pg_catalog")
				continue;
			else if(ns == "information_schema")
				continue;

			vec_database_namespace.emplace_back(std::make_pair(db, ns));
		}
		computer->free_pgsql_result();
		computer->close_conn();
	}

	////////////////////////////////////////////////////////
	//get tables' size&number from every shard
	for(auto &shard:storage_shards)
	{
		Shard_node *master_sn = shard->get_master();
		if(shard->get_type() == Shard::METADATA || master_sn == NULL)
			continue;

		////////////////////////////////////////////////////////
		//get tables' size&number from every databases _$$_ namespace
		uint num_tablets = 0;
		uint64_t space_volumn = 0;
		
		for(auto &db_ns:vec_database_namespace)
		{
			str_sql = "select count(*),sum(DATA_LENGTH) from information_schema.tables where table_type='BASE TABLE' and TABLE_SCHEMA='" + 
						db_ns.first + "_$$_" + db_ns.second + "'";
			//syslog(Logger::INFO, "str_sql777777 = %s", str_sql.c_str());

			Scopped_mutex sm(shard->mtx);
			ret = master_sn->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
			
			if (ret)
			   return ret;
			result = master_sn->get_result();
			endptr = NULL;

			if ((row = mysql_fetch_row(result)))
			{
				if(row[0] != NULL && row[1] != NULL)
				{
					num_tablets += strtol(row[0], &endptr, 10);
					Assert(endptr == NULL || *endptr == '\0');
					space_volumn += strtol(row[1], &endptr, 10);
					Assert(endptr == NULL || *endptr == '\0');
				}
			}
			
			master_sn->free_mysql_result();
		}

		map_shard_tables_space[shard->get_id()] = std::make_pair(num_tablets, space_volumn);
	}

	////////////////////////////////////////////////////////
	// refresh tables' size&number to MetadataShard by master meta
	Shard_node *meta_master_sn = meta_shard.get_master();
	if(meta_master_sn)
	{
		for(auto &sd_tb_sp:map_shard_tables_space)
		{
			str_sql = "update shards set space_volumn=" + std::to_string(sd_tb_sp.second.second) +
				",num_tablets=" + std::to_string(sd_tb_sp.second.first) +
				" where id=" + std::to_string(sd_tb_sp.first) +
				" and db_cluster_id=" + std::to_string(get_id());
			//syslog(Logger::INFO, "str_sql88888 = %s", str_sql.c_str());

			Scopped_mutex sm(meta_shard.mtx);
			meta_master_sn->send_stmt(SQLCOM_UPDATE, str_sql.c_str(), str_sql.length(), stmt_retries);
			meta_master_sn->free_mysql_result();
		}
	}
	
	////////////////////////////////////////////////////////
	// refresh tables' size&number to computer_nodes by any database
	for(auto &comp:computer_nodes)
	{
		for(auto &sd_tb_sp:map_shard_tables_space)
		{
			str_sql = "update pg_shard set space_volumn=" + std::to_string(sd_tb_sp.second.second) +
						",num_tablets=" + std::to_string(sd_tb_sp.second.first) +
						" where id=" + std::to_string(sd_tb_sp.first);
			
			//syslog(Logger::INFO, "str_sql99999 = %s", str_sql.c_str());
			bool ret = comp->send_stmt(PG_COPYRES_EVENTS, "postgres", str_sql.c_str(), stmt_retries);
			comp->free_pgsql_result();
		}
	}

	return 0;
}

/*
  Connect to meta data master node, truncate unused commit log partitions.
*/
int KunlunCluster::truncate_commit_log_from_metadata_server(std::vector<KunlunCluster *> &kl_clusters, MetadataShard &meta_shard)
{
	Shard_node *meta_master_sn = meta_shard.get_master();
	if(meta_master_sn == NULL)
		return -1;

	int ret;
	PGresult *presult;
	MYSQL_RES *result;
	MYSQL_ROW row;
	char *endptr = NULL;
	
	std::string str_sql;
	std::vector<std::pair<std::string, std::string>> vec_partition_tb;

	////////////////////////////////////////////////////////
	//get the time need to be truncate
	time_t timesp;
	time(&timesp);
	timesp = timesp - 60*60*commit_log_retention_hours;

	////////////////////////////////////////////////////////
	// get partitions need to truncate from information_schema.partitions
	str_sql = "select TABLE_NAME,SUBPARTITION_NAME from information_schema.partitions where \
TABLE_SCHEMA='kunlun_metadata_db' and TABLE_NAME like 'commit_log_%' and TABLE_ROWS>0 and \
unix_timestamp(UPDATE_TIME)<" + std::to_string(timesp);

	{
		Scopped_mutex sm(meta_shard.mtx);
		ret = meta_master_sn->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
		if (ret)
			return ret;
		result = meta_master_sn->get_result();
		
		while ((row = mysql_fetch_row(result)))
		{
			//syslog(Logger::ERROR, "vec_partition_tb row[0]=%s,row[1]=%s", row[0],row[1]);
			vec_partition_tb.emplace_back(std::make_pair(std::string(row[0]),std::string(row[1])));
		}
		
		meta_master_sn->free_mysql_result();
	}

	////////////////////////////////////////////////////////
	// get txnid by xa recover from every storage_shards
	std::set<std::string> set_recover;

	std::vector<Shard *> all_shards;
	all_shards.emplace_back(&meta_shard);
	for (auto &cluster:kl_clusters)
		for (auto &shard:cluster->storage_shards)
			all_shards.emplace_back(shard);

	for(auto &shard:all_shards)
	{
		Shard_node *master_sn = shard->get_master();
		if(shard->get_type() == Shard::METADATA || master_sn == NULL)
			continue;

		Scopped_mutex sm(shard->mtx);
		ret = master_sn->send_stmt(SQLCOM_SELECT, CONST_STR_PTR_LEN("xa recover"), stmt_retries);
		if (ret)
		{
			syslog(Logger::ERROR, "xa recover is no granted, please update install-mysql.py");
			continue;
		}
		result = master_sn->get_result();
		
		while ((row = mysql_fetch_row(result)))
		{
			set_recover.insert(std::string(row[3]));
		}
		
		master_sn->free_mysql_result();
	}

	////////////////////////////////////////////////////////
	// truncate the unused partition
	for(auto &ptb:vec_partition_tb)
	{
		if(set_recover.size() != 0)
		{
			bool txnid_unused = true;
			////////////////////////////////////////////////////////
			// get the whole partition txn_id form commit_log_%
			str_sql = "select comp_node_id,(txn_id>>32) as timestamp,(txn_id&0xffffffff) as txnid from " + ptb.first +
						" partition(" + ptb.second + ")";

			Scopped_mutex sm(meta_shard.mtx);
			ret = meta_master_sn->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
			if (ret)
				continue;
			result = meta_master_sn->get_result();

			while (txnid_unused && (row = mysql_fetch_row(result)))
			{
				std::string str_data(row[0]);
				str_data = str_data + "-" + row[1] + "-" + row[2];
				//syslog(Logger::ERROR, "commit log str_data=%s", str_data.c_str());
				//compare txnid with recover
				for(auto &recover:set_recover)
				{
					if(recover == str_data)
					{
						txnid_unused = false;
						syslog(Logger::ERROR, "xa recover date=%s as txnid is exist, it maybe error", recover.c_str());
						break;
					}
				}
			}
			
			meta_master_sn->free_mysql_result();

			if(!txnid_unused)
				continue;
		}

		////////////////////////////////////////////////////////
		// truncate unused partition
		str_sql = "alter table " + ptb.first +
					" truncate partition " + ptb.second;

		{
			Scopped_mutex sm(meta_shard.mtx);
			meta_master_sn->send_stmt(SQLCOM_ALTER_TABLE, str_sql.c_str(), str_sql.length(), stmt_retries);
			meta_master_sn->free_mysql_result();
		}
	}

	return 0;
}

