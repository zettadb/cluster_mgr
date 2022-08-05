/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "sys_config.h"
#include "global.h"
#include "config.h"
#include "zettalib/op_log.h"
#include "zettalib/tool_func.h"
#include "sys.h"
#include "shard.h"
#include "kl_cluster.h"
#include "os.h"
#include <unistd.h>
#include <utility>
#include <time.h>
#include <sys/time.h>

bool Computer_node::pg_connect(const std::string& database) {
	if(gpsql_conn) {
		if(db == database)
			return true;
		else 
			close_conn(); 
	}

	PGConnectionOption option;
	option.ip = ip;
    option.port_str = std::to_string(port);
    option.port_num = port;
    option.user = user;
    option.password = pwd;
    option.database = database;

	gpsql_conn = new PGConnection(option);
	bool ret = gpsql_conn->Connect();
	if(ret)
		db = database;
	else {
		KLOG_ERROR("connect pg failed: {}", gpsql_conn->getErr());
		delete gpsql_conn;
		gpsql_conn = nullptr;
	}
	return ret;
}

/*
  If send stmt fails because connection broken, reconnect and
  retry sending the stmt. Retry pgsql_stmt_conn_retries times.
  @retval 1 on error, 0 if successful.
*/
int Computer_node::
send_stmt(const char *database, const char *stmt, PgResult *result, int nretries) {
	KlWrapGuard<KlWrapMutex> guard(sql_mtx_);

	std::string db;
	db.assign(database, strlen(database));

	int ret = 1;
	for (int i = 0; i < nretries; i++) {
		if(pg_connect(db)) {
			if(gpsql_conn->ExecuteQuery(stmt, result) != -1) {
				ret = 0;
				break;
			} else {
				KLOG_ERROR("pg execute sql failed: {}", gpsql_conn->getErr());
			}
		}

		if (!System::get_instance()->get_cluster_mgr_working())
			return 1;

		close_conn();
		usleep(stmt_retry_interval_ms * 1000);
	}
	return ret;
}

bool Computer_node::get_variables(std::string &variable, std::string &value)
{
	int ret;
	size_t zero = 0;
	PgResult presult;
	std::string str_sql;

	str_sql = "show " + variable;
	ret = send_stmt("postgres", str_sql.c_str(), &presult, stmt_retries);
	if(ret)
		return -1;

	uint32_t nrows = presult.GetNumRows();
	if(nrows == 1) {
		value = presult[0][zero];
	}
	else
	{
		ret = -1;
	}

	return ret;
}

bool Computer_node::set_variables(std::string &variable, std::string &value_int, std::string &value_str)
{
	return true;
}

KunlunCluster::KunlunCluster(int id_, const std::string &name_):
	id(id_),name(name_), is_delete_(false) {
}

KunlunCluster::~KunlunCluster()
{
	//for (auto &i:storage_shards)
	//	delete i;
	//for (auto &i:computer_nodes)
	//	delete i;
}

/*
  Connect to storage node, get tables' rows & pages, 
  and update to computer nodes.
*/
int KunlunCluster::refresh_storages_to_computers()
{
	int ret;
	PgResult presult;
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
	
	ObjectPtr<Computer_node> computer;
	
	for(auto &node:computer_nodes) {
		if(node->send_stmt("postgres", "select datname from pg_database",
							&presult, stmt_retries)==0) {
			computer = node;
			break;
		}
	}

	if(!computer.Invalid()) {
		KLOG_ERROR("none of computer node available!");
		return 1;
	}
	
	//get database
	ret = computer->send_stmt("postgres", "select datname from pg_database", 
			&presult, stmt_retries);
	if(ret)
		return ret;

	uint32_t nrows = presult.GetNumRows();
	for(uint32_t i=0; i<nrows; i++) {
		std::string db = presult[i]["datname"];
		if(db == "template1")
			continue;
		else if(db == "template0")
			continue;

		vec_database.emplace_back(db);
	}

	////////////////////////////////////////////////////////
	//get namespace from every database
	for(auto &db:vec_database) {
		ret = computer->send_stmt(db.c_str(), "select oid,nspname from pg_namespace", 
					&presult, stmt_retries);
		if(ret)
			return ret;

		uint32_t nrows = presult.GetNumRows();
		for(uint32_t i=0; i<nrows; i++) {
			std::string ns = presult[i]["nspname"];//PQgetvalue(presult,i,1);
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

			uint oid = strtol(presult[i]["oid"], &endptr, 10);
			vec_database_namespace_oid.emplace_back(std::make_tuple(db, ns, oid));
		}
	}

	////////////////////////////////////////////////////////
	//get TABLE_NAME,TABLE_ROWS by TABLE_SCHEMA from storage_shards
	for(auto &shard:storage_shards) {
		ObjectPtr<Shard_node> master_sn = shard->get_master();
		if(shard->get_type() == Shard::METADATA || !master_sn.Invalid())
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

			
			//KlWrapGuard<KlWrapMutex> guard(shard->mtx);
			MysqlResult res;
			ret = master_sn->send_stmt(str_sql, &res, stmt_retries);
			if (ret)
			   continue;

			endptr = NULL;

			uint32_t nrows = res.GetResultLinesNum();
			for(uint32_t i=0; i<nrows; i++) {
				if(strcmp(res[i]["TABLE_ROWS"], "NULL") == 0 || strcmp(res[i]["TABLE_NAME"], "NULL") == 0 ||
						strcmp(res[i]["DATA_LENGTH"], "NULL") == 0)
					continue;

				uint rows = strtol(res[i]["TABLE_ROWS"], &endptr, 10);
				Assert(endptr == NULL || *endptr == '\0');
				uint pages = strtol(res[i]["DATA_LENGTH"], &endptr, 10);
				Assert(endptr == NULL || *endptr == '\0');
				pages = pages/page_size;

				auto it0 = map_dbnsid_table_page_row.find(db_ns_id);
				if(it0 == map_dbnsid_table_page_row.end()) {
					std::map<std::string, std::pair<uint, uint>> map_table_page_row;
					map_table_page_row[res[i]["TABLE_NAME"]] = std::make_pair(pages, rows);
					map_dbnsid_table_page_row[db_ns_id] = map_table_page_row;
				} else {
					auto it1 = it0->second.find(res[i]["TABLE_NAME"]);
					if(it1 == it0->second.end()) {
						it0->second[res[i]["TABLE_NAME"]] = std::make_pair(pages, rows);
					} else {	//may be a table in two shards
						it1->second.first += pages;
						it1->second.second += rows;
					}
				}
			}
			
		}
	}

	////////////////////////////////////////////////////////
	// refresh tables' pages&rows to computer_nodes by db,ns,ns_oid
	for(auto &comp:computer_nodes) {
		for(auto &dbnsid:map_dbnsid_table_page_row) {
			for(auto &tb_p_r:dbnsid.second) {
				str_sql = "update pg_class set relpages=" + std::to_string(tb_p_r.second.first) +
							",reltuples=" + std::to_string(tb_p_r.second.second) +
							" where relname='" + tb_p_r.first + 
							"' and relnamespace=" + std::to_string(std::get<2>(dbnsid.first));

				//syslog(Logger::INFO, "str_sql222222 = %s", str_sql.c_str());
				comp->send_stmt(std::get<0>(dbnsid.first).c_str(),
								str_sql.c_str(), &presult, stmt_retries);
			}
		}
	}

	return 0;
}

/*
  Connect to storage node, get num_tablets & space_volumn, 
  and update to computer nodes and meta shard.
*/
int KunlunCluster::refresh_storages_to_computers_metashard(ObjectPtr<MetadataShard> meta_shard) {
	int ret;
	PgResult presult;
	MysqlResult res;
	char *endptr = NULL;
	
	std::string str_sql;
	std::vector<std::string> vec_database;
	std::vector<std::pair<std::string, std::string>> vec_database_namespace;
	std::map<uint, std::pair<uint, uint>> map_shard_tables_space;
	
	////////////////////////////////////////////////////////
	//get TABLE_SCHEMA from one comp
	if(computer_nodes.size() == 0)
		return 0;
	
	ObjectPtr<Computer_node> computer;
	
	for(auto &node:computer_nodes)
	{
		//if(node->send_stmt(PG_COPYRES_TUPLES, "postgres", "select datname from pg_database", stmt_retries)==0)
		if(node->send_stmt("postgres", "select datname from pg_database", &presult, stmt_retries) == 0)
		{
			computer = node;
			break;
		}
	}

	if(!computer.Invalid()) {
		KLOG_ERROR("none of computer node available!");
		return 1;
	}

	uint32_t nrows = presult.GetNumRows();
	for(uint32_t i=0; i<nrows; i++) {
		//syslog(Logger::ERROR, "presult %d = %s", i, PQgetvalue(presult,i,0));
		std::string db = presult[i]["datname"];//PQgetvalue(presult,i,0);
		if(db == "template1")
			continue;
		else if(db == "template0")
			continue;

		vec_database.emplace_back(db);
	}

	////////////////////////////////////////////////////////
	//get namespace from every database
	for(auto &db:vec_database) {
		ret = computer->send_stmt(db.c_str(), "select nspname from pg_namespace", &presult, stmt_retries);
		if(ret)
			return ret;

		uint32_t nrows = presult.GetNumRows();
		for(uint32_t i=0; i<nrows; i++) {
			std::string ns = presult[i]["nspname"];//PQgetvalue(presult,i,0);
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
	}

	////////////////////////////////////////////////////////
	//get tables' size&number from every shard
	for(auto &shard:storage_shards)
	{
		ObjectPtr<Shard_node> master_sn = shard->get_master();
		if(shard->get_type() == Shard::METADATA || !master_sn.Invalid())
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
			ret = master_sn->send_stmt(str_sql, &res, stmt_retries);
			if (ret)
			   return ret;

			endptr = NULL;

			if (res.GetResultLinesNum() > 0) {
				if((strcmp(res[0]["count(*)"], "NULL") != 0) && (strcmp(res[0]["sum(DATA_LENGTH)"], "NULL") != 0)) {
					num_tablets += strtol(res[0]["count(*)"], &endptr, 10);
					Assert(endptr == NULL || *endptr == '\0');
					space_volumn += strtol(res[0]["sum(DATA_LENGTH)"], &endptr, 10);
					Assert(endptr == NULL || *endptr == '\0');
				}
			}
		}

		map_shard_tables_space[shard->get_id()] = std::make_pair(num_tablets, space_volumn);
	}

	////////////////////////////////////////////////////////
	// refresh tables' size&number to MetadataShard by master meta
	ObjectPtr<Shard_node> meta_master_sn = meta_shard->get_master();
	if(meta_master_sn.Invalid())
	{
		for(auto &sd_tb_sp:map_shard_tables_space)
		{
			str_sql = "update shards set space_volumn=" + std::to_string(sd_tb_sp.second.second) +
				",num_tablets=" + std::to_string(sd_tb_sp.second.first) +
				" where id=" + std::to_string(sd_tb_sp.first) +
				" and db_cluster_id=" + std::to_string(get_id());
			//syslog(Logger::INFO, "str_sql88888 = %s", str_sql.c_str());

			meta_master_sn->send_stmt(str_sql, &res, stmt_retries);
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
			comp->send_stmt("postgres", str_sql.c_str(), &presult, stmt_retries);
		}
	}

	return 0;
}

/*
  Connect to meta data master node, truncate unused commit log partitions.
*/
int KunlunCluster::truncate_commit_log_from_metadata_server(std::vector<ObjectPtr<KunlunCluster> > &kl_clusters, 
				ObjectPtr<MetadataShard> meta_shard) {
	ObjectPtr<Shard_node> meta_master_sn = meta_shard->get_master();
	if(!meta_master_sn.Invalid())
		return -1;

	int ret;
	char *endptr = NULL;
	
	std::string str_sql;
	std::vector<std::pair<std::string, std::string>> vec_partition_tb;

	////////////////////////////////////////////////////////
	//get the time need to be truncate
	time_t timesp;
	time(&timesp);
	timesp = timesp - 60*60*commit_log_retention_hours;

	MysqlResult res;
	////////////////////////////////////////////////////////
	// get partitions need to truncate from information_schema.partitions
	str_sql = "select TABLE_NAME,SUBPARTITION_NAME from information_schema.partitions where \
TABLE_SCHEMA='kunlun_metadata_db' and TABLE_NAME like 'commit_log_%' and TABLE_ROWS>0 and \
unix_timestamp(UPDATE_TIME)<" + std::to_string(timesp);

	{
		//Scopped_mutex sm(meta_shard.mtx);
		ret = meta_master_sn->send_stmt(str_sql, &res, stmt_retries);
		if (ret)
			return ret;
		
		uint32_t nrows = res.GetResultLinesNum();
		for(uint32_t i=0; i<nrows; i++) {
			vec_partition_tb.emplace_back(std::make_pair(std::string(res[i]["TABLE_NAME"]),
						std::string(res[i]["SUBPARTITION_NAME"])));
		}
	}

	////////////////////////////////////////////////////////
	// get txnid by xa recover from every storage_shards
	std::set<std::string> set_recover;

	std::vector<ObjectPtr<Shard> > all_shards;
	ObjectPtr<Shard> sd(dynamic_cast<Shard*>(meta_shard.GetTRaw()));
	all_shards.emplace_back(sd);
	for (auto &cluster:kl_clusters)
		for (auto &shard:cluster->storage_shards)
			all_shards.emplace_back(shard);

	for(auto &shard:all_shards)
	{
		ObjectPtr<Shard_node> master_sn = shard->get_master();
		if(shard->get_type() == Shard::METADATA || !master_sn.Invalid())
			continue;

		//Scopped_mutex sm(shard->mtx);
		//ret = master_sn->send_stmt(SQLCOM_SELECT, CONST_STR_PTR_LEN("xa recover"), stmt_retries);
		ret = master_sn->send_stmt("xa recover", &res, stmt_retries);
		if (ret)
		{
			KLOG_ERROR("xa recover is no granted, please update install-mysql.py");
			continue;
		}
		//result = master_sn->get_result();
		
		uint32_t nrows = res.GetResultLinesNum();
		//while ((row = mysql_fetch_row(result)))
		for(uint32_t i=0; i<nrows; i++)
		{
			set_recover.insert(std::string(res[i][3]));
		}
		
		//master_sn->free_mysql_result();
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

			
			KlWrapGuard<KlWrapMutex> guard(meta_shard->mtx);
			//ret = meta_master_sn->send_stmt(SQLCOM_SELECT, str_sql.c_str(), str_sql.length(), stmt_retries);
			ret = meta_master_sn->send_stmt(str_sql, &res, stmt_retries);
			if (ret)
				continue;
			//result = meta_master_sn->get_result();
			uint32_t nrows = res.GetResultLinesNum();
			//while (txnid_unused && (row = mysql_fetch_row(result)))
			for(uint32_t i=0; i<nrows && txnid_unused; i++)
			{
				std::string str_data(res[i]["comp_node_id"]);
				str_data = str_data + "-" + res[i]["timestamp"] + "-" + res[i]["txnid"];
				//syslog(Logger::ERROR, "commit log str_data=%s", str_data.c_str());
				//compare txnid with recover
				for(auto &recover:set_recover)
				{
					if(recover == str_data)
					{
						txnid_unused = false;
						KLOG_ERROR("xa recover date={} as txnid is exist, it maybe error", recover);
						break;
					}
				}
			}
			
			//meta_master_sn->free_mysql_result();

			if(!txnid_unused)
				continue;
		}

		////////////////////////////////////////////////////////
		// truncate unused partition
		str_sql = "alter table " + ptb.first +
					" truncate partition " + ptb.second;

		{
			//Scopped_mutex sm(meta_shard.mtx);
			meta_master_sn->send_stmt(str_sql, &res, stmt_retries);
			//meta_master_sn->send_stmt(SQLCOM_ALTER_TABLE, str_sql.c_str(), str_sql.length(), stmt_retries);
			//meta_master_sn->free_mysql_result();
		}
	}

	return 0;
}

/*
* 1. lock ddl_ops_log, get max id
* 2. scan all computer_nodes, find one computer_node which replay all ddl_ops_log
* 3. connect computer_node, and get database and database_tablename
* 4. connect master node of all storages in cluster, get database and database_tablename
* 5. send quest to node_mgr, and get database and database_tablename
* 6. unlock ddl_ops_log
* 7. find storage node surplus records and drop these records later
* 	7.1 check tables in database which will be dropped
* 	7.2 check records in table which will be dropped
* 8. remove surplus records when step 7, and get left records
* 	8.1 check files in database directory which will be remove
*/
void KunlunCluster::deal_cn_ddl_undoing_jobs(ObjectPtr<MetadataShard> meta_shard) {
	ObjectPtr<Shard_node> master = meta_shard->get_master();
	if(!master.Invalid())
		return;

	//step 1
	bool ret = lock_ddl_locks(master);
	if(ret) {
		return;
	}

	//step 2
	std::string sql = string_sprintf("select max(id) from %s.ddl_ops_log_%s", KUNLUN_METADATA_DB_NAME,
						name.c_str());
	MysqlResult result;
	ret = master->send_stmt(sql, &result, stmt_retries);
	if(ret) {
		KLOG_ERROR("get max id from ddl_ops_log_{} failed", name);
		unlock_ddl_locks(master);
		return;
	}

	int max_id = atoi(result[0]["max(id)"]);
	ObjectPtr<Computer_node> computer;
	PgResult presult;
	for(auto cn : computer_nodes) {
		if(cn->send_stmt("postgres", "select max(ddl_op_id) from pg_ddl_log_progress", &presult, stmt_retries) == 0) {
			if(presult.GetNumRows() != 1)
				continue;

			int ddl_op_id = atoi(presult[0]["max"]);
			if(max_id == ddl_op_id) {
				computer = cn;
				break;
			}	
		}
	}

	if(!computer.Invalid()) {
		KLOG_ERROR("cluster {} can't get computer node which replay all ddl_ops_logs", name);
		unlock_ddl_locks(master);
		return;
	}

	//step 3
	std::map<std::string, std::vector<std::string> > cn_db_map_table;
	if(get_computer_database_namespace(computer, cn_db_map_table)) {
		KLOG_ERROR("get db_map_table from computer_node failed");
		unlock_ddl_locks(master);
		return;
	}

	std::vector<std::tuple<ObjectPtr<Shard>, std::map<std::string, std::vector<std::string> > > > sn_db_map_tables;
	std::vector<std::tuple<ObjectPtr<Shard>, std::map<std::string, std::vector<std::string> > > > nm_db_map_tables;
	for(auto ss : storage_shards) {
		//step 4
		std::map<std::string, std::vector<std::string> > sn_db_map_table;
		if(get_storage_database_namespace(ss, sn_db_map_table)) {
			KLOG_ERROR("get shard {} db_map_table failed");
			continue;
		}
		std::tuple<ObjectPtr<Shard>, std::map<std::string, std::vector<std::string> > > tmp{ss, sn_db_map_table};
		sn_db_map_tables.emplace_back(tmp);
		//step 5
		std::map<std::string, std::vector<std::string> > nm_db_map_table;
		if(get_node_mgr_database_namespace(ss, nm_db_map_table)) {
			KLOG_ERROR("get db_map_table from node_mgr failed");
			continue;
		}
		std::tuple<ObjectPtr<Shard>, std::map<std::string, std::vector<std::string> > > tmp1{ss, nm_db_map_table};
		nm_db_map_tables.emplace_back(tmp1);
	}

	//step 6
	unlock_ddl_locks(master);

	//step 7
	std::vector<std::tuple<ObjectPtr<Shard>, std::vector<std::string> > > drop_dbs;
	std::vector<std::tuple<ObjectPtr<Shard>, std::vector<std::string> > > drop_tbs;

	for(auto sn_dts : sn_db_map_tables) {
		ObjectPtr<Shard> shard = std::get<0>(sn_dts);
		auto sn_db_map_table = std::get<1>(sn_dts);

		std::vector<std::string> dbs;
		std::vector<std::string> tbs;
		for(auto &sn_dt : sn_db_map_table) {
			if(cn_db_map_table.find(sn_dt.first) == cn_db_map_table.end()) {
				if(sn_dt.second.size() == 0)
					dbs.emplace_back(sn_dt.first);
			} else {
				std::vector<std::string> sn_tables = sn_dt.second;
				std::vector<std::string> cn_tables = cn_db_map_table[sn_dt.first];
				for(auto st : sn_tables) {
					if(std::find(cn_tables.begin(), cn_tables.end(), st) == cn_tables.end())
						tbs.emplace_back(sn_dt.first+"."+st);
				}
			}
		}
		std::tuple<ObjectPtr<Shard>, std::vector<std::string> > tmp{shard, dbs};
		drop_dbs.emplace_back(tmp);
		std::tuple<ObjectPtr<Shard>, std::vector<std::string> > tmp1{shard, tbs};
		drop_tbs.emplace_back(tmp1);
	}

	for(auto dt : drop_tbs) {
		ObjectPtr<Shard> shard = std::get<0>(dt);
		std::vector<std::string> tbs = std::get<1>(dt);
		ObjectPtr<Shard_node> master = shard->get_master();
		if(!master.Invalid()) 
			continue;

		for(auto tb : tbs) {
			sql = string_sprintf("select count(*) from %s", tb.c_str());
			ret = master->send_stmt(sql, &result, stmt_retries);
			if(ret) {
				KLOG_ERROR("check table {} record failed", tb);
				continue;
			}
			if(result.GetResultLinesNum() != 1)
				continue;

			int count = atoi(result[0]["count(*)"]);
			if(count == 0) {
				sql = string_sprintf("drop table %s", tb.c_str());
				KLOG_INFO("cluster_mgr will execute sql: {}", sql);
				master->send_stmt(sql, &result, stmt_retries);
			}
		}
	}

	for(auto dd : drop_dbs) {
		ObjectPtr<Shard> shard = std::get<0>(dd);
		std::vector<std::string> dbs = std::get<1>(dd);
		ObjectPtr<Shard_node> master = shard->get_master();
		if(!master.Invalid()) 
			continue;

		for(auto db : dbs) {
			sql = string_sprintf("drop database %s", db.c_str());
			KLOG_INFO("cluster_mgr will execute sql: {}", sql);
			master->send_stmt(sql, &result, stmt_retries);
		}
	}
}

bool KunlunCluster::lock_ddl_locks(ObjectPtr<Shard_node> master) {
	std::string sql = "SELECT GET_LOCK('DDL', 30)";
	MysqlResult result;

	bool ret = master->send_stmt(sql, &result, 1);
	if(ret) {
		KLOG_ERROR("get ddl ops log lock failed");
		return true;
	}
	return false;
}

void KunlunCluster::unlock_ddl_locks(ObjectPtr<Shard_node> master) {
	std::string sql = "SELECT RELEASE_LOCK('DDL')";
	MysqlResult result;

  	bool ret = master->send_stmt(sql, &result, stmt_retries);
  	if(ret) {
    	KLOG_ERROR("release ddl ops log lock failed");
    	return;
  	}
}

int KunlunCluster::get_storage_database_namespace(ObjectPtr<Shard> shard, 
					std::map<std::string, std::vector<std::string> >& db_map_tables) {
	ObjectPtr<Shard_node> master = shard->get_master();
	if(!master.Invalid()) {
		KLOG_ERROR("get shard {} master node failed", shard->get_id());
		return 1;
	}
	std::vector<std::string> vec_dbs;
	MysqlResult result;
	std::string sql;
	bool ret = master->send_stmt("show databases", &result, stmt_retries);
	if(ret) {
		KLOG_ERROR("shard {} master node show databases failed", shard->get_id());
		return 1;
	}

	int nrows = result.GetResultLinesNum();
	for(int i=0; i<nrows; i++) {
		std::string db = result[i]["Database"];
		if(db == "information_schema" || db == "kunlun_sysdb" || db == "mysql" || 
				db == "performance_schema" || db == "sys")
			continue;
		vec_dbs.emplace_back(db);
	}

	for(auto db : vec_dbs) {
		sql = string_sprintf("select TABLE_NAME from information_schema.tables where TABLE_SCHEMA='%s' and TABLE_TYPE='BASE TYPE'",
					db.c_str());
		ret = master->send_stmt(sql.c_str(), &result, stmt_retries);
		if(ret) {
			KLOG_ERROR("shard {} master node get table name failed", shard->get_id());
			return 1;
		}
		std::vector<std::string> tables;
		nrows = result.GetResultLinesNum();
		for(int i=0; i<nrows; i++) {
			std::string tb = result[i]["TABLE_NAME"];
			tables.emplace_back(tb);
		}

		db_map_tables[db] = tables;
	}
	return 0;
}

int KunlunCluster::get_node_mgr_database_namespace(ObjectPtr<Shard> shard, 
					std::map<std::string, std::vector<std::string> >& db_map_tables) {
	return 0;
}

int KunlunCluster::get_computer_database_namespace(ObjectPtr<Computer_node> computer,
			std::map<std::string, std::vector<std::string> >& db_map_tables) {
	std::vector<std::string> vec_database;
	std::vector<std::string> vec_namespace;
	PgResult presult;
	int ret = computer->send_stmt("postgres", "select datname from pg_database", &presult, stmt_retries);
	if(ret) {
		KLOG_ERROR("select pg_database failed");
		return ret;
	}
	uint32_t nrows = presult.GetNumRows();
	for(uint32_t i=0; i<nrows; i++) {
		std::string db = presult[i]["datname"];
		if(db == "template1")
			continue;
		else if(db == "template0")
			continue;
		else if(db == "postgres")
			continue;

		vec_database.emplace_back(db);
	}

	std::string sql;
	for(auto db: vec_database) {
		ret = computer->send_stmt(db.c_str(), "select nspname from pg_namespace", &presult, stmt_retries);
		if(ret) {
			KLOG_ERROR("select pg_namespace failed");
			return ret;
		}

		uint32_t nrows = presult.GetNumRows();
		for(uint32_t i=0; i<nrows; i++) {
			std::string ns = presult[i]["nspname"];
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

			vec_namespace.emplace_back(ns);
		}

		for(auto ns : vec_namespace) {
			sql = string_sprintf("select tablename from pg_tables where schemaname='%s'", ns.c_str());
			ret = computer->send_stmt(db.c_str(), sql.c_str(), &presult, stmt_retries);
			if(ret) {
				KLOG_ERROR("select pg_tables failed");
				return ret;
			}

			std::vector<std::string> tables;
			nrows = presult.GetNumRows();
			for(uint32_t i=0; i<nrows; i++) {
				std::string tb = presult[i]["tablename"];
				tables.emplace_back(tb);
			}
			std::string dbname = db+"_$$_"+ns;
			db_map_tables[dbname] = tables;
		}
	}
	return 0;
}

ObjectPtr<Computer_node> KunlunCluster::get_coldbackup_comp_node(){
  // TODO: is valide compute node

  if(computer_nodes.size() == 0){
    KLOG_INFO("no avilable compute node in current klcluster ({} {})",get_name(),get_id());
    return nullptr;
  }
  auto begin = computer_nodes.begin();
  return *begin;  
  //auto iter = computer_nodes.begin();
  //for(;iter != computer_nodes.end();iter++){
  //  return *iter;
  //}

}

