/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "machine_new.h"
#include <errno.h>
#include <unistd.h>

Machine_New::Machine_New() {

}

Machine_New::~Machine_New() {

}

bool Machine_New::update_machine_path_space(Json::Value &json_info) {

	std::string path;
	int used,free;

	std::vector<std::string> vec_path_index;
	vec_path_index.emplace_back("path0");
	vec_path_index.emplace_back("path1");
	vec_path_index.emplace_back("path2");
	vec_path_index.emplace_back("path3");

	for(int i=0; i<4; i++) {
		std::vector<Tpye_Path_Used_Free> vec_path_used_free;

		Json::Value json_path = json_info[vec_path_index[i]];
		int n = json_path.size();
		for(int j=0; j<n; j++)
		{
			Json::Value path_sub = json_path[j];
			path = path_sub["path"].asString();
			used = path_sub["used"].asInt();
			free = path_sub["free"].asInt();
			vec_path_used_free.emplace_back(std::make_tuple(path, used, free));
		}
		vec_vec_path_used_free.emplace_back(vec_path_used_free);
	}

	return true;
}

bool Machine_New::insert_machine_on_meta() {
	std::string str_sql;
	std::vector<std::pair<int,int>> vec_used_free;

	for(auto &vec_path_used_free: vec_vec_path_used_free) {
		int used = 0;
		int free = 0;

		for(auto &path_used_free:vec_path_used_free) {
			used += std::get<1>(path_used_free);
			free += std::get<2>(path_used_free);
		}

		vec_used_free.emplace_back(std::make_pair(used, free));
	}

	//insert server_nodes
	str_sql = "INSERT INTO server_nodes(hostaddr,rack_id,datadir,logdir,wal_log_dir,comp_datadir,total_mem,total_cpu_cores,svc_since) VALUES('";
	str_sql += hostaddr + "','" + rack_id + "','";
	str_sql += vec_paths[0] + "','" + vec_paths[1] + "','" + vec_paths[2] + "','" + vec_paths[3] + "',";
	str_sql += std::to_string(total_mem) + "," + std::to_string(total_cpu_cores) + ",NOW())";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_INSERT, str_sql))	{
		syslog(Logger::ERROR, "insert server_nodes error");
		return false;
	}

	//insert server_nodes_stats
	str_sql = "INSERT INTO server_nodes_stats(id,datadir_used,datadir_avail,wal_log_dir_used,wal_log_dir_avail,log_dir_used,log_dir_avail,";
	str_sql += "comp_datadir_used,comp_datadir_avail,avg_network_usage_pct) VALUES((select id from server_nodes where hostaddr='" + hostaddr + "'),";
	str_sql += std::to_string(vec_used_free[0].first) + "," + std::to_string(vec_used_free[0].second) + ",";
	str_sql += std::to_string(vec_used_free[1].first) + "," + std::to_string(vec_used_free[1].second) + ",";
	str_sql += std::to_string(vec_used_free[2].first) + "," + std::to_string(vec_used_free[2].second) + ",";
	str_sql += std::to_string(vec_used_free[3].first) + "," + std::to_string(vec_used_free[3].second) + ",0)";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_INSERT, str_sql))	{
		syslog(Logger::ERROR, "insert server_nodes_stats error!");
		return false;
	}

	return true;
}

bool Machine_New::update_machine_on_meta() {
	std::string str_sql;
	std::vector<std::pair<int,int>> vec_used_free;

	for(auto &vec_path_used_free: vec_vec_path_used_free) {
		int used = 0;
		int free = 0;

		for(auto &path_used_free:vec_path_used_free) {
			used += std::get<1>(path_used_free);
			free += std::get<2>(path_used_free);
		}

		vec_used_free.emplace_back(std::make_pair(used, free));
	}

	//update server_nodes
	str_sql = "UPDATE server_nodes set rack_id='" + rack_id + "',datadir='" + vec_paths[0] + "',logdir='";
	str_sql += vec_paths[1] + "',wal_log_dir='" + vec_paths[2] + "',comp_datadir='" + vec_paths[3];
	str_sql += "',total_mem=" + std::to_string(total_mem) + ",total_cpu_cores=" + std::to_string(total_cpu_cores);
	str_sql += " where hostaddr='" + hostaddr + "'";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_UPDATE, str_sql))	{
		syslog(Logger::ERROR, "update server_nodes error");
		return false;
	}

	//insert server_nodes_stats
	str_sql = "INSERT INTO server_nodes_stats(id,datadir_used,datadir_avail,wal_log_dir_used,wal_log_dir_avail,log_dir_used,log_dir_avail,";
	str_sql += "comp_datadir_used,comp_datadir_avail,avg_network_usage_pct) VALUES((select id from server_nodes where hostaddr='" + hostaddr + "'),";
	str_sql += std::to_string(vec_used_free[0].first) + "," + std::to_string(vec_used_free[0].second) + ",";
	str_sql += std::to_string(vec_used_free[1].first) + "," + std::to_string(vec_used_free[1].second) + ",";
	str_sql += std::to_string(vec_used_free[2].first) + "," + std::to_string(vec_used_free[2].second) + ",";
	str_sql += std::to_string(vec_used_free[3].first) + "," + std::to_string(vec_used_free[3].second) + ",0)";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_INSERT, str_sql)) {
		//update server_nodes_stats
		str_sql = "UPDATE server_nodes_stats set datadir_used=" + std::to_string(vec_used_free[0].first) + ",datadir_avail=" + std::to_string(vec_used_free[0].second);
		str_sql += ",log_dir_used=" + std::to_string(vec_used_free[1].first) + ",log_dir_avail=" + std::to_string(vec_used_free[1].second);
		str_sql += ",wal_log_dir_used=" + std::to_string(vec_used_free[2].first) + ",wal_log_dir_avail=" + std::to_string(vec_used_free[2].second);
		str_sql += ",comp_datadir_used=" + std::to_string(vec_used_free[3].first) + ",comp_datadir_avail=" + std::to_string(vec_used_free[3].second);
		str_sql += " where id=(select id from server_nodes where hostaddr='" + hostaddr + "')";
		//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

		if(System::get_instance()->execute_metadate_opertation(SQLCOM_UPDATE, str_sql)) {
			syslog(Logger::ERROR, "update server_nodes_stats error");
			return false;
		}
	}

	return true;
}

bool Machine_New::delete_machine_on_meta() {
	std::string str_sql;

	//delete server_nodes_stats
	str_sql = "DELETE FROM server_nodes_stats where id=(select id from server_nodes where hostaddr='" + hostaddr + "')";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_DELETE, str_sql))	{
		syslog(Logger::ERROR, "delete server_nodes_stats error");
		return false;
	}

	//delete server_nodes
	str_sql = "DELETE FROM server_nodes where hostaddr='" + hostaddr + "'";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_DELETE, str_sql))	{
		syslog(Logger::ERROR, "delete server_nodes error");
		return false;
	}

	return true;
}

