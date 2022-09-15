/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include <regex>
#include "cluster_comm.h"
#include "zettalib/op_mysql.h"
#include "zettalib/op_pg.h"
#include "kl_mentain/shard.h"
#include "zettalib/op_log.h"
#include "zettalib/tool_func.h"
#include "http_server/node_channel.h"
#include "util_func/error_code.h"

namespace kunlun
{

//std::mutex port_mux_;
KlWrapMutex port_mux_;

bool UpdateInstallingPort(ColOp_Type type, MachType itype, 
                          const std::map<std::string, std::vector<int> >& host_ports) {
  KlWrapGuard<KlWrapMutex> guard(port_mux_);
  bool ret = true;
  MysqlResult result;
  std::string sql;
  std::string machine_type = "storage";
  if(itype == M_COMPUTER)
    machine_type = "computer";

  for(auto &it : host_ports) {
    sql = string_sprintf("select installing_port from %s.server_nodes where hostaddr='%s' and machine_type='%s'",
                  KUNLUN_METADATA_DB_NAME, it.first.c_str(), machine_type.c_str());
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
      KLOG_ERROR("get hostaddr {} machine_type {} installing port from server_nodes failed {}", it.first, 
              machine_type, g_node_channel_manager->getErr());
      return false;
    }

    if(result.GetResultLinesNum() != 1) {
      KLOG_ERROR("get hostaddr {} machine_type {} is too many, please check", it.first, machine_type);
      return false;
    }

    std::string install_ports = result[0]["installing_port"];
    if(install_ports == "NULL")
      install_ports.clear();

    std::vector<int> ports = it.second;
    if(type == K_APPEND) {
      for(size_t i=0; i<ports.size();i++) 
        install_ports += std::to_string(ports[i])+",";
    } else if(type == K_CLEAR) {
      std::vector<std::string> install_vec = StringTokenize(install_ports, ",");
      std::vector<int> left_ports;
      for(size_t i=0; i<install_vec.size(); i++) {
        int port = atoi(install_vec[i].c_str());
        if(std::find(ports.begin(), ports.end(), port) != ports.end())
          continue;

        left_ports.emplace_back(port);
      }
      install_ports.clear();
      for(size_t i=0; i<left_ports.size();i++) 
        install_ports += std::to_string(left_ports[i])+",";

      if(install_ports.empty())
        install_ports = "NULL";
    }

    sql = string_sprintf("update %s.server_nodes set installing_port='%s' where hostaddr='%s' and machine_type='%s'",
                KUNLUN_METADATA_DB_NAME, install_ports.c_str(), it.first.c_str(), machine_type.c_str());
    
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
      KLOG_ERROR("set hostaddr {} machine_type {} installing port from server_nodes failed {}", it.first, 
              machine_type, g_node_channel_manager->getErr());
      return false;
    }
  }

  return true;
}

bool UpdateUsedPort(ColOp_Type type, MachType itype, 
                      const std::map<std::string, std::vector<int> >& host_ports) {
  KlWrapGuard<KlWrapMutex> guard(port_mux_);                        
  bool ret = true;
  MysqlResult result;
  std::string sql;
  std::string machine_type = "storage";
  if(itype == M_COMPUTER)
    machine_type = "computer";

  for(auto &it : host_ports) {
    sql = string_sprintf("select used_port from %s.server_nodes where hostaddr='%s' and machine_type='%s'",
                  KUNLUN_METADATA_DB_NAME, it.first.c_str(), machine_type.c_str());
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
      KLOG_ERROR("get hostaddr {} machine_type {} used port from server_nodes failed {}", it.first, 
              machine_type, g_node_channel_manager->getErr());
      return false;
    }

    if(result.GetResultLinesNum() != 1) {
      KLOG_ERROR("get hostaddr {} machine_type {} is too many, please check", it.first, machine_type);
      return false;
    }

    std::string used_ports = result[0]["used_port"];
    if(used_ports == "NULL")
      used_ports.clear();

    std::vector<int> ports = it.second;
    if(type == K_APPEND) {
      for(size_t i=0; i<ports.size();i++) 
        used_ports += std::to_string(ports[i])+",";
    } else if(type == K_CLEAR) {
      std::vector<std::string> used_vec = StringTokenize(used_ports, ",");
      std::vector<int> left_ports;
      for(size_t i=0; i<used_vec.size(); i++) {
        int port = atoi(used_vec[i].c_str());
        if(std::find(ports.begin(), ports.end(), port) != ports.end())
          continue;

        left_ports.emplace_back(port);
      }
      used_ports.clear();
      for(size_t i=0; i<left_ports.size();i++) 
        used_ports += std::to_string(left_ports[i])+",";
      if(used_ports.empty())
        used_ports = "NULL";
    }

    sql = string_sprintf("update %s.server_nodes set used_port='%s' where hostaddr='%s' and machine_type='%s'",
                KUNLUN_METADATA_DB_NAME, used_ports.c_str(), it.first.c_str(), machine_type.c_str());
    
    ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
      KLOG_ERROR("set hostaddr {} machine_type {} used port from server_nodes failed {}", it.first, 
              machine_type, g_node_channel_manager->getErr());
      return false;
    }
  }

  return true;
}

bool GetStorageInstallHost(const std::string& hostaddr, std::vector<Host_Param>& avail_hosts,
                        std::unordered_map<std::string, std::vector<int> >& host_ports, int err_code) {
  KlWrapGuard<KlWrapMutex> guard(port_mux_);
  std::string sql = string_sprintf("select used_port, installing_port, port_range from %s.server_nodes where hostaddr='%s' and machine_type='storage' and node_stats='running'",
                KUNLUN_METADATA_DB_NAME, hostaddr.c_str());
  MysqlResult result;
  bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  if(ret) {
    err_code = GET_STORAGE_HOSTADDR_PARAM_SERVER_NODES_SQL_ERROR;
    KLOG_ERROR("get hostaddr {} parameters from server_nodes failed {}", hostaddr, g_node_channel_manager->getErr());
    return false;
  }

  if(result.GetResultLinesNum() != 1) {
    err_code = GET_STORAGE_HOSTADDR_PARAM_NUMBER_ERROR;
    KLOG_ERROR("get hostaddr {} storage node failed", hostaddr);
    return false;
  }

  std::string used_port = result[0]["used_port"];
  std::string installing_port = result[0]["installing_port"];
  std::string port_range = result[0]["port_range"];
  std::string sport_end = port_range.substr(port_range.rfind("-")+1);
  std::string sport_begin = port_range.substr(0, port_range.rfind("-"));
  int port_num = 0, max_port=0;
  int port_end = atoi(sport_end.c_str());
  int port_begin = atoi(sport_begin.c_str());

  if(!(used_port.empty() || used_port == "NULL")) {
    std::vector<std::string> used_ports_vec = StringTokenize(used_port, ",");
    port_num += (int)used_ports_vec.size();
    std::vector<int> ports_int;
    for(size_t i=0; i<used_ports_vec.size(); i++) {
      int port = atoi(used_ports_vec[i].c_str());
      ports_int.push_back(port);
      if(port < max_port)
        max_port = port;
    }
    host_ports[hostaddr] = ports_int;
  }

  if(!(installing_port.empty() || installing_port == "NULL")) {
    std::vector<std::string> install_ports_vec = StringTokenize(installing_port, ",");
    port_num += (int)install_ports_vec.size();
    std::vector<int> ports_int;
    if(host_ports.find(hostaddr) != host_ports.end())
      ports_int = host_ports[hostaddr];

    for(size_t i=0; i<install_ports_vec.size(); i++) {
      int port = atoi(install_ports_vec[i].c_str());
      ports_int.push_back(port);
      if(port < max_port) 
        max_port = port;
    }
    host_ports[hostaddr] = ports_int;
  }

  Host_Param hparam{hostaddr, max_port, port_begin, port_end, port_num, 0};
  avail_hosts.emplace_back(hparam);
  return true;
}

bool GetComputerInstallHost(const std::string& hostaddr, std::vector<Host_Param>& avail_hosts,
                        std::unordered_map<std::string, std::vector<int> >& host_ports, int err_code) {
  KlWrapGuard<KlWrapMutex> guard(port_mux_);
  std::string sql = string_sprintf("select used_port, installing_port, port_range from %s.server_nodes where hostaddr='%s' and machine_type='computer' and node_stats='running'",
                KUNLUN_METADATA_DB_NAME, hostaddr.c_str());
  MysqlResult result;
  bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  if(ret) {
    err_code = GET_COMPUTER_HOSTADDR_PARAM_SERVER_NODES_SQL_ERROR;
    KLOG_ERROR("get hostaddr {} parameters from server_nodes failed {}", hostaddr, g_node_channel_manager->getErr());
    return false;
  }

  if(result.GetResultLinesNum() != 1) {
    err_code = GET_COMPUTER_HOSTADDR_PARAM_NUMBER_ERROR;
    KLOG_ERROR("get hostaddr {} computer node failed", hostaddr);
    return false;
  }

  std::string used_port = result[0]["used_port"];
  std::string installing_port = result[0]["installing_port"];
  std::string port_range = result[0]["port_range"];
  std::string sport_end = port_range.substr(port_range.rfind("-")+1);
  std::string sport_begin = port_range.substr(0, port_range.rfind("-"));
  int port_num = 0, max_port=0;
  int port_end = atoi(sport_end.c_str());
  int port_begin = atoi(sport_begin.c_str());

  if(!(used_port.empty() || used_port == "NULL")) {
    std::vector<std::string> used_ports_vec = StringTokenize(used_port, ",");
    port_num += (int)used_ports_vec.size();
    std::vector<int> ports_int;
    for(size_t i=0; i<used_ports_vec.size(); i++) {
      int port = atoi(used_ports_vec[i].c_str());
      ports_int.push_back(port);
      if(port > max_port)
        max_port = port;
    }
    host_ports[hostaddr] = ports_int;
  }
  if(!(installing_port.empty() || installing_port == "NULL")) {
    std::vector<std::string> install_ports_vec = StringTokenize(installing_port, ",");
    port_num += (int)install_ports_vec.size();
    std::vector<int> ports_int;
    if(host_ports.find(hostaddr) != host_ports.end())
      ports_int = host_ports[hostaddr];

    for(size_t i=0; i<install_ports_vec.size(); i++) {
      int port = atoi(install_ports_vec[i].c_str());
      ports_int.push_back(port);
      if(port > max_port) 
        max_port = port;
    }
    host_ports[hostaddr] = ports_int;
  }

  Host_Param hparam{hostaddr, max_port, port_begin, port_end, port_num, 1};
  avail_hosts.emplace_back(hparam);
  return true;
}

bool GetBestInstallHosts(std::vector<Host_Param>& avail_hosts, int node_num, std::vector<IComm_Param>& iparams, 
                std::unordered_map<std::string, std::vector<int> >& host_ports) {
  if(avail_hosts.size() == 0) 
    return false;
  
  Host_Param hparam;
  while(true) {
    if(avail_hosts.size() <= 0)
      break;

    if(iparams.size() == node_num) {
        break;
    }

    std::sort(avail_hosts.begin(), avail_hosts.end(), HostParamComp());
    hparam = avail_hosts[0];
    avail_hosts.erase(avail_hosts.begin());

    int port_delta = 4;
    if(std::get<5>(hparam) == 1)
      port_delta = 3;

    int port_num = std::get<4>(hparam);
    int port_begin = std::get<2>(hparam);
    int port_end = std::get<3>(hparam);
    std::string hostaddr = std::get<0>(hparam);
    if(port_num >= (port_end - port_begin)/port_delta)
      continue;

    int max_port = std::get<1>(hparam);
    if(max_port + port_delta >= port_end || max_port == 0) {
      max_port = port_begin;
      max_port++;
    } else
      max_port += port_delta;
      
    std::vector<int> ports_int;
    if(host_ports.find(hostaddr) != host_ports.end()) {
      ports_int = host_ports[hostaddr];

      for(size_t i=0; i<ports_int.size(); i++) {
        if(std::find(ports_int.begin(), ports_int.end(), max_port) == ports_int.end())
          break;

        max_port += port_delta;
      }
    }

    if(max_port + port_delta <= port_end) {
      IComm_Param iparam {hostaddr, max_port};
      iparams.emplace_back(iparam);
    }

    std::get<4>(hparam) += 1;
    std::get<1>(hparam) = max_port;
    ports_int.push_back(max_port);
    host_ports[hostaddr] = ports_int;
    avail_hosts.push_back(hparam);
  }
  return true;
}

std::string GetMysqlUuidByHost(const std::string& shard_name, const std::string& cluster_name, 
                const std::string& host) {
	std::string value;
	std::string ip = host.substr(0, host.rfind("_"));
	std::string port = host.substr(host.rfind("_")+1);

	MysqlConnectionOption op;
    op.ip = ip;
    op.port_str = port;
    op.port_num = atoi(port.c_str());
	op.user = meta_svr_user;
	op.password = meta_svr_pwd;

	MysqlConnection mysql_conn(op);
	if(!mysql_conn.Connect()) {
		KLOG_ERROR("connect host: {} mysql failed {}", host, mysql_conn.getErr());
		return value;
	}

	MysqlResult res;
  std::string sql = string_sprintf("insert into kunlun_sysdb.cluster_info(cluster_name,shard_name) values('%s','%s')",
                cluster_name.c_str(), shard_name.c_str());
  if(mysql_conn.ExcuteQuery(sql.c_str(), &res) == -1) {
    KLOG_ERROR("insert cluster_info failed {}", mysql_conn.getErr());
    return value;
  }

	sql = string_sprintf("show global variables like 'server_uuid'");
	if(mysql_conn.ExcuteQuery(sql.c_str(), &res) == -1) {
		KLOG_ERROR("mysql execute failed {}", mysql_conn.getErr());
		return value;
	}

	if(res.GetResultLinesNum() != 1) {
		KLOG_ERROR("get sql value num wrong");
		return value;
	}
	value = res[0]["Value"];
	return value;
}

bool PersistFullsyncLevel(const std::vector<std::string>& hosts, int fullsync_level) {
  if(fullsync_level == 1)
    return true;

  std::string sql = string_sprintf("set persist fullsync_consistency_level=%d", fullsync_level);
  for(auto it : hosts) {
    std::string ip = it.substr(0, it.rfind("_"));
    std::string port = it.substr(it.rfind("_")+1);

    MysqlConnectionOption op;
    op.ip = ip;
    op.port_str = port;
    op.port_num = atoi(port.c_str());
    op.user = meta_svr_user;
    op.password = meta_svr_pwd;

    MysqlConnection mysql_conn(op);
    if(!mysql_conn.Connect()) {
      KLOG_ERROR("connect host: {} mysql failed {}", it, mysql_conn.getErr());
      return false;
    }

    MysqlResult res;
    if(mysql_conn.ExcuteQuery(sql.c_str(), &res) == -1) {
      KLOG_ERROR("host {} persist fullsync_level failed {}", it, mysql_conn.getErr());
      return false;
    }
  }
  return true;
}

bool UpdatePgTables(const std::string& ip, const std::string& port, 
                    const std::string& comp_user, const std::string& comp_pwd,
                    int cluster_id, int type,
                    const std::unordered_map<std::string, std::vector<IComm_Param> >& storage_iparams,
                    const std::unordered_map<std::string, std::vector<std::string> >& shard_map_nids,
                    const std::unordered_map<std::string, std::string>& shard_map_sids,
                    std::vector<std::string>& sql_stmts) {
  bool ret = true;
  PGConnectionOption option;
  option.ip = ip;
  option.port_num = atoi(port.c_str());
  option.user = comp_user;
  option.password = comp_pwd;
  option.database = "postgres";

  PGConnection pg_conn(option);
  if(!pg_conn.Connect()) {
    KLOG_ERROR("connect pg failed {}", pg_conn.getErr());
    ret = false;
  }
  PgResult pgresult;
  std::string sql;

  for(auto &shard : storage_iparams) {
    sql = "start transaction";
    if(ret && (pg_conn.ExecuteQuery(sql.c_str(), &pgresult) == -1)) {
      KLOG_ERROR("postgres start transaction failed {}", pg_conn.getErr());
      ret = false;
    }

    std::string shard_name = shard.first;
    std::vector<IComm_Param> iparams = shard.second;
    std::vector<std::string> node_ids = shard_map_nids.at(shard_name);
    std::string shard_id = shard_map_sids.at(shard_name);
    if(type == 0) {
        sql = string_sprintf("insert into pg_shard(name, id, num_nodes, master_node_id, space_volumn, num_tablets, db_cluster_id, when_created) values('%s', %s, %d, %s, 0, 0, %d, now())",
              shard_name.c_str(), shard_id.c_str(), shard.second.size(), node_ids[0].c_str(), cluster_id);
        sql_stmts.push_back(sql);
        KLOG_INFO("insert pg_shard sql: {}", sql);
        if(ret && (pg_conn.ExecuteQuery(sql.c_str(), &pgresult) == -1)) {
            KLOG_ERROR("postgres insert pg_shard failed {}", pg_conn.getErr());
            ret = false;
        }
    } else {
        sql = string_sprintf("select num_nodes from pg_shard where id=%s", shard_id.c_str());
        KLOG_INFO("select pg_shard sql: {}", sql);
        if(ret && (pg_conn.ExecuteQuery(sql.c_str(), &pgresult) == -1)) {
            KLOG_ERROR("postgres select pg_shard failed {}", pg_conn.getErr());
            ret = false;
        }

        if(ret) {
          if(pgresult.GetNumRows() != 1) {
              KLOG_ERROR("get shard_id {} pg_shard records too many", shard_id);
              ret = false;
          }

          if(ret) {
            int num_nodes = atoi(pgresult[0]["num_nodes"]);
            sql = string_sprintf("update pg_shard set num_nodes=%d where id=%s",
                        num_nodes+(int)(iparams.size()), shard_id.c_str());
            sql_stmts.push_back(sql);
            KLOG_INFO("update pg_shard sql: {}", sql);
            if(ret && (pg_conn.ExecuteQuery(sql.c_str(), &pgresult) == -1)) {
                KLOG_ERROR("postgres update pg_shard failed {}", pg_conn.getErr());
                ret = false;
            }
          }
        }
    }

    for(size_t i=0; i<iparams.size(); i++) {
      sql = string_sprintf("insert into pg_shard_node(id, port, shard_id, svr_node_id, ro_weight, ping, latency, user_name, hostaddr, passwd, when_created) values(%s, %d, %s, 0, 0, 0, 0, 'pgx', '%s', 'pgx_pwd', now())",
              node_ids[i].c_str(), std::get<1>(iparams[i]), shard_id.c_str(),  
              std::get<0>(iparams[i]).c_str());
      sql_stmts.push_back(sql);
      KLOG_INFO("insert pg_shard_node sql: {}", sql);
      if(ret && (pg_conn.ExecuteQuery(sql.c_str(), &pgresult) == -1)) {
        KLOG_ERROR("postgres insert pg_shard_node failed {}", pg_conn.getErr());
        ret = false;
      }
    }
    sql = "commit";
    if(ret && (pg_conn.ExecuteQuery(sql.c_str(), &pgresult) == -1)) {
      KLOG_ERROR("postgres commit failed {}", pg_conn.getErr());
      ret = false;
    }
  }
  return ret;
}

bool AddShardToComputerNode(int cluster_id, const std::string& comp_user, const std::string& comp_pwd,
                    const std::unordered_map<std::string, std::vector<IComm_Param> >& storage_iparams,
                    const std::unordered_map<std::string, std::vector<std::string> >& shard_map_nids,
                    const std::unordered_map<std::string, std::string>& shard_map_sids, 
                    std::vector<std::string>& sql_stmts, int type) {                    
  MysqlResult result;
  std::string sql = string_sprintf("select * from %s.comp_nodes where db_cluster_id=%d",
            KUNLUN_METADATA_DB_NAME, cluster_id);
  bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  if(ret) {
    KLOG_ERROR("get comp_nodes failed {}", g_node_channel_manager->getErr());
    return false;
  }

  int nrows = result.GetResultLinesNum();
  for(int i=0; i<nrows; i++) {
    //for(int j=0; j < 10; j++) {
      if(i == 0)
        ret = UpdatePgTables(result[i]["hostaddr"], result[i]["port"], comp_user, comp_pwd, cluster_id,
                type, storage_iparams, shard_map_nids, shard_map_sids, sql_stmts);
      else {
        std::vector<std::string> sqls;
        ret = UpdatePgTables(result[i]["hostaddr"], result[i]["port"], comp_user, comp_pwd, cluster_id,
                type, storage_iparams, shard_map_nids, shard_map_sids, sqls);
      }

      //if(!ret)
      //  sleep(2);
      //else
      //  break;
    //}
    //if(!ret)
    //  return false;
  }
  return true;
}

bool WriteSqlToDdlLog(const std::string& cluster_name, const std::vector<std::string>& sql_stmt) {
  return true;

  MysqlResult result;
  std::string sql = "begin";
  bool ret = false, retg = false;
  std::string stmt, new_ss;
  std::regex re("'");

  ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  if(ret) {
    KLOG_ERROR("begin failed {}", g_node_channel_manager->getErr());
    return false;
  }

  sql = "SELECT GET_LOCK('DDL', 50)";
  ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  if(ret) {
    KLOG_ERROR("get lock failed {}", g_node_channel_manager->getErr());
    return false;
  }

  sql = "set @my_opid=0";
  ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  if(ret) {
    KLOG_ERROR("set my_opid failed {}", g_node_channel_manager->getErr());
    goto end;
  }

  for(auto ss : sql_stmt) {
    new_ss.clear();
    std::regex_replace(std::back_inserter(new_ss), ss.begin(), 
                        ss.end(), re, "\\\'");
    stmt += new_ss + ";";
  }

  sql = string_sprintf("CALL %s.append_ddl_log_entry('ddl_ops_log_%s','postgres','public','','','','','others','others',0,'%s','',0,0,0,@my_opid)",
        KUNLUN_METADATA_DB_NAME, cluster_name.c_str(), stmt.c_str());
  KLOG_INFO("will execute sql: {}", sql);
  ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  if(ret) {
    KLOG_ERROR("call append_ddl_log_entry failed {}", g_node_channel_manager->getErr());
    goto end;
  }

  sql = "commit";
  ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  if(ret) {
    KLOG_ERROR("commit failed {}", g_node_channel_manager->getErr());
  }

end:
  sql = "SELECT RELEASE_LOCK('DDL')";
  retg = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  if(retg) {
    KLOG_ERROR("release lock failed {}", g_node_channel_manager->getErr());
    return false;
  }

  if(ret)
    return false;

  return true;
}

bool CheckMachineIplists(MachType mtype, const std::vector<std::string>& iplists) {
    std::string machine_type = "storage";
    if(mtype == M_COMPUTER)
        machine_type = "computer";

    std::string sql;
    MysqlResult result;
    for(auto ip : iplists) {
        sql = string_sprintf("select id from %s.server_nodes where hostaddr='%s' and machine_type='%s'",
                    KUNLUN_METADATA_DB_NAME, ip.c_str(), machine_type.c_str());
        
        bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
        if(ret) {
            KLOG_ERROR("check hostaddir {} in server_nodes failed", ip.c_str());
            return false;
        }

        if(result.GetResultLinesNum() != 1) {
            KLOG_ERROR("get hostaddr {} record too many", ip.c_str());
            return false;
        }
    }

    return true;
}

bool CheckTableExists(const std::string& table_name) {
  std::string sql = string_sprintf("select count(*) from information_schema.TABLES where table_name='%s'",
          table_name.c_str());
  MysqlResult result;
  bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  if(ret) {
    KLOG_ERROR("get table_name {} from information_schema failed", table_name);
    return false;
  }

  int count = atoi(result[0]["count(*)"]);
  if(count > 0)
    return true;
  
  return false;
}

bool CheckStringIsDigit(const std::string& str) {
    if(str.empty())
        return false;

    for (uint32_t i = 0; i < str.size (); i++) {
        if ((str.at (i) > '9') || (str.at (i) < '0')) {
            return false;
        }
    }
    return true;
}


CMemo_Params GetClusterMemoParams(int cluster_id, const std::vector<std::string>& key_names) {
    MysqlResult result;
    std::map<std::string, std::string> key_vals;

    std::string sql = string_sprintf("select * from %s.db_clusters where id=%d",
                    KUNLUN_METADATA_DB_NAME, cluster_id);
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("get create cluster job memo failed {}", g_node_channel_manager->getErr());
        return {false, GET_MEMO_FROM_DB_CLUSTER_ERROR, key_vals};
    }
        
    if(result.GetResultLinesNum() != 1) {
        KLOG_ERROR("get create cluster job memo too many by cluster_id {}", cluster_id);
        return {false, GET_MEMO_NUM_CLUSTERID_ERROR, key_vals};
    }

    std::string memo = result[0]["memo"];
    if(memo.empty() || memo == "NULL") {
        KLOG_ERROR("get cluster install memo failed");
        return {false, GET_MEMO_NULL_CLUSTERID_ERROR, key_vals};
    }

    Json::Value memo_json;
    Json::Reader reader;
    ret = reader.parse(memo, memo_json);
    if (!ret) {
        KLOG_ERROR("parse memo json failed: {}", reader.getFormattedErrorMessages());
        return {false, PARSE_CLUSTER_MEMO_JSON_ERROR, key_vals};
    }
    
    for(auto it : key_names) {
        if(it.find(".") == std::string::npos) {
            if(result.check_column_name(it)) {
                key_vals[it] = result[0][it.c_str()];
            }
        } else {
            std::string sub_name = it.substr(it.rfind(".")+1);
            if(memo_json.isMember(sub_name))
                key_vals[sub_name] = memo_json[sub_name].asString();
        }
    }
    
    return {true, 0, key_vals};
}

bool UpdateClusterShardTopology(int cluster_id, const std::vector<int>& shard_ids, int max_commit_log_id,
              int max_ddl_log_id) {
  std::string str_ids;
  for(auto si : shard_ids) {
    str_ids += std::to_string(si)+",";
  }

  size_t len = str_ids.length();
  str_ids = str_ids[0, len-1];

  MysqlResult result;
  std::string sql = string_sprintf("insert into %s.cluster_shard_topology (cluster_id, shard_id, max_commit_log_id, max_ddl_log_id) values(%d, '%s', %d, %d)",
          KUNLUN_METADATA_DB_NAME, cluster_id, str_ids.c_str(), max_commit_log_id, max_ddl_log_id);
  bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
  if(ret) {
    KLOG_ERROR("insert cluster_shard_topology failed");
    return false;
  }
  return true;
}

}