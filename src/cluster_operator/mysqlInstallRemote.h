#pragma once
#include "http_server/node_channel.h"
#include "request_framework/remoteTask.h"
#include "request_framework/requestBase.h"
#include "zettalib/tool_func.h"
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>


namespace kunlun {
struct InstanceInfoSt {
  std::string ip = "";
  unsigned long port = 0;
  unsigned long exporter_port = 0;
  int innodb_buffer_size_M = 0;
  int db_cfg = 0;
  // kunlun_metadata_db mysql_pg_install table's primary key
  int related_id = -1;
  std::string role = "slave";
  std::string to_string() {
    return kunlun::string_sprintf("(%s | %lu | %lu |%d | %d | %s)", ip.c_str(), port, exporter_port,
                                  innodb_buffer_size_M, db_cfg, role.c_str());
  }
};
class MySQLInstallRemoteTask : public RemoteTask {
  typedef RemoteTask super;

public:
  MySQLInstallRemoteTask(const char *task_name, std::string request_id)
      : super(task_name), request_id_(request_id){
        mysql_pg_install_log_id_ = "";
      };
  virtual ~MySQLInstallRemoteTask() {}
  // ip_port | buffer_size ,ip_port | buffer_size,ip_port | buffer_size ...
  bool InitInstanceInfo(std::vector<std::tuple<string, unsigned long, int, int>> &);

  bool InitInstanceInfoOneByOne(std::string ip, std::string port, std::string exporter_port,
                                int buffer_size, int db_cfg);

  bool InitInstanceInfoOneByOne(std::string ip, unsigned long port, unsigned long exporter_port,
                                int buffer_size, int db_cfg);

  bool virtual TaskReportImpl() override;
  void virtual SetUpStatus() override;

private:
  bool ComposeNodeChannelsAndParas();
  // key: ip_port
  bool ComposeOneNodeChannelAndPara(std::string key);
  bool Init_mysql_pg_intall_log_table_id();

private:
  // key: ip_port
  unordered_map<std::string, InstanceInfoSt> instance_info_;
  std::string request_id_;
  std::string mysql_pg_install_log_id_;

};

class MySQLUninstallRemoteTask : public RemoteTask {
  typedef RemoteTask super;

public:
  MySQLUninstallRemoteTask(const char *task_name, std::string request_id)
      : super(task_name), request_id_(request_id){
        mysql_pg_install_log_id_ = "";
      };
  virtual ~MySQLUninstallRemoteTask() {}
  // ip_port | buffer_size ,ip_port | buffer_size,ip_port | buffer_size ...
  bool InitInstanceInfo(std::vector<std::tuple<string, unsigned long, int>> &);

  bool InitInstanceInfoOneByOne(std::string ip, std::string port, std::string exporter_port,
                                int buffer_size);

  bool InitInstanceInfoOneByOne(std::string ip, unsigned long port, unsigned long exporter_port,
                                int buffer_size);

  bool virtual TaskReportImpl() override;
  void virtual SetUpStatus() override;
  
private:
  bool ComposeNodeChannelsAndParas();
  // key: ip_port
  bool ComposeOneNodeChannelAndPara(std::string key);
  bool Init_mysql_pg_intall_log_table_id();

private:
  // key: ip_port
  unordered_map<std::string, InstanceInfoSt> instance_info_;
  std::string request_id_;
  std::string mysql_pg_install_log_id_;

};
} // namespace kunlun
