
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
struct ComputeInstanceInfoSt {
  std::string ip = "";
  unsigned long pg_port = 0;
  unsigned long mysql_port = 0;
  unsigned long exporter_port = 0;
  int compute_id = 0;
  std::string init_user = "abc";
  std::string init_pwd = "abc";
  // kunlun_metadata_db mysql_pg_install table's primary key
  int related_id = -1;
  std::string to_string() {
    return kunlun::string_sprintf("(%s | pg_port:%lu | mysql_port:%lu | exporter_port:%lu |"
                                  "comp_id:%d | init_user:%s | init_pwd:%s)",
                                  ip.c_str(), pg_port, mysql_port, exporter_port, compute_id,
                                  init_user.c_str(), init_pwd.c_str());
  }
};
class PostgresInstallRemoteTask : public RemoteTask {
  typedef RemoteTask super;

public:
  PostgresInstallRemoteTask(const char *task_name, std::string request_id)
      : super(task_name), request_id_(request_id){
        mysql_pg_install_log_id_ = "";
      }
  virtual ~PostgresInstallRemoteTask(){}
  // ip_port | buffer_size ,ip_port | buffer_size,ip_port | buffer_size ...
  bool InitInstanceInfo(std::vector<ComputeInstanceInfoSt> &);

  bool InitInstanceInfoOneByOne(ComputeInstanceInfoSt &info_st);

  bool virtual TaskReportImpl() override;
  void virtual SetUpStatus() override;

private:
  bool ComposeNodeChannelsAndParas();
  // key: ip_port
  bool ComposeOneNodeChannelAndPara(std::string key);
  bool Init_mysql_pg_intall_log_table_id();
private:
  // key: ip_port
  unordered_map<std::string, ComputeInstanceInfoSt> instance_info_;
  std::string request_id_;
  std::string mysql_pg_install_log_id_;
};


class PostgresUninstallRemoteTask : public RemoteTask {
  typedef RemoteTask super;

public:
  PostgresUninstallRemoteTask(const char *task_name, std::string request_id)
      : super(task_name), request_id_(request_id){
        mysql_pg_install_log_id_ = "";
      }
  virtual ~PostgresUninstallRemoteTask(){}
  // ip_port | buffer_size ,ip_port | buffer_size,ip_port | buffer_size ...
  bool InitInstanceInfo(std::vector<ComputeInstanceInfoSt> &);

  bool InitInstanceInfoOneByOne(ComputeInstanceInfoSt &info_st);

  bool virtual TaskReportImpl() override;
  void virtual SetUpStatus() override;

private:
  bool ComposeNodeChannelsAndParas();
  // key: ip_port
  bool ComposeOneNodeChannelAndPara(std::string key);
  bool Init_mysql_pg_intall_log_table_id();
private:
  // key: ip_port
  unordered_map<std::string, ComputeInstanceInfoSt> instance_info_;
  std::string request_id_;
  std::string mysql_pg_install_log_id_;
};
} // namespace kunlun
