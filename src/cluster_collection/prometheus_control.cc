/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "prometheus_control.h"
#include "zettalib/op_log.h"
#include "zettalib/biodirectpopen.h"
#include "zettalib/tool_func.h"
#include "request_framework/handleRequestThread.h"
#include "http_server/http_server.h"
#include "http_server/node_channel.h"
#include "kl_mentain/shard.h"
#include "raft_ha/raft_ha.h"
#include "util_func/meta_info.h"
#include <fstream>
#include <sys/wait.h>

std::string prometheus_path;
int64_t prometheus_port_start;
extern std::string local_ip;
extern NodeHa* g_cluster_ha_handler;
extern GlobalNodeChannelManager* g_node_channel_manager;

namespace kunlun 
{

bool KProcStat::Parse(const char * buf) {
    std::vector<std::string> vec = StringTokenize(buf , " ");
    if (vec.size () < 3) {
        setErr("get buf is not valid ,size < 3,buf:%s" , buf);
        return false;
    }
    size_t len = vec.size();

    pid_ = strtoull(vec[0].c_str(), NULL, 10);
    binName_ = trim(vec[2]);
    if(len == 3)
        return true;

    for(size_t i=3; i<len - 1; i++)
        binArgs_ += vec[i]+" ";

    binArgs_ += vec[len-1];
    return true;
}

KPrometheusControl::KPrometheusControl(int loop_interval) {
    loop_interval_ = loop_interval;
    binName_ = "./prometheus";
    binArgs_ = string_sprintf("--config.file=prometheus.yml --web.listen-address=:%lu --web.enable-lifecycle", prometheus_port_start);

}

int KPrometheusControl::Start() {
    if(!PrometheusPathIsExist()) {
        return 1;
    }

    std::string start_cmd = string_sprintf("cd %s; ./prometheus --config.file=prometheus.yml --web.listen-address=:%lu --web.enable-lifecycle > ./prometheus.log_%lu_%ld 2>&1 &",
            prometheus_path.c_str(), prometheus_port_start, prometheus_port_start, GetNowTimestamp());

    KLOG_INFO("Will execute : {}", start_cmd);
    pid_t prometheus_pid = fork();
    if(prometheus_pid < 0) {
        KLOG_ERROR("fork process for prometheus failed {}.{}", errno, strerror(errno));
        return 1;
    }

    if(prometheus_pid == 0) {
        const char *argvs[4];
        argvs[0] = "sh";
        argvs[1] = "-c";
        argvs[2] = start_cmd.c_str();
        argvs[3] = nullptr;

        /*execv*/
        execv("/bin/sh", (char *const *)(argvs));     
    }
    
    int status;
    while(waitpid(prometheus_pid, &status, 0) == -1) {
        if(errno == EINTR) {
            status = -1;
            break;
        }
        sleep(1);
    }
    if(status) {
        KLOG_ERROR("start prometheus process failed, status: {}", status);
        return 1;
    }
    return 0;
}

int KPrometheusControl::Stop() {
    pid_t pid;
    if(GetPrometheusPid(pid) == 0) {
        std::string stop_cmd = string_sprintf("kill -9 %d", pid);
        KLOG_INFO("Will execute : {}", stop_cmd);
        BiodirectPopen bi_open(stop_cmd.c_str());

        bool ret = bi_open.Launch("r");
        if(!ret || bi_open.get_chiled_status() != 0) {
            FILE *stderr_fp = bi_open.getReadStdErrFp();
            if(stderr_fp == nullptr){
                KLOG_ERROR("Biooppen launch failed: {}",bi_open.getErr());
                return 1;
            }
            char buffer[8192];
            if (fgets(buffer, 8192, stderr_fp) != nullptr) {
                KLOG_ERROR("Biopopen excute failed and stderr: {}", buffer);
            }
            return 1;
        }
    }
    
    return 0;
}

void KPrometheusControl::GetRunStatByMeta() {
    std::string sql = string_sprintf("select distinct hostaddr, used_port, nodemgr_prometheus_port, machine_type from %s.server_nodes",
                KUNLUN_METADATA_DB_NAME);
    MysqlResult result;
    bool ret = g_node_channel_manager->send_stmt(sql, &result, stmt_retries);
    if(ret) {
        KLOG_ERROR("get hostaddr from server_nodes failed");
        return;
    }

    int nrows = result.GetResultLinesNum();
    for(int i=0; i<nrows; i++) {
        std::string hostaddr = result[i]["hostaddr"];
        if(hostaddr == "pseudo_server_useless")
            continue;

        std::string machine_type = result[i]["machine_type"];
        if(machine_type == "NULL")
            continue;
    
        std::string used_port = result[i]["used_port"];
        std::string prometheus_port = result[i]["nodemgr_prometheus_port"];
        std::vector<std::string> used_port_vec;
        if(!(used_port.empty() || used_port == "NULL"))
            used_port_vec = StringTokenize(used_port, ",");

        if(machine_type == "storage") {
            storage_iplists_.push_back(hostaddr);
            for(auto up : used_port_vec) {
                int port = atoi(up.c_str());
                storage_exporters_.push_back(hostaddr+":"+std::to_string(port+1));
            }
        } else if(machine_type == "computer") {
            computer_iplists_.push_back(hostaddr);
            for(auto up : used_port_vec) {
                int port = atoi(up.c_str());
                computer_exporters_.push_back(hostaddr+":"+std::to_string(port+2));
            }
        }

        std::string host = hostaddr+":"+prometheus_port;
        if(std::find(node_exporters_.begin(), node_exporters_.end(), host) == node_exporters_.end())
            node_exporters_.emplace_back(host);
    }
    UpdatePrometheusConf(0);
}

int KPrometheusControl::run() {
    GetRunStatByMeta();
    while(m_state) {
        if(!PrometheusPidAlive()) {
            //KLOG_ERROR("check prometheus pid alive failed, so start again");
            Start();
        }

        sleep(loop_interval_);
    }
    return 0;
}

bool KPrometheusControl::PrometheusPathIsExist() {
    return true;
}

bool KPrometheusControl::PrometheusPidAlive() {
    pid_t pid;
    if(GetPrometheusPid(pid) == 2)
        return false;
    
    return true;
}

int KPrometheusControl::GetPrometheusPid(pid_t& pid) {
    KlWrapGuard<KlWrapMutex> guard(mux_);
    std::string ps_cmd = "ps -eo pid,ppid,command > ./.ps_log";
    BiodirectPopen bi_open(ps_cmd.c_str());
    bool ret = bi_open.Launch("r");
    if(!ret || bi_open.get_chiled_status() != 0) {
        FILE *stderr_fp = bi_open.getReadStdErrFp();
        if(stderr_fp == nullptr){
            KLOG_ERROR("Biooppen launch failed: {}",bi_open.getErr());
            return 1;
        }
        
        char buffer[8192];
        if (fgets(buffer, 8192, stderr_fp) != nullptr) {
            KLOG_ERROR("Biopopen execute failed and stderr: {}", buffer);
        }
        return 1;
    }

    std::ifstream fin("./.ps_log", std::ios::in);
    if(!fin.is_open()) {
        KLOG_ERROR("open ps_log file failed: {}.{}", errno, strerror(errno));
        return 1;
    }

    std::string sbuf;
    while(std::getline(fin, sbuf)) {
        trim(sbuf);
        KProcStat procStat;
        if(!procStat.Parse(sbuf.c_str())) {
            KLOG_ERROR("parse proc line: {} failed", sbuf);
            return 1;
        }
        
        if(procStat.GetBinArgs() == binArgs_ && procStat.GetBinName() == binName_) {
            pid = procStat.GetPid();
            return 0;
        }
    }
    //KLOG_ERROR("prometheus process is not exist, so restart");
    return 2;
}

bool KPrometheusControl::ReportAddMachine(const std::string& ip,
            const std::string& machine_type) {
    int exist_flag = 0, storage_flag = 0, computer_flag=0;
    if(std::find(storage_iplists_.begin(), storage_iplists_.end(), ip) != storage_iplists_.end()) {
        exist_flag = 1;
        storage_flag = 1;
    }
    
    if(std::find(computer_iplists_.begin(), computer_iplists_.end(), ip) != computer_iplists_.end()) {
        exist_flag = 1;
        computer_flag = 1;
    }
    
    if((machine_type == "storage") && !exist_flag) {
        std::string exporter_port;
        if(StartNodeExporter(ip, exporter_port)) {
            return false;
        }

        node_exporters_.emplace_back(ip+":"+exporter_port);
        if(!UpdatePrometheusConf())
            return false;
    }
    if((machine_type == "storage") && !storage_flag)
        storage_iplists_.emplace_back(ip);
    
    if((machine_type == "computer") && !exist_flag) {
        std::string exporter_port;
        if(StartNodeExporter(ip, exporter_port))
            return false;

        node_exporters_.push_back(ip+":"+exporter_port);
        if(!UpdatePrometheusConf())
            return false;
    }
    if((machine_type == "computer") && !computer_flag)
        computer_iplists_.emplace_back(ip);

    return true;
}

bool KPrometheusControl::ReportDelMachine(const std::string& ip, const std::string& machine_type) {
    if(machine_type == "storage") {
        if(std::find(storage_iplists_.begin(), storage_iplists_.end(), ip) != storage_iplists_.end()) {
            if(std::find(computer_iplists_.begin(), computer_iplists_.end(), ip) == computer_iplists_.end()) {
                std::string exporter_port;
                for(std::vector<std::string>::iterator it = node_exporters_.begin(); it != node_exporters_.end();) {
                //for(auto ne : node_exporters_) {
                    std::string hostaddr = (*it).substr(0, (*it).rfind(":"));
                    if(hostaddr == ip) {
                        exporter_port = (*it).substr((*it).rfind(":")+1);
                        node_exporters_.erase(it);
                        break;
                    }
                    it++;
                }
                if(!exporter_port.empty()) {
                    if(StopNodeExporter(ip, exporter_port))
                        return false;

                    if(!UpdatePrometheusConf())
                        return false;
                }
            }
            
            for(std::vector<std::string>::iterator it = storage_iplists_.begin(); it != storage_iplists_.end();) {
                if(*it == ip) {
                    storage_iplists_.erase(it);
                    break;
                }
                it++;
            } 
        }
    } else if(machine_type == "computer") {
        if(std::find(computer_iplists_.begin(), computer_iplists_.end(), ip) != computer_iplists_.end()) {
            if(std::find(storage_iplists_.begin(), storage_iplists_.end(), ip) == storage_iplists_.end()) {
                std::string exporter_port;
                for(std::vector<std::string>::iterator it = node_exporters_.begin(); it != node_exporters_.end();) {
                //for(auto ne : node_exporters_) {
                    std::string hostaddr = (*it).substr(0, (*it).rfind(":"));
                    if(hostaddr == ip) {
                        exporter_port = (*it).substr((*it).rfind(":")+1);
                        node_exporters_.erase(it);
                        break;
                    }
                    it++;
                }
                if(!exporter_port.empty()) {
                    if(StopNodeExporter(ip, exporter_port))
                        return false;
                    
                    if(!UpdatePrometheusConf())
                        return false;
                }
            }

            for(std::vector<std::string>::iterator it=computer_iplists_.begin(); it !=computer_iplists_.end();) {
                if(*it == ip) {
                    computer_iplists_.erase(it);
                    break;
                }
                it++;
            }
        }
    }
    return true;
}

bool KPrometheusControl::UpdatePrometheusConf(int need_reload) {
    std::string node_str, mysql_str, pgsql_str;
    std::string localip_str = "\""+local_ip +":"+std::to_string(prometheus_port_start)+"\"";

    std::vector<std::string> exist_ips;
    for(auto ne : node_exporters_) {
        if(node_str.length() > 0)
            node_str += ",";
        node_str += "\"" + ne + "\"";
    }

    for(auto se : storage_exporters_) {
        if(mysql_str.length() > 0)
            mysql_str += ",";
        mysql_str += "\"" + se + "\"";
    }

    for(auto ce : computer_exporters_) {
        if(pgsql_str.length() > 0)
            pgsql_str += ",";
        pgsql_str += "\"" + ce + "\"";
    }

	/////////////////////////////////////////////////////////
	// save yml file
	std::string ymlfile_path = prometheus_path + "/prometheus.yml.bak";
	std::string yml_buf = "global:\r\n  scrape_interval: 15s\r\n  evaluation_interval: 15s\r\nscrape_configs:\r\n";
	yml_buf += "  - job_name: \"prometheus\"\r\n    static_configs:\r\n";
	yml_buf += "      - targets: [" + localip_str + "]\r\n";
	yml_buf += "  - job_name: \"node\"\r\n    static_configs:\r\n";
	yml_buf += "      - targets: [" + node_str + "]\r\n";
	yml_buf += "  - job_name: \"postgres\"\r\n    static_configs:\r\n";
	yml_buf += "      - targets: [" + pgsql_str + "]\r\n";
	yml_buf += "  - job_name: \"mysql\"\r\n    static_configs:\r\n";
	yml_buf += "      - targets: [" + mysql_str + "]\r\n";

	if(SavePrometheusFile(ymlfile_path, (char*)yml_buf.c_str())) {
		KLOG_ERROR( "save prometheus yml file error");
		return false;
	}

    if(need_reload) {
        if(ReloadConf()) {
            KLOG_ERROR("reload new prometheus conf failed");
            return false;
        }
    }

    return true;
}

int KPrometheusControl::SavePrometheusFile(const std::string &path, char* buf) {
    FILE* pfd = fopen(path.c_str(), "wb");
	if(pfd == NULL)	{
		KLOG_ERROR("Create file error {}", path);
		return 1;
	}

	fwrite(buf,1,strlen(buf),pfd);
	fclose(pfd);
	
    std::string target_path = prometheus_path + "/prometheus.yml";
    if(rename(path.c_str(), target_path.c_str()) != 0) {
        KLOG_ERROR("rename to target file failed, {}.{}", errno, strerror(errno));
    } 
    return 0;
}

int KPrometheusControl::ReloadConf() {
    pid_t pid;
    if(GetPrometheusPid(pid)) {
        return 1;
    }
    std::string reload_cmd = string_sprintf("kill -HUP %d", pid);
    KLOG_INFO("Will execute : {}", reload_cmd);
    BiodirectPopen bi_open(reload_cmd.c_str());

    bool ret = bi_open.Launch("r");
    if(!ret || bi_open.get_chiled_status() != 0) {
        FILE *stderr_fp = bi_open.getReadStdErrFp();
        if(stderr_fp == nullptr){
            KLOG_ERROR("Biooppen launch failed: {}",bi_open.getErr());
            return 1;
        }
        char buffer[8192];
        if (fgets(buffer, 8192, stderr_fp) != nullptr) {
            KLOG_ERROR("Biopopen excute failed and stderr: {}", buffer);
        }
        return 1;
    }
    return 0;
}

int KPrometheusControl::StartNodeExporter(const std::string& ip, std::string& exporter_port) {
    //std::string node_exporter_port = std::to_string(prometheus_port_start+1);
    std::string port = g_node_channel_manager->GetNodeMgrPort(ip);

    SyncNodeChannel nodechannel(600, ip, port);
    if(!nodechannel.Init()) {
        KLOG_ERROR("node channel init failed: {}", nodechannel.getErr());
        return 1;
    }

    Json::Value root;
    root["job_type"] = "install_node_exporter";
    root["paras"]["exporter_port"] = exporter_port;
    std::string sid = g_cluster_ha_handler->GetGenerateId()->GetStringId();
    root["cluster_mgr_request_id"] = sid;
    
    if(nodechannel.ExecuteCmd(root)) {
        KLOG_ERROR("execute cmd failed: {}", nodechannel.getErr());
        return 1;
    }

    std::string resp = nodechannel.GetRespStr();
    Json::Value doc;
    Json::Reader reader;
    bool ret = reader.parse(resp, doc);
    if (!ret) {
        KLOG_ERROR("JSON parse error: {}, JSON string: {}",
            reader.getFormattedErrorMessages(), resp);
        return 1;
    }
    exporter_port = doc["exporter_port"].asString();
    return 0;
}

int KPrometheusControl::StopNodeExporter(const std::string& ip, const std::string& exporter_port) {
    std::string port = g_node_channel_manager->GetNodeMgrPort(ip);

    SyncNodeChannel nodechannel(600, ip, port);
    if(!nodechannel.Init()) {
        KLOG_ERROR("node channel init failed: {}", nodechannel.getErr());
        return 1;
    }

    Json::Value root;
    root["job_type"] = "uninstall_node_exporter";
    root["paras"]["exporter_port"] = exporter_port;
    std::string sid = g_cluster_ha_handler->GetGenerateId()->GetStringId();
    root["cluster_mgr_request_id"] = sid;
    
    if(nodechannel.ExecuteCmd(root)) {
        KLOG_ERROR("execute cmd failed: {}", nodechannel.getErr());
        return 1;
    }
    return 0;
}

bool KPrometheusControl::AddComputerConf(const std::vector<std::string>& hosts) {
    int update_flag = 0;
    for(auto hs : hosts) {
        if(std::find(computer_exporters_.begin(), computer_exporters_.end(), hs) == computer_exporters_.end()) {
            computer_exporters_.push_back(hs);
            update_flag = 1;
        }
    }

    if(update_flag) {
        if(!UpdatePrometheusConf())
            return true;
    }

    return false;
}
    
bool KPrometheusControl::DelComputerConf(const std::vector<std::string>& hosts) {
    int update_flag = 0;
    for(auto hs : hosts) {
        for(std::vector<std::string>::iterator it = computer_exporters_.begin(); it != computer_exporters_.end();) {
            if(*it == hs) {
                computer_exporters_.erase(it);
                update_flag = 1;
                break;
            } 
            it++;
        }
    }

    if(update_flag) {
        if(!UpdatePrometheusConf())
            return true;
    }

    return false;
}
    
bool KPrometheusControl::AddStorageConf(const std::vector<std::string>& hosts) {
    int update_flag = 0;
    for(auto hs : hosts) {
        if(std::find(storage_exporters_.begin(), storage_exporters_.end(), hs) == storage_exporters_.end()) {
            storage_exporters_.push_back(hs);
            update_flag = 1;
        }
    }

    if(update_flag) {
        if(!UpdatePrometheusConf())
            return true;
    }
    return false;
}

bool KPrometheusControl::DelStorageConf(const std::vector<std::string>& hosts) {
    int update_flag = 0;
    for(auto hs : hosts) {
        for(std::vector<std::string>::iterator it = storage_exporters_.begin(); it != storage_exporters_.end();) {
            if(*it == hs) {
                storage_exporters_.erase(it);
                update_flag = 1;
                break;
            } 
            it++;
        }
    }

    if(update_flag) {
        if(!UpdatePrometheusConf())
            return true;
    }
    return false;
}

}
