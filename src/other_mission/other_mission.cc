/*
  Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "other_mission.h"

std::string prometheus_path;
int64_t prometheus_port_start;

void ControlInstanceCallBack(OtherMission *mission, std::string &response){
  Json::Value root,array,info;
  Json::Reader reader;
  std::string job_status,job_memo;

  job_status = "failed";
  bool ret = reader.parse(response.c_str(), root);
  if (!ret) {
    job_memo = "JSON parse error: " + response;
    goto end;
  }
  array = root["response_array"][0];
  info = array["info"];

  job_status = info["status"].asString();
  if(job_status == "failed") {
    job_memo = info["info"].asString();
    goto end;
  }

  job_status = "done";
  job_memo = "control instance succeed";

end:
  syslog(Logger::INFO, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(mission->job_id, job_status, job_memo);
  System::get_instance()->set_cluster_mgr_working(true);
}

void OTHER_Call_Back(void *cb_context) {
  OtherRemoteTask *task = static_cast<OtherRemoteTask *>(cb_context);
  std::string response = task->get_response()->SerializeResponseToStr();

  switch (task->getMission()->request_type) {
  case kunlun::kControlInstanceType:
    ControlInstanceCallBack(task->getMission(), response);
    break;

  default:
    break;
  }
}

bool OtherMission::ArrangeRemoteTask() {
  request_type = get_request_type();
  job_id = get_request_unique_id();

  switch (request_type) {
  case kunlun::kControlInstanceType:
    ControlInstance();
    break;
  case kunlun::kUpdatePrometheusType:
    UpdatePrometheus();
    break;
  case kunlun::kPostgresExporterType:
    PostgresExporter();
    break;
  case kunlun::kMysqldExporterType:
    MysqldExporter();
    break;

  default:
    break;
  }

  return true;
}

void OtherMission::ControlInstance() {
	std::string job_status;
	std::string job_memo;
  OtherRemoteTask *task;
  Json::Value root_node;
  Json::Value paras_node;
  std::string hostaddr,control,instance_status,type;
  int port,instance_type;
  Tpye_Ip_Port ip_port;

  if (!super::get_body_json_document().isMember("paras")) {
    setExtraErr("missing `paras` key-value pair in the request body");
    return;
  }
  Json::Value paras = super::get_body_json_document()["paras"];

  if (!paras.isMember("hostaddr")) {
    job_memo = "missing `hostaddr` key-value pair in the request body";
    goto end;
  }
  hostaddr = paras["hostaddr"].asString();

  if (!paras.isMember("port")) {
    job_memo = "missing `port` key-value pair in the request body";
    goto end;
  }
  port = stoi(paras["port"].asString());

  if (!paras.isMember("control")) {
    job_memo = "missing `control` key-value pair in the request body";
    goto end;
  }
  control = paras["control"].asString();

	job_status = "not_started";
	job_memo = "control instance start";
  syslog(Logger::INFO, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id, job_status, job_memo);

  System::get_instance()->set_cluster_mgr_working(false);

  //init channel again
  g_node_channel_manager.Init();

	//////////////////////////////////////////////////////////
	// update meta table status by ip and port
	if(control == "stop")
		instance_status = "inactive";
	else if(control == "start" || control == "restart")
		instance_status = "active";
	else {
		job_memo = "control type error";
		goto end;
	}

	ip_port = std::make_pair(hostaddr, port);
	if(!System::get_instance()->update_instance_status(ip_port, instance_status, instance_type)) {
		job_memo = "update_instance_status error";
		goto end;
	}

	//////////////////////////////////////////////////////////
	// stop instance by ip and port
	if(instance_type == 1)
		type = "storage";
	else if(instance_type == 2)
		type = "computer";
	else {
		job_memo = "instance ip_port no find";
		goto end;
	}

	//////////////////////////////////////////////////////////
	// control instance task
  task =
      new OtherRemoteTask("control_instance", job_id.c_str(), this);
  task->AddNodeSubChannel(
      hostaddr.c_str(),
      g_node_channel_manager.getNodeChannel(hostaddr.c_str()));

  root_node["cluster_mgr_request_id"] = job_id;
  root_node["task_spec_info"] = task->get_task_spec_info();
  root_node["job_type"] = "control_instance";

  paras_node["type"] = type;
  paras_node["control"] = control;
  paras_node["ip"] = hostaddr;
  paras_node["port"] = port;
  root_node["paras"] = paras_node;

  task->SetPara(hostaddr.c_str(), root_node);
  get_task_manager()->PushBackTask(task);
  return;

end:
	job_status = "failed";
	syslog(Logger::ERROR, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id, job_status, job_memo);
}

void OtherMission::UpdatePrometheus(){
	std::string job_status;
	std::string job_memo;

	job_status = "not_started";
	job_memo = "control instance working";
  syslog(Logger::INFO, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id, job_status, job_memo);

	if(!update_prometheus()) {
		job_memo = "update prometheus error";
		goto end;
	}

	job_status = "done";
	job_memo = "update prometheus succeed";
	syslog(Logger::INFO, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id, job_status, job_memo);
	return;

end:
	job_status = "failed";
	syslog(Logger::ERROR, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id, job_status, job_memo);
}

void OtherMission::PostgresExporter(){
  
}

void OtherMission::MysqldExporter(){

}


bool OtherMission::restart_node_exporter(std::vector<std::string> &vec_node)
{
	bool ret = false;
/*
	cJSON *root = NULL;
	cJSON *item;
	char *cjson = NULL;

	cJSON *ret_root = NULL;
	cJSON *ret_item;

	root = cJSON_CreateObject();
	cJSON_AddStringToObject(root, "job_type", "node_exporter");
	cjson = cJSON_Print(root);
	cJSON_Delete(root);
	root = NULL;
	
	for(auto &node: vec_node)
	{
		std::string post_url = "http://" + node + ":" + std::to_string(node_mgr_http_port);
		//syslog(Logger::INFO, "post_url=%s",post_url.c_str());
		
		std::string result, result_str;
		int retry = 3;
		while(retry-->0)
		{
			if(Http_client::get_instance()->Http_client_post_para(post_url.c_str(), cjson, result_str)==0)
			{
				syslog(Logger::INFO, "job_start_node_exporter result_str=%s",result_str.c_str());

				if(ret_root!=NULL)
				{
					cJSON_Delete(ret_root);
					ret_root = NULL;
				}

				ret_root = cJSON_Parse(result_str.c_str());
				if(ret_root == NULL)
				{
					syslog(Logger::ERROR, "cJSON_Parse error");	
					break;
				}

				ret_item = cJSON_GetObjectItem(ret_root, "result");
				if(ret_item == NULL || ret_item->valuestring == NULL)
				{
					syslog(Logger::ERROR, "get result item error");
					break;
				}
				result = ret_item->valuestring;

				if(result == "error")
				{
					syslog(Logger::ERROR, "get result error");
					break;
				}
				else if(result == "succeed")
				{
					break;
				}
			}
		}
	}

end:
	if(ret_root!=NULL)
		cJSON_Delete(ret_root);
	if(root!=NULL)
		cJSON_Delete(root);
	if(cjson!=NULL)
		free(cjson);
*/
	return true;
}

bool OtherMission::restart_postgres_exporter(std::string &ip, int port)
{
	FILE* pfd;
	char buf[256];

	std::string cmd, process_id;

	/////////////////////////////////////////////////////////
	// get process_id of postgres_exporter
	cmd = "netstat -tnpl | grep tcp6 | grep " + std::to_string(prometheus_port_start+2);
	syslog(Logger::INFO, "restart_postgres_exporter cmd %s", cmd.c_str());

	pfd = popen(cmd.c_str(), "r");
	if(!pfd) {
		syslog(Logger::ERROR, "kill error %s", cmd.c_str());
		return false;
	}
	if(fgets(buf, 256, pfd)!=NULL) {
		char *p, *q;
		p = strstr(buf, "LISTEN");
		if(p != NULL) {
			p = strchr(p, 0x20);
			if(p != NULL) {
				while(*p == 0x20)
					p++;

				q = strchr(p, '/');

				if(p != NULL)
					process_id = std::string(p, q - p);
			}
		}
	}
	pclose(pfd);

	/////////////////////////////////////////////////////////
	// kill postgres_exporter
	if(process_id.length() > 0) {
		cmd = "kill -9 " + process_id;
		syslog(Logger::INFO, "restart_postgres_exporter cmd %s", cmd.c_str());

		pfd = popen(cmd.c_str(), "r");
		if(!pfd) {
			syslog(Logger::ERROR, "kill error %s", cmd.c_str());
			return false;
		}
		while(fgets(buf, 256, pfd)!=NULL) {
			//if(strcasestr(buf, "error") != NULL)
				syslog(Logger::INFO, "%s", buf);
		}
		pclose(pfd);
	}

	/////////////////////////////////////////////////////////
	// start postgres_exporter
	cmd = "export DATA_SOURCE_NAME=\"postgresql://abc:abc@" + ip + ":" + std::to_string(port) + "/postgres?sslmode=disable\";";
	cmd += "cd " + prometheus_path + "/postgres_exporter;";
	cmd += "./postgres_exporter --web.listen-address=:" + std::to_string(prometheus_port_start+2) + " &";
	syslog(Logger::INFO, "restart_postgres_exporter cmd %s", cmd.c_str());

	pfd = popen(cmd.c_str(), "r");
	if(!pfd) {
		syslog(Logger::ERROR, "start error %s", cmd.c_str());
		return false;
	}
	pclose(pfd);

	return true;
}

bool OtherMission::restart_mysql_exporter(std::string &ip, int port) {
	FILE* pfd;
	char buf[256];

	std::string cmd, process_id;

	/////////////////////////////////////////////////////////
	// get process_id of mysql_exporter
	cmd = "netstat -tnpl | grep tcp6 | grep " + std::to_string(prometheus_port_start+3);
	syslog(Logger::INFO, "restart_mysql_exporter cmd %s", cmd.c_str());

	pfd = popen(cmd.c_str(), "r");
	if(!pfd) {
		syslog(Logger::ERROR, "kill error %s", cmd.c_str());
		return false;
	}
	if(fgets(buf, 256, pfd)!=NULL) {
		char *p, *q;
		p = strstr(buf, "LISTEN");
		if(p != NULL) {
			p = strchr(p, 0x20);
			if(p != NULL) {
				while(*p == 0x20)
					p++;

				q = strchr(p, '/');

				if(p != NULL)
					process_id = std::string(p, q - p);
			}
		}
	}
	pclose(pfd);

	/////////////////////////////////////////////////////////
	// kill mysql_exporter
	if(process_id.length() > 0) {
		cmd = "kill -9 " + process_id;
		syslog(Logger::INFO, "restart_mysql_exporter cmd %s", cmd.c_str());

		pfd = popen(cmd.c_str(), "r");
		if(!pfd) {
			syslog(Logger::ERROR, "kill error %s", cmd.c_str());
			return false;
		}
		while(fgets(buf, 256, pfd)!=NULL) {
			//if(strcasestr(buf, "error") != NULL)
				syslog(Logger::INFO, "%s", buf);
		}
		pclose(pfd);
	}

	/////////////////////////////////////////////////////////
	// start mysql_exporter
	cmd = "export DATA_SOURCE_NAME=\"pgx:pgx_pwd@tcp(" + ip + ":" + std::to_string(port) + ")/\";";
	cmd += "cd " + prometheus_path + "/mysqld_exporter;";
	cmd += "./mysqld_exporter --web.listen-address=:" + std::to_string(prometheus_port_start+3) + " &";
	syslog(Logger::INFO, "restart_mysql_exporter cmd %s", cmd.c_str());

	pfd = popen(cmd.c_str(), "r");
	if(!pfd) {
		syslog(Logger::ERROR, "start error %s", cmd.c_str());
		return false;
	}
	pclose(pfd);

	return true;
}

bool OtherMission::restart_prometheus() {
	FILE* pfd;
	char buf[256];

	std::string cmd, process_id;

	/////////////////////////////////////////////////////////
	// get process_id of prometheus
	cmd = "netstat -tnpl | grep tcp6 | grep " + std::to_string(prometheus_port_start);
	syslog(Logger::INFO, "restart_prometheus cmd %s", cmd.c_str());

	pfd = popen(cmd.c_str(), "r");
	if(!pfd) {
		syslog(Logger::ERROR, "kill error %s", cmd.c_str());
		return false;
	}
	if(fgets(buf, 256, pfd)!=NULL) {
		char *p, *q;
		p = strstr(buf, "LISTEN");
		if(p != NULL) {
			p = strchr(p, 0x20);
			if(p != NULL) {
				while(*p == 0x20)
					p++;

				q = strchr(p, '/');

				if(q != NULL)
					process_id = std::string(p, q - p);
			}
		}
	}
	pclose(pfd);

	/////////////////////////////////////////////////////////
	// kill prometheus
	if(process_id.length() > 0) {
		cmd = "kill -9 " + process_id;
		syslog(Logger::INFO, "restart_prometheus cmd %s", cmd.c_str());

		pfd = popen(cmd.c_str(), "r");
		if(!pfd) {
			syslog(Logger::ERROR, "kill error %s", cmd.c_str());
			return false;
		}
		while(fgets(buf, 256, pfd)!=NULL) {
			//if(strcasestr(buf, "error") != NULL)
				syslog(Logger::INFO, "%s", buf);
		}
		pclose(pfd);
	}

	/////////////////////////////////////////////////////////
	// start prometheus
	cmd = "cd " + prometheus_path + ";./prometheus --config.file=\"prometheus.yml\"";
	cmd += " --web.listen-address=:" + std::to_string(prometheus_port_start) + " &";
	syslog(Logger::INFO, "restart_prometheus cmd %s", cmd.c_str());

	pfd = popen(cmd.c_str(), "r");
	if(!pfd) {
		syslog(Logger::ERROR, "start error %s", cmd.c_str());
		return false;
	}
	pclose(pfd);

	return true;
}

bool OtherMission::update_prometheus() {
	std::vector<std::string> vec_machine;
	std::string localhost_str, node_str, pgsql_str, mysql_str;
	std::string ymlfile_path, yml_buf;

	if(!System::get_instance()->get_machine_info_from_metadata(vec_machine)) {
		syslog(Logger::ERROR, "get_machine_info_from_metadata error");
		return false;
	}

	//generate machine str
	for(auto &machine_ip: vec_machine) {
		if(node_str.length()>0)
			node_str += ",";
		node_str += "\"" + machine_ip + ":" + std::to_string(prometheus_port_start+1) + "\"";

		if(pgsql_str.length()>0)
			pgsql_str += ",";
		pgsql_str += "\"" + machine_ip + ":" + std::to_string(prometheus_port_start+2) + "\"";

		if(mysql_str.length()>0)
			mysql_str += ",";
		mysql_str += "\"" + machine_ip + ":" + std::to_string(prometheus_port_start+3) + "\"";
	}

	/////////////////////////////////////////////////////////
	// save yml file
	ymlfile_path = prometheus_path + "/prometheus.yml";
	yml_buf = "global:\r\n  scrape_interval: 15s\r\n  evaluation_interval: 15s\r\nscrape_configs:\r\n";
	yml_buf += "  - job_name: \"prometheus\"\r\n    static_configs:\r\n";
	yml_buf += "      - targets: [" + localhost_str + "]\r\n";
	yml_buf += "  - job_name: \"node\"\r\n    static_configs:\r\n";
	yml_buf += "      - targets: [" + node_str + "]\r\n";
	yml_buf += "  - job_name: \"postgres\"\r\n    static_configs:\r\n";
	yml_buf += "      - targets: [" + pgsql_str + "]\r\n";
	yml_buf += "  - job_name: \"mysql\"\r\n    static_configs:\r\n";
	yml_buf += "      - targets: [" + mysql_str + "]\r\n";

	if(!save_file(ymlfile_path, (char*)yml_buf.c_str())) {
		syslog(Logger::ERROR, "save prometheus yml file error");
		return false;
	}

	/////////////////////////////////////////////////////////
	// restart prometheus
	if(!restart_prometheus()) {
		syslog(Logger::ERROR, "restart prometheus error");
		return false;
	}

	/////////////////////////////////////////////////////////
	// restart node_exporter in every machine
	if(!restart_node_exporter(vec_machine)) {
		syslog(Logger::ERROR, "start node_exporter error");
		return false;
	}

	return true;
}

bool OtherMission::save_file(std::string &path, char* buf) {
	FILE* pfd = fopen(path.c_str(), "wb");
	if(pfd == NULL)	{
		syslog(Logger::ERROR, "Create file error %s", path.c_str());
		return false;
	}

	fwrite(buf,1,strlen(buf),pfd);
	fclose(pfd);
	
	return true;
}
