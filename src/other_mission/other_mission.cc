/*
  Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "other_mission.h"
#include "zettalib/op_log.h"

using namespace kunlun;

extern std::string prometheus_path;
extern int64_t prometheus_port_start;

void Other_Call_Back(void *cb_context) {
  OtherRemoteTask *task = static_cast<OtherRemoteTask *>(cb_context);
  OtherMission *mission = task->getMission();
  std::string response = task->get_response()->SerializeResponseToStr();

  switch (mission->request_type) {
  case kunlun::kControlInstanceType:
    mission->ControlInstanceCallBack(response);
    break;
  case kunlun::kUpdatePrometheusType:
    mission->UpdatePrometheusCallBack(response);
    break;

  default:
    break;
  }
}

void OtherMission::ControlInstanceCallBack(std::string &response){
  Json::Value root,array,info;
  Json::Reader reader;

  job_status = "failed";
  bool ret = reader.parse(response.c_str(), root);
  if (!ret) {
    job_error_code = EintToStr(ERR_PARA_NODE);
    job_error_info = "JSON parse error: " + response;
    goto end;
  }
  array = root["response_array"][0];
  info = array["info"];

  job_status = info["status"].asString();
  if(job_status == "failed") {
    job_error_code = EintToStr(ERR_CONTROL_INSTANCE);
    job_error_info = info["info"].asString();
    goto end;
  }

  job_status = "done";
  job_error_code = EintToStr(EOK);
  job_error_info = "control instance successfully";

end:
  KLOG_INFO("{}", job_error_info);
  update_operation_record();
  System::get_instance()->set_cluster_mgr_working(true);
}

void OtherMission::UpdatePrometheusCallBack(std::string &response){
	Json::Value root,array,info;
	Json::Reader reader;

	task_wait--;

	job_status = "failed";
	bool ret = reader.parse(response.c_str(), root);
	if (!ret) {
		job_error_code = EintToStr(ERR_PARA_NODE);
		job_error_info = "JSON parse error: " + response;
		KLOG_INFO("{}", job_error_info);
		goto end;
	}
	array = root["response_array"][0];
	info = array["info"];

	job_status = info["status"].asString();
	if(job_status == "failed") {
		job_error_code = EintToStr(ERR_UPDATE_PROMETHEUS);
		job_error_info = info["info"].asString();
		KLOG_INFO("{}", job_error_info);
		goto end;
	}

	task_incomplete--;

end:
	if(task_wait == 0){
		if(task_incomplete == 0){
			job_status = "done";
			job_error_code = EintToStr(EOK);
			job_error_info = "update prometheus successfully";
		}else{
			job_status = "failed";
			job_error_code = EintToStr(ERR_UPDATE_PROMETHEUS);
			job_error_info = "update prometheus failed";
		}
		KLOG_INFO( "{}", job_error_info);
		update_operation_record();
	}
}

void OtherMission::ControlInstance() {
  OtherRemoteTask *task;
  Json::Value root_node;
  Json::Value paras_node;
  Json::Value paras;
  std::string hostaddr,control,instance_status,type;
  int port,instance_type;
  Tpye_Ip_Port ip_port;

  if (!super::get_body_json_document().isMember("paras")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `paras` key-value pair in the request body";
    goto end;
  }
  paras = super::get_body_json_document()["paras"];

  if (!paras.isMember("hostaddr")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `hostaddr` key-value pair in the request body";
    goto end;
  }
  hostaddr = paras["hostaddr"].asString();

  if (!paras.isMember("port")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `port` key-value pair in the request body";
    goto end;
  }
  port = stoi(paras["port"].asString());

  if (!paras.isMember("control")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `control` key-value pair in the request body";
    goto end;
  }
  control = paras["control"].asString();

  job_status = "not_started";
  job_error_info = "control instance start";
  KLOG_INFO( "{}", job_error_info);
  update_operation_record();

  System::get_instance()->set_cluster_mgr_working(false);

  //init channel again
  g_node_channel_manager->Init();

	//////////////////////////////////////////////////////////
	// update meta table status by ip and port
	if(control == "stop")
		instance_status = "inactive";
	else if(control == "start" || control == "restart")
		instance_status = "active";
	else {
		job_error_info = "control type error";
		goto end;
	}

	ip_port = std::make_pair(hostaddr, port);
	if(!System::get_instance()->update_instance_status(ip_port, instance_status, instance_type)) {
		job_error_info = "update_instance_status error";
		goto end;
	}

	//////////////////////////////////////////////////////////
	// stop instance by ip and port
	if(instance_type == 1)
		type = "storage";
	else if(instance_type == 2)
		type = "computer";
	else {
		job_error_info = "instance ip_port no find";
		goto end;
	}

	//////////////////////////////////////////////////////////
	// control instance task
  task =
      new OtherRemoteTask("control_instance", job_id.c_str(), this);
  task->AddNodeSubChannel(
      hostaddr.c_str(),
      g_node_channel_manager->getNodeChannel(hostaddr.c_str()));

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
  KLOG_ERROR( "%s", job_error_info.c_str());
  update_operation_record();
}

void OtherMission::UpdatePrometheus(){

	job_status = "not_started";
	job_error_info = "update prometheus start";
	KLOG_INFO( "{}", job_error_info);
	update_operation_record();

	//init channel again
	g_node_channel_manager->Init();

	if(!update_prometheus()) {
		job_error_info = "update prometheus error";
		goto end;
	}

	job_error_info = "update prometheus working";
	KLOG_INFO( "{}", job_error_info);
	update_operation_record();
	return;

end:
	job_status = "failed";
	KLOG_ERROR( "{}", job_error_info);
	update_operation_record();
}

void OtherMission::PostgresExporter() {
	Json::Value paras;
	std::string hostaddr;
	int port;

	if (!super::get_body_json_document().isMember("paras")) {
		job_error_code = EintToStr(ERR_JSON);
		job_error_info = "missing `paras` key-value pair in the request body";
		goto end;
	}
	paras = super::get_body_json_document()["paras"];

	if (!paras.isMember("hostaddr")) {
		job_error_code = EintToStr(ERR_JSON);
		job_error_info = "missing `hostaddr` key-value pair in the request body";
		goto end;
	}
	hostaddr = paras["hostaddr"].asString();

	if (!paras.isMember("port")) {
		job_error_code = EintToStr(ERR_JSON);
		job_error_info = "missing `port` key-value pair in the request body";
		goto end;
	}
	port = stoi(paras["port"].asString());

	job_status = "not_started";
	job_error_info = "postgres exporter start";
	KLOG_INFO( "{}", job_error_info);
	update_operation_record();

	if(!restart_postgres_exporter(hostaddr, port)) {
		job_error_info = "restart_postgres_exporter error";
		goto end;
	}

	job_status = "done";
	job_error_code = EintToStr(EOK);
	job_error_info = "postgres exporter successfully";
	KLOG_INFO( "{}", job_error_info);
	update_operation_record();
	return;

end:
	job_status = "failed";
	KLOG_ERROR("{}", job_error_info);
	update_operation_record();
}

void OtherMission::MysqldExporter() {
	Json::Value paras;
	std::string hostaddr;
	int port;

	if (!super::get_body_json_document().isMember("paras")) {
		job_error_code = EintToStr(ERR_JSON);
		job_error_info = "missing `paras` key-value pair in the request body";
		goto end;
	}
	paras = super::get_body_json_document()["paras"];

	if (!paras.isMember("hostaddr")) {
		job_error_code = EintToStr(ERR_JSON);
		job_error_info = "missing `hostaddr` key-value pair in the request body";
		goto end;
	}
	hostaddr = paras["hostaddr"].asString();

	if (!paras.isMember("port")) {
		job_error_code = EintToStr(ERR_JSON);
		job_error_info = "missing `port` key-value pair in the request body";
		goto end;
	}
	port = stoi(paras["port"].asString());

	job_status = "not_started";
	job_error_info = "mysql exporter start";
	KLOG_INFO( "{}", job_error_info);
	update_operation_record();

	if(!restart_mysql_exporter(hostaddr, port)) {
		job_error_info = "restart_mysql_exporter error";
		goto end;
	}

	job_status = "done";
	job_error_code = EintToStr(EOK);
	job_error_info = "mysql exporter successfully";
	KLOG_INFO( "{}", job_error_info);
	update_operation_record();
	return;

end:
	job_status = "failed";
	KLOG_ERROR( "{}", job_error_info);
	update_operation_record();
}

bool OtherMission::restart_node_exporter(std::vector<std::string> &vec_node) {
	OtherRemoteTask *node_exporter;
	Json::Value root_node;
	Json::Value paras_node;

	root_node["cluster_mgr_request_id"] = job_id;
	root_node["task_spec_info"] = "node_exporter";
	root_node["job_type"] = "node_exporter";
	root_node["paras"] = paras_node;

	for(auto &node: vec_node) {
		brpc::Channel *chann = g_node_channel_manager->getNodeChannel(node.c_str());
		if(chann == nullptr)
			continue;

		node_exporter =
			new OtherRemoteTask("node_exporter", job_id.c_str(), this);
		node_exporter->AddNodeSubChannel(
			node.c_str(),
			chann);

		node_exporter->SetPara(node.c_str(), root_node);
		get_task_manager()->PushBackTask(node_exporter);
	}

	task_wait = vec_node.size();
	task_incomplete = task_wait;

	return true;
}

bool OtherMission::restart_postgres_exporter(std::string &hostaddr, int port) {
	FILE* pfd;
	char buf[256];

	std::string cmd, process_id;

	/////////////////////////////////////////////////////////
	// get process_id of postgres_exporter
	cmd = "netstat -tnpl | grep tcp6 | grep " + std::to_string(prometheus_port_start+2);
	KLOG_INFO( "restart_postgres_exporter cmd {}", cmd);

	pfd = popen(cmd.c_str(), "r");
	if(!pfd) {
		KLOG_ERROR( "kill error {}", cmd);
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
		KLOG_INFO( "restart_postgres_exporter cmd {}", cmd);

		pfd = popen(cmd.c_str(), "r");
		if(!pfd) {
			KLOG_ERROR( "kill error {}", cmd);
			return false;
		}
		while(fgets(buf, 256, pfd)!=NULL) {
			//if(strcasestr(buf, "error") != NULL)
				KLOG_INFO( "{}", buf);
		}
		pclose(pfd);
	}

	/////////////////////////////////////////////////////////
	// start postgres_exporter
	cmd = "export DATA_SOURCE_NAME=\"postgresql://abc:abc@" + hostaddr + ":" + std::to_string(port) + "/postgres?sslmode=disable\";";
	cmd += "cd " + prometheus_path + "/postgres_exporter;";
	cmd += "./postgres_exporter --web.listen-address=:" + std::to_string(prometheus_port_start+2) + " &";
	KLOG_INFO( "restart_postgres_exporter cmd {}", cmd);

	pfd = popen(cmd.c_str(), "r");
	if(!pfd) {
		KLOG_ERROR( "start error {}", cmd);
		return false;
	}
	pclose(pfd);

	return true;
}

bool OtherMission::restart_mysql_exporter(std::string &hostaddr, int port) {
	FILE* pfd;
	char buf[256];

	std::string cmd, process_id;

	/////////////////////////////////////////////////////////
	// get process_id of mysql_exporter
	cmd = "netstat -tnpl | grep tcp6 | grep " + std::to_string(prometheus_port_start+3);
	KLOG_INFO( "restart_mysql_exporter cmd {}", cmd);

	pfd = popen(cmd.c_str(), "r");
	if(!pfd) {
		KLOG_ERROR( "kill error {}", cmd);
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
		KLOG_INFO( "restart_mysql_exporter cmd {}", cmd);

		pfd = popen(cmd.c_str(), "r");
		if(!pfd) {
			KLOG_ERROR( "kill error {}", cmd);
			return false;
		}
		while(fgets(buf, 256, pfd)!=NULL) {
			//if(strcasestr(buf, "error") != NULL)
				KLOG_INFO( "{}", buf);
		}
		pclose(pfd);
	}

	/////////////////////////////////////////////////////////
	// start mysql_exporter
	cmd = "export DATA_SOURCE_NAME=\"pgx:pgx_pwd@tcp(" + hostaddr + ":" + std::to_string(port) + ")/\";";
	cmd += "cd " + prometheus_path + "/mysqld_exporter;";
	cmd += "./mysqld_exporter --web.listen-address=:" + std::to_string(prometheus_port_start+3) + " &";
	KLOG_INFO( "restart_mysql_exporter cmd {}", cmd);

	pfd = popen(cmd.c_str(), "r");
	if(!pfd) {
		KLOG_ERROR( "start error {}", cmd);
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
	KLOG_INFO( "restart_prometheus cmd {}", cmd);

	pfd = popen(cmd.c_str(), "r");
	if(!pfd) {
		KLOG_ERROR("kill error {}", cmd);
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
		KLOG_INFO( "restart_prometheus cmd {}", cmd);

		pfd = popen(cmd.c_str(), "r");
		if(!pfd) {
			KLOG_ERROR( "kill error {}", cmd);
			return false;
		}
		while(fgets(buf, 256, pfd)!=NULL) {
			//if(strcasestr(buf, "error") != NULL)
				KLOG_INFO( "{}", buf);
		}
		pclose(pfd);
	}

	/////////////////////////////////////////////////////////
	// start prometheus
	cmd = "cd " + prometheus_path + ";./prometheus --config.file=\"prometheus.yml\"";
	cmd += " --web.listen-address=:" + std::to_string(prometheus_port_start) + " &";
	KLOG_INFO( "restart_prometheus cmd {}", cmd);

	pfd = popen(cmd.c_str(), "r");
	if(!pfd) {
		KLOG_ERROR( "start error {}", cmd);
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
		KLOG_ERROR( "get_machine_info_from_metadata error");
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
		KLOG_ERROR( "save prometheus yml file error");
		return false;
	}

	/////////////////////////////////////////////////////////
	// restart prometheus
	if(!restart_prometheus()) {
		KLOG_ERROR( "restart prometheus error");
		return false;
	}

	/////////////////////////////////////////////////////////
	// restart node_exporter in every machine
	if(!restart_node_exporter(vec_machine)) {
		KLOG_ERROR( "start node_exporter error");
		return false;
	}

	return true;
}

bool OtherMission::save_file(std::string &path, char* buf) {
	FILE* pfd = fopen(path.c_str(), "wb");
	if(pfd == NULL)	{
		KLOG_ERROR("Create file error {}", path);
		return false;
	}

	fwrite(buf,1,strlen(buf),pfd);
	fclose(pfd);
	
	return true;
}

bool OtherMission::update_operation_record(){
	std::string str_sql,memo;
  Json::Value memo_json;
  Json::FastWriter writer;

  memo_json["error_code"] = job_error_code;
  memo_json["error_info"] = job_error_info;
  writer.omitEndingLineFeed();
  memo = writer.write(memo_json);

	str_sql = "UPDATE cluster_general_job_log set status='" + job_status + "',memo='" + memo;
	str_sql += "',when_ended=current_timestamp(6) where id=" + job_id;
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_UPDATE, str_sql)) {
		KLOG_ERROR( "execute_metadate_opertation error");
		return false;
	}

	return true;
}

bool OtherMission::ArrangeRemoteTask() {
  request_type = get_request_type();
  job_id = get_request_unique_id();
  job_status = "not_started";
  job_error_code = EintToStr(EOK);
  job_error_info = "";

  switch (request_type) {
  case kunlun::kControlInstanceType:
    ControlInstance();
    break;
  case kunlun::kUpdatePrometheusType:
    //UpdatePrometheus();
    break;
  case kunlun::kPostgresExporterType:
    //PostgresExporter();
    break;
  case kunlun::kMysqldExporterType:
    //MysqldExporter();
    break;

  default:
    break;
  }

  return true;
}