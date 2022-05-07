/*
  Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "machine_mission.h"
#include "kl_mentain/global.h"
#include "kl_mentain/sys.h"

void Machine_Call_Back(void *cb_context) {
  MachineRemoteTask *task = static_cast<MachineRemoteTask *>(cb_context);
  MachineMission *mission = task->getMission();
  std::string response = task->get_response()->SerializeResponseToStr();

  switch (mission->request_type) {
  case kunlun::kCreateMachineType:
    mission->CreateMachineCallBack(response);
    break;
  case kunlun::kUpdateMachineType:
    mission->UpdateMachineCallBack(response);
    break;

  default:
    break;
  }
}

void MachineMission::CreateMachineCallBack(std::string &response) {
  Json::Value root,array,info,info2;
  Json::Reader reader;
  
  job_status = "failed";
  bool ret = reader.parse(response.c_str(), root);
  if (!ret) {
    job_error_info = "JSON parse error: " + response;
    goto end;
  }
  array = root["response_array"][0];
  info = array["info"];

  job_status = info["status"].asString();
  if(job_status == "failed") {
    job_error_info = info["info"].asString();
    goto end;
  }
  info2 = info["info"];

  if(!Machine_info::get_instance()->update_machine_path_space(&machine, info2)) {
    job_status = "failed";
    job_error_info = "update_machine_path_space error";
    goto end;
  }
  
  if(!Machine_info::get_instance()->insert_machine_on_meta(&machine)) {
    job_status = "failed";
    job_error_info = "update_machine_to_meta error";
    goto end;
  }

  job_status = "done";
  job_error_code = EintToStr(EOK);
  job_error_info = "create machine successfully";

end:
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();
}

void MachineMission::UpdateMachineCallBack(std::string &response) {
  Json::Value root,array,info,info2;
  Json::Reader reader;
  
  job_status = "failed";
  bool ret = reader.parse(response.c_str(), root);
  if (!ret) {
    job_error_info = "JSON parse error: " + response;
    goto end;
  }
  array = root["response_array"][0];
  info = array["info"];

  job_status = info["status"].asString();
  if(job_status == "failed") {
    job_error_info = info["info"].asString();
    goto end;
  }
  info2 = info["info"];

  if(!Machine_info::get_instance()->update_machine_path_space(&machine, info2)) {
    job_status = "failed";
    job_error_info = "update_machine_path_space error";
    goto end;
  }

  if(!Machine_info::get_instance()->update_machine_on_meta(&machine)) {
    job_status = "failed";
    job_error_info = "insert_machine_to_meta error";
    goto end;
  }

  job_status = "done";
  job_error_code = EintToStr(EOK);
  job_error_info = "update machine successfully";

end:
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();
}

void MachineMission::CreateMachine() {
  MachineRemoteTask *update_machine;
  brpc::Channel *channel;
  Json::Value root_node;
  Json::Value paras_node;
  Json::Value paras;

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
  machine.hostaddr = paras["hostaddr"].asString();

  if (!paras.isMember("rack_id")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `rack_id` key-value pair in the request body";
    goto end;
  }
  machine.rack_id = paras["rack_id"].asString();

  if (!paras.isMember("datadir")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `datadir` key-value pair in the request body";
    goto end;
  }
  machine.vec_paths.emplace_back(paras["datadir"].asString());

  if (!paras.isMember("logdir")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `logdir` key-value pair in the request body";
    goto end;
  }
  machine.vec_paths.emplace_back(paras["logdir"].asString());

  if (!paras.isMember("wal_log_dir")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `wal_log_dir` key-value pair in the request body";
    goto end;
  }
  machine.vec_paths.emplace_back(paras["wal_log_dir"].asString());

  if (!paras.isMember("comp_datadir")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `comp_datadir` key-value pair in the request body";
    goto end;
  }
  machine.vec_paths.emplace_back(paras["comp_datadir"].asString());

  if (!paras.isMember("total_mem")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `total_mem` key-value pair in the request body";
    goto end;
  }
  machine.total_mem = stoi(paras["total_mem"].asString());

  if (!paras.isMember("total_cpu_cores")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `total_cpu_cores` key-value pair in the request body";
    goto end;
  }
  machine.total_cpu_cores = stoi(paras["total_cpu_cores"].asString());

	job_status = "not_started";
	job_error_info = "create machine start";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();

  //init channel again
  g_node_channel_manager.Init();

	//////////////////////////////////////////////////////////
	if(System::get_instance()->check_machine_hostaddr(machine.hostaddr)) {
		job_error_info = "error, machine_hostaddr is exist";
		goto end;
	}

	//////////////////////////////////////////////////////////
	// UpdateMachineTask from node
  channel = g_node_channel_manager.getNodeChannel(machine.hostaddr.c_str());
  if(channel == nullptr) {
		job_error_info = "error, channel is no exist";
		goto end;
	}

  update_machine = new MachineRemoteTask("Update_machine", job_id.c_str(), this);
  update_machine->AddNodeSubChannel(machine.hostaddr.c_str(),channel);

  root_node["cluster_mgr_request_id"] = job_id;
  root_node["task_spec_info"] = update_machine->get_task_spec_info();
  root_node["job_type"] = "get_paths_space";

  paras_node["path0"] = machine.vec_paths[0];
  paras_node["path1"] = machine.vec_paths[1];
  paras_node["path2"] = machine.vec_paths[2];
  paras_node["path3"] = machine.vec_paths[3];
  root_node["paras"] = paras_node;

  update_machine->SetPara(machine.hostaddr.c_str(), root_node);
  get_task_manager()->PushBackTask(update_machine);
  return;

end:
	job_status = "failed";
	syslog(Logger::ERROR, "%s", job_error_info.c_str());
  update_operation_record();
}

void MachineMission::UpdateMachine() {
  MachineRemoteTask *update_machine;
  brpc::Channel *channel;
  Json::Value root_node;
  Json::Value paras_node;
  Json::Value paras;

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
  machine.hostaddr = paras["hostaddr"].asString();

  if (!paras.isMember("rack_id")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `rack_id` key-value pair in the request body";
    goto end;
  }
  machine.rack_id = paras["rack_id"].asString();

  if (!paras.isMember("datadir")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `datadir` key-value pair in the request body";
    goto end;
  }
  machine.vec_paths.emplace_back(paras["datadir"].asString());

  if (!paras.isMember("logdir")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `logdir` key-value pair in the request body";
    goto end;
  }
  machine.vec_paths.emplace_back(paras["logdir"].asString());

  if (!paras.isMember("wal_log_dir")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `wal_log_dir` key-value pair in the request body";
    goto end;
  }
  machine.vec_paths.emplace_back(paras["wal_log_dir"].asString());

  if (!paras.isMember("comp_datadir")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `comp_datadir` key-value pair in the request body";
    goto end;
  }
  machine.vec_paths.emplace_back(paras["comp_datadir"].asString());

  if (!paras.isMember("total_mem")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `total_mem` key-value pair in the request body";
    goto end;
  }
  machine.total_mem = stoi(paras["total_mem"].asString());

  if (!paras.isMember("total_cpu_cores")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `total_cpu_cores` key-value pair in the request body";
    goto end;
  }
  machine.total_cpu_cores = stoi(paras["total_cpu_cores"].asString());

	job_status = "not_started";
	job_error_info = "update machine start";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();

  //init channel again
  g_node_channel_manager.Init();
  
	//////////////////////////////////////////////////////////
	if(!System::get_instance()->check_machine_hostaddr(machine.hostaddr))	{
		job_error_info = "error, machine_hostaddr is no exist";
		goto end;
	}

	//////////////////////////////////////////////////////////
	// UpdateMachineTask from node
  channel = g_node_channel_manager.getNodeChannel(machine.hostaddr.c_str());
  if(channel == nullptr) {
		job_error_info = "error, channel is no exist";
		goto end;
	}

  update_machine = new MachineRemoteTask("Update_machine", job_id.c_str(), this);
  update_machine->AddNodeSubChannel(machine.hostaddr.c_str(),channel);

  root_node["cluster_mgr_request_id"] = job_id;
  root_node["task_spec_info"] = update_machine->get_task_spec_info();
  root_node["job_type"] = "get_paths_space";

  paras_node["path0"] = machine.vec_paths[0];
  paras_node["path1"] = machine.vec_paths[1];
  paras_node["path2"] = machine.vec_paths[2];
  paras_node["path3"] = machine.vec_paths[3];
  root_node["paras"] = paras_node;

  update_machine->SetPara(machine.hostaddr.c_str(), root_node);
  get_task_manager()->PushBackTask(update_machine);
  return;

end:
	job_status = "failed";
	syslog(Logger::ERROR, "%s", job_error_info.c_str());
  update_operation_record();
}

void MachineMission::DeleteMachine() {
	Json::Value paras;

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
  machine.hostaddr = paras["hostaddr"].asString();

	job_status = "not_started";
	job_error_info = "delete machine start";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();

	//////////////////////////////////////////////////////////
	if(!System::get_instance()->check_machine_hostaddr(machine.hostaddr)) {
		job_error_info = "error, machine_hostaddr is no exist";
		goto end;
	}

  if(!Machine_info::get_instance()->delete_machine_on_meta(&machine)) {
    job_error_info = "delete_machine_on_meta error";
    goto end;
  }

  g_node_channel_manager.removeNodeChannel(machine.hostaddr.c_str());

  job_status = "done";
  job_error_code = EintToStr(EOK);
  job_error_info = "delete machine successfully";
	syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();
  return;

end:
	job_status = "failed";
	syslog(Logger::ERROR, "%s", job_error_info.c_str());
  update_operation_record();
}

bool MachineMission::update_operation_record(){
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
		syslog(Logger::ERROR, "execute_metadate_opertation error");
		return false;
	}

	return true;
}

bool MachineMission::ArrangeRemoteTask() {
  request_type = get_request_type();
  job_id = get_request_unique_id();
	job_status = "not_started";
	job_error_code = EintToStr(EOK);
  job_error_info = "";

  switch (request_type) {
  case kunlun::kCreateMachineType:
    // CreateMachine(); // update port and paths by node_mgr
    break;
  case kunlun::kUpdateMachineType:
    UpdateMachine(); // update port and paths by node_mgr
    break;
  case kunlun::kDeleteMachineType:
    DeleteMachine();
    break;

  default:
    break;
  }

  return true;
}