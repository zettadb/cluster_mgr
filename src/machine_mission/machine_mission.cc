/*
  Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "machine_mission.h"
#include "kl_mentain/global.h"
#include "kl_mentain/sys.h"

void CreateMachineCallBack(MachineMission *mission, std::string &response, std::string &job_status, std::string &job_memo) {
  Json::Value root;
  Json::Reader reader;
  job_status = "failed";
  bool ret = reader.parse(response.c_str(), root);
  if (!ret) {
    job_memo = "JSON parse error: " + response;
    return;
  }
  Json::Value array = root["response_array"][0];
  Json::Value info = array["info"];

  job_status = info["status"].asString();
  if(job_status == "failed") {
    job_memo = info["info"].asString();
    return;
  }

  Json::Value info2 = info["info"];

  if(!Machine_info::get_instance()->update_machine_path_space(&(mission->machine), info2)) {
    job_status = "failed";
    job_memo = "update_machine_path_space error";
    return;
  }
  
  if(!Machine_info::get_instance()->insert_machine_on_meta(&(mission->machine))) {
    job_status = "failed";
    job_memo = "update_machine_to_meta error";
    return;
  }

  job_status = "done";
  job_memo = "create machine succeed";

  g_node_channel_manager.Init();
}

void UpdateMachineCallBack(MachineMission *mission, std::string &response, std::string &job_status, std::string &job_memo) {
  Json::Value root;
  Json::Reader reader;
  job_status = "failed";
  bool ret = reader.parse(response.c_str(), root);
  if (!ret) {
    job_memo = "JSON parse error: " + response;
    return;
  }
  Json::Value array = root["response_array"][0];
  Json::Value info = array["info"];

  job_status = info["status"].asString();
  if(job_status == "failed") {
    job_memo = info["info"].asString();
    return;
  }

  Json::Value info2 = info["info"];

  if(!Machine_info::get_instance()->update_machine_path_space(&(mission->machine), info2)) {
    job_status = "failed";
    job_memo = "update_machine_path_space error";
    return;
  }

  if(!Machine_info::get_instance()->update_machine_on_meta(&(mission->machine))) {
    job_status = "failed";
    job_memo = "insert_machine_to_meta error";
    return;
  }

  job_status = "done";
  job_memo = "update machine succeed";
}

void Machine_Call_Back(void *cb_context) {
  MachineRemoteTask *task = static_cast<MachineRemoteTask *>(cb_context);

	std::string job_status;
	std::string job_memo;
  std::string job_id = task->getMission()->job_id;
  std::string response = task->get_response()->SerializeResponseToStr();

  switch (task->getMission()->request_type) {
  case kunlun::kCreateMachineType:
    CreateMachineCallBack(task->getMission(), response, job_status, job_memo);
    break;
  case kunlun::kUpdateMachineType:
    UpdateMachineCallBack(task->getMission(), response, job_status, job_memo);
    break;

  default:
    break;
  }

  syslog(Logger::INFO, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id, job_status, job_memo);
}

bool MachineMission::ArrangeRemoteTask() {
  request_type = get_request_type();
  job_id = get_request_unique_id();

  switch (request_type) {
  case kunlun::kCreateMachineType:
    CreateMachine();
    break;
  case kunlun::kUpdateMachineType:
    UpdateMachine();
    break;
  case kunlun::kDeleteMachineType:
    DeleteMachine();
    break;

  default:
    break;
  }

  return true;
}

void MachineMission::CreateMachine() {
	std::string job_status;
	std::string job_memo;
  MachineRemoteTask *update_machine;
  Json::Value root_node;
  Json::Value paras_node;

  if (!super::get_body_json_document().isMember("paras")) {
    setExtraErr("missing `paras` key-value pair in the request body");
    return;
  }
  Json::Value paras = super::get_body_json_document()["paras"];

  if (!paras.isMember("hostaddr")) {
    job_memo = "missing `hostaddr` key-value pair in the request body";
    goto end;
  }
  machine.hostaddr = paras["hostaddr"].asString();

  if (!paras.isMember("rack_id")) {
    job_memo = "missing `rack_id` key-value pair in the request body";
    goto end;
  }
  machine.rack_id = paras["rack_id"].asString();

  if (!paras.isMember("datadir")) {
    job_memo = "missing `datadir` key-value pair in the request body";
    goto end;
  }
  machine.vec_paths.emplace_back(paras["datadir"].asString());

  if (!paras.isMember("logdir")) {
    job_memo = "missing `logdir` key-value pair in the request body";
    goto end;
  }
  machine.vec_paths.emplace_back(paras["logdir"].asString());

  if (!paras.isMember("wal_log_dir")) {
    job_memo = "missing `wal_log_dir` key-value pair in the request body";
    goto end;
  }
  machine.vec_paths.emplace_back(paras["wal_log_dir"].asString());

  if (!paras.isMember("comp_datadir")) {
    job_memo = "missing `comp_datadir` key-value pair in the request body";
    goto end;
  }
  machine.vec_paths.emplace_back(paras["comp_datadir"].asString());

  if (!paras.isMember("total_mem")) {
    job_memo = "missing `total_mem` key-value pair in the request body";
    goto end;
  }
  machine.total_mem = stoi(paras["total_mem"].asString());

  if (!paras.isMember("total_cpu_cores")) {
    job_memo = "missing `total_cpu_cores` key-value pair in the request body";
    goto end;
  }
  machine.total_cpu_cores = stoi(paras["total_cpu_cores"].asString());

	job_status = "not_started";
	job_memo = "create machine start";
  syslog(Logger::INFO, "%s", job_memo.c_str());

  //init channel again
  g_node_channel_manager.Init();

	//////////////////////////////////////////////////////////
	if(System::get_instance()->check_machine_hostaddr(machine.hostaddr))	{
		job_memo = "error, machine_hostaddr is exist";
		goto end;
	}

	//////////////////////////////////////////////////////////
	// UpdateMachineTask from node
  update_machine =
      new MachineRemoteTask("Update_machine", job_id.c_str(), this);
  update_machine->AddNodeSubChannel(
      machine.hostaddr.c_str(),
      g_node_channel_manager.getNodeChannel(machine.hostaddr.c_str()));

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
	syslog(Logger::ERROR, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id, job_status, job_memo);
}

void MachineMission::UpdateMachine() {
	std::string job_status;
	std::string job_memo;
  MachineRemoteTask *update_machine;
  Json::Value root_node;
  Json::Value paras_node;

  if (!super::get_body_json_document().isMember("paras")) {
    setExtraErr("missing `paras` key-value pair in the request body");
    return;
  }
  Json::Value paras = super::get_body_json_document()["paras"];

  if (!paras.isMember("hostaddr")) {
    job_memo = "missing `hostaddr` key-value pair in the request body";
    goto end;
  }
  machine.hostaddr = paras["hostaddr"].asString();

  if (!paras.isMember("rack_id")) {
    job_memo = "missing `rack_id` key-value pair in the request body";
    goto end;
  }
  machine.rack_id = paras["rack_id"].asString();

  if (!paras.isMember("datadir")) {
    job_memo = "missing `datadir` key-value pair in the request body";
    goto end;
  }
  machine.vec_paths.emplace_back(paras["datadir"].asString());

  if (!paras.isMember("logdir")) {
    job_memo = "missing `logdir` key-value pair in the request body";
    goto end;
  }
  machine.vec_paths.emplace_back(paras["logdir"].asString());

  if (!paras.isMember("wal_log_dir")) {
    job_memo = "missing `wal_log_dir` key-value pair in the request body";
    goto end;
  }
  machine.vec_paths.emplace_back(paras["wal_log_dir"].asString());

  if (!paras.isMember("comp_datadir")) {
    job_memo = "missing `comp_datadir` key-value pair in the request body";
    goto end;
  }
  machine.vec_paths.emplace_back(paras["comp_datadir"].asString());

  if (!paras.isMember("total_mem")) {
    job_memo = "missing `total_mem` key-value pair in the request body";
    goto end;
  }
  machine.total_mem = stoi(paras["total_mem"].asString());

  if (!paras.isMember("total_cpu_cores")) {
    job_memo = "missing `total_cpu_cores` key-value pair in the request body";
    goto end;
  }
  machine.total_cpu_cores = stoi(paras["total_cpu_cores"].asString());

	job_status = "not_started";
	job_memo = "update machine start";
  syslog(Logger::INFO, "%s", job_memo.c_str());

  //init channel again
  g_node_channel_manager.Init();
  
	//////////////////////////////////////////////////////////
	if(!System::get_instance()->check_machine_hostaddr(machine.hostaddr))	{
		job_memo = "error, machine_hostaddr is no exist";
		goto end;
	}

	//////////////////////////////////////////////////////////
	// UpdateMachineTask from node
  update_machine =
      new MachineRemoteTask("Update_machine", job_id.c_str(), this);
  update_machine->AddNodeSubChannel(
      machine.hostaddr.c_str(),
      g_node_channel_manager.getNodeChannel(machine.hostaddr.c_str()));

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
	syslog(Logger::ERROR, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id, job_status, job_memo);
}

void MachineMission::DeleteMachine() {
	std::string job_status;
	std::string job_memo;

  Json::Value root_node;
  Json::Value paras_node;

  if (!super::get_body_json_document().isMember("paras")) {
    setExtraErr("missing `paras` key-value pair in the request body");
    return;
  }
  Json::Value paras = super::get_body_json_document()["paras"];

  if (!paras.isMember("hostaddr")) {
    job_memo = "missing `hostaddr` key-value pair in the request body";
    goto end;
  }
  machine.hostaddr = paras["hostaddr"].asString();

	job_status = "not_started";
	job_memo = "delete machine start";
  syslog(Logger::INFO, "%s", job_memo.c_str());

	//////////////////////////////////////////////////////////
	if(!System::get_instance()->check_machine_hostaddr(machine.hostaddr)) {
		job_memo = "error, machine_hostaddr is no exist";
		goto end;
	}

  if(!Machine_info::get_instance()->delete_machine_on_meta(&machine)) {
    job_memo = "delete_machine_on_meta error";
    goto end;
  }

  g_node_channel_manager.removeNodeChannel(machine.hostaddr.c_str());

  job_status = "done";
  job_memo = "delete machine succeed";
	syslog(Logger::INFO, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id, job_status, job_memo);
  return;

end:
	job_status = "failed";
	syslog(Logger::ERROR, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id, job_status, job_memo);
}
