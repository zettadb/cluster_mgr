/*
  Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "cluster_mission.h"

extern int64_t thread_work_interval;
std::string program_binaries_path;
std::string instance_binaries_path;
std::string storage_prog_package_name;
std::string computer_prog_package_name;
int64_t storage_instance_port_start;
int64_t computer_instance_port_start;
int g_cluster_id = 0;
int g_comps_id_seq = 0;
std::mutex mutex_cluster;

void CLuster_Call_Back(void *cb_context) {
  ClusterRemoteTask *task = static_cast<ClusterRemoteTask *>(cb_context);
  ClusterMission *mission = task->getMission();
  std::string response = task->get_response()->SerializeResponseToStr();
  //syslog(Logger::INFO, "response=%s", response.c_str());

  switch (mission->request_type) {
  case kunlun::kCreateClusterType:
    mission->CreateClusterCallBack(response);
    break;
  case kunlun::kDeleteClusterType:
    mission->DeleteClusterCallBack(response);
    break;

  case kunlun::kAddShardsType:
    mission->AddShardsCallBack(response);
    break;
  case kunlun::kDeleteShardType:
    mission->DeleteShardCallBack(response);
    break;

  case kunlun::kAddCompsType:
    mission->AddCompsCallBack(response);
    break;
  case kunlun::kDeleteCompType:
    mission->DeleteCompCallBack(response);
    break;

  case kunlun::kAddNodesType:
    mission->AddNodesCallBack(response);
    break;
  case kunlun::kDeleteNodeType:
    mission->DeleteNodeCallBack(response);
    break;

  case kunlun::kBackupClusterType:
    mission->BackupClusterCallBack(response);
    break;
  case kunlun::kRestoreNewClusterType:
    mission->RestoreNewCallBack(response);
    break;

  default:
    break;
  }
}

void ClusterMission::UpdateMachinePathSize(std::vector<Machine*> &vec_machine, std::string &response) {
  Json::Value root;
  Json::Reader reader;
  std::string hostaddr;
  Machine *machine = nullptr;

  bool ret = reader.parse(response.c_str(), root);
  if (!ret) {
    return;
  }
  Json::Value array = root["response_array"][0];
  Json::Value info = array["info"];

  hostaddr = array["piece"].asString();
  std::size_t pos = hostaddr.find("#"); 
  hostaddr = hostaddr.substr(pos+1);
  pos = hostaddr.find(":"); 
  hostaddr = hostaddr.substr(0, pos);
  
  for(auto &mach: vec_machine){
    if(hostaddr == mach->hostaddr){
      machine = mach;
      break;
    }
  }
  if(machine == nullptr)
    return;

  if(info["status"].asString() == "failed") {
    machine->available = false;
    syslog(Logger::INFO, "path error=%s", info["info"].asString().c_str());
    return;
  }

  Json::Value info2 = info["info"];

  if(!Machine_info::get_instance()->update_machine_path_space(machine, info2)) {
    machine->available = false;
    syslog(Logger::INFO, "update_machine_path_space error");
    return;
  }

  if(!Machine_info::get_instance()->update_machine_on_meta(machine)) {
    machine->available = false;
    syslog(Logger::INFO, "update_machine_on_meta error");
    return;
  }

  machine->available = true;
}

void ClusterMission::CheckInstanceInstall(std::string &response) {
  Json::Value root;
  Json::Reader reader;

  bool ret = reader.parse(response.c_str(), root);
  if (!ret) {
    return;
  }
  Json::Value array = root["response_array"][0];
  Json::Value info = array["info"];

  if(info["status"].asString() == "failed") {
    syslog(Logger::ERROR, "install error : %s", info["info"].asString().c_str());
    return;
  }

  task_incomplete--;
}

void ClusterMission::CheckInstanceDelete(std::string &response) {
  Json::Value root;
  Json::Reader reader;

  bool ret = reader.parse(response.c_str(), root);
  if (!ret) {
    return;
  }
  Json::Value array = root["response_array"][0];
  Json::Value info = array["info"];

  if(info["status"].asString() == "failed") {
    syslog(Logger::ERROR, "delete error : %s", info["info"].asString().c_str());
    return;
  }

  task_incomplete--;
}

void ClusterMission::CheckBackupCluster(std::string &response) {
  Json::Value root;
  Json::Reader reader;
  std::string str_sql,end_time;

  bool ret = reader.parse(response.c_str(), root);
  if (!ret) {
    return;
  }
  Json::Value array = root["response_array"][0];
  Json::Value info = array["info"];

  if(info["status"].asString() == "failed") {
    syslog(Logger::ERROR, "backup error : %s", info["info"].asString().c_str());
    return;
  } else {
    Json::Value info2 = info["info"];
    get_datatime(end_time);
    str_sql = "UPDATE cluster_shard_backup_restore_log set status='done',shard_backup_path='" + info2["path"].asString();
    str_sql += "',when_ended='" + end_time + "' where when_started='" + start_time + "' and shard_id=" + info2["shard_id"].asString();
    //syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

    if(System::get_instance()->execute_metadate_opertation(SQLCOM_UPDATE, str_sql)) {
      syslog(Logger::ERROR, "update cluster_shard_backup_restore_log error");
      return;
    }
  }

  task_incomplete--;
}

void ClusterMission::CheckInstanceRestore(std::string &response) {
  Json::Value root;
  Json::Reader reader;

  bool ret = reader.parse(response.c_str(), root);
  if (!ret) {
    return;
  }
  Json::Value array = root["response_array"][0];
  Json::Value info = array["info"];

  if(info["status"].asString() == "failed") {
    syslog(Logger::ERROR, "delete error : %s", info["info"].asString().c_str());
    return;
  }

  task_incomplete--;
}

void ClusterMission::CreateClusterCallBack(std::string &response) {
  switch (task_step) {
  case ClusterMission::ClusterStep::GET_PATH_SIZE:
    UpdateMachinePathSize(vec_machine, response);
    task_wait--;
    if(task_wait == 0) {
      auto iter = vec_machine.begin();
      for(; iter != vec_machine.end(); ){
        if((*iter)->available){
          iter++;
        }else{
          iter = vec_machine.erase(iter);
        }
      }

      if(vec_machine.size() > 0){
        createStorageInfo();
      }else{
        missiom_finish = true;
        job_status = "failed";
        job_error_info = "error, no available machine";
        syslog(Logger::ERROR, "job_error_info=%s", job_error_info.c_str());
        update_operation_record();
      }
    }
    break;

  case ClusterMission::ClusterStep::INSTALL_STORAGE:
    CheckInstanceInstall(response);
    task_wait--;
    if(task_wait == 0) {
      syslog(Logger::INFO, "INSTALL_STORAGE task_incomplete = %d", task_incomplete);
      if(task_incomplete == 0){
        createComputerInfo();
      } else {
        job_error_info = "install storage error";
        syslog(Logger::ERROR, "job_error_info=%s", job_error_info.c_str());
        update_operation_record();
        roll_back_record();
      }
    }
    break;

  case ClusterMission::ClusterStep::INSTALL_COMPUTER:
    CheckInstanceInstall(response);
    task_wait--;
    if(task_wait == 0) {
      syslog(Logger::INFO, "INSTALL_COMPUTER task_incomplete = %d", task_incomplete);
      if(task_incomplete == 0){
        startClusterInfo();
      } else {
        job_error_info = "install computer error";
        syslog(Logger::ERROR, "job_error_info=%s", job_error_info.c_str());
        update_operation_record();
        roll_back_record();
      }
    }
    break;

  default:
    break;
  }
}

void ClusterMission::DeleteClusterCallBack(std::string &response) {
  switch (task_step) {
  case ClusterMission::ClusterStep::DELETE_INSTANCE:
    CheckInstanceDelete(response);
    task_wait--;
    if(task_wait == 0) {
      syslog(Logger::INFO, "DELETE_INSTANCE task_incomplete = %d", task_incomplete);
      stop_cluster();
    }
    break;

  default:
    break;
  }
}

void ClusterMission::AddShardsCallBack(std::string &response) {
  switch (task_step) {
  case ClusterMission::ClusterStep::GET_PATH_SIZE:
    UpdateMachinePathSize(vec_machine, response);
    task_wait--;
    if(task_wait == 0) {
      auto iter = vec_machine.begin();
      for(; iter != vec_machine.end(); ){
        if((*iter)->available){
          iter++;
        }else{
          iter = vec_machine.erase(iter);
        }
      }

      if(vec_machine.size() > 0){
        addShardsInfo();
      }else{
        missiom_finish = true;
        job_status = "failed";
        job_error_info = "error, no available machine";
        syslog(Logger::ERROR, "job_error_info=%s", job_error_info.c_str());
        update_operation_record();
      }
    }
    break;

  case ClusterMission::ClusterStep::INSTALL_STORAGE:
    CheckInstanceInstall(response);
    task_wait--;
    if(task_wait == 0) {
      syslog(Logger::INFO, "INSTALL_STORAGE task_incomplete = %d", task_incomplete);
      if(task_incomplete == 0){
        startShardsInfo();
      } else {
        job_error_info = "install storage error";
        syslog(Logger::ERROR, "job_error_info=%s", job_error_info.c_str());
        update_operation_record();
        roll_back_record();
      }
    }
    break;

  default:
    break;
  }
}

void ClusterMission::DeleteShardCallBack(std::string &response) {
  switch (task_step) {
  case ClusterMission::ClusterStep::DELETE_INSTANCE:
    CheckInstanceDelete(response);
    task_wait--;
    if(task_wait == 0) {
      syslog(Logger::INFO, "DELETE_INSTANCE task_incomplete = %d", task_incomplete);
      stop_shard();
    }
    break;

  default:
    break;
  }
}

void ClusterMission::AddCompsCallBack(std::string &response) {
  switch (task_step) {
  case ClusterMission::ClusterStep::GET_PATH_SIZE:
    UpdateMachinePathSize(vec_machine, response);
    task_wait--;
    if(task_wait == 0) {
      auto iter = vec_machine.begin();
      for(; iter != vec_machine.end(); ){
        if((*iter)->available){
          iter++;
        }else{
          iter = vec_machine.erase(iter);
        }
      }

      if(vec_machine.size() > 0){
        addCompsInfo();
      }else{
        missiom_finish = true;
        job_status = "failed";
        job_error_info = "error, no available machine";
        syslog(Logger::ERROR, "job_error_info=%s", job_error_info.c_str());
        update_operation_record();
      }
    }
    break;

  case ClusterMission::ClusterStep::INSTALL_COMPUTER:
    CheckInstanceInstall(response);
    task_wait--;
    if(task_wait == 0) {
      syslog(Logger::INFO, "INSTALL_COMPUTER task_incomplete = %d", task_incomplete);
      if(task_incomplete == 0){
        startCompsInfo();
      } else {
        job_error_info = "install computer error";
        syslog(Logger::ERROR, "job_error_info=%s", job_error_info.c_str());
        update_operation_record();
        roll_back_record();
      }
    }
    break;

  default:
    break;
  }
}

void ClusterMission::DeleteCompCallBack(std::string &response) {
  switch (task_step) {
  case ClusterMission::ClusterStep::DELETE_INSTANCE:
    CheckInstanceDelete(response);
    task_wait--;
    if(task_wait == 0) {
      syslog(Logger::INFO, "DELETE_INSTANCE task_incomplete = %d", task_incomplete);
      stop_comp();
    }
    break;

  default:
    break;
  }
}

void ClusterMission::AddNodesCallBack(std::string &response) {
  switch (task_step) {
  case ClusterMission::ClusterStep::GET_PATH_SIZE:
    UpdateMachinePathSize(vec_machine, response);
    task_wait--;
    if(task_wait == 0) {
      auto iter = vec_machine.begin();
      for(; iter != vec_machine.end(); ){
        if((*iter)->available){
          iter++;
        }else{
          iter = vec_machine.erase(iter);
        }
      }

      if(vec_machine.size() > 0){
        backup_nodes();
      }else{
        missiom_finish = true;
        job_status = "failed";
        job_error_info = "error, no available machine";
        syslog(Logger::ERROR, "job_error_info=%s", job_error_info.c_str());
        update_operation_record();
      }
    }
    break;

  case ClusterMission::ClusterStep::BACKUP_STORAGE:
    CheckBackupCluster(response);
    task_wait--;
    if(task_wait == 0) {
      syslog(Logger::INFO, "BACKUP_STORAGE task_incomplete = %d", task_incomplete);
      update_backup_nodes();
    }
    break;

  case ClusterMission::ClusterStep::INSTALL_STORAGE:
    CheckInstanceInstall(response);
    task_wait--;
    if(task_wait == 0) {
      syslog(Logger::INFO, "INSTALL_STORAGE task_incomplete = %d", task_incomplete);
      if(task_incomplete == 0){
        restoreNodes();
      } else {
        job_error_info = "install storage error";
        syslog(Logger::ERROR, "job_error_info=%s", job_error_info.c_str());
        update_operation_record();
        roll_back_record();
      }
    }
    break;

  case ClusterMission::ClusterStep::RESTORE_INSTANCE:
    CheckInstanceRestore(response);
    task_wait--;
    if(task_wait == 0) {
      syslog(Logger::INFO, "RESTORE_INSTANCE task_incomplete = %d", task_incomplete);
      if(task_incomplete == 0){
        updateRestoreNodesInfo();
      } else {
        job_error_info = "restore instance error";
        syslog(Logger::ERROR, "job_error_info=%s", job_error_info.c_str());
        update_operation_record();
        roll_back_record();
      }
    }
    break;

  default:
    break;
  }
}

void ClusterMission::DeleteNodeCallBack(std::string &response) {
  switch (task_step) {
  case ClusterMission::ClusterStep::DELETE_INSTANCE:
    CheckInstanceDelete(response);
    task_wait--;
    if(task_wait == 0) {
      syslog(Logger::INFO, "DELETE_INSTANCE task_incomplete = %d", task_incomplete);
      missiom_finish = true;
      job_status = "done";
      job_error_info = "delete node successfully";
      syslog(Logger::INFO, "%s", job_error_info.c_str());
      update_operation_record();
      System::get_instance()->set_cluster_mgr_working(true);
    }
    break;

  default:
    break;
  }
}

void ClusterMission::BackupClusterCallBack(std::string &response) {
  switch (task_step) {
  case ClusterMission::ClusterStep::BACKUP_STORAGE:
    CheckBackupCluster(response);
    task_wait--;
    if(task_wait == 0) {
      syslog(Logger::INFO, "BACKUP_STORAGE task_incomplete = %d", task_incomplete);
      update_backup_cluster();
    }
    break;

  default:
    break;
  }
}

void ClusterMission::RestoreNewCallBack(std::string &response) {
  switch (task_step) {
  case ClusterMission::ClusterStep::GET_PATH_SIZE:
    UpdateMachinePathSize(vec_machine, response);
    task_wait--;
    if(task_wait == 0) {
      auto iter = vec_machine.begin();
      for(; iter != vec_machine.end(); ){
        if((*iter)->available){
          iter++;
        }else{
          iter = vec_machine.erase(iter);
        }
      }

      if(vec_machine.size() > 0){
        createStorageInfo();
      }else{
        missiom_finish = true;
        job_status = "failed";
        job_error_info = "error, no available machine";
        syslog(Logger::ERROR, "job_error_info=%s", job_error_info.c_str());
        update_operation_record();
      }
    }
    break;

  case ClusterMission::ClusterStep::INSTALL_STORAGE:
    CheckInstanceInstall(response);
    task_wait--;
    if(task_wait == 0) {
      syslog(Logger::INFO, "INSTALL_STORAGE task_incomplete = %d", task_incomplete);
      if(task_incomplete == 0){
        createComputerInfo();
      } else {
        job_error_info = "install storage error";
        syslog(Logger::ERROR, "job_error_info=%s", job_error_info.c_str());
        update_operation_record();
        roll_back_record();
      }
    }
    break;

  case ClusterMission::ClusterStep::INSTALL_COMPUTER:
    CheckInstanceInstall(response);
    task_wait--;
    if(task_wait == 0) {
      syslog(Logger::INFO, "INSTALL_COMPUTER task_incomplete = %d", task_incomplete);
      if(task_incomplete == 0){
        startClusterInfo();
      } else {
        job_error_info = "install computer error";
        syslog(Logger::ERROR, "job_error_info=%s", job_error_info.c_str());
        update_operation_record();
        roll_back_record();
      }
    }
    break;

  case ClusterMission::ClusterStep::RESTORE_INSTANCE:
    CheckInstanceRestore(response);
    task_wait--;
    if(task_wait == 0) {
      syslog(Logger::INFO, "RESTORE_INSTANCE task_incomplete = %d", task_incomplete);
      if(task_incomplete == 0){
        updateRestoreInfo();
      } else {
        job_error_info = "restore instance error";
        syslog(Logger::ERROR, "job_error_info=%s", job_error_info.c_str());
        update_operation_record();
        roll_back_record();
      }
    }
    break;

  default:
    break;
  }
}

void ClusterMission::renameCluster() {
  Json::Value paras;

  if (!super::get_body_json_document().isMember("paras")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `paras` key-value pair in the request body";
    goto end;
  }
  paras = super::get_body_json_document()["paras"];

  if (!paras.isMember("cluster_name")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `cluster_name` key-value pair in the request body";
    goto end;
  }
  cluster_name = paras["cluster_name"].asString();

  if (!paras.isMember("nick_name")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `nick_name` key-value pair in the request body";
    goto end;
  }
  nick_name = paras["nick_name"].asString();

	//////////////////////////////////////////////////////////
	if(System::get_instance()->check_nick_name(nick_name)) {
		job_error_info = "new nick_name have existed";
		goto end;
	}

	//////////////////////////////////////////////////////////
	if(!System::get_instance()->rename_cluster(cluster_name, nick_name)) {
		job_error_info = "rename cluster error";
		goto end;
	}

  missiom_finish = true;
  job_status = "done";
  job_error_info = "rename cluster successfully";
  syslog(Logger::ERROR, "%s", job_error_info.c_str());
  update_operation_record();
  return;

end:
  missiom_finish = true;
  job_status = "failed";
  syslog(Logger::ERROR, "%s", job_error_info.c_str());
  update_operation_record();
}

void ClusterMission::createCluster() {
  Json::Value paras;
	std::set<std::string> set_machine;

  if (!super::get_body_json_document().isMember("paras")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `paras` key-value pair in the request body";
    goto end;
  }
  paras = super::get_body_json_document()["paras"];

  if (paras.isMember("nick_name")) {
    nick_name = paras["nick_name"].asString();
  }

  if (!paras.isMember("ha_mode")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `ha_mode` key-value pair in the request body";
    goto end;
  }
  std::get<0>(cluster_info) = paras["ha_mode"].asString();

  if (!paras.isMember("shards")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `shards` key-value pair in the request body";
    goto end;
  }
  std::get<1>(cluster_info) = stoi(paras["shards"].asString());
	if(std::get<1>(cluster_info)<1 || std::get<1>(cluster_info)>256) {
    job_error_code = EintToStr(ERR_PARA);
		job_error_info = "shards error(must in 1-256)";
		goto end;
	}

  if (!paras.isMember("nodes")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `nodes` key-value pair in the request body";
    goto end;
  }
  std::get<2>(cluster_info) = stoi(paras["nodes"].asString());
	if(std::get<2>(cluster_info)<1 || std::get<2>(cluster_info)>256) {
    job_error_code = EintToStr(ERR_PARA);
		job_error_info = "nodes error(must in 1-256)";
		goto end;
	}

  if (!paras.isMember("comps")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `comps` key-value pair in the request body";
    goto end;
  }
  std::get<3>(cluster_info) = stoi(paras["comps"].asString());
	if(std::get<3>(cluster_info)<1 || std::get<3>(cluster_info)>256) {
    job_error_code = EintToStr(ERR_PARA);
		job_error_info = "comps error(must in 1-256)";
		goto end;
	}

  if (!paras.isMember("max_storage_size")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `max_storage_size` key-value pair in the request body";
    goto end;
  }
  std::get<4>(cluster_info) = stoi(paras["max_storage_size"].asString());

  if (!paras.isMember("max_connections")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `max_connections` key-value pair in the request body";
    goto end;
  }
  std::get<5>(cluster_info) = stoi(paras["max_connections"].asString());

  if (!paras.isMember("cpu_cores")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `cpu_cores` key-value pair in the request body";
    goto end;
  }
  std::get<6>(cluster_info) = stoi(paras["cpu_cores"].asString());

  if (!paras.isMember("innodb_size")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `innodb_size` key-value pair in the request body";
    goto end;
  }
  std::get<7>(cluster_info) = stoi(paras["innodb_size"].asString());
	if(std::get<7>(cluster_info)<1)	{
    job_error_code = EintToStr(ERR_PARA);
		job_error_info = "innodb_size error(must > 0)";
		goto end;
	}

  if (paras.isMember("machinelist")) {
    Json::Value machinelist = paras["machinelist"];
    int n = machinelist.size();
    for(int i=0; i<n; i++) {
      set_machine.insert(machinelist[i]["hostaddr"].asString());
    }
  }

	/////////////////////////////////////////////////////////
	if(std::get<0>(cluster_info) == "no_rep")	{
		if(std::get<2>(cluster_info)!=1) {
      job_error_code = EintToStr(ERR_PARA);
			job_error_info = "error, nodes=1 in no_rep mode";
			goto end;
		}
	}	else if(std::get<0>(cluster_info) == "mgr")	{
		if(std::get<2>(cluster_info)<3 || std::get<2>(cluster_info)>256) {
      job_error_code = EintToStr(ERR_PARA);
			job_error_info = "error, nodes>=3 && nodes<=256 in mgr mode";
			goto end;
		}
	}	else if(std::get<0>(cluster_info) == "rbr")	{
		if(std::get<2>(cluster_info)<3 || std::get<2>(cluster_info)>256) {
      job_error_code = EintToStr(ERR_PARA);
			job_error_info = "error, nodes>=3 && nodes<=256 in rbr mode";
			goto end;
		}
	}

  job_status = "ongoing";
  job_error_info = "create cluster start";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();

  /////////////////////////////////////////////////////////
  // for install cluster cmd
	if(!create_program_path()) {
		job_error_info = "create_cmd_path error";
		goto end;
	}

  //create user name for install
  get_user_name();
  //generate as timestamp and serial number
  generate_cluster_name();
  //init channel again
  g_node_channel_manager.Init();

	/////////////////////////////////////////////////////////
  //get machine and info
  if(!Machine_info::get_instance()->get_machines_info(vec_machine, set_machine)) {
    job_error_info = "error, no machine to install";
    goto end;
  }

  //check machine path
  if(!Machine_info::get_instance()->check_machines_path(vec_machine)) {
    job_error_info = "error, machine path must set first";
    goto end;
  }

	/////////////////////////////////////////////////////////
  //prepare task
  // to get path size from machines
  task_num = vec_machine.size(); 
  //erery instance need  1. install task, 2. rollback task
  // shards * nodes * 2
  task_num += std::get<1>(cluster_info) * std::get<2>(cluster_info) * 2;
  // comps * 2
  task_num += std::get<3>(cluster_info) * 2;
  syslog(Logger::INFO, "task_num=%d", task_num);

  for(int i=0; i<task_num; i++) {
    std::string task_name = "cluster_task_" + std::to_string(i);
    ClusterRemoteTask *cluster_task =
        new ClusterRemoteTask(task_name.c_str(), get_request_unique_id().c_str(), this);
    get_task_manager()->PushBackTask(cluster_task);
  }

  // get path size from machines
  task_step = GET_PATH_SIZE;
  task_wait = vec_machine.size();
  syslog(Logger::INFO, "vec_machine.size()=%d", task_wait);
  for(int i=0; i<task_wait; i++){
    ClusterRemoteTask *cluster_task;
    bool bGetTask = false;

    //get a empty task;
    auto &task_vec = get_task_manager()->get_remote_task_vec();
    auto iter = task_vec.begin();
    for (; iter != task_vec.end(); iter++) {
      cluster_task = static_cast<ClusterRemoteTask *>(*iter);
      if(cluster_task->getStatus() == 0){
        cluster_task->setStatus(1);
        bGetTask = true;
        break;
      }
    }

    if(!bGetTask){
      job_error_info = "error, no task to update machine path size";
			goto end;
    }

    //update task info to run
    cluster_task->AddNodeSubChannel(
        vec_machine[i]->hostaddr.c_str(),
        g_node_channel_manager.getNodeChannel(vec_machine[i]->hostaddr.c_str()));

    Json::Value root_node;
    Json::Value paras_node;
    root_node["cluster_mgr_request_id"] = job_id;
    root_node["task_spec_info"] = cluster_task->get_task_spec_info();
    root_node["job_type"] = "get_paths_space";

    paras_node["path0"] = vec_machine[i]->vec_paths[0];
    paras_node["path1"] = vec_machine[i]->vec_paths[1];
    paras_node["path2"] = vec_machine[i]->vec_paths[2];
    paras_node["path3"] = vec_machine[i]->vec_paths[3];
    root_node["paras"] = paras_node;

    cluster_task->SetPara(vec_machine[i]->hostaddr.c_str(), root_node);
  }

  job_error_info = "update machine path size start";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();
  return;

end:
  missiom_finish = true;
  job_status = "failed";
  syslog(Logger::ERROR, "%s", job_error_info.c_str());
  update_operation_record();
}

void ClusterMission::deleteCluster() {
  Json::Value paras;

  if (!super::get_body_json_document().isMember("paras")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `paras` key-value pair in the request body";
    goto end;
  }
  paras = super::get_body_json_document()["paras"];

  if (!paras.isMember("cluster_name")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `cluster_name` key-value pair in the request body";
    goto end;
  }
  cluster_name = paras["cluster_name"].asString();

	job_status = "ongoing";
	job_error_info = "delete cluster start";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();

	/////////////////////////////////////////////////////////
  //stop cluster working
	System::get_instance()->set_cluster_mgr_working(false);
	
	if(!System::get_instance()->check_cluster_name(cluster_name))	{
		job_error_info = "error, cluster_name is no exist";
		goto end;
	}

  delete_cluster(cluster_name);
  return;

end:
  missiom_finish = true;
  job_status = "failed";
  syslog(Logger::ERROR, "%s", job_error_info.c_str());
  update_operation_record();
}

void ClusterMission::addShards() {
  Json::Value paras;
	std::set<std::string> set_machine;

  if (!super::get_body_json_document().isMember("paras")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `paras` key-value pair in the request body";
    goto end;
  }
  paras = super::get_body_json_document()["paras"];

  if (!paras.isMember("cluster_name")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `cluster_name` key-value pair in the request body";
    goto end;
  }
  cluster_name = paras["cluster_name"].asString();

  if (!paras.isMember("shards")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `shards` key-value pair in the request body";
    goto end;
  }
  add_shards = stoi(paras["shards"].asString());
	if(add_shards<1 || add_shards>256) {
    job_error_code = EintToStr(ERR_PARA);
		job_error_info = "shards error(must in 1-256)";
		goto end;
	}

  if (paras.isMember("machinelist")) {
    Json::Value machinelist = paras["machinelist"];
    int n = machinelist.size();
    for(int i=0; i<n; i++) {
      set_machine.insert(machinelist[i]["hostaddr"].asString());
    }
  }

  job_status = "ongoing";
  job_error_info = "add shards start";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();

	if(!System::get_instance()->check_cluster_name(cluster_name))	{
		job_error_info = "error, cluster_name is no exist";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// get cluster_info
	if(!get_cluster_info(cluster_name))	{
		job_error_info = "get_cluster_info error";
		goto end;
	}

  /////////////////////////////////////////////////////////
  // for install cluster cmd
	if(!create_program_path()) {
		job_error_info = "create_cmd_path error";
		goto end;
	}

  //create user name for install
  get_user_name();
  //init channel again
  g_node_channel_manager.Init();

	/////////////////////////////////////////////////////////
  //get machine and info
  if(!Machine_info::get_instance()->get_machines_info(vec_machine, set_machine)) {
    job_error_info = "error, no machine to install";
    goto end;
  }

  //check machine path
  if(!Machine_info::get_instance()->check_machines_path(vec_machine)) {
    job_error_info = "error, machine path must set first";
    goto end;
  }

	/////////////////////////////////////////////////////////
  //prepare task
  // to get path size from machines
  task_num = vec_machine.size(); 
  //erery instance need  1. install task, 2. rollback task
  // shards * nodes * 2
  task_num += add_shards * std::get<2>(cluster_info) * 2;
  syslog(Logger::INFO, "task_num=%d", task_num);

  for(int i=0; i<task_num; i++) {
    std::string task_name = "cluster_task_" + std::to_string(i);
    ClusterRemoteTask *cluster_task =
        new ClusterRemoteTask(task_name.c_str(), get_request_unique_id().c_str(), this);
    get_task_manager()->PushBackTask(cluster_task);
  }

  // get path size from machines
  task_step = GET_PATH_SIZE;
  task_wait = vec_machine.size();
  syslog(Logger::INFO, "vec_machine.size()=%d", task_wait);
  for(int i=0; i<task_wait; i++){
    ClusterRemoteTask *cluster_task;
    bool bGetTask = false;

    //get a empty task;
    auto &task_vec = get_task_manager()->get_remote_task_vec();
    auto iter = task_vec.begin();
    for (; iter != task_vec.end(); iter++) {
      cluster_task = static_cast<ClusterRemoteTask *>(*iter);
      if(cluster_task->getStatus() == 0){
        cluster_task->setStatus(1);
        bGetTask = true;
        break;
      }
    }

    if(!bGetTask){
      job_error_info = "error, no task to update machine path size";
			goto end;
    }

    //update task info to run
    cluster_task->AddNodeSubChannel(
        vec_machine[i]->hostaddr.c_str(),
        g_node_channel_manager.getNodeChannel(vec_machine[i]->hostaddr.c_str()));

    Json::Value root_node;
    Json::Value paras_node;
    root_node["cluster_mgr_request_id"] = job_id;
    root_node["task_spec_info"] = cluster_task->get_task_spec_info();
    root_node["job_type"] = "get_paths_space";

    paras_node["path0"] = vec_machine[i]->vec_paths[0];
    paras_node["path1"] = vec_machine[i]->vec_paths[1];
    paras_node["path2"] = vec_machine[i]->vec_paths[2];
    paras_node["path3"] = vec_machine[i]->vec_paths[3];
    root_node["paras"] = paras_node;

    cluster_task->SetPara(vec_machine[i]->hostaddr.c_str(), root_node);
  }

  job_error_info = "update machine path size start";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();
  return;

end:
  missiom_finish = true;
  job_status = "failed";
  syslog(Logger::ERROR, "%s", job_error_info.c_str());
  update_operation_record();
}

void ClusterMission::deleteShard() {
  Json::Value paras;
  std::vector<Tpye_Ip_Port> vec_storage_ip_port;

  if (!super::get_body_json_document().isMember("paras")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `paras` key-value pair in the request body";
    goto end;
  }
  paras = super::get_body_json_document()["paras"];

  if (!paras.isMember("cluster_name")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `cluster_name` key-value pair in the request body";
    goto end;
  }
  cluster_name = paras["cluster_name"].asString();

  if (!paras.isMember("shard_name")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `shard_name` key-value pair in the request body";
    goto end;
  }
  shard_name = paras["shard_name"].asString();

	job_status = "ongoing";
	job_error_info = "delete shard start";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();

	/////////////////////////////////////////////////////////
  //stop cluster working
	System::get_instance()->set_cluster_mgr_working(false);

	if(!System::get_instance()->check_cluster_name(cluster_name))	{
		job_error_info = "error, cluster_name is no exist";
		goto end;
	}

	if(!System::get_instance()->check_cluster_shard_name(cluster_name, shard_name))	{
		job_error_info = "error, shard_name is no exist";
		goto end;
	}

	if(!System::get_instance()->check_cluster_shard_more(cluster_name))	{
		job_error_info = "error, shard <= 1";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// get shards_ip_port by cluster_name and shard_name
	if(!System::get_instance()->get_shards_ip_port(cluster_name, shard_name, vec_storage_ip_port)) {
		job_error_info = "get_shards_ip_port error";
		goto end;
	}

	/////////////////////////////////////////////////////////
  //prepare task
  // to delete instance
  task_num = vec_storage_ip_port.size(); 

  for(int i=0; i<task_num; i++) {
    std::string task_name = "cluster_task_" + std::to_string(i);
    ClusterRemoteTask *cluster_task =
        new ClusterRemoteTask(task_name.c_str(), get_request_unique_id().c_str(), this);
    get_task_manager()->PushBackTask(cluster_task);
  }

	/////////////////////////////////////////////////////////
	// delete storages from shard
  for(auto &storage: vec_storage_ip_port)
    delete_storage(storage);

  task_step = DELETE_INSTANCE;
  task_wait = vec_storage_ip_port.size();
  task_incomplete = task_wait;

  syslog(Logger::INFO, "delete_shard task start");
  return;

end:
  missiom_finish = true;
  job_status = "failed";
  syslog(Logger::ERROR, "%s", job_error_info.c_str());
  update_operation_record();
}

void ClusterMission::addComps() {
  Json::Value paras;
	std::set<std::string> set_machine;

  if (!super::get_body_json_document().isMember("paras")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `paras` key-value pair in the request body";
    goto end;
  }
  paras = super::get_body_json_document()["paras"];

  if (!paras.isMember("cluster_name")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `cluster_name` key-value pair in the request body";
    goto end;
  }
  cluster_name = paras["cluster_name"].asString();

  if (!paras.isMember("comps")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `comps` key-value pair in the request body";
    goto end;
  }
  add_comps = stoi(paras["comps"].asString());
	if(add_comps<1 || add_comps>256) {
    job_error_code = EintToStr(ERR_PARA);
		job_error_info = "comps error(must in 1-256)";
		goto end;
	}

  if (paras.isMember("machinelist")) {
    Json::Value machinelist = paras["machinelist"];
    int n = machinelist.size();
    for(int i=0; i<n; i++) {
      set_machine.insert(machinelist[i]["hostaddr"].asString());
    }
  }

  job_status = "ongoing";
  job_error_info = "add comps start";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();

	if(!System::get_instance()->check_cluster_name(cluster_name))	{
		job_error_info = "error, cluster_name is no exist";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// get cluster_info
	if(!get_cluster_info(cluster_name))	{
		job_error_info = "get_cluster_info error";
		goto end;
	}

  /////////////////////////////////////////////////////////
  // for install cluster cmd
	if(!create_program_path()) {
		job_error_info = "create_cmd_path error";
		goto end;
	}

  //init channel again
  g_node_channel_manager.Init();

	/////////////////////////////////////////////////////////
  //get machine and info
  if(!Machine_info::get_instance()->get_machines_info(vec_machine, set_machine)) {
    job_error_info = "error, no machine to install";
    goto end;
  }

  //check machine path
  if(!Machine_info::get_instance()->check_machines_path(vec_machine)) {
    job_error_info = "error, machine path must set first";
    goto end;
  }

	/////////////////////////////////////////////////////////
  //prepare task
  // to get path size from machines
  task_num = vec_machine.size(); 
  //erery instance need  1. install task, 2. rollback task
  // add_comps * 2
  task_num += add_comps * 2;
  syslog(Logger::INFO, "task_num=%d", task_num);

  for(int i=0; i<task_num; i++) {
    std::string task_name = "cluster_task_" + std::to_string(i);
    ClusterRemoteTask *cluster_task =
        new ClusterRemoteTask(task_name.c_str(), get_request_unique_id().c_str(), this);
    get_task_manager()->PushBackTask(cluster_task);
  }

  // get path size from machines
  task_step = GET_PATH_SIZE;
  task_wait = vec_machine.size();
  syslog(Logger::INFO, "vec_machine.size()=%d", task_wait);
  for(int i=0; i<task_wait; i++){
    ClusterRemoteTask *cluster_task;
    bool bGetTask = false;

    //get a empty task;
    auto &task_vec = get_task_manager()->get_remote_task_vec();
    auto iter = task_vec.begin();
    for (; iter != task_vec.end(); iter++) {
      cluster_task = static_cast<ClusterRemoteTask *>(*iter);
      if(cluster_task->getStatus() == 0){
        cluster_task->setStatus(1);
        bGetTask = true;
        break;
      }
    }

    if(!bGetTask){
      job_error_info = "error, no task to update machine path size";
			goto end;
    }

    //update task info to run
    cluster_task->AddNodeSubChannel(
        vec_machine[i]->hostaddr.c_str(),
        g_node_channel_manager.getNodeChannel(vec_machine[i]->hostaddr.c_str()));

    Json::Value root_node;
    Json::Value paras_node;
    root_node["cluster_mgr_request_id"] = job_id;
    root_node["task_spec_info"] = cluster_task->get_task_spec_info();
    root_node["job_type"] = "get_paths_space";

    paras_node["path0"] = vec_machine[i]->vec_paths[0];
    paras_node["path1"] = vec_machine[i]->vec_paths[1];
    paras_node["path2"] = vec_machine[i]->vec_paths[2];
    paras_node["path3"] = vec_machine[i]->vec_paths[3];
    root_node["paras"] = paras_node;

    cluster_task->SetPara(vec_machine[i]->hostaddr.c_str(), root_node);
  }

  job_error_info = "update machine path size start";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();
  return;

end:
  missiom_finish = true;
  job_status = "failed";
  syslog(Logger::ERROR, "%s", job_error_info.c_str());
  update_operation_record();
}

void ClusterMission::deleteComp() {
  Json::Value paras;
  std::vector<Tpye_Ip_Port> vec_comps_ip_port;

  if (!super::get_body_json_document().isMember("paras")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `paras` key-value pair in the request body";
    goto end;
  }
  paras = super::get_body_json_document()["paras"];

  if (!paras.isMember("cluster_name")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `cluster_name` key-value pair in the request body";
    goto end;
  }
  cluster_name = paras["cluster_name"].asString();

  if (!paras.isMember("comp_name")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `comp_name` key-value pair in the request body";
    goto end;
  }
  comp_name = paras["comp_name"].asString();

	job_status = "ongoing";
	job_error_info = "delete comp start";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();

	/////////////////////////////////////////////////////////
  //stop cluster working
	System::get_instance()->set_cluster_mgr_working(false);

	if(!System::get_instance()->check_cluster_name(cluster_name))	{
		job_error_info = "error, cluster_name is no exist";
		goto end;
	}

	if(!System::get_instance()->check_cluster_comp_name(cluster_name, comp_name)) {
		job_error_info = "error, comp_name is no exist";
		goto end;
	}

	if(!System::get_instance()->check_cluster_comp_more(cluster_name)) {
		job_error_info = "error, comp <= 1";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// get comps_ip_port by cluster_name  and comp_name
	if(!System::get_instance()->get_comps_ip_port(cluster_name, comp_name, vec_comps_ip_port)) {
		job_error_info = "get_comps_ip_port error";
		goto end;
	}

	/////////////////////////////////////////////////////////
  //prepare task
  // to delete instance
  task_num = vec_comps_ip_port.size(); 

  for(int i=0; i<task_num; i++) {
    std::string task_name = "cluster_task_" + std::to_string(i);
    ClusterRemoteTask *cluster_task =
        new ClusterRemoteTask(task_name.c_str(), get_request_unique_id().c_str(), this);
    get_task_manager()->PushBackTask(cluster_task);
  }

	/////////////////////////////////////////////////////////
	// delete comp
  for(auto &computer: vec_comps_ip_port)
    delete_computer(computer);

  task_step = DELETE_INSTANCE;
  task_wait = vec_comps_ip_port.size();
  task_incomplete = task_wait;

  syslog(Logger::INFO, "delete_comp task start");
  return;

end:
  missiom_finish = true;
  job_status = "failed";
  syslog(Logger::ERROR, "%s", job_error_info.c_str());
  update_operation_record();
}

void ClusterMission::addNodes() {
  Json::Value paras;
  std::set<std::string> set_machine;
  std::string backup_storage; //no used
  std::string shard_uuid;

  if (!super::get_body_json_document().isMember("paras")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `paras` key-value pair in the request body";
    goto end;
  }
  paras = super::get_body_json_document()["paras"];

  if (!paras.isMember("cluster_name")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `cluster_name` key-value pair in the request body";
    goto end;
  }
  cluster_name = paras["cluster_name"].asString();

  if (!paras.isMember("shard_name")){
    all_shard = true;
  }else{
    all_shard = false;
    shard_name = paras["shard_name"].asString();
  }

  if (!paras.isMember("nodes")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `nodes` key-value pair in the request body";
    goto end;
  }
  add_nodes = stoi(paras["nodes"].asString());
	if(add_nodes<1 || add_nodes>256) {
    job_error_code = EintToStr(ERR_PARA);
		job_error_info = "nodes error(must in 1-256)";
		goto end;
	}

  if (paras.isMember("machinelist")) {
    Json::Value machinelist = paras["machinelist"];
    int n = machinelist.size();
    for(int i=0; i<n; i++) {
      set_machine.insert(machinelist[i]["hostaddr"].asString());
    }
  }

  job_status = "ongoing";
  job_error_info = "add nodes start";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();

	if(!System::get_instance()->check_cluster_name(cluster_name))	{
		job_error_info = "error, cluster_name is no exist";
		goto end;
	}

	if(System::get_instance()->get_cluster_no_rep_mode(cluster_name)) {
		job_error_info = "error, add nodes must not in no_rep mode";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// get cluster_info
	if(!get_cluster_info(cluster_name))	{
		job_error_info = "get_cluster_info error";
		goto end;
	}

  // get vec_shard_name
	if(all_shard)	{
		// get every shard_name
		if(!System::get_instance()->get_cluster_shard_name(cluster_name, vec_shard_name)) {
			job_error_info = "get_cluster_shard_name error";
			goto end;
		}
	}	else {
		if(!System::get_instance()->check_cluster_shard_name(cluster_name, shard_name)) {
			job_error_info = "error, shard_name is no exist";
			goto end;
		}
		vec_shard_name.emplace_back(shard_name);
	}

	if(!System::get_instance()->get_backup_storage_string(backup_storage, backup_storage_id, backup_storage_str)) {
		job_error_info = "get_backup_storage error, create_backup_storage first";
		goto end;
	}

  //create user name for install
  get_user_name();
  //init channel again
  g_node_channel_manager.Init();

	/////////////////////////////////////////////////////////
  //get machine and info
  if(!Machine_info::get_instance()->get_machines_info(vec_machine, set_machine)) {
    job_error_info = "error, no machine to install";
    goto end;
  }

  //check machine path
  if(!Machine_info::get_instance()->check_machines_path(vec_machine)) {
    job_error_info = "error, machine path must set first";
    goto end;
  }

  /////////////////////////////////////////////////////////
  //prepare task
  // to get path size from machines
  task_num = vec_machine.size(); 
  //erery instance need  1. install task, 2. restore task, 3. rollback task
  // shards * nodes * 2
  task_num += vec_shard_name.size() * add_nodes * 3;
  // shards 1. backup
  task_num += vec_shard_name.size();
  syslog(Logger::INFO, "task_num=%d", task_num);

  for(int i=0; i<task_num; i++) {
    std::string task_name = "cluster_task_" + std::to_string(i);
    ClusterRemoteTask *cluster_task =
        new ClusterRemoteTask(task_name.c_str(), get_request_unique_id().c_str(), this);
    get_task_manager()->PushBackTask(cluster_task);
  }

  // get path size from machines
  task_step = GET_PATH_SIZE;
  task_wait = vec_machine.size();
  syslog(Logger::INFO, "vec_machine.size()=%d", task_wait);
  for(int i=0; i<task_wait; i++){
    ClusterRemoteTask *cluster_task;
    bool bGetTask = false;

    //get a empty task;
    auto &task_vec = get_task_manager()->get_remote_task_vec();
    auto iter = task_vec.begin();
    for (; iter != task_vec.end(); iter++) {
      cluster_task = static_cast<ClusterRemoteTask *>(*iter);
      if(cluster_task->getStatus() == 0){
        cluster_task->setStatus(1);
        bGetTask = true;
        break;
      }
    }

    if(!bGetTask){
      job_error_info = "error, no task to update machine path size";
			goto end;
    }

    //update task info to run
    cluster_task->AddNodeSubChannel(
        vec_machine[i]->hostaddr.c_str(),
        g_node_channel_manager.getNodeChannel(vec_machine[i]->hostaddr.c_str()));

    Json::Value root_node;
    Json::Value paras_node;
    root_node["cluster_mgr_request_id"] = job_id;
    root_node["task_spec_info"] = cluster_task->get_task_spec_info();
    root_node["job_type"] = "get_paths_space";

    paras_node["path0"] = vec_machine[i]->vec_paths[0];
    paras_node["path1"] = vec_machine[i]->vec_paths[1];
    paras_node["path2"] = vec_machine[i]->vec_paths[2];
    paras_node["path3"] = vec_machine[i]->vec_paths[3];
    root_node["paras"] = paras_node;

    cluster_task->SetPara(vec_machine[i]->hostaddr.c_str(), root_node);
  }

  job_error_info = "update machine path size start";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();
  return;

end:
  missiom_finish = true;
  job_status = "failed";
  syslog(Logger::ERROR, "%s", job_error_info.c_str());
  update_operation_record();
}

void ClusterMission::deleteNode() {
  Json::Value paras;
  std::string strtmp;
  std::string group_seeds;
  std::size_t pos,len;
  Tpye_Ip_Port ip_port;
  ClusterRemoteTask *cluster_task;

  if (!super::get_body_json_document().isMember("paras")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `paras` key-value pair in the request body";
    goto end;
  }
  paras = super::get_body_json_document()["paras"];

  if (!paras.isMember("cluster_name")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `cluster_name` key-value pair in the request body";
    goto end;
  }
  cluster_name = paras["cluster_name"].asString();

  if (!paras.isMember("shard_name")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `shard_name` key-value pair in the request body";
    goto end;
  }
  shard_name = paras["shard_name"].asString();

  if (!paras.isMember("hostaddr")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `hostaddr` key-value pair in the request body";
    goto end;
  }
  ip_port.first = paras["hostaddr"].asString();

  if (!paras.isMember("port")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `shard_name` key-value pair in the request body";
    goto end;
  }
  ip_port.second = stoi(paras["port"].asString());

	job_status = "ongoing";
	job_error_info = "delete node start";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();

	/////////////////////////////////////////////////////////
  //stop cluster working
	System::get_instance()->set_cluster_mgr_working(false);

	if(!System::get_instance()->check_cluster_name(cluster_name))	{
		job_error_info = "error, cluster_name is no exist";
		goto end;
	}

	if(System::get_instance()->get_cluster_no_rep_mode(cluster_name)) {
		job_error_info = "error, delete node must not in no_rep mode";
		goto end;
	}

	if(!System::get_instance()->check_cluster_shard_name(cluster_name, shard_name))	{
		job_error_info = "error, shard_name is no exist";
		goto end;
	}

	if(!System::get_instance()->check_cluster_shard_node_more(cluster_name, shard_name)) {
		job_error_info = "error, node <= 1";
		goto end;
	}

  /////////////////////////////////////////////////////////
  //prepare task
  cluster_task = new ClusterRemoteTask("delete_node", get_request_unique_id().c_str(), this);
  get_task_manager()->PushBackTask(cluster_task);

	/////////////////////////////////////////////////////////
	// stop node
  stop_shard_node(ip_port);

	/////////////////////////////////////////////////////////
	// delete storages
  delete_storage(ip_port);

  task_step = DELETE_INSTANCE;
  task_wait = 1;
  task_incomplete = task_wait;

  syslog(Logger::INFO, "delete_node task start");
  return;

end:
  missiom_finish = true;
  job_status = "failed";
  syslog(Logger::ERROR, "%s", job_error_info.c_str());
  update_operation_record();
}

void ClusterMission::backupCluster(){
  Json::Value paras;
  std::string backup_storage; //no used

  if (!super::get_body_json_document().isMember("paras")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `paras` key-value pair in the request body";
    goto end;
  }
  paras = super::get_body_json_document()["paras"];

  if (!paras.isMember("backup_cluster_name")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `backup_cluster_name` key-value pair in the request body";
    goto end;
  }
  backup_cluster_name = paras["backup_cluster_name"].asString();

	job_status = "ongoing";
	job_error_info = "backup cluster start";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();

  /////////////////////////////////////////////////////////
	if(!System::get_instance()->check_cluster_name(backup_cluster_name)) {
		job_error_info = "error, backup_cluster_name is no exist";
		goto end;
	}

	if(!System::get_instance()->get_backup_storage_string(backup_storage, backup_storage_id, backup_storage_str))	{
		job_error_info = "get_backup_storage error, create_backup_storage first";
		goto end;
	}

  // get every shard_name
  if(!System::get_instance()->get_cluster_shard_name(backup_cluster_name, vec_shard_name)) {
    job_error_info = "get_cluster_shard_name error";
    goto end;
  }

	/////////////////////////////////////////////////////////
  //prepare task
  // to get path size from machines
  task_num = vec_shard_name.size(); 
  syslog(Logger::INFO, "task_num=%d", task_num);

  for(int i=0; i<task_num; i++) {
    std::string task_name = "cluster_task_" + std::to_string(i);
    ClusterRemoteTask *cluster_task =
        new ClusterRemoteTask(task_name.c_str(), get_request_unique_id().c_str(), this);
    get_task_manager()->PushBackTask(cluster_task);
  }

	backup_cluster();
  return;

end:
  missiom_finish = true;
  job_status = "failed";
  syslog(Logger::ERROR, "%s", job_error_info.c_str());
  update_operation_record();
}

void ClusterMission::restoreNewCluster(){
  Json::Value paras;
  std::string backup_storage; //no used
  std::set<std::string> set_machine;

  if (!super::get_body_json_document().isMember("paras")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `paras` key-value pair in the request body";
    goto end;
  }
  paras = super::get_body_json_document()["paras"];

  if (!paras.isMember("backup_cluster_name")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `backup_cluster_name` key-value pair in the request body";
    goto end;
  }
  backup_cluster_name = paras["backup_cluster_name"].asString();

  if (paras.isMember("nick_name")) {
    nick_name = paras["nick_name"].asString();
  }
  
  if (!paras.isMember("timestamp")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `timestamp` key-value pair in the request body";
    goto end;
  }
  timestamp = paras["timestamp"].asString();

  if (paras.isMember("machinelist")) {
    Json::Value machinelist = paras["machinelist"];
    int n = machinelist.size();
    for(int i=0; i<n; i++) {
      set_machine.insert(machinelist[i]["hostaddr"].asString());
    }
  }

	job_status = "ongoing";
	job_error_info = "restore new cluster start";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();

  /////////////////////////////////////////////////////////
	if(!System::get_instance()->check_cluster_name(backup_cluster_name)) {
		job_error_info = "error, backup_cluster_name is no exist";
		goto end;
	}

	if(!System::get_instance()->get_backup_storage_string(backup_storage, backup_storage_id, backup_storage_str))	{
		job_error_info = "get_backup_storage error, create_backup_storage first";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// get backup_info from metadata
	if(!System::get_instance()->get_backup_info_from_metadata(backup_cluster_name, timestamp, vec_backup_shard_name))	{
		job_error_info = "get_backup_info error, maybe timestamp too early";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// get cluster_info
	if(!get_cluster_info(backup_cluster_name))	{
		job_error_info = "get_cluster_info error";
		goto end;
	}
	std::get<1>(cluster_info) = vec_backup_shard_name.size();

  /////////////////////////////////////////////////////////
  // for install cluster cmd
	if(!create_program_path()) {
		job_error_info = "create_cmd_path error";
		goto end;
	}

  //create user name for install
  get_user_name();
  //generate as timestamp and serial number
  generate_cluster_name();
  //init channel again
  g_node_channel_manager.Init();

	/////////////////////////////////////////////////////////
  //get machine and info
  if(!Machine_info::get_instance()->get_machines_info(vec_machine, set_machine)) {
    job_error_info = "error, no machine to install";
    goto end;
  }

  //check machine path
  if(!Machine_info::get_instance()->check_machines_path(vec_machine)) {
    job_error_info = "error, machine path must set first";
    goto end;
  }

	/////////////////////////////////////////////////////////
  //prepare task
  // to get path size from machines
  task_num = vec_machine.size(); 
  //erery instance need  1. install task, 2. rollback task, 3. restore task
  // shards * nodes * 3
  task_num += std::get<1>(cluster_info) * std::get<2>(cluster_info) * 3;
  // comps * 3
  task_num += std::get<3>(cluster_info) * 3;
  syslog(Logger::INFO, "task_num=%d", task_num);

  for(int i=0; i<task_num; i++) {
    std::string task_name = "cluster_task_" + std::to_string(i);
    ClusterRemoteTask *cluster_task =
        new ClusterRemoteTask(task_name.c_str(), get_request_unique_id().c_str(), this);
    get_task_manager()->PushBackTask(cluster_task);
  }

  // get path size from machines
  task_step = GET_PATH_SIZE;
  task_wait = vec_machine.size();
  syslog(Logger::INFO, "vec_machine.size()=%d", task_wait);
  for(int i=0; i<task_wait; i++){
    ClusterRemoteTask *cluster_task;
    bool bGetTask = false;

    //get a empty task;
    auto &task_vec = get_task_manager()->get_remote_task_vec();
    auto iter = task_vec.begin();
    for (; iter != task_vec.end(); iter++) {
      cluster_task = static_cast<ClusterRemoteTask *>(*iter);
      if(cluster_task->getStatus() == 0){
        cluster_task->setStatus(1);
        bGetTask = true;
        break;
      }
    }

    if(!bGetTask){
      job_error_info = "error, no task to update machine path size";
			goto end;
    }

    //update task info to run
    cluster_task->AddNodeSubChannel(
        vec_machine[i]->hostaddr.c_str(),
        g_node_channel_manager.getNodeChannel(vec_machine[i]->hostaddr.c_str()));

    Json::Value root_node;
    Json::Value paras_node;
    root_node["cluster_mgr_request_id"] = job_id;
    root_node["task_spec_info"] = cluster_task->get_task_spec_info();
    root_node["job_type"] = "get_paths_space";

    paras_node["path0"] = vec_machine[i]->vec_paths[0];
    paras_node["path1"] = vec_machine[i]->vec_paths[1];
    paras_node["path2"] = vec_machine[i]->vec_paths[2];
    paras_node["path3"] = vec_machine[i]->vec_paths[3];
    root_node["paras"] = paras_node;

    cluster_task->SetPara(vec_machine[i]->hostaddr.c_str(), root_node);
  }

  job_error_info = "update machine path size start";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();
  return;

end:
  missiom_finish = true;
  job_status = "failed";
  syslog(Logger::ERROR, "%s", job_error_info.c_str());
  update_operation_record();
}

void ClusterMission::createStorageInfo() {

  int shards = std::get<1>(cluster_info);
  int nodes = std::get<2>(cluster_info);

  syslog(Logger::INFO, "createStorageInfo start");

	///////////////////////////////////////////////////////////////////////////////
	// get storage of shard 
	for(int i=0; i<shards; i++)	{
		std::vector<Tpye_Ip_Port_Paths> vec_storage_ip_port_paths;
		if(!Machine_info::get_instance()->get_storage_nodes(nodes, nodes_select, vec_storage_ip_port_paths, vec_machine))	{
			job_error_info = "Machine_info, no available machine";
			goto end;
		}
		vec_shard_storage_ip_port_paths.emplace_back(vec_storage_ip_port_paths);
		vec_shard_name.emplace_back("shard"+std::to_string(i + 1));
	}

	///////////////////////////////////////////////////////////////////////////////
	// create storage of shard
	for(int i=0; i<shards; i++)
	  create_shard(vec_shard_storage_ip_port_paths[i], vec_shard_name[i]);

  task_step = INSTALL_STORAGE;
  task_wait = shards*nodes;
  task_incomplete = task_wait;

	job_error_info = "install storage start";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();
  return;

end:
  syslog(Logger::ERROR, "%s", job_error_info.c_str());
  update_operation_record();
  roll_back_record();
}

void ClusterMission::createComputerInfo() {

  int comps = std::get<3>(cluster_info);
	int comps_id_seq = 0;

  syslog(Logger::INFO, "createComputerInfo start");

  //////////////////////////////////////////////////////////////////////////////
	System::get_instance()->get_comp_nodes_id_seq(comps_id_seq);
	syslog(Logger::INFO, "comps_id_seq=%d", comps_id_seq);
	comps_id_seq += 1;
  if(comps_id_seq <= g_comps_id_seq)
    comps_id_seq = g_comps_id_seq + 1;
  g_comps_id_seq = comps_id_seq + comps;

	///////////////////////////////////////////////////////////////////////////////
	// get computer 
	if(!Machine_info::get_instance()->get_computer_nodes(comps, nodes_select, vec_comps_ip_port_paths, vec_machine)) {
		job_error_info = "Machine_info, no available machine";
		goto end;
	}
	for(int i=0; i<comps; i++)
		vec_comp_name.emplace_back("comp"+std::to_string(i + 1));

	///////////////////////////////////////////////////////////////////////////////
	// create computers
	create_comps(vec_comps_ip_port_paths, vec_comp_name, comps_id_seq);

  task_step = INSTALL_COMPUTER;
  task_wait = comps;
  task_incomplete = task_wait;

  job_status = "ongoing";
	job_error_info = "install computer start";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();
  return;

end:
  syslog(Logger::ERROR, "%s", job_error_info.c_str());
  update_operation_record();
  roll_back_record();
}

void ClusterMission::startClusterInfo() {

	FILE* pfd;
	char buf[256];

	int retry;
	std::string cmd,file_path;

	Json::Value shards_json;
	Json::Value meta_json;
  Json::Value roll_back;
  Json::FastWriter writer;

  syslog(Logger::INFO, "startClusterInfo start");

	/////////////////////////////////////////////////////////
	//create storage shards json
	for(int i=0; i<vec_shard_name.size(); i++) {
		Json::Value shards_json_sub;
    shards_json_sub["shard_name"] = vec_shard_name[i];

		Json::Value shards_json_sub_sub;
		for(auto &ip_port_paths: vec_shard_storage_ip_port_paths[i]) {
			Json::Value shards_json_sub_sub_sub;
      shards_json_sub_sub_sub["ip"] = std::get<0>(ip_port_paths);
      shards_json_sub_sub_sub["port"] = std::get<1>(ip_port_paths);
      shards_json_sub_sub_sub["user"] = "pgx";
      shards_json_sub_sub_sub["password"] = "pgx_pwd";
      shards_json_sub_sub.append(shards_json_sub_sub_sub);
		}
    shards_json_sub["shard_nodes"] = shards_json_sub_sub;

    shards_json.append(shards_json_sub);
	}

	// save json file to cmd_path
	file_path = program_binaries_path + "/" + computer_prog_package_name + "/scripts/pgsql_shards.json";

  writer.omitEndingLineFeed();
  cmd = writer.write(shards_json);
  save_file(file_path, cmd.c_str());

	/////////////////////////////////////////////////////////
	//create comps json
  file_path = program_binaries_path + "/" + computer_prog_package_name + "/scripts/pgsql_comps.json";
  save_file(file_path, comps_json.c_str());
 
	/////////////////////////////////////////////////////////
	//create meta json
	std::vector<Tpye_Ip_Port_User_Pwd> meta;
	if(!System::get_instance()->get_meta_info(meta)) {
		job_error_info = "get_meta_info error";
		goto end;
	}

	for(auto &ip_port_user_pwd: meta)	{
		Json::Value meta_json_sub;
    meta_json_sub["ip"] = std::get<0>(ip_port_user_pwd);
    meta_json_sub["port"] = std::get<1>(ip_port_user_pwd);
    meta_json_sub["user"] = std::get<2>(ip_port_user_pwd);
    meta_json_sub["password"] = std::get<3>(ip_port_user_pwd);
    meta_json.append(meta_json_sub);
	}

	/////////////////////////////////////////////////////////
	// save json file to cmd_path
	file_path = program_binaries_path + "/" + computer_prog_package_name + "/scripts/pgsql_meta.json";

  writer.omitEndingLineFeed();
  cmd = writer.write(meta_json);
  save_file(file_path, cmd.c_str());

  /////////////////////////////////////////////////////////
  //add roll back to meta
  roll_back["job_type"] = "start_cluster";
  roll_back["cluster_name"] = cluster_name;
  insert_roll_back_record(roll_back);

	/////////////////////////////////////////////////////////
	// start cluster cmd
	cmd = "cd " + program_binaries_path + "/" + computer_prog_package_name + "/scripts/;";
	cmd += "python2 create_cluster.py --shards_config ./pgsql_shards.json --comps_config ./pgsql_comps.json --meta_config ./pgsql_meta.json --cluster_name ";
	cmd += cluster_name + " --cluster_owner abc --cluster_biz kunlun --ha_mode " + std::get<0>(cluster_info);
	syslog(Logger::INFO, "startClusterInfo cmd %s", cmd.c_str());

	pfd = popen(cmd.c_str(), "r");
	if(!pfd) {
		job_error_info = "startClusterInfo start cmd error";
		goto end;
	}
	while(fgets(buf, 256, pfd)!=NULL)	{
		//if(strcasestr(buf, "error") != NULL)
			syslog(Logger::INFO, "%s", buf);
	}
	pclose(pfd);

	/////////////////////////////////////////////////////////////
	// check cluster_name by meta
	retry = thread_work_interval * 30;
	while(retry-->0) {
		sleep(1);
		if(System::get_instance()->check_cluster_name(cluster_name))
			break;
	}

	if(retry<0)	{
		job_error_info = "cluster start error";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// update cluster info
	if(!updateClusterInfo()) {
		job_error_info = "update cluster info error";
		goto end;
	}

  sleep(thread_work_interval * 3);  //wait cluster shard update
  syslog(Logger::INFO, "create cluster successfully : %s", cluster_name.c_str());

  if(request_type == kRestoreNewClusterType) {
    restoreCluster();
    return;
  }

  missiom_finish = true;
  job_status = "done";
  job_error_info = "create cluster successfully";
  System::get_instance()->get_cluster_info(cluster_name, job_memo_json);
  update_operation_record();
  delete_roll_back_record();
  return;

end:
  syslog(Logger::ERROR, "%s", job_error_info.c_str());
  update_operation_record();
  roll_back_record();
}

bool ClusterMission::updateClusterInfo() {
  Json::Value paras;
	std::string str_sql;
  std::string memo;

  paras["ha_mode"] = std::get<0>(cluster_info);
  paras["shards"] = std::to_string(std::get<1>(cluster_info));
  paras["nodes"] = std::to_string(std::get<2>(cluster_info));
  paras["comps"] = std::to_string(std::get<3>(cluster_info));
  paras["max_storage_size"] = std::to_string(std::get<4>(cluster_info));
  paras["max_connections"] = std::to_string(std::get<5>(cluster_info));
  paras["cpu_cores"] = std::to_string(std::get<6>(cluster_info));
  paras["innodb_size"] = std::to_string(std::get<7>(cluster_info));

  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  memo = writer.write(paras);

	str_sql = "UPDATE db_clusters set memo='" + memo + "' where name='" + cluster_name + "'";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_UPDATE, str_sql))	{
		syslog(Logger::ERROR, "update cluster info error");
		return false;
	}
	
	//////////////////////////////////////////////////////////
	if(!System::get_instance()->rename_cluster(cluster_name, nick_name)) {
		syslog(Logger::ERROR, "rename cluster error");
		return false;
	}

	return true;
}

void ClusterMission::addShardsInfo() {

  int nodes = std::get<2>(cluster_info);
  int shards_id = 0;

  syslog(Logger::INFO, "addShardsInfo start");

	/////////////////////////////////////////////////////////
	// get max index for add
	System::get_instance()->get_max_shard_name_id(cluster_name, shards_id);
	syslog(Logger::INFO, "shards_id=%d", shards_id);
	shards_id += 1;

	///////////////////////////////////////////////////////////////////////////////
	// get storage of shard 
	for(int i=0; i<add_shards; i++)	{
		std::vector<Tpye_Ip_Port_Paths> vec_storage_ip_port_paths;
		if(!Machine_info::get_instance()->get_storage_nodes(nodes, nodes_select, vec_storage_ip_port_paths, vec_machine))	{
			job_error_info = "Machine_info, no available machine";
			goto end;
		}
		vec_shard_storage_ip_port_paths.emplace_back(vec_storage_ip_port_paths);
		vec_shard_name.emplace_back("shard"+std::to_string(shards_id + i));
	}

	///////////////////////////////////////////////////////////////////////////////
	// create storage of shard
	for(int i=0; i<add_shards; i++)
	  create_shard(vec_shard_storage_ip_port_paths[i], vec_shard_name[i]);

  task_step = INSTALL_STORAGE;
  task_wait = add_shards*nodes;
  task_incomplete = task_wait;

	job_error_info = "install storage start";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();
  return;

end:
  syslog(Logger::ERROR, "%s", job_error_info.c_str());
  update_operation_record();
  roll_back_record();
}

void ClusterMission::startShardsInfo() {

	FILE* pfd;
	char buf[256];

	int retry;
	std::string cmd,file_path;

	Json::Value shards_json;
	Json::Value meta_json;
  Json::Value roll_back;
  Json::FastWriter writer;

  syslog(Logger::INFO, "startShardsInfo start");

	/////////////////////////////////////////////////////////
	//create storage shards json
	for(int i=0; i<vec_shard_name.size(); i++) {
		Json::Value shards_json_sub;
    shards_json_sub["shard_name"] = vec_shard_name[i];

		Json::Value shards_json_sub_sub;
		for(auto &ip_port_paths: vec_shard_storage_ip_port_paths[i]) {
			Json::Value shards_json_sub_sub_sub;
      shards_json_sub_sub_sub["ip"] = std::get<0>(ip_port_paths);
      shards_json_sub_sub_sub["port"] = std::get<1>(ip_port_paths);
      shards_json_sub_sub_sub["user"] = "pgx";
      shards_json_sub_sub_sub["password"] = "pgx_pwd";
      shards_json_sub_sub.append(shards_json_sub_sub_sub);
		}
    shards_json_sub["shard_nodes"] = shards_json_sub_sub;

    shards_json.append(shards_json_sub);
	}

	// save json file to cmd_path
	file_path = program_binaries_path + "/" + computer_prog_package_name + "/scripts/pgsql_shards.json";

  writer.omitEndingLineFeed();
  cmd = writer.write(shards_json);
  save_file(file_path, cmd.c_str());

	/////////////////////////////////////////////////////////
	//create meta json
	std::vector<Tpye_Ip_Port_User_Pwd> meta;
	if(!System::get_instance()->get_meta_info(meta)) {
		job_error_info = "get_meta_info error";
		goto end;
	}

	for(auto &ip_port_user_pwd: meta)	{
		Json::Value meta_json_sub;
    meta_json_sub["ip"] = std::get<0>(ip_port_user_pwd);
    meta_json_sub["port"] = std::get<1>(ip_port_user_pwd);
    meta_json_sub["user"] = std::get<2>(ip_port_user_pwd);
    meta_json_sub["password"] = std::get<3>(ip_port_user_pwd);
    meta_json.append(meta_json_sub);
	}

	/////////////////////////////////////////////////////////
	// save json file to cmd_path
	file_path = program_binaries_path + "/" + computer_prog_package_name + "/scripts/pgsql_meta.json";

  writer.omitEndingLineFeed();
  cmd = writer.write(meta_json);
  save_file(file_path, cmd.c_str());

  /////////////////////////////////////////////////////////
  //add roll back to meta
  roll_back["job_type"] = "start_shard";
  roll_back["cluster_name"] = cluster_name;
  for(int i=0; i<vec_shard_name.size(); i++) {
    roll_back["shard_name" + std::to_string(i)] = vec_shard_name[i];
  }
  insert_roll_back_record(roll_back);

	/////////////////////////////////////////////////////////
	// start shards cmd
	cmd = "cd " + program_binaries_path + "/" + computer_prog_package_name + "/scripts/;";
	cmd += "python2 add_shards.py --config ./pgsql_shards.json --meta_config ./pgsql_meta.json --cluster_name ";
	cmd += cluster_name + " --ha_mode " + std::get<0>(cluster_info);
	syslog(Logger::INFO, "startShardsInfo cmd %s", cmd.c_str());

	pfd = popen(cmd.c_str(), "r");
	if(!pfd) {
		job_error_info = "startShardsInfo start cmd error";
		goto end;
	}
	while(fgets(buf, 256, pfd)!=NULL)	{
		//if(strcasestr(buf, "error") != NULL)
			syslog(Logger::INFO, "%s", buf);
	}
	pclose(pfd);

	/////////////////////////////////////////////////////////////
	// check cluster_name by meta
	retry = thread_work_interval * 30;
	while(retry-->0) {
		sleep(1);
		bool all_shard_start = true;
		for(auto &shard_name:vec_shard_name) {
			if(!System::get_instance()->check_cluster_shard_name(cluster_name, shard_name)) {
				all_shard_start = false;
				break;
			}
		}

		if(all_shard_start)
			break;
	}

	if(retry<0)	{
		job_error_info = "shard start error";
		goto end;
	}

  missiom_finish = true;
  job_status = "done";
  job_error_info = "";
	for(auto &shard_name: vec_shard_name)
	{
		if(job_error_info.length()>0)
			job_error_info += ";";
		job_error_info += shard_name;
	}
  syslog(Logger::INFO, "add shards successfully: %s", job_error_info.c_str());
  job_error_info = "add shards successfully";
  System::get_instance()->get_cluster_shard_info(cluster_name, vec_shard_name, job_memo_json);
  update_operation_record();
  delete_roll_back_record();
  return;

end:
  syslog(Logger::ERROR, "%s", job_error_info.c_str());
  update_operation_record();
  roll_back_record();
}

void ClusterMission::addCompsInfo() {

	int comps_id_seq = 0;
  int comps_id = 0;

  syslog(Logger::INFO, "addCompsInfo start");

  //////////////////////////////////////////////////////////////////////////////
	System::get_instance()->get_comp_nodes_id_seq(comps_id_seq);
	syslog(Logger::INFO, "comps_id_seq=%d", comps_id_seq);
	comps_id_seq += 1;
  if(comps_id_seq <= g_comps_id_seq)
    comps_id_seq = g_comps_id_seq + 1;
  g_comps_id_seq = comps_id_seq + add_comps;

	// get max index for add
	System::get_instance()->get_max_comp_name_id(cluster_name, comps_id);
	syslog(Logger::INFO, "comps_id=%d", comps_id);
	comps_id += 1;

	///////////////////////////////////////////////////////////////////////////////
	// get computer 
	if(!Machine_info::get_instance()->get_computer_nodes(add_comps, nodes_select, vec_comps_ip_port_paths, vec_machine)) {
		job_error_info = "Machine_info, no available machine";
		goto end;
	}
	for(int i=0; i<add_comps; i++)
		vec_comp_name.emplace_back("comp"+std::to_string(comps_id + i));

	///////////////////////////////////////////////////////////////////////////////
	// create computers
	create_comps(vec_comps_ip_port_paths, vec_comp_name, comps_id_seq);

  task_step = INSTALL_COMPUTER;
  task_wait = add_comps;
  task_incomplete = task_wait;

  job_status = "ongoing";
	job_error_info = "install computer start";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();
  return;

end:
  syslog(Logger::ERROR, "%s", job_error_info.c_str());
  update_operation_record();
  roll_back_record();
}

void ClusterMission::startCompsInfo() {

	FILE* pfd;
	char buf[256];

	int retry;
	std::string cmd,file_path;

	Json::Value shards_json;
	Json::Value meta_json;
  Json::Value roll_back;
  Json::FastWriter writer;

  syslog(Logger::INFO, "startCompsInfo start");

	/////////////////////////////////////////////////////////
	//create comps json
  file_path = program_binaries_path + "/" + computer_prog_package_name + "/scripts/pgsql_comps.json";
  save_file(file_path, comps_json.c_str());

	/////////////////////////////////////////////////////////
	//create meta json
	std::vector<Tpye_Ip_Port_User_Pwd> meta;
	if(!System::get_instance()->get_meta_info(meta)) {
		job_error_info = "get_meta_info error";
		goto end;
	}

	for(auto &ip_port_user_pwd: meta)	{
		Json::Value meta_json_sub;
    meta_json_sub["ip"] = std::get<0>(ip_port_user_pwd);
    meta_json_sub["port"] = std::get<1>(ip_port_user_pwd);
    meta_json_sub["user"] = std::get<2>(ip_port_user_pwd);
    meta_json_sub["password"] = std::get<3>(ip_port_user_pwd);
    meta_json.append(meta_json_sub);
	}

	/////////////////////////////////////////////////////////
	// save json file to cmd_path
	file_path = program_binaries_path + "/" + computer_prog_package_name + "/scripts/pgsql_meta.json";

  writer.omitEndingLineFeed();
  cmd = writer.write(meta_json);
  save_file(file_path, cmd.c_str());

  /////////////////////////////////////////////////////////
  //add roll back to meta
  roll_back["job_type"] = "start_comp";
  roll_back["cluster_name"] = cluster_name;
  for(int i=0; i<vec_comp_name.size(); i++) {
    roll_back["shard_name" + std::to_string(i)] = vec_comp_name[i];
  }
  insert_roll_back_record(roll_back);

	/////////////////////////////////////////////////////////
	// start shards cmd
	cmd = "cd " + program_binaries_path + "/" + computer_prog_package_name + "/scripts/;";
	cmd += "python2 add_comp_nodes.py --config ./pgsql_comps.json --meta_config ./pgsql_meta.json --cluster_name ";
	cmd += cluster_name + " --ha_mode " + std::get<0>(cluster_info);
	syslog(Logger::INFO, "startCompsInfo cmd %s", cmd.c_str());

	pfd = popen(cmd.c_str(), "r");
	if(!pfd) {
		job_error_info = "startCompsInfo start cmd error";
		goto end;
	}
	while(fgets(buf, 256, pfd)!=NULL)	{
		//if(strcasestr(buf, "error") != NULL)
			syslog(Logger::INFO, "%s", buf);
	}
	pclose(pfd);

	/////////////////////////////////////////////////////////////
	// check cluster_name by meta
	retry = thread_work_interval * 30;
	while(retry-->0) {
		sleep(1);
		bool all_comp_start = true;
		for(auto &comp_name:vec_comp_name) {
			if(!System::get_instance()->check_cluster_comp_name(cluster_name, comp_name))	{
				all_comp_start = false;
				break;
			}
		}

		if(all_comp_start)
			break;
	}

	if(retry<0)	{
		job_error_info = "comps start error";
		goto end;
	}

  missiom_finish = true;
  job_status = "done";
	job_error_info = "";
	for(auto &comp_name: vec_comp_name)
	{
		if(job_error_info.length()>0)
			job_error_info += ";";
		job_error_info += comp_name;
	}
  syslog(Logger::INFO, "add comps successfully: %s", job_error_info.c_str());
  job_error_info = "add comps successfully";
  System::get_instance()->get_cluster_comp_info(cluster_name, vec_comp_name, job_memo_json);
  update_operation_record();
  delete_roll_back_record();
  return;

end:
  syslog(Logger::ERROR, "%s", job_error_info.c_str());
  update_operation_record();
  roll_back_record();
}

void ClusterMission::create_shard(std::vector<Tpye_Ip_Port_Paths> &storages, std::string &shard_name) {

  std::string strtmp,shard_uuid,shard_json;
	get_uuid(shard_uuid);

	/////////////////////////////////////////////////////////
	// create json parameter
	Json::Value paras;
  paras["cluster_name"] = cluster_name;
  paras["shard_name"] = shard_name;
  paras["ha_mode"] = std::get<0>(cluster_info);
  paras["innodb_buffer_pool_size"] = std::get<7>(cluster_info);
  paras["group_uuid"] = shard_uuid;

  Json::Value nodes;
	for(int i=0; i<storages.size(); i++) {
		Json::Value nodes_list;
		
		if(i == 0)
      nodes_list["is_primary"] = true;
		else
			nodes_list["is_primary"] = false;
    nodes_list["ip"] = std::get<0>(storages[i]);
    nodes_list["port"] = std::get<1>(storages[i]);
    nodes_list["xport"] = std::get<1>(storages[i])+1;
    nodes_list["mgr_port"] = std::get<1>(storages[i])+2;
		strtmp = std::get<2>(storages[i])[0] + "/instance_data/data_dir_path/" 
					    + std::to_string(std::get<1>(storages[i]));
    nodes_list["data_dir_path"] = strtmp;
		strtmp = std::get<2>(storages[i])[1] + "/instance_data/log_dir_path/" 
					    + std::to_string(std::get<1>(storages[i]));
    nodes_list["log_dir_path"] = strtmp;
		strtmp = std::get<2>(storages[i])[2] + "/instance_data/innodb_log_dir_path/" 
					    + std::to_string(std::get<1>(storages[i]));
    nodes_list["innodb_log_dir_path"] = strtmp;
    strtmp = std::to_string(std::get<7>(cluster_info)) + "GB";
    nodes_list["innodb_buffer_pool_size"] = strtmp;
    nodes_list["user"] = user_name;
    nodes_list["election_weight"] = 50;
    nodes.append(nodes_list);
	}
  paras["nodes"] = nodes;

  //////////////////////////////////////////////////////
  // save shard_json
  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  shard_json = writer.write(paras);
  vec_shard_json.emplace_back(shard_json);
  //syslog(Logger::ERROR, "shard_json=%s", shard_json.c_str());

	/////////////////////////////////////////////////////////
	// create every storage
	for(int i=0; i<storages.size(); i++) {
    paras["install_id"] = i;
		create_storage(storages[i], paras);
	}
}

void ClusterMission::create_shard_nodes(std::vector<Tpye_Ip_Port_Paths> &storages, std::string &shard_name) {

  std::string strtmp,shard_uuid,shard_json;

	/////////////////////////////////////////////////////////
	// get shard_uuid
  strtmp = "group_replication_group_name";
	if(!System::get_instance()->get_cluster_shard_variable(cluster_name, shard_name, strtmp, shard_uuid))	{
		job_error_info = "get_cluster_shard_variable error";
    syslog(Logger::ERROR, "%s", job_error_info.c_str());
    return;
	}

	/////////////////////////////////////////////////////////
	// create json parameter
	Json::Value paras;
  paras["cluster_name"] = cluster_name;
  paras["shard_name"] = shard_name;
  paras["ha_mode"] = std::get<0>(cluster_info);
  paras["innodb_buffer_pool_size"] = std::get<7>(cluster_info);
  paras["group_uuid"] = shard_uuid;

  Json::Value nodes;
	for(int i=0; i<storages.size(); i++) {
		Json::Value nodes_list;
		
		nodes_list["is_primary"] = false;
    nodes_list["ip"] = std::get<0>(storages[i]);
    nodes_list["port"] = std::get<1>(storages[i]);
    nodes_list["xport"] = std::get<1>(storages[i])+1;
    nodes_list["mgr_port"] = std::get<1>(storages[i])+2;
		strtmp = std::get<2>(storages[i])[0] + "/instance_data/data_dir_path/" 
					    + std::to_string(std::get<1>(storages[i]));
    nodes_list["data_dir_path"] = strtmp;
		strtmp = std::get<2>(storages[i])[1] + "/instance_data/log_dir_path/" 
					    + std::to_string(std::get<1>(storages[i]));
    nodes_list["log_dir_path"] = strtmp;
		strtmp = std::get<2>(storages[i])[2] + "/instance_data/innodb_log_dir_path/" 
					    + std::to_string(std::get<1>(storages[i]));
    nodes_list["innodb_log_dir_path"] = strtmp;
    strtmp = std::to_string(std::get<7>(cluster_info)) + "GB";
    nodes_list["innodb_buffer_pool_size"] = strtmp;
    nodes_list["user"] = user_name;
    nodes_list["election_weight"] = 50;
    nodes.append(nodes_list);
	}
  paras["nodes"] = nodes;

  //////////////////////////////////////////////////////
  // save shard_json
  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  shard_json = writer.write(paras);
  vec_shard_json.emplace_back(shard_json);
  //syslog(Logger::ERROR, "shard_json=%s", shard_json.c_str());

	/////////////////////////////////////////////////////////
	// create every storage
	for(int i=0; i<storages.size(); i++) {
    paras["install_id"] = i;
		create_storage(storages[i], paras);
	}
}

void ClusterMission::create_storage(Tpye_Ip_Port_Paths &storage, Json::Value &para) {

  Json::Value root_node,roll_back;

  //get a empty task;
  bool bGetTask = false;
  ClusterRemoteTask *cluster_task;
  auto &task_vec = get_task_manager()->get_remote_task_vec();
  auto iter = task_vec.begin();
  for (; iter != task_vec.end(); iter++) {
    cluster_task = static_cast<ClusterRemoteTask *>(*iter);
    if(cluster_task->getStatus() == 0){
      cluster_task->setStatus(1);
      bGetTask = true;
      break;
    }
  }

  if(!bGetTask){
    syslog(Logger::ERROR, "no task to install storage");
    return;
  }

  //update task info to run
  cluster_task->AddNodeSubChannel(
      std::get<0>(storage).c_str(),
      g_node_channel_manager.getNodeChannel(std::get<0>(storage).c_str()));

  root_node["cluster_mgr_request_id"] = job_id;
  root_node["task_spec_info"] = cluster_task->get_task_spec_info();
  root_node["job_type"] = "install_storage";
  root_node["paras"] = para;

  cluster_task->SetPara(std::get<0>(storage).c_str(), root_node);

  //add roll back to meta
  roll_back["job_type"] = "create_storage";
  roll_back["ip"] = std::get<0>(storage);
  roll_back["port"] = std::get<1>(storage);
  insert_roll_back_record(roll_back);
}

void ClusterMission::create_comps(std::vector<Tpye_Ip_Port_Paths> &comps, 
                            std::vector<std::string> &vec_comp_name, int comps_id) {

	std::string strtmp;

	/////////////////////////////////////////////////////////
	// create json parameter
	Json::Value paras;
  Json::Value nodes;

	for(int i=0; i<comps.size(); i++) {
		Json::Value nodes_list;
		
		nodes_list["id"] = comps_id+i;
    nodes_list["name"] = vec_comp_name[i];
    nodes_list["ip"] = std::get<0>(comps[i]);
    nodes_list["port"] = std::get<1>(comps[i]);
    nodes_list["user"] = "abc";
    nodes_list["password"] = "abc";
		strtmp = std::get<2>(comps[i])[0] + "/instance_data/comp_datadir/" 
					+ std::to_string(std::get<1>(comps[i]));
    nodes_list["datadir"] = strtmp;
    nodes.append(nodes_list);
	}
  paras["nodes"] = nodes;

  //////////////////////////////////////////////////////
  // save comp_json
  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  comps_json = writer.write(nodes);
  //syslog(Logger::ERROR, "comp_json=%s", comp_json.c_str());

	/////////////////////////////////////////////////////////
	// create every computer
	for(int i=0; i<comps.size(); i++)
	{
    paras["install_id"] = i;
		create_computer(comps[i], paras);
	}
}

void ClusterMission::create_computer(Tpye_Ip_Port_Paths &comp, Json::Value &para) {

  Json::Value root_node,roll_back;

  //get a empty task;
  bool bGetTask = false;
  ClusterRemoteTask *cluster_task;
  auto &task_vec = get_task_manager()->get_remote_task_vec();
  auto iter = task_vec.begin();
  for (; iter != task_vec.end(); iter++) {
    cluster_task = static_cast<ClusterRemoteTask *>(*iter);
    if(cluster_task->getStatus() == 0){
      cluster_task->setStatus(1);
      bGetTask = true;
      break;
    }
  }

  if(!bGetTask){
    syslog(Logger::ERROR, "no task to install computer");
    return;
  }

  //update task info to run
  cluster_task->AddNodeSubChannel(
      std::get<0>(comp).c_str(),
      g_node_channel_manager.getNodeChannel(std::get<0>(comp).c_str()));

  root_node["cluster_mgr_request_id"] = job_id;
  root_node["task_spec_info"] = cluster_task->get_task_spec_info();
  root_node["job_type"] = "install_computer";
  root_node["paras"] = para;

  cluster_task->SetPara(std::get<0>(comp).c_str(), root_node);

  //add roll back to meta
  roll_back["job_type"] = "create_computer";
  roll_back["ip"] = std::get<0>(comp);
  roll_back["port"] = std::get<1>(comp);
  insert_roll_back_record(roll_back);
}

void ClusterMission::delete_cluster(std::string &cluster_name) {
	std::vector <std::vector<Tpye_Ip_Port>> vec_shard_storage_ip_port;
	std::vector<Tpye_Ip_Port> vec_comps_ip_port;
  int storage_count = 0;

	/////////////////////////////////////////////////////////
	// get shards_ip_port by cluster_name
	if(!System::get_instance()->get_shards_ip_port(cluster_name, vec_shard_storage_ip_port)) {
		syslog(Logger::ERROR, "get_shards_ip_port error");
	}
	for(auto &storages: vec_shard_storage_ip_port)
		for(auto &storage: storages)
      storage_count++;

	/////////////////////////////////////////////////////////
	// get comps_ip_port by cluster_name
	if(!System::get_instance()->get_comps_ip_port(cluster_name, vec_comps_ip_port))	{
		syslog(Logger::ERROR, "get_comps_ip_port error");
	}

	/////////////////////////////////////////////////////////
  //prepare task
  // to delete instance
  task_num = storage_count + vec_comps_ip_port.size(); 

  for(int i=0; i<task_num; i++) {
    std::string task_name = "cluster_task_" + std::to_string(i);
    ClusterRemoteTask *cluster_task =
        new ClusterRemoteTask(task_name.c_str(), get_request_unique_id().c_str(), this);
    get_task_manager()->PushBackTask(cluster_task);
  }

	/////////////////////////////////////////////////////////
	// delete comps from every node
	for(auto &computer: vec_comps_ip_port)
	  delete_computer(computer);

	/////////////////////////////////////////////////////////
	// delete storages from every node
	for(auto &storages: vec_shard_storage_ip_port)
		for(auto &storage: storages)
			delete_storage(storage);

  task_step = DELETE_INSTANCE;
  task_wait = storage_count + vec_comps_ip_port.size();
  task_incomplete = task_wait;

  syslog(Logger::INFO, "delete_cluster task start");
}

void ClusterMission::delete_storage(Tpye_Ip_Port &storage) {

  Json::Value root_node,para;

  //get a empty task;
  bool bGetTask = false;
  ClusterRemoteTask *cluster_task;
  auto &task_vec = get_task_manager()->get_remote_task_vec();
  auto iter = task_vec.begin();
  for (; iter != task_vec.end(); iter++) {
    cluster_task = static_cast<ClusterRemoteTask *>(*iter);
    if(cluster_task->getStatus() == 0){
      cluster_task->setStatus(1);
      bGetTask = true;
      break;
    }
  }

  if(!bGetTask){
    syslog(Logger::ERROR, "no task to delete storage");
    return;
  }

  //update task info to run
  cluster_task->AddNodeSubChannel(
      storage.first.c_str(),
      g_node_channel_manager.getNodeChannel(storage.first.c_str()));

  root_node["cluster_mgr_request_id"] = job_id;
  root_node["task_spec_info"] = cluster_task->get_task_spec_info();
  root_node["job_type"] = "delete_storage";

  para["ip"] = storage.first;
  para["port"] = storage.second;
  root_node["paras"] = para;

  cluster_task->SetPara(storage.first.c_str(), root_node);
}

void ClusterMission::delete_computer(Tpye_Ip_Port &computer) {

  Json::Value root_node,para;

  //get a empty task;
  bool bGetTask = false;
  ClusterRemoteTask *cluster_task;
  auto &task_vec = get_task_manager()->get_remote_task_vec();
  auto iter = task_vec.begin();
  for (; iter != task_vec.end(); iter++) {
    cluster_task = static_cast<ClusterRemoteTask *>(*iter);
    if(cluster_task->getStatus() == 0){
      cluster_task->setStatus(1);
      bGetTask = true;
      break;
    }
  }

  if(!bGetTask){
    syslog(Logger::ERROR, "no task to delete computer");
    return;
  }

  //update task info to run
  cluster_task->AddNodeSubChannel(
      computer.first.c_str(),
      g_node_channel_manager.getNodeChannel(computer.first.c_str()));

  root_node["cluster_mgr_request_id"] = job_id;
  root_node["task_spec_info"] = cluster_task->get_task_spec_info();
  root_node["job_type"] = "delete_computer";

  para["ip"] = computer.first;
  para["port"] = computer.second;
  root_node["paras"] = para;

  cluster_task->SetPara(computer.first.c_str(), root_node);
}

void ClusterMission::stop_shard_node(Tpye_Ip_Port &ip_port){

  std::string strtmp;
  std::string group_seeds;
  std::size_t pos,len;

  // stop node
  if(!System::get_instance()->stop_cluster_shard_node(cluster_name, shard_name, ip_port))
    syslog(Logger::ERROR, "stop_cluster_shard_node error");

  // get old group_seeds
  strtmp = "group_replication_group_seeds";
  if(!System::get_instance()->get_cluster_shard_variable(cluster_name, shard_name, strtmp, group_seeds)) {
    syslog(Logger::ERROR, "get_cluster_shard_variable error");
    return;
  }

  // remove ip,prot from group_seeds
  strtmp = ip_port.first + ":" + std::to_string(ip_port.second+2);
  pos = group_seeds.find(strtmp);
  len = strtmp.length();
  if(pos == -1)
    return;

  //find ","
  if(pos != 0){
    if(group_seeds.at(pos-1) == ','){
      pos = pos-1;
      len++;
    }
  }else{
    if(group_seeds.at(pos+len+1) == ','){
      len++;
    }
  }

  strtmp = group_seeds.substr(pos+len+1);
  group_seeds = group_seeds.substr(0, pos) + strtmp;

  // update group_seeds to every node
  if(!System::get_instance()->update_shard_group_seeds(cluster_name, shard_name, group_seeds)) {
    syslog(Logger::ERROR, "update_shard_group_seeds error");
  }
}

void ClusterMission::stop_cluster() {

	/////////////////////////////////////////////////////////
	// delete cluster info from meta talbes
	if(!System::get_instance()->stop_cluster(cluster_name))	{
		syslog(Logger::ERROR, "stop_cluster error");
	}

  missiom_finish = true;
  job_status = "done";
  job_error_info = "delete cluster successfully";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();
  System::get_instance()->set_cluster_mgr_working(true);
}

void ClusterMission::stop_shard(){
	/////////////////////////////////////////////////////////
	// delete shard info from meta talbes
	if(!System::get_instance()->stop_cluster_shard(cluster_name, shard_name))	{
		syslog(Logger::ERROR, "stop_cluster_shard error");
	}

  missiom_finish = true;
  job_status = "done";
  job_error_info = "delete shard successfully";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();
  System::get_instance()->set_cluster_mgr_working(true);
}

void ClusterMission::stop_comp(){
	/////////////////////////////////////////////////////////
	// delete comp info from meta talbes
	if(!System::get_instance()->stop_cluster_comp(cluster_name, comp_name))	{
		syslog(Logger::ERROR, "stop_cluster_comp error");
	}

  missiom_finish = true;
  job_status = "done";
  job_error_info = "delete comp successfully";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();
  System::get_instance()->set_cluster_mgr_working(true);
}

void ClusterMission::update_backup_cluster() {
  std::string str_sql;

  syslog(Logger::INFO, "update_backup_cluster");

  job_status = "failed";
  if(task_incomplete){
    job_error_info = "backup cluster error";
    goto end;
  }

  get_datatime(timestamp);
  str_sql = "INSERT INTO cluster_backups(storage_id,cluster_id,backup_type,has_comp_node_dump,start_ts,end_ts,name) VALUES(";
  str_sql += backup_storage_id + "," + cluster_id + ",'storage_shards',0,'" + start_time + "','" + timestamp + "','" + shard_names + "')";
  syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

  if(System::get_instance()->execute_metadate_opertation(SQLCOM_INSERT, str_sql))	{
    job_error_info = "insert cluster_backups error";
    goto end;
  }

  missiom_finish = true;
  job_status = "done";
  syslog(Logger::INFO, "backup cluster successfully: %s", timestamp.c_str());
  job_error_info = "backup cluster successfully";
  job_memo_json["timestamp"] = timestamp;
  update_operation_record();
  return;

end:
  missiom_finish = true;
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();
}

void ClusterMission::update_backup_nodes(){
  std::string str_sql;

  syslog(Logger::INFO, "update_backup_nodes");

  job_status = "failed";
  if(task_incomplete){
    job_error_info = "backup nodes error";
    goto end;
  }

  get_datatime(timestamp);
  if(all_shard){
    str_sql = "INSERT INTO cluster_backups(storage_id,cluster_id,backup_type,has_comp_node_dump,start_ts,end_ts,name) VALUES(";
    str_sql += backup_storage_id + "," + cluster_id + ",'storage_shards',0,'" + start_time + "','" + timestamp + "','" + shard_names + "')";
    syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

    if(System::get_instance()->execute_metadate_opertation(SQLCOM_INSERT, str_sql))	{
      job_error_info = "insert cluster_backups error";
      goto end;
    }
  }

	for(int i=0; i<vec_shard_name.size(); i++) {
		std::vector<Tpye_Ip_Port_Paths> vec_storage_ip_port_paths;
		//get storage ip and port
		if(!Machine_info::get_instance()->get_storage_nodes(add_nodes, nodes_select, vec_storage_ip_port_paths, vec_machine))	{
			job_error_info = "Machine_info, no available machine";
			goto end;
		}
		vec_shard_storage_ip_port_paths.emplace_back(vec_storage_ip_port_paths);
  }

	///////////////////////////////////////////////////////////////////////////////
	// create storage of shard
	for(int i=0; i<vec_shard_name.size(); i++)
	  create_shard_nodes(vec_shard_storage_ip_port_paths[i], vec_shard_name[i]);

  task_step = INSTALL_STORAGE;
  task_wait = vec_shard_name.size()*add_nodes;
  task_incomplete = task_wait;

	job_error_info = "install storage start";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();
  return;

end:
  missiom_finish = true;
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();
}

void ClusterMission::backup_cluster() {

  std::vector<Tpye_Shard_Id_Ip_Port_Id> vec_shard_id_ip_port_id;
	get_datatime(start_time);

	/////////////////////////////////////////////////////////
	// get one node from erver shard
	if(!System::get_instance()->get_shard_info_for_backup(backup_cluster_name, cluster_id, vec_shard_id_ip_port_id)){
		job_error_info = "get_shard_info_for_backup error";
		goto end;
	}

	///////////////////////////////////////////////////////////////////////////////
	// backup every shard
	for(auto &shard_id_ip_port_id: vec_shard_id_ip_port_id)	{
		if(!backup_shard_node(cluster_id, shard_id_ip_port_id))	{
			job_error_info = "backup_shard_node error";
			goto end;
		}

		if(shard_names.length()>0)
			shard_names += ";";
		shard_names += std::get<0>(shard_id_ip_port_id);
	}

  task_step = BACKUP_STORAGE;
  task_wait = vec_shard_id_ip_port_id.size();
  task_incomplete = task_wait;
  return;

end:
  missiom_finish = true;
  job_status = "failed";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();
}

void ClusterMission::backup_nodes() {

  std::vector<Tpye_Shard_Id_Ip_Port_Id> vec_shard_id_ip_port_id;
	get_datatime(start_time);

	if(all_shard)	{
    job_error_info = "backup cluster working";
    // get one node from erver shard
    if(!System::get_instance()->get_shard_info_for_backup(cluster_name, cluster_id, vec_shard_id_ip_port_id)){
      job_error_info = "get_shard_info_for_backup error";
      goto end;
    }
	}	else {
    Tpye_Shard_Id_Ip_Port_Id shard_id_ip_port_id;
		job_error_info = "backup shard working";
    // get one node from a shard
		if(!System::get_instance()->get_node_info_for_backup(cluster_name, shard_name, cluster_id, shard_id_ip_port_id)) {
      job_error_info = "get_node_info_for_backup error";
      goto end;
    }
    vec_shard_id_ip_port_id.emplace_back(shard_id_ip_port_id);
	}
  syslog(Logger::INFO, "%s", job_error_info.c_str());

	///////////////////////////////////////////////////////////////////////////////
	// backup every shard
	for(auto &shard_id_ip_port_id: vec_shard_id_ip_port_id)	{
		if(!backup_shard_node(cluster_id, shard_id_ip_port_id))	{
			job_error_info = "backup_shard_node error";
			goto end;
		}

		if(shard_names.length()>0)
			shard_names += ";";
		shard_names += std::get<0>(shard_id_ip_port_id);
	}

  task_step = BACKUP_STORAGE;
  task_wait = vec_shard_id_ip_port_id.size();
  task_incomplete = task_wait;
  return;

end:
  missiom_finish = true;
  job_status = "failed";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();
}

bool ClusterMission::backup_shard_node(std::string &cluster_id, Tpye_Shard_Id_Ip_Port_Id &shard_id_ip_port_id) {

  ClusterRemoteTask *cluster_task;
  Json::Value root_node;
  Json::Value paras_node;
  std::string str_sql;
  bool bGetTask = false;

  //get a empty task;
  auto &task_vec = get_task_manager()->get_remote_task_vec();
  auto iter = task_vec.begin();
  for (; iter != task_vec.end(); iter++) {
    cluster_task = static_cast<ClusterRemoteTask *>(*iter);
    if(cluster_task->getStatus() == 0){
      cluster_task->setStatus(1);
      bGetTask = true;
      break;
    }
  }

  if(!bGetTask){
    syslog(Logger::ERROR, "error, no task to backup node");
    return false;
  }

  //update task info to run
  cluster_task->AddNodeSubChannel(
      std::get<2>(shard_id_ip_port_id).c_str(),
      g_node_channel_manager.getNodeChannel(std::get<2>(shard_id_ip_port_id).c_str()));

  root_node["cluster_mgr_request_id"] = job_id;
  root_node["task_spec_info"] = cluster_task->get_task_spec_info();
  root_node["job_type"] = "backup_shard";

  paras_node["ip"] = std::get<2>(shard_id_ip_port_id);
  paras_node["port"] = std::get<3>(shard_id_ip_port_id);
  paras_node["cluster_name"] = backup_cluster_name;
  paras_node["shard_name"] = std::get<0>(shard_id_ip_port_id);
  paras_node["shard_id"] = std::get<1>(shard_id_ip_port_id);
  paras_node["backup_storage"] = backup_storage_str;
  root_node["paras"] = paras_node;

  cluster_task->SetPara(std::get<2>(shard_id_ip_port_id).c_str(), root_node);

	///////////////////////////////////////////////////////////////////////////////
	// insert metadata table
	str_sql = "INSERT INTO cluster_shard_backup_restore_log(storage_id,cluster_id,shard_id,shard_node_id,optype,status,when_started) VALUES(";
	str_sql += backup_storage_id + "," + cluster_id + "," + std::to_string(std::get<1>(shard_id_ip_port_id)) + "," + std::to_string(std::get<4>(shard_id_ip_port_id));
	str_sql += ",'backup','ongoing','" + start_time + "')";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_INSERT, str_sql))	{
		syslog(Logger::ERROR, "insert cluster_shard_backup_restore_log error");
		return false;
	}

  return true;
}

void ClusterMission::restoreCluster() {

  std::string shard_map, meta_str;
  Tpye_Ip_Port_User_Pwd meta_ip_port;

  job_error_info = "restoreCluster start";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();
	System::get_instance()->set_cluster_mgr_working(false);

	/////////////////////////////////////////////////////////
	// get all storage nodes from erver shard
	if(!System::get_instance()->get_shard_ip_port_restore(cluster_name, vec_vec_storage_ip_port))	{
		job_error_info = "get get_shard_ip_port_restore error";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// get all comps
	if(!System::get_instance()->get_comps_ip_port_restore(cluster_name, vec_computer_ip_port)) {
		job_error_info = "get comps_ip_port_restore error";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// get shard map
	if(!System::get_instance()->get_shard_map_for_restore(backup_cluster_name, cluster_name, shard_map))
	{
		job_error_info = "get_shard_map_for_restore error";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// get master meta
	if(!System::get_instance()->get_meta_master(meta_ip_port))
	{
		job_error_info = "get_meta_master error";
		goto end;
	}
  meta_str = "pgx:pgx_pwd@\\(" + std::get<0>(meta_ip_port) + ":" 
              + std::to_string(std::get<1>(meta_ip_port)) + "\\)/mysql";

	/////////////////////////////////////////////////////////
	// restore every shard
	for(int i=0; i<vec_vec_storage_ip_port.size(); i++) {
		job_error_info = "restore " + vec_backup_shard_name[i] + " working";
		syslog(Logger::INFO, "%s", job_error_info.c_str());

		for(auto &ip_port: vec_vec_storage_ip_port[i]) {
			syslog(Logger::INFO, "restore shard node working");
			if(!restore_storage(vec_backup_shard_name[i], ip_port)) {
				job_error_info = "job_restore_storage error";
				goto end;
			}
		}
	}

  /////////////////////////////////////////////////////////
	// restore every computer
	for(auto &ip_port: vec_computer_ip_port) {
		syslog(Logger::INFO, "restore computer node working");
		if(!restore_computer(shard_map, meta_str, ip_port)) {
			job_error_info = "job_restore_computer error";
			goto end;
		}
	}
  
  task_step = RESTORE_INSTANCE;
  task_wait = std::get<1>(cluster_info) * std::get<2>(cluster_info) + std::get<3>(cluster_info);
  task_incomplete = task_wait;
  return;

end:
  syslog(Logger::ERROR, "%s", job_error_info.c_str());
  update_operation_record();
  roll_back_record();
}

bool ClusterMission::restore_storage(std::string &shard_name, Tpye_Ip_Port &ip_port){

  Json::Value root_node;
  Json::Value paras_node;

  //get a empty task;
  bool bGetTask = false;
  ClusterRemoteTask *cluster_task;
  auto &task_vec = get_task_manager()->get_remote_task_vec();
  auto iter = task_vec.begin();
  for (; iter != task_vec.end(); iter++) {
    cluster_task = static_cast<ClusterRemoteTask *>(*iter);
    if(cluster_task->getStatus() == 0){
      cluster_task->setStatus(1);
      bGetTask = true;
      break;
    }
  }

  if(!bGetTask){
    syslog(Logger::ERROR, "no task to restore storage");
    return false;
  }

  //update task info to run
  cluster_task->AddNodeSubChannel(
      std::get<0>(ip_port).c_str(),
      g_node_channel_manager.getNodeChannel(std::get<0>(ip_port).c_str()));

  root_node["cluster_mgr_request_id"] = job_id;
  root_node["task_spec_info"] = cluster_task->get_task_spec_info();
  root_node["job_type"] = "restore_storage";

  paras_node["ip"] = std::get<0>(ip_port);
  paras_node["port"] = std::get<1>(ip_port);
  paras_node["cluster_name"] = backup_cluster_name;
  paras_node["shard_name"] = shard_name;
  paras_node["timestamp"] = timestamp;
  paras_node["backup_storage"] = backup_storage_str;
  root_node["paras"] = paras_node;

  cluster_task->SetPara(std::get<0>(ip_port).c_str(), root_node);

  return true;
}

bool ClusterMission::restore_computer(std::string &shard_map, std::string &meta_str, Tpye_Ip_Port &ip_port) {

  Json::Value root_node;
  Json::Value paras_node;

  //get a empty task;
  bool bGetTask = false;
  ClusterRemoteTask *cluster_task;
  auto &task_vec = get_task_manager()->get_remote_task_vec();
  auto iter = task_vec.begin();
  for (; iter != task_vec.end(); iter++) {
    cluster_task = static_cast<ClusterRemoteTask *>(*iter);
    if(cluster_task->getStatus() == 0){
      cluster_task->setStatus(1);
      bGetTask = true;
      break;
    }
  }

  if(!bGetTask){
    syslog(Logger::ERROR, "no task to restore computer");
    return false;
  }

  //update task info to run
  cluster_task->AddNodeSubChannel(
      std::get<0>(ip_port).c_str(),
      g_node_channel_manager.getNodeChannel(std::get<0>(ip_port).c_str()));

  root_node["cluster_mgr_request_id"] = job_id;
  root_node["task_spec_info"] = cluster_task->get_task_spec_info();
  root_node["job_type"] = "restore_computer";

  paras_node["ip"] = std::get<0>(ip_port);
  paras_node["port"] = std::get<1>(ip_port);
  paras_node["cluster_name"] = backup_cluster_name;
  paras_node["shard_map"] = shard_map;
  paras_node["meta_str"] = meta_str;
  root_node["paras"] = paras_node;

  cluster_task->SetPara(std::get<0>(ip_port).c_str(), root_node);

  return true;
}

void ClusterMission::updateRestoreInfo() {

	syslog(Logger::INFO, "updateRestoreInfo");

	// update every instance cluster info
	System::get_instance()->set_cluster_mgr_working(true);
	sleep(thread_work_interval * 6);  //wait cluster shard update
	int retry = thread_work_interval * 30;
	while(retry-->0) {
		sleep(1);
		if(System::get_instance()->update_instance_cluster_info(cluster_name))
			break;
	}

	if(retry<0) {
		job_error_info = "update_instance_cluster_info timeout";
		goto end;
	}

  missiom_finish = true;
	job_status = "done";
  syslog(Logger::INFO, "restore new cluster successfully: %s", cluster_name.c_str());
  job_error_info = "restore new cluster successfully";
  System::get_instance()->get_cluster_info(cluster_name, job_memo_json);
  update_operation_record();
  delete_roll_back_record();
  return;

end:
  syslog(Logger::ERROR, "%s", job_error_info.c_str());
  update_operation_record();
  roll_back_record();
}

void ClusterMission::restoreNodes() {
  
	for(int i=0; i<vec_shard_name.size(); i++) {
		for(auto &ip_port_paths: vec_shard_storage_ip_port_paths[i]) {
			Tpye_Ip_Port ip_port = std::make_pair(std::get<0>(ip_port_paths), std::get<1>(ip_port_paths));
			if(!restore_storage(vec_shard_name[i], ip_port)){
				job_error_info = "restore_storage error";
				goto end;
			}
		}
  }

  task_step = RESTORE_INSTANCE;
  task_wait = vec_shard_storage_ip_port_paths.size()*vec_shard_storage_ip_port_paths[0].size();
  task_incomplete = task_wait;

	job_error_info = "restore storage start";
  syslog(Logger::INFO, "%s", job_error_info.c_str());
  update_operation_record();
  return;

end:
  syslog(Logger::ERROR, "%s", job_error_info.c_str());
  update_operation_record();
  roll_back_record();
}

void ClusterMission::updateRestoreNodesInfo() {

	for(int i=0; i<vec_shard_name.size(); i++) {
    std::vector<Tpye_Ip_Port_User_Pwd> vec_ip_port_user_pwd;
    std::string strtmp, group_seeds;
    Json::Value roll_back;
    int retry;

    //add roll back to meta
    roll_back["job_type"] = "add_nodes";
    roll_back["cluster_name"] = cluster_name;
    roll_back["shard_name"] = vec_shard_name[i];
  	for(int j=0; j<vec_shard_storage_ip_port_paths[i].size(); j++) {
			strtmp = "ip" + std::to_string(j);
      roll_back[strtmp] = std::get<0>(vec_shard_storage_ip_port_paths[i][j]);
			strtmp = "port" + std::to_string(j);
      roll_back[strtmp] = std::get<1>(vec_shard_storage_ip_port_paths[i][j]);
		}
    insert_roll_back_record(roll_back);

    // get old group_seeds
    strtmp = "group_replication_group_seeds";
    if(!System::get_instance()->get_cluster_shard_variable(cluster_name, vec_shard_name[i], strtmp, group_seeds)) {
      job_error_info = "get_cluster_shard_variable error";
      goto end;
    }

    // add new nodes to group_seeds and shard
    for(auto &ip_port_paths: vec_shard_storage_ip_port_paths[i]){
      group_seeds += "," + std::get<0>(ip_port_paths) + ":" + std::to_string(std::get<1>(ip_port_paths)+2);
      vec_ip_port_user_pwd.emplace_back(std::make_tuple(std::get<0>(ip_port_paths), std::get<1>(ip_port_paths), "pgx", "pgx_pwd"));
    }

    // add new nodes to shard
    if(!System::get_instance()->add_shard_nodes(cluster_name, vec_shard_name[i], vec_ip_port_user_pwd)) {
      job_error_info = "job_add_shard_node error";
      goto end;
    }

    // check new nodes add to shard
    for(auto &ip_port_paths: vec_shard_storage_ip_port_paths[i]){
      Tpye_Ip_Port ip_port = std::make_pair(std::get<0>(ip_port_paths),std::get<1>(ip_port_paths));
      retry = thread_work_interval * 10;
      while(retry-->0){
        sleep(1); 
        if(System::get_instance()->check_cluster_shard_ip_port(cluster_name, vec_shard_name[i], ip_port))
          break;
      }

      if(retry<0) {
        job_error_info = "check_cluster_shard_ip_port failed";
        goto end;
      }
    }

	  // update group_seeds to every node
    if(!System::get_instance()->update_shard_group_seeds(cluster_name, vec_shard_name[i], group_seeds)) {
      job_error_info = "update_shard_group_seeds error";
      goto end;
    }
  }

  sleep(thread_work_interval * 3);  //wait cluster shard node update
  missiom_finish = true;
	job_status = "done";
	job_error_info = "";
	for(auto &storages: vec_shard_storage_ip_port_paths)
		for(auto &ip_port_paths: storages) {
			if(job_error_info.length()>0)
				job_error_info += ";";
			job_error_info += std::get<0>(ip_port_paths) + ":" + std::to_string(std::get<1>(ip_port_paths));
		}
  syslog(Logger::INFO, "add new nodes successfully: %s", job_error_info.c_str());
  job_error_info = "add new nodes successfully";
  System::get_instance()->get_cluster_shard_node_info(cluster_name, vec_shard_name, 
                                      vec_shard_storage_ip_port_paths, job_memo_json);
  update_operation_record();
  delete_roll_back_record();
  return;

end:
  syslog(Logger::ERROR, "%s", job_error_info.c_str());
  update_operation_record();
  roll_back_record();
}

bool ClusterMission::insert_roll_back_record(Json::Value &para) {

	std::string str_sql,roll_info;
  Json::FastWriter writer;

  writer.omitEndingLineFeed();
  roll_info = writer.write(para);

	str_sql = "INSERT INTO cluster_roll_back_record(job_id,roll_info) VALUES('"	+ job_id + "','" + roll_info + "')";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_INSERT, str_sql))	{
		syslog(Logger::ERROR, "insert roll_back_record error");
		return false;
	}

	return true;
}

bool ClusterMission::delete_roll_back_record()
{
	std::string str_sql;

	str_sql = "delete from cluster_roll_back_record where job_id='" + job_id + "'";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_DELETE, str_sql))	{
		syslog(Logger::ERROR, "delete roll_back_record error");
		return false;
	}

	return true;
}

bool ClusterMission::roll_back_record()
{
	std::vector<std::string> vec_roll_info;
	std::string job_type;

	if(!System::get_instance()->get_roll_info_from_metadata(job_id, vec_roll_info))	{
		syslog(Logger::ERROR, "get_roll_info_from_metadata error");
		return false;
	}

	////////////////////////////////////////////////////////////////////////////////
	//roll back every record
	for(auto &roll_info: vec_roll_info)	{
		syslog(Logger::INFO, "roll back info: %s", roll_info.c_str());

    Json::Value root;
    Json::Reader reader;
    reader.parse(roll_info,root);
		job_type = root["job_type"].asString();

		if(job_type == "create_storage") {
			std::string ip;
			int port;

      ip = root["ip"].asString();
      port = root["port"].asInt();

			Tpye_Ip_Port ip_port = std::make_pair(ip, port);
			delete_storage(ip_port);

		}	else if(job_type == "create_computer") {
			std::string ip;
			int port;

      ip = root["ip"].asString();
      port = root["port"].asInt();

			Tpye_Ip_Port ip_port = std::make_pair(ip, port);
			delete_computer(ip_port);

		}	else if(job_type == "start_cluster") {
			std::string cluster_name;

      cluster_name = root["cluster_name"].asString();

			if(!System::get_instance()->stop_cluster(cluster_name))
				syslog(Logger::ERROR, "stop_cluster error");
		}	else if(job_type == "start_shard") {
			std::string cluster_name;
			std::string shard_name;

			cluster_name = root["cluster_name"].asString();

			for(int i=0; ; i++)	{
				shard_name = root["shard_name" + std::to_string(i)].asString();

				if(!System::get_instance()->stop_cluster_shard(cluster_name, shard_name))
					syslog(Logger::ERROR, "stop_cluster_shard error");
			}
		} else if(job_type == "start_comp") {
			std::string cluster_name;
			std::string comp_name;

			cluster_name = root["cluster_name"].asString();

			for(int i=0; ; i++)	{
				comp_name = root["comp_name" + std::to_string(i)].asString();

				if(!System::get_instance()->stop_cluster_comp(cluster_name, comp_name))
					syslog(Logger::ERROR, "stop_cluster_comp error");
			}
		}	else if (job_type == "add_nodes") {
      std::string strtmp;
			std::string ip;
			int port;

			cluster_name = root["cluster_name"].asString();
      shard_name = root["shard_name"].asString();

			for(int i=0; ; i++) {
				strtmp = "ip" + std::to_string(i);
        if (!root.isMember(strtmp))
          break;
				ip = root[strtmp].asString();

				strtmp = "port" + std::to_string(i);
        if (!root.isMember(strtmp))
          break;
				port = root[strtmp].asInt();

        // stop node
				Tpye_Ip_Port ip_port = std::make_pair(ip, port);
				stop_shard_node(ip_port);
			}
		}
	}

	return true;
}

bool ClusterMission::system_cmd(std::string &cmd) {
  kunlun::BiodirectPopen *popen_p = new kunlun::BiodirectPopen(cmd.c_str());
  FILE *stderr_fp;

  if (!popen_p->Launch("rw")) {
    goto end;
  }
  stderr_fp = popen_p->getReadStdErrFp();
  char buf[256];
  if (fgets(buf, 256, stderr_fp) != nullptr) {
    syslog(Logger::ERROR, "Biopopen stderr: %s", buf);
    goto end;
  }

end:
  if (popen_p != nullptr)
    delete popen_p;

  return true;
}

void ClusterMission::get_uuid(std::string &uuid) {
	FILE *fp = fopen("/proc/sys/kernel/random/uuid", "rb");
	if (fp == NULL)	{
		syslog(Logger::ERROR, "open file uuid error");
		return;
	}

	char buf[60];
	memset(buf, 0, 60);
	size_t n = fread(buf, 1, 36, fp);
	fclose(fp);
	
	if(n != 36)
		return;
	
	uuid = buf;
}

void ClusterMission::get_timestamp(std::string &timestamp) {
	char sysTime[128];
	struct timespec ts = {0,0};

	clock_gettime(CLOCK_REALTIME, &ts);
	snprintf(sysTime, 128, "%lu", ts.tv_sec); 
	timestamp = sysTime;
}

void ClusterMission::get_datatime(std::string &datatime)
{
	char sysTime[128];
	struct timespec ts = {0,0};
	clock_gettime(CLOCK_REALTIME, &ts);

	struct tm *tm;
	tm = localtime(&ts.tv_sec);
		 
	snprintf(sysTime, 128, "%04u-%02u-%02u %02u:%02u:%02u", 
		tm->tm_year+1900, tm->tm_mon+1,	tm->tm_mday, 
		tm->tm_hour, tm->tm_min, tm->tm_sec); 

	datatime = sysTime;
}

bool ClusterMission::save_file(std::string &path, const char *buf) {
  FILE *pfd = fopen(path.c_str(), "wb");
  if (pfd == NULL) {
    syslog(Logger::ERROR, "Creat json file error %s", path.c_str());
    return false;
  }

  fwrite(buf, 1, strlen(buf), pfd);
  fclose(pfd);

  return true;
}

void ClusterMission::generate_cluster_name() {
	int cluster_id = 0;
	System::get_instance()->get_max_cluster_id(cluster_id);
	cluster_id += 1;
	if(cluster_id <= g_cluster_id)
		cluster_id = g_cluster_id+1;
	g_cluster_id = cluster_id;

	while(true)	{
		char buf[10];
		snprintf(buf, 10, "_%06d", cluster_id);
		get_timestamp(cluster_name);
		cluster_name = "cluster_" + cluster_name + buf;
		//check for no repeat
		if(!System::get_instance()->check_cluster_name(cluster_name))	{
			syslog(Logger::INFO, "cluster_name:%s", cluster_name.c_str());
			break;
		}
	}
}

bool ClusterMission::get_cluster_info(std::string &cluster_name) {

  Json::Value root;
  Json::Reader reader;
	std::string json_buf;
	int shards, nodes, comps;

	if(!System::get_instance()->get_cluster_info_from_metadata(cluster_name, json_buf))	{
		syslog(Logger::ERROR, "get_cluster_info_from_metadata error");
		return false;
	}

  if (!reader.parse(json_buf.c_str(), root)) {
    return false;
  }
	std::get<0>(cluster_info) = root["ha_mode"].asString();
  std::get<4>(cluster_info) = stoi(root["max_storage_size"].asString());
  std::get<5>(cluster_info) = stoi(root["max_connections"].asString());
  std::get<6>(cluster_info) = stoi(root["cpu_cores"].asString());
  std::get<7>(cluster_info) = stoi(root["innodb_size"].asString());

	//shards, nodes, comps, maybe add or remove after create
	//must get from current cluster
	if(!System::get_instance()->get_cluster_shards_nodes_comps(cluster_name, shards, nodes, comps))	{
		syslog(Logger::ERROR, "get_cluster_shards_nodes_comps error");
		return false;
	}

	std::get<1>(cluster_info) = shards;
	std::get<2>(cluster_info) = nodes;
	std::get<3>(cluster_info) = comps;

	return true;
}

bool ClusterMission::create_program_path() {
	std::string cmd, cmd_path, program_path;

	//unzip to program_binaries_path for install cmd
	//storage
	cmd_path = program_binaries_path + "/" + storage_prog_package_name + "/dba_tools";
	if(access(cmd_path.c_str(), F_OK) != 0)	{
		syslog(Logger::INFO, "unzip %s.tgz" , storage_prog_package_name.c_str());
		program_path = program_binaries_path + "/" + storage_prog_package_name + ".tgz";

		cmd = "tar zxf " + program_path + " -C " + program_binaries_path;
		if(!system_cmd(cmd))
			return false;
	}

	//computer
	cmd_path = program_binaries_path + "/" + computer_prog_package_name + "/scripts";
	if(access(cmd_path.c_str(), F_OK) != 0)	{
		syslog(Logger::INFO, "unzip %s.tgz" , computer_prog_package_name.c_str());
		program_path = program_binaries_path + "/" + computer_prog_package_name + ".tgz";

		cmd = "tar zxf " + program_path + " -C " + program_binaries_path;
		if(!system_cmd(cmd))
			return false;
	}

	return true;
}

void ClusterMission::get_user_name() {
	FILE* pfd;

	char *p;
	char buf[256];
	std::string str_cmd;

	str_cmd = "whoami";
	//syslog(Logger::INFO, "get_user_name str_cmd : %s",str_cmd.c_str());

	pfd = popen(str_cmd.c_str(), "r");
	if(!pfd)
		goto end;

	if(fgets(buf, 256, pfd) == NULL)
		goto end;
	
	p = strchr(buf, '\n');
	if(p == NULL)
		goto end;

	user_name = std::string(buf, p-buf);
	syslog(Logger::INFO, "current user=%s", user_name.c_str());

end:
	if(pfd != NULL)
		pclose(pfd);
}

bool ClusterMission::update_operation_record(){
	std::string str_sql,memo;
  Json::FastWriter writer;

  job_memo_json["error_code"] = job_error_code;
  job_memo_json["error_info"] = job_error_info;
  writer.omitEndingLineFeed();
  memo = writer.write(job_memo_json);

	str_sql = "UPDATE cluster_general_job_log set status='" + job_status + "',memo='" + memo;
	str_sql += "',when_ended=current_timestamp(6) where id=" + job_id;
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_UPDATE, str_sql)) {
		syslog(Logger::ERROR, "execute_metadate_opertation error");
		return false;
	}

	return true;
}

bool ClusterMission::ArrangeRemoteTask() {
  request_type = get_request_type();
  job_id = get_request_unique_id();
  nodes_select = 0;
  missiom_finish = false;
	job_status = "not_started";
	job_error_code = EintToStr(EOK);
  job_error_info = "";

  switch (request_type) {
  case kunlun::kRenameClusterType:
    renameCluster();
    break;
  case kunlun::kCreateClusterType:
    createCluster();
    break;
  case kunlun::kDeleteClusterType:
    deleteCluster();
    break;

  case kunlun::kAddShardsType:
    addShards();
    break;
  case kunlun::kDeleteShardType:
    // deleteShard(); // no support now
    break;

  case kunlun::kAddCompsType:
    addComps();
    break;
  case kunlun::kDeleteCompType:
    deleteComp();
    break;

  case kunlun::kAddNodesType:
    addNodes();
    break;
  case kunlun::kDeleteNodeType:
    deleteNode();
    break;

  case kunlun::kBackupClusterType:
    backupCluster();
    break;
  case kunlun::kRestoreNewClusterType:
    restoreNewCluster();
    break;

  default:
    break;
  }

  return true;
}