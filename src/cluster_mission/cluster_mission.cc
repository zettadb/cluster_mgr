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

void UpdateMachinePathSize(std::vector<Machine*> &vec_machine, std::string &response) {
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

void CheckInstanceInstall(ClusterMission *mission, std::string &response) {
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

  mission->task_incomplete--;
}

void CheckInstanceDelete(ClusterMission *mission, std::string &response) {
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

  mission->task_incomplete--;
}

void CreateClusterCallBack(ClusterMission *mission, std::string &response){
  syslog(Logger::INFO, "response=%s", response.c_str());
	std::string job_status;
	std::string job_memo;

  switch (mission->task_step) {
  case ClusterMission::CreateClusterStep::GET_PATH_SIZE:
    UpdateMachinePathSize(mission->vec_machine, response);
    mission->task_wait--;
    if(mission->task_wait == 0) {
      auto iter = mission->vec_machine.begin();
      for(; iter != mission->vec_machine.end(); ){
        if((*iter)->available){
          iter++;
        }else{
          iter = mission->vec_machine.erase(iter);
        }
      }

      if(mission->vec_machine.size() > 0){
        mission->createStorageInfo();
      }else{
        job_status = "failed";
        job_memo = "error, no available machine";
      }
    }
    return;

  case ClusterMission::CreateClusterStep::INSTALL_STORAGE:
    CheckInstanceInstall(mission, response);
    mission->task_wait--;
    if(mission->task_wait == 0) {
      syslog(Logger::INFO, "task_incomplete = %d", mission->task_incomplete);
      if(mission->task_incomplete == 0){
        mission->createComputerInfo();
      } else {
        job_status = "failed";
        job_memo = "install storage error";
        goto end;
      }
    }
    return;

  case ClusterMission::CreateClusterStep::INSTALL_COMPUTER:
    CheckInstanceInstall(mission, response);
    mission->task_wait--;
    if(mission->task_wait == 0) {
      syslog(Logger::INFO, "task_incomplete = %d", mission->task_incomplete);
      if(mission->task_incomplete == 0){
        mission->createClusterInfo();
      } else {
        job_status = "failed";
        job_memo = "install computer error";
        goto end;
      }
    }
    return;

  default:
    return;
  }

end:
  syslog(Logger::INFO, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(mission->job_id, job_status, job_memo);
}

void DeleteClusterCallBack(ClusterMission *mission, std::string &response){
  syslog(Logger::INFO, "response=%s", response.c_str());
	std::string job_status;
	std::string job_memo;

  switch (mission->task_step) {
  case ClusterMission::CreateClusterStep::DELETE_INSTANCE:
    CheckInstanceDelete(mission, response);
    mission->task_wait--;
    if(mission->task_wait == 0) {
      syslog(Logger::INFO, "task_incomplete = %d", mission->task_incomplete);
      mission->stop_cluster();
    }
    return;

  default:
    return;
  }

end:
  syslog(Logger::INFO, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(mission->job_id, job_status, job_memo);
}

void CLuster_Call_Back(void *cb_context) {
  ClusterRemoteTask *task = static_cast<ClusterRemoteTask *>(cb_context);
  std::string response = task->get_response()->SerializeResponseToStr();

  switch (task->getMission()->request_type) {
  case kunlun::kCreateClusterType:
    CreateClusterCallBack(task->getMission(), response);
    break;
  case kunlun::kDeleteClusterType:
    DeleteClusterCallBack(task->getMission(), response);
    break;

  default:
    break;
  }
}

bool ClusterMission::ArrangeRemoteTask() {
  request_type = get_request_type();
  job_id = get_request_unique_id();

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

  default:
    break;
  }

  return true;
}

void ClusterMission::renameCluster() {
	std::string job_status;
	std::string job_memo;

  if (!super::get_body_json_document().isMember("paras")) {
    setExtraErr("missing `paras` key-value pair in the request body");
    return;
  }
  Json::Value paras = super::get_body_json_document()["paras"];

  if (!paras.isMember("cluster_name")) {
    job_memo = "missing `cluster_name` key-value pair in the request body";
    goto end;
  }
  cluster_name = paras["cluster_name"].asString();

  if (!paras.isMember("nick_name")) {
    job_memo = "missing `nick_name` key-value pair in the request body";
    goto end;
  }
  nick_name = paras["nick_name"].asString();

	//////////////////////////////////////////////////////////
	if(System::get_instance()->check_nick_name(nick_name)) {
		job_memo = "new nick_name have existed";
		goto end;
	}

	//////////////////////////////////////////////////////////
	if(!System::get_instance()->rename_cluster(cluster_name, nick_name)) {
		job_memo = "rename cluster error";
		goto end;
	}

	job_status = "done";
	job_memo = "rename cluster succeed";
  syslog(Logger::ERROR, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id, job_status, job_memo);
  return;

end:
  job_status = "failed";
  syslog(Logger::ERROR, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id, job_status, job_memo);
}

void ClusterMission::createCluster() {
	std::string job_status;
	std::string job_memo;
	std::set<std::string> set_machine;

  if (!super::get_body_json_document().isMember("paras")) {
    setExtraErr("missing `paras` key-value pair in the request body");
    return;
  }
  Json::Value paras = super::get_body_json_document()["paras"];

  if (paras.isMember("nick_name")) {
    nick_name = paras["nick_name"].asString();
  }

  if (!paras.isMember("ha_mode")) {
    job_memo = "missing `ha_mode` key-value pair in the request body";
    goto end;
  }
  std::get<0>(cluster_info) = paras["ha_mode"].asString();

  if (!paras.isMember("shards")) {
    job_memo = "missing `shards` key-value pair in the request body";
    goto end;
  }
  std::get<1>(cluster_info) = stoi(paras["shards"].asString());
	if(std::get<1>(cluster_info)<1 || std::get<1>(cluster_info)>256) {
		job_memo = "shards error(must in 1-256)";
		goto end;
	}

  if (!paras.isMember("nodes")) {
    job_memo = "missing `nodes` key-value pair in the request body";
    goto end;
  }
  std::get<2>(cluster_info) = stoi(paras["nodes"].asString());
	if(std::get<2>(cluster_info)<1 || std::get<2>(cluster_info)>256) {
		job_memo = "nodes error(must in 1-256)";
		goto end;
	}

  if (!paras.isMember("comps")) {
    job_memo = "missing `comps` key-value pair in the request body";
    goto end;
  }
  std::get<3>(cluster_info) = stoi(paras["comps"].asString());
	if(std::get<3>(cluster_info)<1 || std::get<3>(cluster_info)>256) {
		job_memo = "comps error(must in 1-256)";
		goto end;
	}

  if (!paras.isMember("max_storage_size")) {
    job_memo = "missing `max_storage_size` key-value pair in the request body";
    goto end;
  }
  std::get<4>(cluster_info) = stoi(paras["max_storage_size"].asString());

  if (!paras.isMember("max_connections")) {
    job_memo = "missing `max_connections` key-value pair in the request body";
    goto end;
  }
  std::get<5>(cluster_info) = stoi(paras["max_connections"].asString());

  if (!paras.isMember("cpu_cores")) {
    job_memo = "missing `cpu_cores` key-value pair in the request body";
    goto end;
  }
  std::get<6>(cluster_info) = stoi(paras["cpu_cores"].asString());

  if (!paras.isMember("innodb_size")) {
    job_memo = "missing `innodb_size` key-value pair in the request body";
    goto end;
  }
  std::get<7>(cluster_info) = stoi(paras["innodb_size"].asString());
	if(std::get<7>(cluster_info)<1)	{
		job_memo = "innodb_size error(must > 0)";
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
			job_memo = "error, nodes=1 in no_rep mode";
			goto end;
		}
	}	else if(std::get<0>(cluster_info) == "mgr")	{
		if(std::get<2>(cluster_info)<3 || std::get<2>(cluster_info)>256) {
			job_memo = "error, nodes>=3 && nodes<=256 in mgr mode";
			goto end;
		}
	}	else if(std::get<0>(cluster_info) == "rbr")	{
		if(std::get<2>(cluster_info)<3 || std::get<2>(cluster_info)>256) {
			job_memo = "error, nodes>=3 && nodes<=256 in rbr mode";
			goto end;
		}
	}

  job_status = "ongoing";
  job_memo = "create cluster start";
  syslog(Logger::INFO, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id, job_status, job_memo);

  /////////////////////////////////////////////////////////
  // for install cluster cmd
	if(!create_program_path())
	{
		job_memo = "create_cmd_path error";
		goto end;
	}

  //create user name for install
  get_user_name();
  //generate as timestamp and serial number
  generate_cluster_name(cluster_name);
  //init channel again
  g_node_channel_manager.Init();

	/////////////////////////////////////////////////////////
  //get machine and info
  if(!Machine_info::get_instance()->get_machines_info(vec_machine, set_machine)) {
    job_memo = "error, no machine to install";
    goto end;
  }

  //check machine path
  if(!Machine_info::get_instance()->check_machines_path(vec_machine)) {
    job_memo = "error, machine path must set first";
    goto end;
  }

	/////////////////////////////////////////////////////////
  //prepare task
  // to get path size from machines
  task_num = vec_machine.size(); 
  //erery instance need 1. install task, 3. rollback task
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
      job_memo = "error, no task to update machine path size";
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

  job_memo = "update machine path size start";
  syslog(Logger::INFO, "%s", job_memo.c_str());
  return;

end:
  job_status = "failed";
  syslog(Logger::ERROR, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id, job_status, job_memo);
}

void ClusterMission::deleteCluster() {
	std::string job_status;
	std::string job_memo;

  if (!super::get_body_json_document().isMember("paras")) {
    setExtraErr("missing `paras` key-value pair in the request body");
    return;
  }
  Json::Value paras = super::get_body_json_document()["paras"];

  if (!paras.isMember("cluster_name")) {
    job_memo = "missing `cluster_name` key-value pair in the request body";
    goto end;
  }
  cluster_name = paras["cluster_name"].asString();

	job_status = "ongoing";
	job_memo = "delete cluster start";
  syslog(Logger::INFO, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id, job_status, job_memo);

	/////////////////////////////////////////////////////////
  //stop cluster working
	System::get_instance()->set_cluster_mgr_working(false);
	
	if(!System::get_instance()->check_cluster_name(cluster_name))	{
		job_memo = "error, cluster_name is no exist";
		goto end;
	}

  delete_cluster(cluster_name);

  return;

end:
  job_status = "failed";
  syslog(Logger::ERROR, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id, job_status, job_memo);
}

void ClusterMission::createStorageInfo()
{
	std::string job_status;
	std::string job_memo;

  int shards = std::get<1>(cluster_info);
  int nodes = std::get<2>(cluster_info);
  nodes_select = 0;

  syslog(Logger::INFO, "createStorageInfo start");

	///////////////////////////////////////////////////////////////////////////////
	// get storage of shard 
	for(int i=0; i<shards; i++)	{
		std::vector<Tpye_Ip_Port_Paths> vec_storage_ip_port_paths;
		if(!Machine_info::get_instance()->get_storage_nodes(nodes, nodes_select, vec_storage_ip_port_paths, vec_machine))	{
			job_memo = "Machine_info, no available machine";
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

  job_status = "ongoing";
	job_memo = "install storage start";
  syslog(Logger::INFO, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id, job_status, job_memo);
  return;

end:
  job_status = "failed";
  syslog(Logger::ERROR, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id, job_status, job_memo);
}

void ClusterMission::createComputerInfo()
{
	std::string job_status;
	std::string job_memo;

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
		job_memo = "Machine_info, no available machine";
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
	job_memo = "install computer start";
  syslog(Logger::INFO, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id, job_status, job_memo);
  return;

end:
  job_status = "failed";
  syslog(Logger::ERROR, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id, job_status, job_memo);
}

void ClusterMission::createClusterInfo() {

	std::string job_status;
	std::string job_memo;

	FILE* pfd;
	char buf[256];

	int retry;
	std::string cmd,file_path;

	Json::Value shards_json;
	Json::Value meta_json;
  Json::FastWriter writer;

  syslog(Logger::INFO, "createClusterInfo start");

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
		job_memo = "get_meta_info error";
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
	// start cluster cmd
	cmd = "cd " + program_binaries_path + "/" + computer_prog_package_name + "/scripts/;";
	cmd += "python2 create_cluster.py --shards_config ./pgsql_shards.json --comps_config ./pgsql_comps.json --meta_config ./pgsql_meta.json --cluster_name ";
	cmd += cluster_name + " --cluster_owner abc --cluster_biz kunlun --ha_mode " + std::get<0>(cluster_info);
	syslog(Logger::INFO, "createClusterInfo cmd %s", cmd.c_str());

	pfd = popen(cmd.c_str(), "r");
	if(!pfd) {
		job_memo = "createClusterInfo start cmd error";
		goto end;
	}
	while(fgets(buf, 256, pfd)!=NULL)	{
		//if(strcasestr(buf, "error") != NULL)
			syslog(Logger::INFO, "%s", buf);
	}
	pclose(pfd);

	/////////////////////////////////////////////////////////////
	// check instance succeed by connect to instance
	retry = thread_work_interval * 30;
	while(retry-->0) {
		sleep(1);
		if(System::get_instance()->check_cluster_name(cluster_name))
			break;
	}

	if(retry<0)	{
		job_memo = "cluster start error";
		goto end;
	}

	/////////////////////////////////////////////////////////
	// update cluster info
	if(!updateClusterInfo()) {
		job_memo = "update cluster info error";
		goto end;
	}

  syslog(Logger::INFO, "create cluster succeed : %s", cluster_name.c_str());
  job_status = "done";
  job_memo = cluster_name;
  System::get_instance()->update_operation_record(job_id, job_status, job_memo);
  return;

end:
  job_status = "failed";
  syslog(Logger::ERROR, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id, job_status, job_memo);
}

bool ClusterMission::updateClusterInfo() {
  Json::Value paras = super::get_body_json_document()["paras"];
	std::string str_sql;
  std::string memo;

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

void ClusterMission::create_shard(std::vector<Tpye_Ip_Port_Paths> &storages, std::string &shard_name) {
  std::string strtmp,uuid_shard,shard_json;
	get_uuid(uuid_shard);

	/////////////////////////////////////////////////////////
	// create json parameter
	Json::Value paras;
  paras["cluster_name"] = cluster_name;
  paras["shard_name"] = shard_name;
  paras["ha_mode"] = std::get<0>(cluster_info);
  paras["innodb_buffer_pool_size"] = std::get<7>(cluster_info);
  paras["group_uuid"] = uuid_shard;

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

void ClusterMission::create_storage(Tpye_Ip_Port_Paths &storage, Json::Value &para) {
  std::string job_status;
  std::string job_memo;
  Json::Value root_node;

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
    job_status = "failed";
    job_memo = "error, no task to install storage";
    goto end;
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
  return;

end:
  syslog(Logger::ERROR, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id, job_status, job_memo);
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
  std::string job_status;
  std::string job_memo;
  Json::Value root_node;

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
    job_status = "failed";
    job_memo = "error, no task to install computer";
    goto end;
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
  return;

end:
  syslog(Logger::ERROR, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id, job_status, job_memo);
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
  std::string job_status;
  std::string job_memo;
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
    job_status = "failed";
    job_memo = "error, no task to delete storage";
    goto end;
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
  return;

end:
  syslog(Logger::ERROR, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id, job_status, job_memo);
}

void ClusterMission::delete_computer(Tpye_Ip_Port &computer) {
  std::string job_status;
  std::string job_memo;
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
    job_status = "failed";
    job_memo = "error, no task to delete storage";
    goto end;
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
  return;

end:
  syslog(Logger::ERROR, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id, job_status, job_memo);
}

void ClusterMission::stop_cluster() {
  std::string job_status;
  std::string job_memo;

	/////////////////////////////////////////////////////////
	// delete cluster info from meta talbes
	if(!System::get_instance()->stop_cluster(cluster_name))	{
		syslog(Logger::ERROR, "stop_cluster error");
	}

  job_status = "done";
  job_memo = "delete cluster succeed";
  syslog(Logger::ERROR, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id, job_status, job_memo);
  System::get_instance()->set_cluster_mgr_working(true);
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

void ClusterMission::generate_cluster_name(std::string &cluster_name) {
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