/*
  Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "backup_storage.h"
#include "kl_mentain/global.h"
#include "kl_mentain/sys.h"

bool BackupStorage::ArrangeRemoteTask() {
  request_type_ = get_request_type();
  job_id_ = get_request_unique_id();

  switch (request_type_) {
  case kunlun::kCreateBackupStorageType:
    CreateBackupStorage();
    break;
  case kunlun::kUpdateBackupStorageType:
    UpdateBackupStorage();
    break;
  case kunlun::kDeleteBackupStorageType:
    DeleteBackupStorage();
    break;

  default:
    break;
  }

  return true;
}

void BackupStorage::CreateBackupStorage() {
	std::string job_status;
	std::string job_memo;

  std::string name,stype,hostaddr,port,conn_str,str_sql;

  if (!super::get_body_json_document().isMember("paras")) {
    setExtraErr("missing `paras` key-value pair in the request body");
    return;
  }
  Json::Value paras = super::get_body_json_document()["paras"];

  if (!paras.isMember("name")) {
    setExtraErr("missing `name` key-value pair in the request body");
    return;
  }
  name = paras["name"].asString();

  if (!paras.isMember("stype")) {
    setExtraErr("missing `stype` key-value pair in the request body");
    return;
  }
  stype = paras["stype"].asString();

  if (!paras.isMember("hostaddr")) {
    setExtraErr("missing `hostaddr` key-value pair in the request body");
    return;
  }
  hostaddr= paras["hostaddr"].asString();

  if (!paras.isMember("port")) {
    setExtraErr("missing `port` key-value pair in the request body");
    return;
  }
  port = paras["port"].asString();

	job_status = "not_started";
	job_memo = "create backup storage start";
  syslog(Logger::INFO, "%s", job_memo.c_str());

	/////////////////////////////////////////////////////////
	if(System::get_instance()->check_backup_storage_name(name))	{
		job_memo = "backup storage name have existed";
		goto end;
	}

	/////////////////////////////////////////////////////////
	if(stype == "HDFS")	{
		conn_str = "hdfs://" + hostaddr + ":" + port;
	}	else {
		job_memo = "stype isn't support";
		goto end;
	}

	str_sql = "insert into backup_storage(name,stype,conn_str,hostaddr,port) value('";
	str_sql += name + "','" + stype + "','" + conn_str + "','" + hostaddr + "'," + port + ")";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_INSERT, str_sql))	{
		job_memo = "job_create_backup_storage error";
		goto end;
	}

	job_status = "done";
	job_memo = "create backup storage succeed";
  syslog(Logger::INFO, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id_, job_status, job_memo);
  return;

end:
	job_status = "failed";
	syslog(Logger::ERROR, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id_, job_status, job_memo);
}

void BackupStorage::UpdateBackupStorage() {
	std::string job_status;
	std::string job_memo;

  std::string name,stype,hostaddr,port,conn_str,str_sql;

  if (!super::get_body_json_document().isMember("paras")) {
    setExtraErr("missing `paras` key-value pair in the request body");
    return;
  }
  Json::Value paras = super::get_body_json_document()["paras"];

  if (!paras.isMember("name")) {
    setExtraErr("missing `name` key-value pair in the request body");
    return;
  }
  name = paras["name"].asString();

  if (!paras.isMember("stype")) {
    setExtraErr("missing `stype` key-value pair in the request body");
    return;
  }
  stype = paras["stype"].asString();

  if (!paras.isMember("hostaddr")) {
    setExtraErr("missing `hostaddr` key-value pair in the request body");
    return;
  }
  hostaddr= paras["hostaddr"].asString();

  if (!paras.isMember("port")) {
    setExtraErr("missing `port` key-value pair in the request body");
    return;
  }
  port = paras["port"].asString();

	job_status = "not_started";
	job_memo = "update backup storage start";
  syslog(Logger::INFO, "%s", job_memo.c_str());

	/////////////////////////////////////////////////////////
	if(!System::get_instance()->check_backup_storage_name(name)) {
		job_memo = "backup storage name no existed";
		goto end;
	}

	/////////////////////////////////////////////////////////
	if(stype == "HDFS")	{
		conn_str = "hdfs://" + hostaddr + ":" + port;
	} else {
		job_memo = "stype isn't support";
		goto end;
	}

	str_sql = "update backup_storage set stype='" + stype + "',conn_str='" + conn_str + "',hostaddr='" + hostaddr;
	str_sql += "',port=" + port + " where name='" + name + "'";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_UPDATE, str_sql))	{
		job_memo = "job_update_backup_storage error";
		goto end;
	}

	job_status = "done";
	job_memo = "update backup storage succeed";
  syslog(Logger::INFO, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id_, job_status, job_memo);
  return;

end:
	job_status = "failed";
	syslog(Logger::ERROR, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id_, job_status, job_memo);
}

void BackupStorage::DeleteBackupStorage() {
	std::string job_status;
	std::string job_memo;

  std::string name,backup_storage_id,backup_storage_str,str_sql;

  if (!super::get_body_json_document().isMember("paras")) {
    setExtraErr("missing `paras` key-value pair in the request body");
    return;
  }
  Json::Value paras = super::get_body_json_document()["paras"];

  if (!paras.isMember("name")) {
    setExtraErr("missing `name` key-value pair in the request body");
    return;
  }
  name = paras["name"].asString();

	job_status = "not_started";
	job_memo = "delete backup storage start";
  syslog(Logger::INFO, "%s", job_memo.c_str());

	/////////////////////////////////////////////////////////
	if(!System::get_instance()->check_backup_storage_name(name)) {
		job_memo = "backup storage name no existed";
		goto end;
	}

	/////////////////////////////////////////////////////////
	//must delete cluster_shard_backup_restore_log
	if(!System::get_instance()->get_backup_storage_string(name, backup_storage_id, backup_storage_str)) {
		job_memo = "get_backup_storage_string error";
		goto end;
	}

	str_sql = "delete from cluster_shard_backup_restore_log where storage_id=" + backup_storage_id;
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_DELETE, str_sql)) {
		job_memo = "job_delete_backup_storage error";
		//goto end;
	}

	/////////////////////////////////////////////////////////
	//must delete cluster_backups 
	str_sql = "delete from cluster_backups where storage_id=" + backup_storage_id;
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_DELETE, str_sql))	{
		job_memo = "job_delete_backup_storage error";
		//goto end;
	}

	/////////////////////////////////////////////////////////
	str_sql = "delete from backup_storage where name='" + name + "'";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_DELETE, str_sql))	{
		job_memo = "job_delete_backup_storage error";
		goto end;
	}

	job_status = "done";
	job_memo = "delete backup storage succeed";
  syslog(Logger::INFO, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id_, job_status, job_memo);
  return;

end:
	job_status = "failed";
	syslog(Logger::ERROR, "%s", job_memo.c_str());
  System::get_instance()->update_operation_record(job_id_, job_status, job_memo);
}
