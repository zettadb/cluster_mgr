/*
  Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "backup_storage.h"
#include "kl_mentain/global.h"
#include "kl_mentain/sys.h"
#include "zettalib/op_log.h"

using namespace kunlun;

void BackupStorage::CreateBackupStorage() {
  std::string name,stype,hostaddr,port,conn_str,str_sql;
  Json::Value paras;

  job_status = "failed";
  if (!super::get_body_json_document().isMember("paras")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `paras` key-value pair in the request body";
    goto end;
  }
  paras = super::get_body_json_document()["paras"];

  if (!paras.isMember("name")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `name` key-value pair in the request body";
    goto end;
  }
  name = paras["name"].asString();

  if (!paras.isMember("stype")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `stype` key-value pair in the request body";
    goto end;
  }
  stype = paras["stype"].asString();

  if (!paras.isMember("hostaddr")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `hostaddr` key-value pair in the request body";
    goto end;
  }
  hostaddr= paras["hostaddr"].asString();

  if (!paras.isMember("port")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `port` key-value pair in the request body";
    goto end;
  }
  port = paras["port"].asString();

	job_error_info = "create backup storage start";
  KLOG_INFO("{}", job_error_info);

	/////////////////////////////////////////////////////////
	if(System::get_instance()->check_backup_storage_name(name))	{
    job_error_code = EintToStr(ERR_BACKUP_NAME_EXIST);
		job_error_info = "backup storage name have existed";
		goto end;
	}

	/////////////////////////////////////////////////////////
	if(stype == "HDFS")	{
		conn_str = "hdfs://" + hostaddr + ":" + port;
	}	else {
    job_error_code = EintToStr(ERR_BACKUP_STYPE_NO_SUPPORT);
		job_error_info = "stype isn't support";
		goto end;
	}

	str_sql = "insert into backup_storage(name,stype,conn_str,hostaddr,port) value('";
	str_sql += name + "','" + stype + "','" + conn_str + "','" + hostaddr + "'," + port + ")";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_INSERT, str_sql))	{
    job_error_code = EintToStr(ERR_META_WRITE);
		job_error_info = "job_create_backup_storage error";
		goto end;
	}

	job_status = "done";
  job_error_code = EintToStr(EOK);
	job_error_info = "create backup storage successfully";

end:
	KLOG_ERROR("{}", job_error_info);
  update_operation_record();
}

void BackupStorage::UpdateBackupStorage() {
  std::string name,stype,hostaddr,port,conn_str,str_sql;
  Json::Value paras;

	job_status = "failed";
  if (!super::get_body_json_document().isMember("paras")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `paras` key-value pair in the request body";
    goto end;
  }
  paras = super::get_body_json_document()["paras"];

  if (!paras.isMember("name")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `name` key-value pair in the request body";
    goto end;
  }
  name = paras["name"].asString();

  if (!paras.isMember("stype")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `stype` key-value pair in the request body";
    goto end;
  }
  stype = paras["stype"].asString();

  if (!paras.isMember("hostaddr")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `hostaddr` key-value pair in the request body";
    goto end;
  }
  hostaddr= paras["hostaddr"].asString();

  if (!paras.isMember("port")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `port` key-value pair in the request body";
    goto end;
  }
  port = paras["port"].asString();

	job_error_info = "update backup storage start";
  KLOG_INFO("{}", job_error_info.c_str());

	/////////////////////////////////////////////////////////
	if(!System::get_instance()->check_backup_storage_name(name)) {
    job_error_code = EintToStr(ERR_BACKUP_NAME_NO_EXIST);
		job_error_info = "backup storage name no existed";
		goto end;
	}

	/////////////////////////////////////////////////////////
	if(stype == "HDFS")	{
		conn_str = "hdfs://" + hostaddr + ":" + port;
	} else {
    job_error_code = EintToStr(ERR_BACKUP_STYPE_NO_SUPPORT);
		job_error_info = "stype isn't support";
		goto end;
	}

	str_sql = "update backup_storage set stype='" + stype + "',conn_str='" + conn_str + "',hostaddr='" + hostaddr;
	str_sql += "',port=" + port + " where name='" + name + "'";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_UPDATE, str_sql))	{
    job_error_code = EintToStr(ERR_META_WRITE);
		job_error_info = "job_update_backup_storage error";
		goto end;
	}

	job_status = "done";
  job_error_code = EintToStr(EOK);
	job_error_info = "update backup storage successfully";

end:
	KLOG_ERROR("{}", job_error_info);
  update_operation_record();
}

void BackupStorage::DeleteBackupStorage() {
  std::string name,backup_storage_id,backup_storage_str,str_sql;
  Json::Value paras;

  job_status = "failed";
  if (!super::get_body_json_document().isMember("paras")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `paras` key-value pair in the request body";
    goto end;
  }
  paras = super::get_body_json_document()["paras"];

  if (!paras.isMember("name")) {
    job_error_code = EintToStr(ERR_JSON);
    job_error_info = "missing `name` key-value pair in the request body";
    goto end;
  }
  name = paras["name"].asString();

	job_error_info = "delete backup storage start";
  KLOG_INFO("{}", job_error_info);

	/////////////////////////////////////////////////////////
	if(!System::get_instance()->check_backup_storage_name(name)) {
    job_error_code = EintToStr(ERR_BACKUP_NAME_NO_EXIST);
		job_error_info = "backup storage name no existed";
		goto end;
	}

	/////////////////////////////////////////////////////////
	//must delete cluster_shard_backup_restore_log
	if(!System::get_instance()->get_backup_storage_string(name, backup_storage_id, backup_storage_str)) {
    job_error_code = EintToStr(ERR_BACKUP_STRING);
		job_error_info = "get_backup_storage_string error";
		goto end;
	}

	str_sql = "delete from cluster_shard_backup_restore_log where storage_id=" + backup_storage_id;
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_DELETE, str_sql)) {
    job_error_code = EintToStr(ERR_META_WRITE);
		job_error_info = "job_delete_backup_storage error";
		//goto end;
	}

	/////////////////////////////////////////////////////////
	//must delete cluster_backups 
	str_sql = "delete from cluster_backups where storage_id=" + backup_storage_id;
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_DELETE, str_sql))	{
    job_error_code = EintToStr(ERR_META_WRITE);
		job_error_info = "job_delete_backup_storage error";
		//goto end;
	}

	/////////////////////////////////////////////////////////
	str_sql = "delete from backup_storage where name='" + name + "'";
	//syslog(Logger::INFO, "str_sql=%s", str_sql.c_str());

	if(System::get_instance()->execute_metadate_opertation(SQLCOM_DELETE, str_sql))	{
    job_error_code = EintToStr(ERR_META_WRITE);
		job_error_info = "job_delete_backup_storage error";
		goto end;
	}

	job_status = "done";
  job_error_code = EintToStr(EOK);
	job_error_info = "delete backup storage successfully";

end:
	KLOG_ERROR("{}", job_error_info);
  update_operation_record();
}

bool BackupStorage::update_operation_record(){
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
		KLOG_ERROR("execute_metadate_opertation error");
		return false;
	}

	return true;
}

bool BackupStorage::ArrangeRemoteTask() {
  request_type_ = get_request_type();
  job_id = get_request_unique_id();
	job_status = "not_started";
	job_error_code = EintToStr(EOK);
  job_error_info = "";

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