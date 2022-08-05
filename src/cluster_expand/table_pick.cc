/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "table_pick.h"
#include "zettalib/tool_func.h"
#include "zettalib/op_log.h"
#include "json/json.h"
#include <vector>

void kunlun::TableShufflePolicy::set_type(KShufflePolicyType type) {
  type_ = type;
}

kunlun::KShufflePolicyType kunlun::TableShufflePolicy::get_type() const {
  return type_;
}

bool kunlun::TableShufflePolicy::toString(const std::vector<std::string> &table_list) {
  struct {
    std::string db = "";
    std::string schema = "";
    std::string table = "";
  } table_spec_;

  for (auto i = 0; i != table_list.size(); i++) {
    std::string item = table_list[i];
    // tokenize to table_spec_
    std::vector<std::string> tokenized = kunlun::StringTokenize(item, ".");
    if (tokenized.size() != 3) {
      setErr("table_list format is illegal,should by db.schema.table");
      return false;
    }
    table_spec_.db = tokenized[0];
    table_spec_.schema = tokenized[1];
    table_spec_.table = tokenized[2];

    std::string dspc = "";
    dspc += table_spec_.db;
    dspc += "_\\$\\$_";
    dspc += table_spec_.schema;

    if (i == 0) {
      table_list_str_.append(item);
      table_list_str_for_storage_.append(dspc + "." + table_spec_.table);
    } else {
      table_list_str_.append(",");
      table_list_str_.append(item);
      table_list_str_for_storage_.append(",");
      table_list_str_for_storage_.append(dspc + "." + table_spec_.table);
    }
  }
  return true;
  
}
#define TOP_TABLE_NUMBER 3
bool kunlun::RandomShufflePolicy::GetTableList(
    std::vector<std::string> &tbl_list) {

  if (!conn_->CheckIsConnected()) {
    setErr("%s", conn_->getErr());
    return false;
  }

  char sql_get_dbname[4096] = {'\0'};
  sprintf(sql_get_dbname, "SELECT schema_name AS dbname "
                          "FROM   information_schema.schemata "
                          "WHERE  schema_name LIKE '%$$%' ");
  kunlun::MysqlResult result;
  int ret = conn_->ExcuteQuery(sql_get_dbname, &result);
  if (ret < 0) {
    setErr("%s", conn_->getErr());
    return false;
  }

  for (int i = 0; i < result.GetResultLinesNum(); i++) {
    std::string dbname = result[i]["dbname"];
    char sql[10240] = {'\0'};
    sprintf(
        sql,
        "SELECT substring_index(dbname,'_$$_',1)       DBNAME,"
        "       substring_index(dbname,'_$$_',2)   SCHEMANAME,"
        "       tblname                               TBLNAME,"
        "       Concat(dat, ' ', Substr(units, 1, 2)) DATSIZE,"
        "       Concat(ndx, ' ', Substr(units, 1, 2)) NDXSIZE,"
        "       Concat(tbl, ' ', Substr(units, 1, 2)) TBLSIZE "
        "FROM   (SELECT dbname,"
        "               tblname,"
        "               dat,"
        "               ndx,"
        "               tbl,"
        "               IF(px > 4, 4, px) pw1,"
        "               IF(py > 4, 4, py) pw2,"
        "               IF(pz > 4, 4, pz) pw3"
        "        FROM   (SELECT table_schema"
        "                       dbname,"
        "                       table_name"
        "                       tblname,"
        "                       data_length"
        "                       DAT,"
        "                       index_length"
        "                       NDX,"
        "                       data_length + index_length"
        "                       TBL,"
        "                       Floor(Log(IF(data_length = 0, 1, data_length)) "
        "/ "
        "Log(1024"
        "                             ))   px"
        "                       ,"
        "                       Floor(Log(IF(index_length = 0, 1, index_length)"
        "                             ) / Log(1024)) py,"
        "                       Floor(Log(data_length + index_length) / "
        "Log(1024))"
        "                       pz"
        "                FROM   information_schema.tables "
        "                WHERE  table_schema = '%s') AA) A,"
        "       (SELECT 'B KBMBGBTB' units) B "
        "ORDER  BY tbl DESC",
        dbname.c_str());
    kunlun::MysqlResult rslt;
    int ret = conn_->ExcuteQuery(sql, &rslt);
    if (ret < 0) {
      setErr("%s", conn_->getErr());
      return false;
    }
    for (int j = 0;
         j < std::min(rslt.GetResultLinesNum(), (unsigned int)TOP_TABLE_NUMBER);
         j++) {
      tbl_list.push_back(std::string(rslt[j]["DBNAME"]) + "." +
                         std::string(rslt[j]["SCHEMANAME"]) + "." +
                         std::string(rslt[j]["TBLNAME"]));
    }
  }
  
  return super::toString(tbl_list);
}

bool kunlun::TopHitShufflePolicy::GetTableList(
    std::vector<std::string> &tbl_list) {
  char sql[4096] = {'\0'};
  sprintf(sql,
          "SELECT substring_index(TABLE_SCHEMA,'_$$_',1)       DBNAME,"
          "       substring_index(TABLE_SCHEMA,'_$$_',2)   SCHEMANAME,"
          "       TABLE_NAME                                  TBLNAME "
          "FROM   information_schema.tables "
          "WHERE  table_schema LIKE '%$$%' "
          "ORDER  BY update_time DESC limit %d",
          TOP_TABLE_NUMBER);
  kunlun::MysqlResult rslt;
  int ret = conn_->ExcuteQuery(sql, &rslt);
  if (ret < 0) {
    setErr("%s", conn_->getErr());
    return false;
  }
  for (int i = 0; i < rslt.GetResultLinesNum(); i++) {
    tbl_list.push_back(std::string(rslt[i]["DBNAME"]) + "." +
                       std::string(rslt[i]["SCHEMANAME"]) + "." +
                       std::string(rslt[i]["TBLNAME"]));
  }
  return  super::toString(tbl_list);
}
