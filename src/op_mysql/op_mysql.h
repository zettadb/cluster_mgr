/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#ifndef _OP_MYSQL_H_
#define _OP_MYSQL_H_

#include "zettalib/errorcup.h"
#include <map>
#include <mysql/mysql.h>
#include <mysql/errmsg.h>
#include <mysql/mysqld_error.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <vector>

// This moudler contain some operation to mysql

using namespace std;
// this class represent the MYSQL_ROW
// all the value is convert to string
namespace kunlun {
// reprsent single row of the query result from mysql
class MysqlResRow {
public:
  MysqlResRow(const map<string, int> &column_index_map)
      : column_index_map_(column_index_map) {
    value_array_ = nullptr;
    length_array_ = nullptr;
  };
  ~MysqlResRow() {
    if (value_array_ != nullptr) {
      for (int i = 0; i < fields_num_; i++) {
        if (value_array_[i] != (char *)"NULL") {
          delete[] value_array_[i];
        }
      }
      delete[] value_array_;
    }
  }

public:
  void initByMysqlRawRes(MYSQL_ROW row, unsigned long *lens_array,
                         unsigned int fields_num) {
    fields_num_ = fields_num;
    if (fields_num_ == 0) {
      return;
    }
    // allocate the memmory once
    value_array_ = new char *[fields_num * 2];
    // start of the lenth_array
    length_array_ = value_array_ + fields_num;

    for (int i = 0; i < fields_num_; i++) {
      if (row[i] == nullptr) {
        length_array_[i] = (char *)4;
        value_array_[i] = (char *)"NULL";
        continue;
      }
      unsigned long len = lens_array[i];
      length_array_[i] = (char *)(len);
      value_array_[i] = new char[len + 1];
      // copy data from the MYSQL_ROW
      memcpy(value_array_[i], row[i], (size_t)len);
      // set tail of the string to \0
      value_array_[i][len] = '\0';
    }
    return;
  }

  char *operator[](const char *column_name) {
    auto iter = column_index_map_.find(column_name);
    if (iter == column_index_map_.end()) {
      return (char *)"NULL";
    }
    int index = iter->second;
    return value_array_[index];
  }

  char *operator[](size_t &index) {
    if (index >= fields_num_) {
      return (char *)"NULL";
    }
    return value_array_[index];
  }
  void to_string() {
    auto iter = column_index_map_.begin();
    for (; iter != column_index_map_.end(); iter++) {
      char name_buf[1024] = {'\0'};
      sprintf(name_buf, "column: %s", iter->first.c_str());
      char value_buf[4096] = {'\0'};
      sprintf(value_buf, ", value: %s", value_array_[iter->second]);

      printf("%s%s\n", name_buf, value_buf);
    }
    return;
  }

public: // getter setter
  unsigned int get_fiedls_num() const { return fields_num_; }

private:
  // frobid copy
  MysqlResRow(const MysqlResRow &row) = delete;
  MysqlResRow &operator=(const MysqlResRow &row) = delete;

private:
  // hold the value of each column
  char **value_array_;
  // hold the lenth of each value of each column
  char **length_array_;
  // fields number
  unsigned int fields_num_;
  // fetch the index in the whole row by column name
  const map<string, int> &column_index_map_;
};

// combination of the MysqlResRow
class MysqlResult : public kunlun::ErrorCup {

public:
  MysqlResult() {}
  ~MysqlResult() { Clean(); }

public:
  // will not release the MYSQL_RES
  bool Parse(MYSQL_RES *);
  void Clean();

  MysqlResRow &operator[](unsigned int index) { return *(result_vec_[index]); }
  unsigned int GetResultLinesNum() const { return result_vec_.size(); }
  unsigned int get_fields_num() const { return fields_num_; }

private:
  // forbid copy
  MysqlResult(const MysqlResult &res) = delete;
  MysqlResult &operator=(const MysqlResult &res) = delete;

private:
  MYSQL_RES *raw_mysql_res_;
  vector<MysqlResRow *> result_vec_;
  map<string, int> column_index_map_;
  map<int, string> index_column_map_;
  unsigned int fields_num_;
};

enum ENUM_MYSQL_CONNECT_TYPE { TCP_CONNECTION = 0, UNIX_DOMAIN_CONNECTION };

// c++11
typedef struct MysqlConnectionOption_ {
  ENUM_MYSQL_CONNECT_TYPE connect_type = TCP_CONNECTION;
  string ip = "";
  string port_str = "";
  unsigned int port_num = 0;
  string user = "";
  string password = "";
  unsigned int timeout_sec = 10;
  unsigned int connect_timeout_sec = 10;
  string charset = "utf8mb4";
  string database = "";
  // if use UNIX-DOMAIN socket
  string file_path = "";
  bool autocommit = true;
} MysqlConnectionOption;

class MysqlConnection : public kunlun::ErrorCup {

public: // constructor
  explicit MysqlConnection(MysqlConnectionOption option)
      : last_errno_(0), mysql_raw_(nullptr), reconnect_support_(true),
        mysql_connection_option_(option) {}
  ~MysqlConnection() { Close(); }

public:
  bool Connect();
  // will do the reconnect if the flag is set
  bool CheckIsConnected();
  void Close();
  bool SetAutoCommit();
  // sql_stmt: select / set / DDL ,return 0 means success, other than failed
  //           result_set hold the retrived data
  // sql_stmt: update / delete / insert , return >0 , 0 , <0 respectively,
  // successfuly, no effect or  failed
  int ExcuteQuery(const char *sql_stmt, MysqlResult *result_set,
                  bool force_retry = false);

public: // getter setter
  bool get_reconnect_support() const { return reconnect_support_; }
  void set_reconnect_support(bool reconnect) { reconnect_support_ = reconnect; }
public:
  int last_errno_;

private:
  void Reconnect();
  bool ConnectImplByTcp();
  bool ConnectImplByUnix();

private:
  MYSQL *mysql_raw_;
  bool reconnect_support_;
  MysqlConnectionOption mysql_connection_option_;
};

} // namespace kunlun
#endif /*_OP_MYSQL_H_*/
