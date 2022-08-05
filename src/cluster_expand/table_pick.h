/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#pragma once
#include "zettalib/errorcup.h"
#include "zettalib/op_mysql.h"
#include <memory>

namespace kunlun {

enum KShufflePolicyType {
  KShufflePolicyUndifined = 0,
  KRandomPolicy,
  KTopHitPolicy,
  // addtional type should add above
  KUnknownPolicy = 1000
};
class TableShufflePolicy : public kunlun::ErrorCup {
public:
  TableShufflePolicy(kunlun::MysqlConnection *conn)
      : conn_(conn), type_(KShufflePolicyUndifined), table_list_str_(""),table_list_str_for_storage_(""){};
  virtual ~TableShufflePolicy(){};

  virtual bool GetTableList(std::vector<std::string> &tbl_list) = 0;
  void set_type(KShufflePolicyType);
  KShufflePolicyType get_type() const;
  std::string get_table_list_str() const { return table_list_str_; }
  std::string get_table_list_str_for_storage() const { return table_list_str_for_storage_; }

protected:
  bool toString(const std::vector<std::string> &tbl_list);

protected:
  std::unique_ptr<kunlun::MysqlConnection> conn_;
  KShufflePolicyType type_;
  // db.schema.table,db.schema.table
  std::string table_list_str_;
  // db_$$_schema.table,db_$$_schema.table
  std::string table_list_str_for_storage_;
};

class RandomShufflePolicy : public TableShufflePolicy {
  typedef TableShufflePolicy super;

public:
  explicit RandomShufflePolicy(kunlun::MysqlConnection *conn) : super(conn) {
    super::set_type(KRandomPolicy);
  };
  virtual ~RandomShufflePolicy(){};
  bool GetTableList(std::vector<std::string> &tbl_list) override;
};

class TopHitShufflePolicy : public TableShufflePolicy {
  typedef TableShufflePolicy super;

public:
  explicit TopHitShufflePolicy(kunlun::MysqlConnection *conn) : super(conn) {
    super::set_type(KTopHitPolicy);
  };
  virtual ~TopHitShufflePolicy(){};
  bool GetTableList(std::vector<std::string> &tbl_list) override;
};
} // namespace kunlun
