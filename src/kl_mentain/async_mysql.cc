/*
   Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include <thread>
#include <algorithm>
#include "async_mysql.h"
#include "util_func/error_code.h"
#include "util_func/meta_info.h"
#include "zettalib/tool_func.h"
//#include "kl_mentain/sys.h"

namespace kunlun 
{

static int MysqlStatus(uint32_t event) {
  int status= 0;
  if (event & EPOLLIN)
    status|= MYSQL_WAIT_READ;
  if (event & EPOLLOUT)
    status|= MYSQL_WAIT_WRITE;
  if (event == 0)
    status|= MYSQL_WAIT_TIMEOUT;
  return status;
}

void FreeStatMysql(StatMysql* cmysql) {
    if(!cmysql) 
        return;
    
    if(cmysql->mysql_ares_) {
        delete cmysql->mysql_ares_;
        cmysql->mysql_ares_ = nullptr;
    }
    if(cmysql->user_) {
        delete cmysql->user_;
        cmysql->user_ = nullptr;
    }
    if(cmysql->passwd_) {
        delete cmysql->passwd_;
        cmysql->passwd_ = nullptr;
    }
    if(cmysql->mysql_) {
        delete cmysql->mysql_;
        cmysql->mysql_ = nullptr;
    }
    if(cmysql->ip_) {
        delete cmysql->ip_;
        cmysql->ip_ = nullptr;
    }
    if(cmysql->socket_file_) {
        delete cmysql->socket_file_;
        cmysql->socket_file_ = nullptr;
    }
    if(cmysql->charset_) {
        delete cmysql->charset_;
        cmysql->charset_ = nullptr;
    }

    delete cmysql;
    cmysql = nullptr;
}

int SqlInsertToPendList(StatMysql* cmysql, const char* sql_stmt, AMysqlRowCB row_cb, void* arg, bool dml_sql) {
    SqlCallFn *sf = (SqlCallFn*)calloc(1, sizeof(*sf));
    assert(sf);  //allocate memory failed, abort
    
    //add begin; sql_stmt; commit;
    int sql_stmt_len = strlen(sql_stmt);
    sf->send_ = 0;
    sf->send_ts_ = GetNowTimestamp();
    sf->dml_sql_ = dml_sql;
    if(sf->dml_sql_)
        sf->query_ = (char*)calloc((sql_stmt_len+14), sizeof(char));
    else
        sf->query_ = (char*)calloc(sql_stmt_len+1, sizeof(char));

    assert(sf->query_);   //allocate memory failed, abort
    
    if(sf->dml_sql_) {
        memcpy(sf->query_, "begin;", 6);
        memcpy(sf->query_ + 6, sql_stmt, sql_stmt_len);
        memcpy(sf->query_ + sql_stmt_len + 6, ";commit", 7);
    } else {
        memcpy(sf->query_, sql_stmt, sql_stmt_len);
    }
    sf->row_cb_ = row_cb;
    sf->arg_ = arg;

    int count = 0;
    cmysql->list_lock_.lock();
    UT_LIST_ADD_LAST(sql_cb_list, cmysql->pending_sql_, sf);
    count = UT_LIST_GET_LEN(cmysql->pending_sql_);
    cmysql->list_lock_.unlock();

    return (count > 1 ? 1 : 0);
}

bool RemovePendListFont(StatMysql* cmysql) {
    bool rb = false;
    SqlCallFn* sf = NULL;
    cmysql->list_lock_.lock();
    if(UT_LIST_GET_LEN(cmysql->pending_sql_) > 0) {
        sf = UT_LIST_GET_FIRST(cmysql->pending_sql_);
        UT_LIST_REMOVE(sql_cb_list, cmysql->pending_sql_, sf);
    }
    cmysql->list_lock_.unlock();
    if(sf) {
        if(sf->query_) {
            if(sf->dml_sql_) {
                if(strncmp(sf->query_, "rollback", 8) == 0)
                    rb = true;
            } else
                rb = true;
            free(sf->query_);
        } 
        free(sf);
    }
    return rb;
}

void RollbackSendSql(StatMysql* cmysql) {
    SqlCallFn *sf = (SqlCallFn*)calloc(1, sizeof(*sf));
    assert(sf);  //allocate memory failed, abort

    //send rollback
    sf->query_ = (char*)calloc(9, sizeof(char));
    assert(sf->query_);   //allocate memory failed, abort

    memcpy(sf->query_, "rollback", 8);
    cmysql->list_lock_.lock();
    UT_LIST_ADD_FIRST(sql_cb_list, cmysql->pending_sql_, sf);
    cmysql->list_lock_.unlock();
}

int GetPendListLen(StatMysql* cmysql) {
    cmysql->list_lock_.lock();
    int count = UT_LIST_GET_LEN(cmysql->pending_sql_);
    cmysql->list_lock_.unlock();
    return count;
}

std::tuple<SqlCallFn*, bool> GetFirstSqlSendStat(StatMysql* cmysql) {
    cmysql->list_lock_.lock();
    int count = UT_LIST_GET_LEN(cmysql->pending_sql_);
    if(count < 1) {
        cmysql->list_lock_.unlock();
        return {NULL, false};
    }

    SqlCallFn* sf = UT_LIST_GET_FIRST(cmysql->pending_sql_);
    if(!sf) {
        cmysql->list_lock_.unlock();
        return {NULL, false};
    }

    if(sf->send_) {
        cmysql->list_lock_.unlock();
        return {NULL, false};
    }
    
    sf->send_ = 1;
    cmysql->list_lock_.unlock();
    return {sf, true};
}

bool ServerGoneLostHandle(StatMysql* cmysql) {
    cmysql->err_ = mysql_errno(cmysql->mysql_);
    if(cmysql->err_ == CR_SERVER_GONE_ERROR || cmysql->err_ == CR_SERVER_LOST || 
                    cmysql->err_ == CR_COMMANDS_OUT_OF_SYNC) {
        cmysql->status_ = A_CONNECTERR;
        do {
            SqlCallFn* sf = UT_LIST_GET_FIRST(cmysql->pending_sql_);
            if(sf && sf->row_cb_) {
                //snprintf(cmysql->desc_, mysql_desc_len, "%s", mysql_error(cmysql->mysql_));
                cmysql->mysql_ares_->SetMysqlErr(mysql_error(cmysql->mysql_));
                cmysql->mysql_ares_->SetMysqlErrno(cmysql->err_);
                sf->row_cb_(cmysql->mysql_ares_, sf->arg_);
            }
            RemovePendListFont(cmysql);
        } while(GetPendListLen(cmysql) > 0);
        cmysql->mysql_manager_->DelWatch(cmysql);
        cmysql->mysql_ares_->Clear();
        return true;
    }
    return false;
}

void SendSqlStartFailRollback(StatMysql* cmysql) {
    if(!RemovePendListFont(cmysql))
        RollbackSendSql(cmysql);
    SendNextPendingSql(cmysql);
}

void SendNextPendingSql(StatMysql* cmysql) {
    int err_;
    auto sql_stat = GetFirstSqlSendStat(cmysql);
    if(!std::get<1>(sql_stat))
        return;

    SqlCallFn* sf = std::get<0>(sql_stat);
    int status = mysql_real_query_start(&err_, cmysql->mysql_, sf->query_,
                                                    strlen(sf->query_));
    if(status) {
        cmysql->mysql_manager_->AddWatch(cmysql, status);
    } else {
        if(err_) {
            if(ServerGoneLostHandle(cmysql))
                return;

            mysql_free_result(cmysql->result_);
            cmysql->result_ = nullptr;
            if(sf->row_cb_) {
                //snprintf(cmysql->desc_, mysql_desc_len, "%s", mysql_error(cmysql->mysql_));
                cmysql->mysql_ares_->SetMysqlErr(mysql_error(cmysql->mysql_));
                cmysql->mysql_ares_->SetMysqlErrno(cmysql->err_);
                sf->row_cb_(cmysql->mysql_ares_, sf->arg_);
                cmysql->mysql_ares_->Clear();
            }
            
            std::thread th(SendSqlStartFailRollback, cmysql);
            th.detach();
        } else
            cmysql->mysql_manager_->ParseMysqlResult(cmysql);
    }

    return;
}

void Reconnect(StatMysql* cmysql) {
    if(cmysql->mysql_)
        delete cmysql->mysql_;

    cmysql->mysql_ = new MYSQL;
    mysql_init(cmysql->mysql_);
    // set options
    // connect timeout
    mysql_options(cmysql->mysql_, MYSQL_OPT_CONNECT_TIMEOUT, &(cmysql->mysql_timeout_));

    mysql_options(cmysql->mysql_, MYSQL_OPT_NONBLOCK, 0);

    // read/write timeout
    mysql_options(cmysql->mysql_, MYSQL_OPT_READ_TIMEOUT, &(cmysql->mysql_rd_timeout_));
    mysql_options(cmysql->mysql_, MYSQL_OPT_WRITE_TIMEOUT, &(cmysql->mysql_wr_timeout_));
    
    mysql_options(cmysql->mysql_, MYSQL_SET_CHARSET_NAME, cmysql->charset_);
    /*
	 * USE result, no difference from STORE result for small result set, but
	 * for large result set, USE can use the result rows before all are recved,
	 * and also, extra copying of result data is avoided.
	 * */
	int use_res = 1;
	mysql_options(cmysql->mysql_, MYSQL_OPT_USE_RESULT, &use_res);
	// Never reconnect, because that messes up txnal status.
	bool reconnect = 0;
	mysql_options(cmysql->mysql_, MYSQL_OPT_RECONNECT, &reconnect);

    bool disable_ssl = 0;
    mysql_options(cmysql->mysql_, MYSQL_OPT_SSL_ENFORCE, &disable_ssl);

    cmysql->reconnect_ = 0;
    cmysql->err_ = 0;
    cmysql->status_ = A_UNINITIALIZE;
    cmysql->mysql_fd_ = -1;
    int status;
    if(cmysql->connect_type_ == ENUM_SQL_CONNECT_TYPE::TCP_CONNECTION) {
        status = mysql_real_connect_start(&cmysql->ret_, cmysql->mysql_, 
                cmysql->ip_, cmysql->user_, cmysql->passwd_,
                         NULL, cmysql->port_, NULL, CLIENT_MULTI_STATEMENTS | 0); 
    } else {
        status = mysql_real_connect_start(&cmysql->ret_, cmysql->mysql_, 
                NULL, cmysql->user_, cmysql->passwd_,
                         NULL, 0, cmysql->socket_file_, CLIENT_MULTI_STATEMENTS | 0);
    }    
    if (status)
        /* Wait for connect to complete. */
        cmysql->mysql_manager_->AddWatch(cmysql, status);
    else {
        if(!cmysql->ret_) {
            cmysql->err_ = mysql_errno(cmysql->mysql_);
            //setErr("connect mysql failed, errno: %d\n", mysql_raw_->err_);
            cmysql->status_ = A_CONNECTCERR;  //connect mysql failed, need retry;
            do {
                SqlCallFn* sf = UT_LIST_GET_FIRST(cmysql->pending_sql_);
                if(sf && sf->row_cb_) {
                    //snprintf(cmysql->desc_, mysql_desc_len, "%s", mysql_error(cmysql->mysql_));
                    cmysql->mysql_ares_->SetMysqlErr(mysql_error(cmysql->mysql_));
                    cmysql->mysql_ares_->SetMysqlErrno(cmysql->err_);
                    sf->row_cb_(cmysql->mysql_ares_, sf->arg_);
                }
                RemovePendListFont(cmysql);
            } while(GetPendListLen(cmysql) > 0);
            cmysql->mysql_ares_->Clear();
        } else {
           cmysql->status_ = A_ESTABLIZE;
           cmysql->mysql_ares_->Clear();
           SendNextPendingSql(cmysql);
        }
    }
}

std::string CAsyncMysql:: GetMysqlConnHost(){
    std::string host = string_sprintf("%s_%d", mysql_raw_->ip_, mysql_raw_->port_);
    return host;
}

bool CAsyncMysql::Connect(bool isReconnect) {
    reconnect_support_ = isReconnect;

    ENUM_SQL_CONNECT_TYPE connect_type = mysql_connection_option_.connect_type;
    if (connect_type == TCP_CONNECTION || connect_type == UNIX_DOMAIN_CONNECTION)
    {
        return ConnectImpl();
    }
    
    setErr("Unknown Connect Type, Tcp or Unix-domain is legal");
    return false;
}

bool CAsyncMysql::ConnectImpl() {
    if (mysql_raw_ && (mysql_raw_->status_ == A_ESTABLIZE)) {
        setErr("connection to mysql already established");
        return true;
    }

    MysqlConnectionOption op = mysql_connection_option_;
    // TODO: connection option is legal
    if(!mysql_raw_) {
        mysql_raw_ = (StatMysql*)calloc(1, sizeof(*mysql_raw_));
        if(!mysql_raw_) {
            setErr("malloc mysql connect failed");
            return false;
        }
        mysql_raw_->mysql_ares_ = nullptr;
        mysql_raw_->user_ = nullptr;
        mysql_raw_->passwd_ = nullptr;
        mysql_raw_->ip_ = nullptr;
        mysql_raw_->socket_file_ = nullptr;
        mysql_raw_->charset_ = nullptr;

        mysql_raw_->mysql_ares_ = new AsyncMysqlResult(); //(AsyncMysqlResult*)calloc(1, sizeof(*(mysql_raw_->mysql_ares_)));
        if(!mysql_raw_->mysql_ares_) {
            setErr("malloc mysql async result failed");
            FreeStatMysql(mysql_raw_);
            return false;
        }

        mysql_raw_->user_ = (char*) calloc(op.user.length()+1, sizeof(char));
        if(!mysql_raw_->user_) {
            setErr("calloc memory for user failed");
            FreeStatMysql(mysql_raw_);
            return false;
        }
        memcpy(mysql_raw_->user_, op.user.c_str(), op.user.length());
        mysql_raw_->passwd_ = (char*) calloc(op.password.length()+1, sizeof(char));
        if(!mysql_raw_->passwd_) {
            setErr("calloc memory for passwd failed");
            FreeStatMysql(mysql_raw_);
            return false;
        }
        memcpy(mysql_raw_->passwd_, op.password.c_str(), op.password.length());
        mysql_raw_->ip_ = (char*) calloc(op.ip.length()+1, sizeof(char));
        if(!mysql_raw_->ip_) {
            setErr("calloc memory for ip failed");
            FreeStatMysql(mysql_raw_);
            return false;
        }
        memcpy(mysql_raw_->ip_, op.ip.c_str(), op.ip.length());
        mysql_raw_->charset_ = (char*) calloc(op.charset.length()+1, sizeof(char));
        if(!mysql_raw_->charset_) {
            setErr("calloc memory for charset failed");
            FreeStatMysql(mysql_raw_);
            return false;
        }
        memcpy(mysql_raw_->charset_, op.charset.c_str(), op.charset.length());
        if(mysql_raw_->connect_type_ == ENUM_SQL_CONNECT_TYPE::UNIX_DOMAIN_CONNECTION) {
            mysql_raw_->socket_file_ = (char*) calloc(op.file_path.length()+1, sizeof(char));
            if(!mysql_raw_->socket_file_) {
                setErr("calloc memory for socke file failed");
                FreeStatMysql(mysql_raw_);
                return false;
            }
            memcpy(mysql_raw_->socket_file_, op.file_path.c_str(), op.file_path.length());
        }
    }

    mysql_raw_->mysql_ = new MYSQL;
    mysql_init(mysql_raw_->mysql_);

    // set options
    // connect timeout
    mysql_options(mysql_raw_->mysql_, MYSQL_OPT_CONNECT_TIMEOUT, &(op.connect_timeout_sec));

    mysql_options(mysql_raw_->mysql_, MYSQL_OPT_NONBLOCK, 0);

    // read/write timeout
    mysql_options(mysql_raw_->mysql_, MYSQL_OPT_READ_TIMEOUT, &(op.timeout_sec));
    mysql_options(mysql_raw_->mysql_, MYSQL_OPT_WRITE_TIMEOUT, &(op.timeout_sec));
    // charset
    mysql_options(mysql_raw_->mysql_, MYSQL_SET_CHARSET_NAME, op.charset.c_str());
    /*
	 * USE result, no difference from STORE result for small result set, but
	 * for large result set, USE can use the result rows before all are recved,
	 * and also, extra copying of result data is avoided.
	 * */
	int use_res = 1;
	mysql_options(mysql_raw_->mysql_, MYSQL_OPT_USE_RESULT, &use_res);
	// Never reconnect, because that messes up txnal status.
	bool reconnect = 0;
	mysql_options(mysql_raw_->mysql_, MYSQL_OPT_RECONNECT, &reconnect);

    bool disable_ssl = 0;
    mysql_options(mysql_raw_->mysql_, MYSQL_OPT_SSL_ENFORCE, &disable_ssl);

    mysql_raw_->mysql_rd_timeout_ = op.timeout_sec;
    mysql_raw_->mysql_wr_timeout_ = op.timeout_sec;
    mysql_raw_->mysql_timeout_ = op.connect_timeout_sec;
    //mysql_raw_->charset_ = op.charset;
    mysql_raw_->mysql_manager_ = mysql_manager_;
    mysql_raw_->reconnect_ = 0;
    mysql_raw_->port_ = op.port_num;
    //mysql_raw_->host_ = op.ip +"_"+ op.port_str;
    mysql_raw_->connect_type_ = op.connect_type;
    //if(mysql_raw_->connect_type_ == ENUM_SQL_CONNECT_TYPE::UNIX_DOMAIN_CONNECTION)
    //    mysql_raw_->socket_file_ = op.file_path;
    mysql_raw_->err_ = 0;
    mysql_raw_->mysql_fd_ = -1;
    UT_LIST_INIT(mysql_raw_->pending_sql_);
    mysql_raw_->status_ = A_UNINITIALIZE;
    mysql_raw_->mysql_ares_->SetMysqlHost(op.ip +"_"+ op.port_str);
    
    //async connect mysql
    int status;
    if(mysql_raw_->connect_type_ == ENUM_SQL_CONNECT_TYPE::TCP_CONNECTION)
        status = mysql_real_connect_start(&mysql_raw_->ret_, mysql_raw_->mysql_, 
                op.ip.c_str(), mysql_raw_->user_, mysql_raw_->passwd_,
                         NULL, op.port_num, NULL, CLIENT_MULTI_STATEMENTS | 0);
    else
        status = mysql_real_connect_start(&mysql_raw_->ret_, mysql_raw_->mysql_, 
                NULL, mysql_raw_->user_, mysql_raw_->passwd_,
                         NULL, 0, mysql_raw_->socket_file_, CLIENT_MULTI_STATEMENTS | 0);
    if (status)
        /* Wait for connect to complete. */
        mysql_raw_->mysql_manager_->AddWatch(mysql_raw_, status);
    else {
        if(!mysql_raw_->ret_) {
            mysql_raw_->err_ = mysql_errno(mysql_raw_->mysql_);
            setErr("connect mysql failed, errno: %d\n", mysql_raw_->err_);
            mysql_raw_->status_ = A_CONNECTCERR;  //connect mysql failed, need retry;
        } else
            mysql_raw_->status_ = A_ESTABLIZE;
        
    }

    return true;
}

bool CAsyncMysql::CheckIsConnected() {
    if (mysql_raw_ && (mysql_raw_->status_ == A_UNINITIALIZE || 
                mysql_raw_->status_ == A_ESTABLIZE)) {
        if(mysql_raw_->status_ == A_CONNECTCLOSE)
            return false;
        return true;
    }
        
    if (reconnect_support_) {
        if(!mysql_raw_)
            return Connect();
        
        if(mysql_raw_->status_ == A_CONNECTERR || mysql_raw_->status_ == A_CONNECTCERR) {
            mysql_raw_->reconnect_ = 1;
            do {
                SqlCallFn* sf = NULL; 
                sf = UT_LIST_GET_FIRST(mysql_raw_->pending_sql_);
                if(sf && sf->row_cb_) {
                    //snprintf(mysql_raw_->desc_, mysql_desc_len, "%s", mysql_error(mysql_raw_->mysql_));
                    mysql_raw_->mysql_ares_->SetMysqlErr(mysql_error(mysql_raw_->mysql_));
                    mysql_raw_->mysql_ares_->SetMysqlErrno(mysql_raw_->err_);
                    sf->row_cb_(mysql_raw_->mysql_ares_, sf->arg_);
                }
                RemovePendListFont(mysql_raw_);
            } while(GetPendListLen(mysql_raw_) > 0);
            mysql_raw_->mysql_ares_->Clear();
            if(mysql_raw_->status_ ==  A_CONNECTERR)
                Close();
            else 
                Reconnect(mysql_raw_);
            return true;
        }
    }
    
    setErr("MySQL connection is not established and reconnect is disabled");
    return false;
}

// sql_stmt: select / set / DDL ,return 0 means success, other than failed
//           result_set hold the retrived data
// sql_stmt: update / delete / insert , return >0 , 0 , <0 respectively,
// successfuly, no effect or  failed
//
// This function support reconnect if set the flag
//
int CAsyncMysql::ExecuteQuery(const char *sql_stmt, AMysqlRowCB row_cb, void* arg, bool dml_sql) {
#define QUERY_FAILD -1
    KlWrapGuard<KlWrapMutex> guard(mutex_);
    if(strlen(sql_stmt) < 1)
        return QUERY_FAILD;

    // if reconnect flag is set, will do connect()
    if (!CheckIsConnected()) {
        return QUERY_FAILD;
    }

    //mysql_raw_->mysql_ares_->Clear();
    //int count = GetPendListLen(mysql_raw_);
    int resend = SqlInsertToPendList(mysql_raw_, sql_stmt, row_cb, arg, dml_sql);
    if(resend == -1)
        return QUERY_FAILD;

    if(mysql_raw_->status_ != A_ESTABLIZE ) {
        return 0;
    } else {
        if(resend)
            return 0;
    }

    SendNextPendingSql(mysql_raw_);
    return 0;
}

void CAsyncMysql::Close() {
    if (mysql_raw_) {
        //mysql_close(&mysql_raw_->mysql);
        int status= mysql_close_start(mysql_raw_->mysql_);
        if(status) {
            mysql_raw_->status_ = A_CONNECTCLOSE;
            mysql_raw_->mysql_manager_->AddWatch(mysql_raw_, status);
            return;
        }
        
        //mysql conn close ok
        if(!mysql_raw_->reconnect_) {
            do {
                RemovePendListFont(mysql_raw_);
            } while(GetPendListLen(mysql_raw_) > 0);
            mysql_raw_->mysql_manager_->DelWatch(mysql_raw_);
            FreeStatMysql(mysql_raw_);
        } else {
            if(mysql_raw_->mysql_) {
                delete mysql_raw_->mysql_;
                mysql_raw_->mysql_ = nullptr;
            }

            Connect();
        }
    }
    return;
}

void CRowResult::init(const MYSQL_ROW& row, unsigned long* lens, uint32_t field_num_) {
    num_ = field_num_;
    if(num_ == 0)
        return;
    
    result_ = new charptr[num_ * 2];
    lens_ = result_ + num_;
    for(uint32_t i=0; i < field_num_; i++) {
        int len = lens[i];
        if(row[i] == NULL) {
            result_[i] = (char*) "NULL";
            lens_[i] = (char*) 4;
        } else {
            result_[i] = new char[len+1];
            memcpy(result_[i], row[i], len);
            result_[i][len] = 0;
            lens_[i] = (char*)(&len);
        }
    }
}

CRowResult::~CRowResult() {
    if(result_) {
        for(uint32_t i=0; i<num_; i++) {
            if(result_[i] != (char*) "NULL") {
                delete[] result_[i];
            }
        }
        delete[] result_;
    }
}

bool AsyncMysqlResult::ParseResult(MYSQL_RES *result, const MYSQL_ROW& row) {
    field_num_ = mysql_num_fields(result);
    MYSQL_FIELD *fields = mysql_fetch_fields(result);
    for(uint32_t i=0; i<field_num_; i++) {
        si_map_[fields[i].name] = i;
        is_map_[i] = fields[i].name;
    }

    unsigned long* lens = mysql_fetch_lengths(result);
    CRowResult* rowresult = new CRowResult(si_map_);
    rowresult->init(row, lens, field_num_);
    row_vec_.emplace_back(rowresult);

    return true;
}

void AsyncMysqlResult::Clear() {
    field_num_ = 0;
    affected_rows_ = 0;
    si_map_.clear();
    is_map_.clear();
    mysql_errno_ = 0;

    for(auto it : row_vec_) {
        if(it)
            delete it;
    }
    
    row_vec_.clear();
}

void CAsyncMysqlManager::ParseMysqlResult(StatMysql* cmysql) {

    cmysql->result_= mysql_use_result(cmysql->mysql_);
    if(cmysql->result_ == nullptr) {
        // maybe the statement dose not generate result set
        // insert/update/delete/set ..
        int ret = 0;
        int affect_rows = mysql_affected_rows(cmysql->mysql_);
        if (affect_rows == (uint64_t)~0) {
            // error occour
            //snprintf(cmysql->desc_, mysql_desc_len, "%s", mysql_error(cmysql->mysql_));
            ret = mysql_errno(cmysql->mysql_);
        } else {
            int status = mysql_next_result(cmysql->mysql_);
            if(status == 0) {
                cmysql->mysql_ares_->AddAffectedRows(affect_rows);
                mysql_free_result(cmysql->result_);
                cmysql->result_ = nullptr;
                ParseMysqlResult(cmysql);
                return;
            }

            if(status > 0) {
                //snprintf(cmysql->desc_, mysql_desc_len, "%s", mysql_error(cmysql->mysql_));
                ret = mysql_errno(cmysql->mysql_);
            } else {
                //snprintf(cmysql->desc_, mysql_desc_len, "fetch row done");
                cmysql->mysql_ares_->AddAffectedRows(affect_rows);
            }
        }
        
        SqlCallFn* sf = UT_LIST_GET_FIRST(cmysql->pending_sql_);
        if(sf && sf->row_cb_) {
            if(ret)
                cmysql->mysql_ares_->SetMysqlErr(mysql_error(cmysql->mysql_));
            else 
                cmysql->mysql_ares_->SetMysqlErr("ok");
            cmysql->mysql_ares_->SetMysqlErrno(ret);
            sf->row_cb_(cmysql->mysql_ares_, sf->arg_);
            cmysql->mysql_ares_->Clear();
        }
        bool rb = RemovePendListFont(cmysql);
        if(ret) {
            if(!rb)
                RollbackSendSql(cmysql);
        }
    } else {
        int status = mysql_fetch_row_start(&cmysql->row_, cmysql->result_);
        if(status) {
            cmysql->status_ = A_FETCHROW;
            cmysql->mysql_manager_->AddWatch(cmysql, status);
            return;
        } else {
            SqlCallFn* sf = UT_LIST_GET_FIRST(cmysql->pending_sql_);
            if(cmysql->row_) {
                if(sf) 
                    cmysql->mysql_ares_->ParseResult(cmysql->result_, cmysql->row_);
                //snprintf(cmysql->desc_, mysql_desc_len, "fetch row");
                //sf->row_cb_(cmysql->mysql_ares_, cmysql->desc_, cmysql->host, 0);
                RetryFetchRow(cmysql);
                return;
            } else {
                if(cmysql->err_) {
                    if(ServerGoneLostHandle(cmysql))
                        return;

                    if(sf && sf->row_cb_) {
                        //snprintf(cmysql->desc_, mysql_desc_len, "%s", mysql_error(cmysql->mysql_));
                        cmysql->mysql_ares_->SetMysqlErr(mysql_error(cmysql->mysql_));
                        cmysql->mysql_ares_->SetMysqlErrno(cmysql->err_);
                        sf->row_cb_(cmysql->mysql_ares_, sf->arg_);
                        cmysql->mysql_ares_->Clear();
                    }
                    
                    mysql_free_result(cmysql->result_);
                    cmysql->result_ = nullptr;
                    if(!RemovePendListFont(cmysql))
                        RollbackSendSql(cmysql);
                } else {
                    int ret = mysql_next_result(cmysql->mysql_);
                    if(ret == 0) {
                        mysql_free_result(cmysql->result_);
                        cmysql->result_ =  nullptr;
                        ParseMysqlResult(cmysql);
                        return;
                    }

                    if(ret > 0) {
                        if(sf && sf->row_cb_) {
                            //snprintf(cmysql->desc_, mysql_desc_len, "%s", mysql_error(cmysql->mysql_));
                            cmysql->mysql_ares_->SetMysqlErr(mysql_error(cmysql->mysql_));
                            cmysql->mysql_ares_->SetMysqlErrno(mysql_errno(cmysql->mysql_));
                            sf->row_cb_(cmysql->mysql_ares_, sf->arg_); 
                            cmysql->mysql_ares_->Clear();
                        }
                        
                        if(!RemovePendListFont(cmysql))
                            RollbackSendSql(cmysql);
                    } else {
                        if(sf && sf->row_cb_) {
                            //snprintf(cmysql->desc_, mysql_desc_len, "fetch row done");
                            cmysql->mysql_ares_->SetMysqlErr("ok");
                            cmysql->mysql_ares_->SetMysqlErrno(0);
                            sf->row_cb_(cmysql->mysql_ares_, sf->arg_); 
                            cmysql->mysql_ares_->Clear();
                        }
                        RemovePendListFont(cmysql);
                    }
                }
            }
        }
    }

    cmysql->status_ = A_ESTABLIZE;
    mysql_free_result(cmysql->result_);
    cmysql->result_ = nullptr;
    SendNextPendingSql(cmysql);
}

void CAsyncMysqlManager::RetryFetchRow(StatMysql* cmysql) {
    int status = mysql_fetch_row_start(&cmysql->row_, cmysql->result_);
    if(status) {
        cmysql->status_ = A_FETCHROW;
        cmysql->mysql_manager_->AddWatch(cmysql, status);
        return;
    } else {
        SqlCallFn* sf = UT_LIST_GET_FIRST(cmysql->pending_sql_);
        if(cmysql->row_) {
            if(sf) 
                cmysql->mysql_ares_->ParseResult(cmysql->result_, cmysql->row_);
            //snprintf(cmysql->desc_, mysql_desc_len, "fetch row");
            //sf->row_cb_(cmysql->mysql_ares_, cmysql->desc_, cmysql->host, 0);
            RetryFetchRow(cmysql);
            return;
        } else {
            if(cmysql->err_) { 
                if(ServerGoneLostHandle(cmysql))
                    return;

                if(sf && sf->row_cb_) {
                    //snprintf(cmysql->desc_, mysql_desc_len, "%s", mysql_error(cmysql->mysql_));
                    cmysql->mysql_ares_->SetMysqlErr(mysql_error(cmysql->mysql_));
                    cmysql->mysql_ares_->SetMysqlErrno(cmysql->err_);
                    sf->row_cb_(cmysql->mysql_ares_, sf->arg_);
                    cmysql->mysql_ares_->Clear();
                }
                
                //mysql_free_result(cmysql->result_);
                if(!RemovePendListFont(cmysql))
                    RollbackSendSql(cmysql);
            } else {
                int ret = mysql_next_result(cmysql->mysql_);
                if(ret == 0) {
                    mysql_free_result(cmysql->result_);
                    cmysql->result_ = nullptr;
                    ParseMysqlResult(cmysql);
                    return;
                }

                if(ret > 0) {
                    if(sf && sf->row_cb_) {
                        //snprintf(cmysql->desc_, mysql_desc_len, "%s", mysql_error(cmysql->mysql_));
                        cmysql->mysql_ares_->SetMysqlErr(mysql_error(cmysql->mysql_));
                        cmysql->mysql_ares_->SetMysqlErrno(mysql_errno(cmysql->mysql_));
                        sf->row_cb_(cmysql->mysql_ares_, sf->arg_);
                        cmysql->mysql_ares_->Clear();
                    }
                    
                    if(!RemovePendListFont(cmysql))
                        RollbackSendSql(cmysql);
                } else {
                    if(sf && sf->row_cb_) {
                        //snprintf(cmysql->desc_, mysql_desc_len, "fetch row done");
                        cmysql->mysql_ares_->SetMysqlErr("ok");
                        cmysql->mysql_ares_->SetMysqlErrno(0);
                        sf->row_cb_(cmysql->mysql_ares_, sf->arg_);
                        cmysql->mysql_ares_->Clear();
                    }
                    RemovePendListFont(cmysql);
                }
            }
        }
    }
    cmysql->status_ = A_ESTABLIZE;
    mysql_free_result(cmysql->result_);
    cmysql->result_ = nullptr;
    SendNextPendingSql(cmysql);
}

void CAsyncMysqlManager::MysqlSocketResult(StatMysql* cmysql, uint32_t event) {
    int status;
    int step = static_cast<int>(cmysql->status_);
    switch(step) {
        case 0:    // connect mysql step
        {
            status = mysql_real_connect_cont(&cmysql->ret_, cmysql->mysql_, MysqlStatus(event));
            if(status)
                cmysql->mysql_manager_->AddWatch(cmysql, status);
            else {
                if(!cmysql->ret_) {
                    //setErr("connect mysql failed, errno: %d", mysql_errno(&cmysql->mysql));
                    //cmysql->mysql_manager_->NotifyQuit();
                    cmysql->err_ = mysql_errno(cmysql->mysql_);
                    cmysql->status_ = A_CONNECTCERR; 
                        
                    do {
                        SqlCallFn* sf = UT_LIST_GET_FIRST(cmysql->pending_sql_);
                        if(sf && sf->row_cb_) {
                            //snprintf(cmysql->desc_, mysql_desc_len, "%s", mysql_error(cmysql->mysql_));
                            cmysql->mysql_ares_->SetMysqlErr(mysql_error(cmysql->mysql_));
                            cmysql->mysql_ares_->SetMysqlErrno(cmysql->err_);
                            sf->row_cb_(cmysql->mysql_ares_, sf->arg_);
                        }
                        RemovePendListFont(cmysql);
                    } while(GetPendListLen(cmysql) > 0);
                    cmysql->mysql_manager_->DelWatch(cmysql);
                    cmysql->mysql_ares_->Clear();
                } else {
                    cmysql->status_ = A_ESTABLIZE;
                    cmysql->mysql_ares_->Clear();
                    SendNextPendingSql(cmysql);
                }  
            }
        }
        break;

        case 1: // execute sql wait result 
        {
            if(GetPendListLen(cmysql) == 0)
                return;
            
            int err_;
            status = mysql_real_query_cont(&err_, cmysql->mysql_, MysqlStatus(event));
            if(status) {
                cmysql->mysql_manager_->AddWatch(cmysql, status);
            } else {
                if(err_) {
                    if(ServerGoneLostHandle(cmysql))
                        return;
                    
                    //mysql_free_result(cmysql->result_);
                    SqlCallFn* sf = UT_LIST_GET_FIRST(cmysql->pending_sql_);
                    if(sf && sf->row_cb_) {
                        //snprintf(cmysql->desc_, mysql_desc_len, "%s", mysql_error(cmysql->mysql_));
                        cmysql->mysql_ares_->SetMysqlErr(mysql_error(cmysql->mysql_));
                        cmysql->mysql_ares_->SetMysqlErrno(cmysql->err_);
                        sf->row_cb_(cmysql->mysql_ares_, sf->arg_);
                        cmysql->mysql_ares_->Clear();
                    }
                    
                    if(!RemovePendListFont(cmysql))
                        RollbackSendSql(cmysql);
                    SendNextPendingSql(cmysql);
                } else {
                    if(cmysql->status_ == A_CONNECTCERR || cmysql->status_ == A_CONNECTERR)
                        return;
                    cmysql->mysql_manager_->ParseMysqlResult(cmysql);
                } 
            }
        }
        break;

        case 2: // fetch row step
        {
            status= mysql_fetch_row_cont(&cmysql->row_, cmysql->result_, MysqlStatus(event));
            if(status) {
                cmysql->mysql_manager_->AddWatch(cmysql, status);
            } else {
                SqlCallFn* sf = UT_LIST_GET_FIRST(cmysql->pending_sql_);
                if(cmysql->row_) {
                    if(sf) 
                        cmysql->mysql_ares_->ParseResult(cmysql->result_, cmysql->row_);
                    //snprintf(cmysql->desc_, mysql_desc_len, "fetch row");
                    //sf->row_cb_(cmysql->mysql_ares_, cmysql->desc_, cmysql->host, 0);
                    cmysql->mysql_manager_->RetryFetchRow(cmysql);
                    return;
                } else {
                    if(mysql_errno(cmysql->mysql_)) {
                        if(ServerGoneLostHandle(cmysql))
                            return;

                        //mysql_free_result(cmysql->result_);
                        if(sf && sf->row_cb_) {
                            //snprintf(cmysql->desc_, mysql_desc_len, "%s", mysql_error(cmysql->mysql_));
                            cmysql->mysql_ares_->SetMysqlErr(mysql_error(cmysql->mysql_));
                            cmysql->mysql_ares_->SetMysqlErrno(mysql_errno(cmysql->mysql_));
                            sf->row_cb_(cmysql->mysql_ares_, sf->arg_);
                            cmysql->mysql_ares_->Clear();
                        }
                        
                        if(!RemovePendListFont(cmysql))
                            RollbackSendSql(cmysql);
                    } else {
                        if(cmysql->status_ == A_CONNECTCERR || cmysql->status_ == A_CONNECTERR)
                            return;

                        int ret = mysql_next_result(cmysql->mysql_);
                        if(ret == 0) {
                            mysql_free_result(cmysql->result_);
                            cmysql->result_ = nullptr;
                            cmysql->mysql_manager_->ParseMysqlResult(cmysql);
                            return;
                        }

                        if(ret > 0) {
                            if(sf && sf->row_cb_) {
                                //snprintf(cmysql->desc_, mysql_desc_len, "%s", mysql_error(cmysql->mysql_));
                                cmysql->mysql_ares_->SetMysqlErr(mysql_error(cmysql->mysql_));
                                cmysql->mysql_ares_->SetMysqlErrno(mysql_errno(cmysql->mysql_));
                                sf->row_cb_(cmysql->mysql_ares_, sf->arg_);
                                cmysql->mysql_ares_->Clear();
                            }
                            
                            if(!RemovePendListFont(cmysql))
                                RollbackSendSql(cmysql);
                        } else {
                            if(sf && sf->row_cb_) {
                                //snprintf(cmysql->desc_, mysql_desc_len, "fetch row done");
                                cmysql->mysql_ares_->SetMysqlErr("ok");
                                cmysql->mysql_ares_->SetMysqlErrno(0);
                                sf->row_cb_(cmysql->mysql_ares_, sf->arg_);
                                cmysql->mysql_ares_->Clear();
                            }
                            RemovePendListFont(cmysql);
                        }
                    }
                }

                cmysql->status_ = A_ESTABLIZE;
                mysql_free_result(cmysql->result_);
                cmysql->result_ = nullptr;
                SendNextPendingSql(cmysql);
            }
        }
        break;

        case 3: // close mysql step
        {
            status = mysql_close_cont(cmysql->mysql_, MysqlStatus(event));
            if(status) {
                cmysql->mysql_manager_->AddWatch(cmysql, status);
            } else {
                if(!cmysql->reconnect_) {
                    do {
                        RemovePendListFont(cmysql);
                    } while(GetPendListLen(cmysql) > 0);
                    cmysql->mysql_manager_->DelWatch(cmysql);
                    FreeStatMysql(cmysql);
                } else {
                    Reconnect(cmysql);
                }
            }
        }
        break;

        default:
            //setErr("stat_mysql status:%d is not defined", cmysql->status);
        break;
    }
    
    return;
}

int CAsyncMysqlManager::AddWatch(StatMysql* cmysql, int status) {
    uint32_t wait_event= 0;

    if (status & MYSQL_WAIT_READ) {
        wait_event |= EPOLLIN;
    }

    if (status & MYSQL_WAIT_WRITE) {
        wait_event |= EPOLLOUT;
    }

    if (wait_event) {
        KlWrapGuard<KlWrapMutex> guard(mux_);
        if(cmysql->mysql_fd_ == -1)
            cmysql->mysql_fd_ = mysql_get_socket(cmysql->mysql_);
        struct epoll_event ev;
        ev.events = wait_event | EPOLLONESHOT;
        ev.data.ptr = cmysql;

        int ret = epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, cmysql->mysql_fd_, &ev);
        if(ret == -1) {
            if(errno == EEXIST) {
                ret = epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, cmysql->mysql_fd_, &ev);
                if(ret == -1) {
                    //syslog(Logger::ERROR, "epoll ctl mod fd: %d failed: %d", cmysql->mysql_fd_, errno);
                    return -1;
                }
                return 0;
            }
            //syslog(Logger::ERROR, "epoll ctl add fd: %d failed: %d", cmysql->mysql_fd_, errno);
            return -1;
        }
        if(std::find(mysql_vec_.begin(), mysql_vec_.end(), cmysql) == mysql_vec_.end())
            mysql_vec_.push_back(cmysql);
    }
    
    return 0;
}

void CAsyncMysqlManager::MysqlSocketUnormal(StatMysql* cmysql) {
    if(cmysql->status_ == 0) 
        cmysql->status_ = A_CONNECTCERR;
    else 
        cmysql->status_ = A_CONNECTERR;
    do {
        SqlCallFn* sf = UT_LIST_GET_FIRST(cmysql->pending_sql_);
        if(sf && sf->row_cb_) {
            //snprintf(cmysql->desc_, mysql_desc_len, "%s", mysql_error(cmysql->mysql_));
            cmysql->mysql_ares_->SetMysqlErr("mysql read/write socket abnormal");
            cmysql->mysql_ares_->SetMysqlErrno(ASYNC_MYSQL_CONNECT_EPOLL_ERR);
            sf->row_cb_(cmysql->mysql_ares_, sf->arg_);
        }
        RemovePendListFont(cmysql);
    } while(GetPendListLen(cmysql) > 0);
    cmysql->mysql_manager_->DelWatch(cmysql);
    cmysql->mysql_ares_->Clear();
}

void CAsyncMysqlManager::MysqlSocketTimeout() {
    KlWrapGuard<KlWrapMutex> guard(mux_);

    uint64_t nowts = GetNowTimestamp();
    std::vector<StatMysql*> del_vec;
    for(auto cmysql : mysql_vec_) {
        SqlCallFn* sf = UT_LIST_GET_FIRST(cmysql->pending_sql_);
        if(!sf) {
            RemovePendListFont(cmysql);
            continue;
        }

        if(labs(nowts - sf->send_ts_) > cmysql->mysql_rd_timeout_) {
            del_vec.push_back(cmysql);
        }
    }

    for(auto cmysql : del_vec) {
        do {
            SqlCallFn* sf = UT_LIST_GET_FIRST(cmysql->pending_sql_);
            if(sf && sf->row_cb_) {
                cmysql->mysql_ares_->SetMysqlErr("mysql read/write socket timeout");
                cmysql->mysql_ares_->SetMysqlErrno(ASYNC_MYSQL_CONNECT_EPOLL_TIMEOUT);
                sf->row_cb_(cmysql->mysql_ares_, sf->arg_);
            }
            RemovePendListFont(cmysql);
        } while(GetPendListLen(cmysql) > 0);
        cmysql->mysql_manager_->DelWatchInn(cmysql);
        cmysql->mysql_ares_->Clear();
    }
}

void CAsyncMysqlManager::CheckMysqlSocketTimeout(void *arg) {
    CAsyncMysqlManager* mysql_mgr = static_cast<CAsyncMysqlManager*>(arg);
    mysql_mgr->GetThreadPool()->commit([mysql_mgr]{
        mysql_mgr->MysqlSocketTimeout();
    });
}

void CAsyncMysqlManager::DelWatchInn(StatMysql* cmysql) {
    if(cmysql->mysql_fd_ != -1) {
        struct epoll_event ev;
        int ret = epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, cmysql->mysql_fd_, &ev);
        if(ret == -1) {
            if(errno != ENOENT) {
                //syslog(Logger::ERROR, "epoll ctl del fd: %d failed: %d", cmysql->mysql_fd_, errno);
            }    
        }
    }

    std::string host = string_sprintf("%s_%d", cmysql->ip_, cmysql->port_);
    size_t pos = -1;
    for(size_t i=0; i < mysql_vec_.size();i++) {
        std::string vec_host = string_sprintf("%s_%d", mysql_vec_[i]->ip_, mysql_vec_[i]->port_);
        if(vec_host == host) {
            pos = i;
            break;
        }
    }
    if(pos != -1) {
        mysql_vec_.erase(mysql_vec_.begin() + pos);
    }
}

void CAsyncMysqlManager::DelWatch(StatMysql* cmysql) {
    //int fd = mysql_get_socket(cmysql->mysql_);
    KlWrapGuard<KlWrapMutex> guard(mux_);
    DelWatchInn(cmysql);
}

int CAsyncMysqlManager::run() {
    if(!ep_pool_) {
        ep_pool_ = new CThreadPool(4);
        assert(ep_pool_);
    }

    epoll_fd_ = epoll_create(EPOLL_FD_CAPACITY);
    if(epoll_fd_ == -1) {
        //syslog(Logger::ERROR, "epoll create failed: %d", errno);
        return 1;
    }

    int ready;
    struct epoll_event evlist[EPOLL_FD_CAPACITY];
    while(m_state) {
        //set epoll wait timeout 5s for cluster_mgr m/s switch
        ready = epoll_wait(epoll_fd_, evlist, EPOLL_FD_CAPACITY, 5000);   
        if(ready == -1) {
            if(errno == EINTR)
                continue;
            else {
                //syslog(Logger::ERROR, "epoll wait failed: %d", errno);
                return 1;
            }
        }

        if(ready == 0) { //timeout handle
            if(!m_state)  
                return 0;
        }

        for(int i=0; i<ready; i++) {
            struct epoll_event ev = evlist[i];
            int events = ev.events;
            StatMysql* cmysql = static_cast<StatMysql*>(ev.data.ptr);
            if(events & EPOLLERR) {
                ep_pool_->commit([this, cmysql, events]{
                    this->MysqlSocketUnormal(cmysql);
                });
            }

            if(events & EPOLLIN) {
                ep_pool_->commit([this, cmysql, events]{
                    this->MysqlSocketResult(cmysql, events);
                });
            }

            if(events & EPOLLOUT) {
                ep_pool_->commit([this, cmysql, events]{
                    this->MysqlSocketResult(cmysql, events);
                });
            }
        }
    }

    return 0;
}

}