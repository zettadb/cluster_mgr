/*
   Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef _CLUSTER_MGR_ASYNC_MYSQL_H_
#define _CLUSTER_MGR_ASYNC_MYSQL_H_

#include <unistd.h>
#include <sys/epoll.h>
#include <vector>
#include <map>
#include "query_list.h"
#include "zettalib/errorcup.h"
#include "zettalib/zthread.h"
#include "zettalib/op_mysql.h"
#include "thread_pool.h"
#include "util_func/kl_mutex.h"

namespace kunlun 
{

#define EPOLL_FD_CAPACITY 1024

class CAsyncMysqlManager;
class AsyncMysqlResult;

typedef void(*AMysqlRowCB)(AsyncMysqlResult* res, void* arg);
/*
*
*/
typedef enum AsyncConnStat {
    A_UNINITIALIZE,
    A_ESTABLIZE,
    A_FETCHROW,
    A_CONNECTCLOSE,
    A_CONNECTERR,
    A_CONNECTCERR,
} AsyncConnStat;

//const int mysql_desc_len = 4096;

typedef struct sql_callfn {
    char* query_;
    AMysqlRowCB row_cb_;
    void* arg_;
    int send_;
    bool dml_sql_;
    uint64_t send_ts_;
    UT_LIST_NODE_T(struct sql_callfn) sql_cb_list;
} SqlCallFn;

typedef UT_LIST_BASE_NODE_T(SqlCallFn) SqlCallFn_t;

/*
* mysql conn status
*/
typedef struct stat_mysql {
    AsyncConnStat status_;
    MYSQL *mysql_;
    MYSQL_RES *result_;
    MYSQL *ret_;
    int err_;
    MYSQL_ROW row_;
    //std::string host_;
    char* ip_;
    int port_;
    CAsyncMysqlManager* mysql_manager_;
    AsyncMysqlResult* mysql_ares_;
    SqlCallFn_t pending_sql_;
    AtomicLock list_lock_;
    int mysql_timeout_;
    int mysql_rd_timeout_;
    int mysql_wr_timeout_;
    ENUM_SQL_CONNECT_TYPE connect_type_;
    //std::string socket_file_;
    char* socket_file_;
    //std::string charset_;
    char* charset_;
    char* user_;
    char* passwd_;
    int reconnect_;
    int mysql_fd_;
} StatMysql;

/*
* for get select result
*/
class CRowResult {
public:
    CRowResult(const std::map<std::string, int>& str_int_map) :
        result_(NULL), lens_(NULL), si_map_(str_int_map) {}
    virtual ~CRowResult();
    void init(const MYSQL_ROW& row, unsigned long* lens, uint32_t field_num);

    typedef char * charptr;
    char * operator[] (const char* name) {
        std::map<std::string, int>::const_iterator it = si_map_.find(name);
        if(it == si_map_.end())
            return (char*) "NULL";

        int field = it->second;
        return result_[field];
    }

    char * operator[] (size_t index) {
        if(index >= si_map_.size())
            return (char*) "NULL";
        return result_[index];
    }
    
private:
    CRowResult(const CRowResult& rht) = delete;
    CRowResult & operator = (const CRowResult &rht) = delete;

    char **result_;
    char **lens_;
    const std::map<std::string, int>& si_map_;
    uint32_t num_;
};

class AsyncMysqlResult {
public:
    AsyncMysqlResult() : affected_rows_(0) {}
    virtual ~AsyncMysqlResult() { Clear(); }

    bool ParseResult(MYSQL_RES *result, const MYSQL_ROW& row);

    void Clear();

    int GetAffectedRows() const {
        return affected_rows_;
    }
    void SetAffectedRows(int affected_rows) {
        affected_rows_ = affected_rows;
    }
    void AddAffectedRows(int affected_rows) {
        affected_rows_ += affected_rows;
    }

    uint32_t GetFieldNum() const {
        return field_num_;
    }

    CRowResult& operator[] (uint32_t i) {
        assert(i < row_vec_.size());
        return *(row_vec_[i]);
    }

    uint32_t GetNumRows() const {
        return row_vec_.size();
    }

    int GetMysqlErrno() const {
        return mysql_errno_;
    }
    void SetMysqlErrno(int mysql_errno) {
        mysql_errno_ = mysql_errno;
    }
    const std::string GetMysqlErr() const {
        return mysql_err_;
    }
    void SetMysqlErr(const std::string& mysql_err) {
        mysql_err_ = mysql_err;
    }
    const std::string& GetMysqlHost() const {
        return host_;
    }
    void SetMysqlHost(const std::string& host) {
        host_ = host;
    }
private:
    /*
    * for insert/update/delete
    * get affected row nums
    */
    int affected_rows_;

    /*
    * for select sql
    */
    std::vector<CRowResult*> row_vec_;
    uint32_t field_num_;
    std::map<std::string, int> si_map_;
    std::map<int, std::string> is_map_;
    /*
    * mysql error
    */
   int mysql_errno_;
   std::string mysql_err_;
   std::string host_;
};

/*
* async mysql api 
*/
class CAsyncMysql : public ErrorCup {
public:
    explicit CAsyncMysql(CAsyncMysqlManager* mysql_manager, const MysqlConnectionOption& option) :
        mysql_raw_(nullptr), reconnect_support_(true), mysql_connection_option_(option),
        mysql_manager_(mysql_manager) {}

    virtual ~CAsyncMysql() { Close(); }

public:
    bool Connect(bool isReconnect = true);

    // sql_stmt: select / set / DDL ,return 0 means success, other than failed
    //           result_set hold the retrived data
    // sql_stmt: update / delete / insert , return >0 , 0 , <0 respectively,
    // successfuly, no effect or  failed
    int ExecuteQuery(const char *sql_stmt, AMysqlRowCB row_cb, void* arg, bool dml_sql=true);

    AsyncConnStat GetMysqlConnStat() {
        return (mysql_raw_->status_);
    }

    bool MysqlConnStatIsOk() {
        AsyncConnStat stat = mysql_raw_->status_;
        return (stat == A_ESTABLIZE || stat == A_FETCHROW);
    }

    std::string GetMysqlConnHost(); 

    void SetMysqlReadTimeout(int timeout) {
        mysql_raw_->mysql_rd_timeout_ = timeout;
    }

private:
    // will do the reconnect if the reconnect_support_ flag is set
    bool CheckIsConnected();
    void Close();
    void CloseImp();
    bool ConnectImpl();

private:
    StatMysql *mysql_raw_;
    bool reconnect_support_;
    MysqlConnectionOption mysql_connection_option_;
    CAsyncMysqlManager* mysql_manager_;
    KlWrapMutex mutex_;
};

/*
* async mysql manager based on libevent listen mysql socket message
* so must start CAsyncMysqlManger before using CAsyncMysql
*/
class CAsyncMysqlManager : public ZThread, public ErrorCup {
public:
    CAsyncMysqlManager() : ep_pool_(nullptr) {}

    virtual ~CAsyncMysqlManager() {
        if(ep_pool_)
            delete ep_pool_;
    }

    int run();

    /*
    * add watch fd to libevent
    */
    int AddWatch(StatMysql* cmysql, int status);
    /*
    *
    */
    void DelWatch(StatMysql* cmysql);

    void DelWatchInn(StatMysql* cmysql);
    /*
    * Parse mysql socket result
    */
    void ParseMysqlResult(StatMysql* cmysql);
    
    /*
    * Get more row result.
    */
    void RetryFetchRow(StatMysql* cmysql);
    void MysqlSocketResult(StatMysql* cmysql, uint32_t event);
    void MysqlSocketUnormal(StatMysql* cmysql);
    void MysqlSocketTimeout();
    CThreadPool *GetThreadPool() {
        return ep_pool_;
    }
public:
    static void CheckMysqlSocketTimeout(void *arg);
    
private:
    int epoll_fd_;
    //add thread pool for handle event callback
    CThreadPool* ep_pool_;
    std::vector<StatMysql*> mysql_vec_;
    KlWrapMutex mux_;
};

void SendNextPendingSql(StatMysql* cmysql);

}
#endif /*_CLUSTER_MGR_ASYNC_MYSQL_H_*/