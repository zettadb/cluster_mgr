/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#ifndef _CLUSTER_MANAGER_ERROR_CODE_H_
#define _CLUSTER_MANAGER_ERROR_CODE_H_

#include <string>
namespace kunlun
{

#define EERROR_NUM_START (70000)

#define EOFFSET(offset) (EERROR_NUM_START) + (offset)

#define EOK 0
#define EFAIL EOFFSET(1)
#define EIVALID_REQUEST_PROTOCAL EOFFSET(1)
#define EIVALID_RESPONSE_PROTOCAL EOFFSET(2)
#define ENODE_UNREACHEABLE EOFFSET(3)


// for rbr consfailover
#define CF_UNALIVE_SSYNC_NODE EOFFSET(6)
#define CF_SSYNCNUM_NOT_MATCH_FULLSYNC_LEVEL EOFFSET(7)
#define CF_REPORT_RELAYLOG_ROW_NUMS_ERR EOFFSET(8)
#define CF_REPLICATION_STILL_RUNNING EOFFSET(9)
#define CF_CONSFAILOVER_TIMEOUT EOFFSET(10)
#define CF_CONSFAILOVER_APPLYLOG_TIMEOUT EOFFSET(11)
#define CF_MASTER_FULLSYNC_WAITING_TXNS EOFFSET(12)
#define CF_KILL_MYSQLD_BY_RPC EOFFSET(13)
#define CF_MANUAL_NOSWITCH_NODE EOFFSET(14)
#define ASYNC_MYSQL_CONNECT_EPOLL_ERR EOFFSET(15)
#define CF_DB_IN_MAX_CONNECTION EOFFSET(16)
#define CF_DB_IN_HUGE_TRANSACTION EOFFSET(17)
#define CF_DB_IN_INNODB_BLOCK     EOFFSET(18)
#define CF_ASSIGN_HOST_NOT_ALIVE EOFFSET(19)
#define CF_ASSIGN_HOST_NO_MAXIMUM_GTIDS   EOFFSET(20)
#define CF_ASSIGN_HOST_MYSQL_CONNECT_ERR  EOFFSET(21)
#define CF_NOT_ALIVE_SLAVE_IN_DEGRADE  EOFFSET(22)
#define CF_REBUILD_SLAVE_CONNECT_ERR  EOFFSET(23)

//
#define JOB_PARAMS_LOST_SHARD_ID           EOFFSET(24)
#define JOB_PARAMS_LOST_CLUSTER_ID           EOFFSET(25)
#define JOB_PARAMS_LOST_MASTER_HOSTADDR       EOFFSET(26)
#define GET_SHARD_BY_ID_ERROR                 EOFFSET(27)
#define GET_META_SHARD_MASTER_CONNECT_ERROR     EOFFSET(28)
#define METADB_SELECT_MASTER_HOSTADDR_ERROR     EOFFSET(29)
#define MANUAL_SW_GET_MASTER_HOST_PORT_ERROR           EOFFSET(30)
#define METADB_SELECT_ASSIGN_HOSTADDR_ERROR       EOFFSET(31)
#define MANUAL_SW_GET_ASSIGN_HOST_PORT_ERROR      EOFFSET(32)
#define MANUAL_SW_MASTER_HOSTADDR_STATE_ERROR      EOFFSET(33)
#define MANUAL_SW_ASSIGN_HOSTADDR_STATE_ERROR      EOFFSET(34)

#define GET_MANUAL_SW_RESULT_FROM_META_DB_ERROR      EOFFSET(35)
#define JOB_PARAMS_LOST_REBUILD_NODE      EOFFSET(36)
#define JOB_PARAMS_REBUILD_NODE_NOT_HOSTADDR      EOFFSET(37)
#define JOB_PARAMS_REBUILD_NODE_NOT_PORT     EOFFSET(38)
#define JOB_PARAMS_NEED_HDFS_HOST_IN_NEEDBACKUP     EOFFSET(39)
#define REBUILD_NODE_GET_HOSTADDR_STATE_FROM_METADB     EOFFSET(40)
#define REBUILD_NODE_NOT_IN_SHARD                   EOFFSET(41)
#define REBUILD_NODE_GET_PULL_HOST_ERROR                   EOFFSET(42)
#define RB_GET_SHARD_CLUSTER_AND_SHARD_NAME_ERROR           EOFFSET(43)
#define RB_GET_RBHOST_PARAM_FROM_METADB_ERROR           EOFFSET(44)
#define RB_GET_PULLHOST_PARAM_FROM_METADB_ERROR           EOFFSET(45)

#define CONNECT_META_EXECUTE_SQL_ERROR           EOFFSET(46)
#define GET_REBUILD_NODE_RB_NODE_TASK_ERROR           EOFFSET(47)
#define PARSE_GENERAL_JOB_MEMO_JSON_ERROR         EOFFSET(48)

//==== machine request============
#define MACHINE_QUEST_LOST_PARAS_ERROR         EOFFSET(49)
#define MACHINE_QUEST_MISS_MACHINE_TYPE_ERROR         EOFFSET(50)
#define MACHINE_QUEST_MACHINE_TYPE_ERROR            EOFFSET(51)
#define CREATE_MACHINE_MISS_HOSTADDR_ERROR            EOFFSET(52)
#define CREATE_MACHINE_MISS_RACKID_ERROR            EOFFSET(53)
#define CREATE_MACHINE_MISS_DATADIR_ERROR            EOFFSET(54)
#define CREATE_MACHINE_MISS_LOGDIR_ERROR            EOFFSET(55)
#define CREATE_MACHINE_MISS_WALLOGDIR_ERROR            EOFFSET(56)
#define CREATE_MACHINE_MISS_TOTALMEM_ERROR            EOFFSET(57)
#define CREATE_MACHINE_MISS_CPUCORES_ERROR            EOFFSET(58)
#define CREATE_MACHINE_CHECK_HOSTADDR_SQL_ERROR            EOFFSET(59)
#define CREATE_MACHINE_INSERT_HOSTADDR_SQL_ERROR            EOFFSET(60)
#define CREATE_MACHINE_MISS_COMPDATADIR_ERROR             EOFFSET(61)
#define CREATE_MACHINE_HOSTADDR_EXIST_ERROR               EOFFSET(62)
#define CREATE_MACHINE_GET_HOSTADDR_RECORD_ERROR            EOFFSET(63)
#define DELETE_MACHINE_HAS_USED_PORT_ERROR            EOFFSET(64)

#define DELETE_MACHINE_HOSTADDR_NOT_EXIST_ERROR             EOFFSET(66)
#define DELETE_MACHINE_MISS_HOSTADDR_ERROR                  EOFFSET(67)
#define DELETE_MACHINE_CHECK_HOSTADDR_SQL_ERROR             EOFFSET(68)
#define DELETE_MACHINE_UPDATE_HOSTADDR_STATE_SQL_ERROR        EOFFSET(69)
#define ADD_NODE_EXPORTER_TO_PROMETHEUS_ERROR             EOFFSET(202)
#define DEL_NODE_EXPORTER_TO_PROMETHEUS_ERROR             EOFFSET(203)

//=====create cluster =================
#define CREATE_CLUSTER_QUEST_LOST_PARAS_ERROR         EOFFSET(70)
#define CREATE_CLUSTER_QUEST_MISS_HAMODE_ERROR         EOFFSET(71)
#define CREATE_CLUSTER_QUEST_MISS_SHARDS_ERROR         EOFFSET(72)
#define CREATE_CLUSTER_QUEST_SHARDS_PARAM_ERROR         EOFFSET(73)
#define CREATE_CLUSTER_QUEST_MISS_NODES_ERROR         EOFFSET(74)
#define CREATE_CLUSTER_QUEST_NODES_PARAM_ERROR         EOFFSET(75)
#define CREATE_CLUSTER_QUEST_MISS_COMPS_ERROR         EOFFSET(76)
#define CREATE_CLUSTER_QUEST_COMPS_PARAM_ERROR         EOFFSET(77)
#define CREATE_CLUSTER_QUEST_MISS_MAX_STORAGE_SIZE_ERROR         EOFFSET(78)
#define CREATE_CLUSTER_QUEST_MISS_MAX_CONNECTIONS_ERROR         EOFFSET(79)
#define CREATE_CLUSTER_QUEST_MISS_CPU_CORES_ERROR         EOFFSET(80)
#define CREATE_CLUSTER_QUEST_MISS_INNODB_SIZE_ERROR         EOFFSET(81)
#define CREATE_CLUSTER_QUEST_INNODB_SIZE_PARAM_ERROR         EOFFSET(82)

#define GET_STORAGE_HOSTADDR_PARAM_SERVER_NODES_SQL_ERROR         EOFFSET(84)
#define GET_STORAGE_HOSTADDR_PARAM_NUMBER_ERROR         EOFFSET(85)

#define GET_COMPUTER_HOSTADDR_PARAM_SERVER_NODES_SQL_ERROR         EOFFSET(87)
#define GET_COMPUTER_HOSTADDR_PARAM_NUMBER_ERROR         EOFFSET(88)
#define CREATE_CLUSTER_INSERT_CLUSTER_NAME_ERROR          EOFFSET(83)
#define CREATE_CLUSTER_DDL_OPS_LOG_TABLE_ERROR            EOFFSET(65)
#define CREATE_CLUSTER_COMMIT_LOG_TABLE_ERROR             EOFFSET(145)
#define CREATE_CLUSTER_GET_CLUSTER_ID_ERROR               EOFFSET(86)
#define CREATE_CLUSTER_GET_CLUSTER_ID_RECORD_ERROR               EOFFSET(147)

#define CREATE_CLUSTER_ADDSHARD_MISSION_ERROR                     EOFFSET(89)
#define CREATE_CLUSTER_ADDCOMPUTER_MISSION_ERROR                  EOFFSET(90)
#define CREATE_CLUSTER_GET_STORAGE_MATCH_NODES_ERROR                  EOFFSET(91)
#define CREATE_CLUSTER_GET_COMPUTER_MATCH_COMPS_ERROR                  EOFFSET(92)

#define CREATE_CLUSTER_INSTALL_STORAGE_ERROR                  EOFFSET(93)
#define CREATE_CLUSTER_INSTALL_COMPUTER_ERROR                  EOFFSET(94)
#define CREATE_CLUSTER_ASSIGN_STORAGE_IPLISTS_ERROR           EOFFSET(181)
#define CREATE_CLUSTER_ASSIGN_COMPUTER_IPLISTS_ERROR          EOFFSET(182)

//=============delete cluster============================
#define DELETE_CLUSTER_QUEST_LOST_PARAS_ERROR                 EOFFSET(95)
#define DELETE_CLUSTER_QUEST_MISSING_PARAM_ERROR                 EOFFSET(96)
#define DELETE_CLUSTER_CHECK_ID_BY_CLUSTER_NAME_ERROR           EOFFSET(97)
#define DELETE_CLUSTER_CHECK_ID_BY_CLUSTER_ID_ERROR           EOFFSET(98)
#define DELETE_CLUSTER_POST_DEAL_ERROR           EOFFSET(99)
#define DELETE_CLUSTER_DELETE_STORAGE_ERROR           EOFFSET(100)
#define DELETE_CLUSTER_DELETE_COMPUTER_ERROR           EOFFSET(101)
#define DELETE_CLUSTER_DISPATCH_STORAGE_JOB_ERROR           EOFFSET(102)
#define DELETE_CLUSTER_DISPATCH_COMPUTER_JOB_ERROR           EOFFSET(103)
#define DELETE_CLUSTER_GET_SHARDS_ERROR           EOFFSET(104)
#define DELETE_CLUSTER_GET_COMPS_ERROR            EOFFSET(105)
#define CLUSTER_MGR_RESTART_ERROR                 EOFFSET(106)


//==============add computer==============================
#define ADD_COMPUTER_QUEST_LOST_PARAS_ERROR     EOFFSET(107)
#define ADD_COMPUTER_QUEST_MISS_COMPS_ERROR     EOFFSET(108)
#define ADD_COMPUTER_QUEST_MISS_CLUSTERID_ERROR  EOFFSET(109)
#define ADD_COMPUTER_QUEST_COMPS_PARAM_ERROR    EOFFSET(110)
#define GET_MEMO_FROM_DB_CLUSTER_ERROR EOFFSET(111)
#define GET_MEMO_NUM_CLUSTERID_ERROR   EOFFSET(112)
#define GET_MEMO_NULL_CLUSTERID_ERROR  EOFFSET(113)
#define PARSE_CLUSTER_MEMO_JSON_ERROR           EOFFSET(114)
#define ADD_COMPUTER_GET_COMPUTER_MATCH_COMPS_ERROR     EOFFSET(115)
#define ADD_COMPUTER_HOSTADDR_SERVER_NODES_SQL_ERROR    EOFFSET(116)
#define ADD_COMPUTER_DISPATCH_MISSION_ERROR          EOFFSET(117)
#define ADD_COMPUTER_INSTALL_COMPUTER_ERROR         EOFFSET(148)
#define ADD_COMPUTER_DEAL_POST_ERROR         EOFFSET(150)
#define ADD_COMPUTER_ASSIGN_COMPUTER_IPLISTS_ERROR    EOFFSET(185)
#define ADD_COMPUTER_GET_EXIST_COMPS_FROM_METADB_ERROR    EOFFSET(186)

//==============add shard=================================
#define ADD_SHARD_QUEST_LOST_PARAS_ERROR                EOFFSET(118)
#define ADD_SHARD_QUEST_MISS_CLUSTERID_ERROR            EOFFSET(119)
#define ADD_SHARD_QUEST_MISS_SHARDS_ERROR               EOFFSET(120)
#define ADD_SHARD_QUEST_SHARDS_PARAM_ERROR              EOFFSET(121)
#define ADD_SHARD_QUEST_MISS_NODES_ERROR                EOFFSET(122)
#define ADD_SHARD_QUEST_NODES_PARAM_ERROR               EOFFSET(123)

#define DELETE_COMPUTER_GET_CLUSTER_NAME_ERROR            EOFFSET(124)
#define DELETE_COMPUTER_GET_CLUSTER_NAME_NUM_ERROR        EOFFSET(125)

#define RB_MISSION_PARSE_MEMO_JSON_ERROR                   EOFFSET(126)
#define DELETE_COMPUTER_GET_COMP_NODES_SQL_ERROR             EOFFSET(127)

#define ADD_SHARD_GET_STORAGE_MATCH_NODES_ERROR         EOFFSET(128)
#define ADD_SHARD_STORAGE_HOSTADDR_SERVER_NODES_SQL_ERROR     EOFFSET(129)
#define ADD_SHARD_INIT_MISSION_ERROR                    EOFFSET(130)
#define ADD_SHARD_INSTALL_STORAGE_ERROR                 EOFFSET(146)
#define ADD_SHARD_SET_RBR_SYNC_ERROR                    EOFFSET(149)
#define ADD_SHARD_DEAL_POST_ERROR         EOFFSET(151)
#define ADD_SHARD_GET_SHARDS_FROM_METADB_ERROR         EOFFSET(152)
#define ADD_SHARD_ASSIGN_STORAGE_IPLISTS_ERROR           EOFFSET(184)
//#define ADD_SHARD_JOB_GET_FIRST_SHARD_MASTER_CLUSTER_ID_ERROR      EOFFSET(204)

//==============delete computer===========================
#define DELETE_COMPUTER_QUEST_LOST_PARAS_ERROR          EOFFSET(131)
#define DELETE_COMPUTER_QUEST_MISS_CLUSTERID_ERROR      EOFFSET(132)
#define DELETE_COMPUTER_QUEST_MISS_COMPID_ERROR         EOFFSET(133)
#define DELETE_COMPUTER_GET_COMPUTER_NODES_ERROR        EOFFSET(134)
#define DELETE_COMPUTER_DISPATCH_COMPUTER_JOB_ERROR     EOFFSET(135)
#define DELETE_COMPUTER_DELETE_COMPUTER_ERROR           EOFFSET(136)
#define DELETE_COMPUTER_POST_DEAL_ERROR                 EOFFSET(137)

//==============delete shard==============================
#define DELETE_SHARD_QUEST_LOST_PARAS_ERROR             EOFFSET(138)
#define DELETE_SHARD_QUEST_MISS_CLUSTERID_ERROR         EOFFSET(139)
#define DELETE_SHARD_QUEST_MISS_SHARDID_ERROR           EOFFSET(140)
#define DELETE_SHARD_GET_STORAGE_NODES_ERROR            EOFFSET(141)
#define DELETE_SHARD_POST_DEAL_ERROR                    EOFFSET(142)
#define DELETE_SHARD_DISPATCH_STORAGE_JOB_ERROR         EOFFSET(143)
#define DELETE_SHARD_DELETE_STORAGE_ERROR               EOFFSET(144)

//==============add node==================================
#define ADD_NODE_QUEST_LOST_PARAS_ERROR       EOFFSET(153)
#define ADD_NODE_QUEST_MISS_CLUSTERID_ERROR   EOFFSET(154)
#define ADD_NODE_QUEST_MISS_NODES_ERROR       EOFFSET(155)
#define ADD_NODE_QUEST_NODES_PARAM_ERROR      EOFFSET(156)

#define DELETE_COMPUTER_GET_COMP_NODES_NUM_ERROR       EOFFSET(157)
#define CREATE_MACHINE_REPORT_ALIVE_ERROR     EOFFSET(158)
#define DELETE_NODE_GET_NODE_NUM_ERROR    EOFFSET(159)
#define DELETE_SHARD_GET_SHARD_SQL_ERROR            EOFFSET(160)
#define ADD_NODE_GET_SHARD_ID_ERROR               EOFFSET(161)
#define ADD_NODE_GET_STORAGE_MATCH_NODES_ERROR    EOFFSET(162)
#define ADD_NODE_STORAGE_HOSTADDR_SERVER_NODES_SQL_ERROR  EOFFSET(163)
#define ADD_NODE_GET_INSTALL_REQUEST_ID_ERROR     EOFFSET(164)
#define ADD_NODE_DISPATH_INSTALL_TASK_ERROR       EOFFSET(165)
#define ADD_NODE_POST_DEAL_ERROR                  EOFFSET(166)
#define ADD_NODE_ASSIGN_STORAGE_IPLISTS_ERROR           EOFFSET(183)

//==============delete node===============================
#define DEL_NODE_QUEST_LOST_PARAS_ERROR         EOFFSET(167)
#define DEL_NODE_QUEST_MISS_CLUSTERID_ERROR     EOFFSET(168)
#define DEL_NODE_QUEST_MISS_SHARDID_ERROR       EOFFSET(169)
#define DEL_NODE_QUEST_MISS_HOSTADDR_ERROR      EOFFSET(170)
#define DEL_NODE_QUEST_MISS_PORT_ERROR          EOFFSET(171)
#define DEL_NODE_IS_MASTER_NODE_ERROR            EOFFSET(172)
#define DEL_NODE_NOT_IN_SHARD_ERROR             EOFFSET(173)
#define DELETE_SHARD_GET_SHARD_NUM_ERROR   EOFFSET(174)
#define CREATE_CLUSTER_ASSIGN_NODES_FULLSYNC_ERROR     EOFFSET(175)
#define ADD_SHARDS_ASSIGN_NODES_FULLSYNC_ERROR      EOFFSET(176)
#define CF_ASSIGN_HOST_FSYNC_STATE              EOFFSET(177)
#define DEL_NODE_DISPATCH_JOB_ERROR               EOFFSET(178)
#define DEL_NODE_POST_DEAL_ERROR                  EOFFSET(179)
#define DEL_NODE_GET_SHARD_NODES_SQL_ERROR        EOFFSET(180)
#define ASYNC_MYSQL_CONNECT_EPOLL_TIMEOUT           EOFFSET(187)
#define MANUAL_SW_SUBMIT_MANUAL_CONSFAILOVER_ERROR      EOFFSET(188)


//================raft mission===============================
#define RAFT_MISSION_QUEST_LOST_PARAS_ERROR         EOFFSET(189)
#define RAFT_MISSION_QUEST_LOST_TASK_TYPE_ERROR         EOFFSET(190)
#define RAFT_MISSION_UNKNOWN_TASK_TYPE_ERROR         EOFFSET(191)
#define RAFT_MISSION_QUEST_LOST_TARGET_LEADER_ERROR         EOFFSET(192)
#define RAFT_MISSION_ASSIGN_TARGET_LEADER_ERROR         EOFFSET(193)
#define RAFT_MISSION_QUEST_LOST_PEER_ERROR         EOFFSET(194)
#define RAFT_MISSION_PARSE_CONFIG_ERROR         EOFFSET(195)
#define RAFT_MISSION_CONV_TARGET_LEADER_ERROR         EOFFSET(196)
#define RAFT_MISSION_TRANSFER_LEADER_ERROR         EOFFSET(197)
#define RAFT_MISSION_ASSIGN_PEER_ERROR            EOFFSET(198)
#define RAFT_MISSION_CONV_PEER_ERROR            EOFFSET(199)
#define RAFT_MISSION_ADD_PEER_ERROR             EOFFSET(200)
#define RAFT_MISSION_REMOVE_PEER_ERROR             EOFFSET(201)

#define ERR_COMMON EOFFSET(100)
#define ERR_JSON (ERR_COMMON + 1)
#define ERR_PARA (ERR_COMMON + 2)
#define ERR_PARA_NODE (ERR_COMMON + 3)
#define ERR_JOBID_MISS (ERR_COMMON + 4)
#define ERR_META_READ (ERR_COMMON + 5)
#define ERR_META_WRITE (ERR_COMMON + 6)
#define ERR_SYSTEM_CMD (ERR_COMMON + 7)
#define ERR_GET_VARIABLE (ERR_COMMON + 8)
#define ERR_SET_VARIABLE (ERR_COMMON + 9)

#define ERR_BACKUP_STORAGE EOFFSET(200)
#define ERR_BACKUP_NAME_EXIST (ERR_BACKUP_STORAGE + 1)
#define ERR_BACKUP_NAME_NO_EXIST (ERR_BACKUP_STORAGE + 2)
#define ERR_BACKUP_STYPE_NO_SUPPORT (ERR_BACKUP_STORAGE + 3)
#define ERR_BACKUP_STRING (ERR_BACKUP_STORAGE + 4)

#define ERR_MACHINE EOFFSET(300)
#define ERR_MACHINE_PATH (ERR_MACHINE + 1)
#define ERR_MACHINE_HOSTADDR (ERR_MACHINE + 2)
#define ERR_MACHINE_CHANNEL (ERR_MACHINE + 3)
#define ERR_NO_MACHINE (ERR_MACHINE + 4)
#define ERR_NO_MACHINE_PATH (ERR_MACHINE + 5)
#define ERR_NO_MACHINE_AVAILABLE (ERR_MACHINE + 6)

#define ERR_OTHER_MISSION EOFFSET(400)
#define ERR_CONTROL_INSTANCE (ERR_OTHER_MISSION + 1)
#define ERR_UPDATE_PROMETHEUS (ERR_OTHER_MISSION + 2)
#define ERR_UPDATE_INSTANCE (ERR_OTHER_MISSION + 3)
#define ERR_POSTGRES_EXPORT (ERR_OTHER_MISSION + 4)
#define ERR_MYSQL_EXPORT (ERR_OTHER_MISSION + 5)

#define ERR_CLUSTER EOFFSET(500)
#define ERR_NICK_NAME_EXIST (ERR_CLUSTER + 1)
#define ERR_RENAME_NAME (ERR_CLUSTER + 2)
#define ERR_CLUSTER_NO_EXIST (ERR_CLUSTER + 3)
#define ERR_SHARD_NO_EXIST (ERR_CLUSTER + 4)
#define ERR_COMP_NO_EXIST (ERR_CLUSTER + 5)
#define ERR_NODE_NO_EXIST (ERR_CLUSTER + 6)
#define ERR_CREATE_PATH (ERR_CLUSTER + 7)
#define ERR_GET_CLUSTER_INFO (ERR_CLUSTER + 8)
#define ERR_NO_TASK (ERR_CLUSTER + 9)
#define ERR_SHARD_NODE_ONE (ERR_CLUSTER + 10)
#define ERR_COMP_ONE (ERR_CLUSTER + 11)
#define ERR_GET_SHARD_IP_PORT (ERR_CLUSTER + 12)
#define ERR_GET_COMP_IP_PORT (ERR_CLUSTER + 13)
#define ERR_NO_REP_MODE (ERR_CLUSTER + 14)
#define ERR_GET_SHARD_NAME (ERR_CLUSTER + 15)
#define ERR_BACKUP_INFO (ERR_CLUSTER + 16)
#define ERR_MATE_INFO (ERR_CLUSTER + 17)
#define ERR_START_CLUSTER (ERR_CLUSTER + 18)
#define ERR_START_SHARD (ERR_CLUSTER + 19)
#define ERR_START_COMP (ERR_CLUSTER + 20)
#define ERR_UPDATE_CLUSTER_INFO (ERR_CLUSTER + 21)
#define ERR_BACKUP_CLUSTER (ERR_CLUSTER + 22)
#define ERR_BACKUP_SHARD (ERR_CLUSTER + 23)
#define ERR_GET_SHARD_INFO (ERR_CLUSTER + 24)
#define ERR_GET_NODE_INFO (ERR_CLUSTER + 25)
#define ERR_GET_SHARD_MAP (ERR_CLUSTER + 26)
#define ERR_GET_META_MASTER (ERR_CLUSTER + 27)
#define ERR_RESTORE_STORAGE (ERR_CLUSTER + 28)
#define ERR_RESTORE_COMPUTER (ERR_CLUSTER + 29)
#define ERR_UPDATE_INSTANCE_INFO (ERR_CLUSTER + 30)
#define ERR_ADD_SHARD (ERR_CLUSTER + 31)
#define ERR_ADD_COMP (ERR_CLUSTER + 32)
#define ERR_ADD_NODE (ERR_CLUSTER + 33)
#define ERR_INSTALL_STORAGE (ERR_CLUSTER + 34)
#define ERR_INSTALL_COMPUTER (ERR_CLUSTER + 35)
#define ERR_RESTORE_INSTANCE (ERR_CLUSTER + 36)


// New item should add above
#define EUNDEFINED_ERROR EOFFSET(1000)

// Defined in error_code.cc
extern const char *g_error_num_str[];

class GlobalErrorNum {
public:
  GlobalErrorNum() { err_num_ = EOK; }
  GlobalErrorNum(int err_num) { err_num_ = err_num; }
  int get_err_num() { return err_num_; }
  void set_err_num(int errnum) { err_num_ = errnum; }
  const char *get_err_num_str();
  bool Ok() { return err_num_ == EOK; }
  std::string EintToStr(int errnum)
  {
      return std::to_string(errnum);
  }

private:
  int err_num_;
};

}; // namespace kunlun

#endif /*_CLUSTER_MANAGER_ERROR_CODE_H_*/
