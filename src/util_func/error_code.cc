/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "error_code.h"

namespace kunlun {
  
const char *g_error_num_str[] = {
  "OK",                                       /*EOFFSET(0)*/
  "Invalid Request Protocal",                 /*EOFFSET(1)*/
  "Invalid Response Protocal",                /*EOFFSET(2)*/
  "Node Machine Or NodeManager Unreacheable", /*EOFFSET(3)*/
  "Json Error",                               /*EOFFSET(4)*/
  "Parameter Error",                          /*EOFFSET(5)*/
  "FullSync Node Unalive",                    /*EOFFSET(6)*/
  "FullSync Node Number Unmatch Fullsync Level",  /*EOFFSET(7)*/
  "Report RelayLog Row Nums Error",          /*EOFFSET(8)*/
  "Replication Data Running",               /*EOFFSET(9)*/
  "Consfailover Timeout",                   /*EOFFSET(10)*/
  "Consfailover ApplyLog Timeout",          /*EOFFSET(11)*/
  "Master FullSync Waiting Txns Status",    /*EOFFSET(12)*/
  "Kill Mysqld By Rpc Error",               /*EOFFSET(13)*/
  "Manual Noswitch Node Exist",             /*EOFFSET(14)*/
  "Async Mysql Connect Poll Error",         /*EOFFSET(15)*/
  "DB In Max Connection Status",            /*EOFFSET(16)*/
  "DB In Huge Transaction Status",          /*EOFFSET(17)*/
  "DB In Innodb Block Status",              /*EOFFSET(18)*/
  "Assign Host Unalive",                    /*EOFFSET(19)*/
  "Assign Host No Maximum GTIDS",           /*EOFFSET(20)*/
  "Assign Host Connect Mysql Error",        /*EOFFSET(21)*/
  "All Slave Nodes Unalive In Degrade",     /*EOFFSET(22)*/
  "Rebuild Slave Connect Error",            /*EOFFSET(23)*/
  "Job param lost shard id",            /*EOFFSET(24)*/
  "Job param lost cluster id",            /*EOFFSET(25)*/
  "Job param lost master hostaddr",            /*EOFFSET(26)*/
  "Get shard class by id failed",            /*EOFFSET(27)*/
  "Get meta shard master connect error",            /*EOFFSET(28)*/
  "Metadb select master port error",            /*EOFFSET(29)*/
  "Get master hostaddr port number error",            /*EOFFSET(30)*/
  "Metadb select assign port error",            /*EOFFSET(31)*/
  "Get assign hostaddr port number error",            /*EOFFSET(32)*/
  "Master hostaddr state is not source",            /*EOFFSET(33)*/
  "Assign hostaddr state is not replica",            /*EOFFSET(34)*/
  "Get manual_sw result from meta failed",            /*EOFFSET(35)*/
  "Rebuild node job dont have rb_node param",            /*EOFFSET(36)*/
  "Rb_node of rebuild node job don't have no hostaddr",            /*EOFFSET(37)*/
  "Rb_node of rebuild node job don't have no port",            /*EOFFSET(38)*/
  "Rebuild node job need input hdfs_host in need_backup",            /*EOFFSET(39)*/
  "Get machine info from metadb for rebuild node error",            /*EOFFSET(40)*/
  "Rebuid hostaddr node is not in shard",            /*EOFFSET(41)*/
  "Get pull host for rebuild node error",            /*EOFFSET(42)*/
  "Get shard and cluster name for rebuild node error",            /*EOFFSET(43)*/
  "Get rebuild host params for rebuild node in metadb error",            /*EOFFSET(44)*/
  "Get pull host params for rebuild node in metadb error",            /*EOFFSET(45)*/
  "Connect meta db execute sql error",            /*EOFFSET(46)*/
  "Get subtask of rebuild node error",            /*EOFFSET(47)*/
  "Parse general job memo to json error",            /*EOFFSET(48)*/
  "Machine request job miss paras in the request body",            /*EOFFSET(49)*/
  "Machine request job miss machine_type in the paras",            /*EOFFSET(50)*/
  "Machine type is not support in request job",            /*EOFFSET(51)*/
  "Create machine job miss hostaddr in request job",            /*EOFFSET(52)*/
  "Create machine job miss rack_id in request job",            /*EOFFSET(53)*/
  "Create machine job miss datadir in request job",            /*EOFFSET(54)*/
  "Create machine job miss logdir in request job",            /*EOFFSET(55)*/
  "Create machine job miss wal_log_dir in request job",            /*EOFFSET(56)*/
  "Create machine job miss total_mem in request job",            /*EOFFSET(57)*/
  "Create machine job miss total_cpu_cores in request job",            /*EOFFSET(58)*/
  "Create machine job check hostaddr exist sql to execute error",            /*EOFFSET(59)*/
  "Create machine job insert hostaddr sql to execute error",            /*EOFFSET(60)*/
  "Create machine job miss comp_datadir in request job",            /*EOFFSET(61)*/
  "Create machine job add hostaddr in server_nodes",            /*EOFFSET(62)*/
  "Create machine job get hostaddr record error",            /*EOFFSET(63)*/
  "Delete machine job used port is not null",            /*EOFFSET(64)*/
  "Create ddl_ops_log table in metadb failed",            /*EOFFSET(65)*/
  "Delete machine hostaddr not in server_nodes",            /*EOFFSET(66)*/
  "Delete machine job miss hostaddr in request job",            /*EOFFSET(67)*/
  "Delete machine job check hostaddr exist sql to execute error",            /*EOFFSET(68)*/
  "Delete machine job update hostaddr state sql to execute error",            /*EOFFSET(69)*/

  "Create cluster job miss paras in the request body",            /*EOFFSET(70)*/
  "Create cluster job miss ha_mode in the request body",            /*EOFFSET(71)*/
  "Create cluster job miss shards in the request body",            /*EOFFSET(72)*/
  "Create cluster job input shards number wrong",            /*EOFFSET(73)*/
  "Create cluster job miss nodes in the request body",            /*EOFFSET(74)*/
  "Create cluster job input nodes number wrong",            /*EOFFSET(75)*/
  "Create cluster job miss comps in the request body",            /*EOFFSET(76)*/
  "Create cluster job input comps number wrong",            /*EOFFSET(77)*/
  "Create cluster job miss max_storage_size in the request body",            /*EOFFSET(78)*/
  "Create cluster job miss max_connections in the request body",            /*EOFFSET(79)*/
  "Create cluster job miss cpu_cores in the request body",            /*EOFFSET(80)*/
  "Create cluster job miss innodb_size in the request body",            /*EOFFSET(81)*/
  "Create cluster job input innodb_size wrong",            /*EOFFSET(82)*/
  "Insert cluster name into db_clusters failed",            /*EOFFSET(83)*/
  "Get storage hostaddr parameters from server nodes failed",            /*EOFFSET(84)*/
  "Get storage hostaddr parameters records too many",            /*EOFFSET(85)*/
  "Get cluster id from db_clusters failed",            /*EOFFSET(86)*/
  "Get computer hostaddr parameters from server nodes failed",            /*EOFFSET(87)*/
  "Get computer hostaddr parameters number wrong",            /*EOFFSET(88)*/
  "Add shard mission error in create cluster job",                                /*EOFFSET(89)*/
  "Add computer mission error in create cluster job",                               /*EOFFSET(90)*/
  "Get storage nodes unmatch nodes in create cluster job",                              /*EOFFSET(91)*/
  "Get computer nodes unmatch comps in create cluster job",                              /*EOFFSET(92)*/
  "Install storage error in create cluster job",                   /*EOFFSET(93)*/
  "Install computer error in create cluster job",                  /*EOFFSET(94)*/
  "Deleter cluster job miss paras in the request body",            /*EOFFSET(95)*/
  "Deleter cluster job lost input parameter",            /*EOFFSET(96)*/
  "Deleter cluster job check cluster name failed",            /*EOFFSET(97)*/
  "Deleter cluster job check cluster id failed",            /*EOFFSET(98)*/
  "Deleter cluster job deal failed in post step",            /*EOFFSET(99)*/
  "Deleter cluster job delete shard failed",            /*EOFFSET(100)*/
  "Deleter cluster job delete computer failed",            /*EOFFSET(101)*/
  "Deleter cluster job dispatch to delete shard job failed",            /*EOFFSET(102)*/
  "Deleter cluster job dispatch to delete computer job failed",            /*EOFFSET(103)*/
  "Deleter cluster job get shard nodes from metadb failed",            /*EOFFSET(104)*/
  "Deleter cluster job get computer nodes from metadb failed",            /*EOFFSET(105)*/
  "Cluster mgr restart to rollback job",                                     /*EOFFSET(106)*/
  
  "Add computer job miss paras in the request body",                                 /*EOFFSET(107)*/
  "Add computer job miss comps in the request body",                                 /*EOFFSET(108)*/
  "Add computer job miss cluster id in the request body",                                 /*EOFFSET(109)*/
  "Add computer job input comps number wrong",                                 /*EOFFSET(110)*/
  "Get memo from db_clusters failed",                                 /*EOFFSET(111)*/
  "Get memo record too many",                                 /*EOFFSET(112)*/
  "Get empty memo by cluster id",                                 /*EOFFSET(113)*/
  "Json parse memo failed",                                 /*EOFFSET(114)*/
  "Add computer job don't get enough node for install computer",             /*EOFFSET(115)*/
  "Add computer job get install computer node from meta failed",             /*EOFFSET(116)*/
  "Add computer job dispatch sub mission job failed",             /*EOFFSET(117)*/

  "Add shard job miss paras in the request body",                 /*EOFFSET(118)*/    
  "Add shard job miss cluster id in the request body",                 /*EOFFSET(119)*/  
  "Add shard job miss shards id in the request body",                 /*EOFFSET(120)*/  
  "Add shard job shards number wrong",                 /*EOFFSET(121)*/  
  "Add shard job miss nodes in the request body",                 /*EOFFSET(122)*/  
  "Add shard job node number wrong",                 /*EOFFSET(123)*/  
  "Delete computer job get cluster name sql failed",                 /*EOFFSET(124)*/  
  "Delete computer job get cluster name too many",                 /*EOFFSET(125)*/  
  "Rb Mission parse memo json failed",                 /*EOFFSET(126)*/  
  "Delete computer job get comp nodes for metadb failed",                 /*EOFFSET(127)*/  
  "Add shard job storage node umatch nodes",                 /*EOFFSET(128)*/  
  "Add shard job get storage node from metadb failed",                 /*EOFFSET(129)*/  
  "Add shard job initialize sub mission failed",                 /*EOFFSET(130)*/  

  "Delete computer job miss paras in the request body",                 /*EOFFSET(131)*/ 
  "Delete computer job miss cluster id in the request body",                 /*EOFFSET(132)*/
  "Delete computer job miss comp id in the request body",                 /*EOFFSET(133)*/
  "Delete computer job get computer node from metadb failed",                 /*EOFFSET(134)*/
  "Delete computer job dispatch sub job failed",                 /*EOFFSET(135)*/
  "Delete computer job delete computer failed",                 /*EOFFSET(136)*/
  "Delete computer job deal post step failed",                 /*EOFFSET(137)*/

  "Delete shard job miss paras in the request body",                 /*EOFFSET(138)*/
  "Delete shard job miss cluster id in the request body",                 /*EOFFSET(139)*/
  "Delete shard job miss shard id in the request body",                 /*EOFFSET(140)*/
  "Delete shard job get storage node from shard_nodes failed",                 /*EOFFSET(141)*/
  "Delete shard job deal post step failed",                 /*EOFFSET(142)*/
  "Delete shard job dispatch sub job failed",                 /*EOFFSET(143)*/
  "Delete shard job delete storage failed",                 /*EOFFSET(144)*/

  "Create commit_log table failed",                 /*EOFFSET(145)*/
  "Add shard job install storage node failed",                 /*EOFFSET(146)*/
  "Create cluster job get cluster id records too many",                 /*EOFFSET(147)*/
  "Add computer job install computer node failed",                 /*EOFFSET(148)*/
  "Add shard job failed during setup rbr replication",            /*EOFFSET(149)*/
  "Add computer job deal post step failed",            /*EOFFSET(150)*/
  "Add shard job deal post step failed",            /*EOFFSET(151)*/
  "Add shard job get shards from metadb failed",            /*EOFFSET(152)*/

  "Add node job miss paras in the request body",            /*EOFFSET(153)*/
  "Add node job miss cluster_id in the request body",            /*EOFFSET(154)*/
  "Add node job miss nodes in the request body",            /*EOFFSET(155)*/
  "Add node job node number wrong",            /*EOFFSET(156)*/
  "Delete computer job get only one comp node",            /*EOFFSET(157)*/
  "Create machine job report machine unalive",            /*EOFFSET(158)*/
  "Delete node job get only one shard node",            /*EOFFSET(159)*/
  "Delete shard job get shard nums from metadb failed",            /*EOFFSET(160)*/
  "Add node job get shard_id from shards failed",            /*EOFFSET(161)*/
  "Add node job get node number unmatch require",            /*EOFFSET(162)*/
  "Add node job get hostaddr from server_node failed",            /*EOFFSET(163)*/
  "Add node job get install id from meta failed",            /*EOFFSET(164)*/
  "Add node job dispatch install storage job failed",            /*EOFFSET(165)*/
  "Add node job deal post step failed",            /*EOFFSET(166)*/

  "Delete node job miss paras in the request body",            /*EOFFSET(167)*/
  "Delete node job miss cluster_id in the request body",            /*EOFFSET(168)*/
  "Delete node job miss shard_id in the request body",            /*EOFFSET(169)*/
  "Delete node job miss hostaddr in the request body",            /*EOFFSET(170)*/
  "Delete node job miss port in the request body",            /*EOFFSET(171)*/
  "Delete node job delete master node",            /*EOFFSET(172)*/
  "Delete node job delete node is not in shards",            /*EOFFSET(173)*/
  "Delete shard job get just only one shard",            /*EOFFSET(174)*/
  "Create cluster job assign fullsync level too big",            /*EOFFSET(175)*/
  "Add shards job assign nodes number too small",            /*EOFFSET(176)*/
  "Assign host is not fullsync state in consfailover",            /*EOFFSET(177)*/
  "Delete node job dispatch delete job failed",            /*EOFFSET(178)*/
  "Delete node job deal post step failed",            /*EOFFSET(179)*/
  "Delete node job get params from shard_node failed",            /*EOFFSET(180)*/

  "Create cluster job assign storage iplists failed",            /*EOFFSET(181)*/
  "Create cluster job assign computer iplists failed",            /*EOFFSET(182)*/
  "Add node job assign storage iplists failed",            /*EOFFSET(183)*/
  "Add shard job assign storage iplists failed",            /*EOFFSET(184)*/
  "Add computer job assign computer iplists failed",            /*EOFFSET(185)*/
  "Add computer job get exist comps from metadb failed",            /*EOFFSET(186)*/
  "Async Mysql Connect Poll Timeout",                          /*EOFFSET(187)*/
  "Submit manual consfailover job failed",                         /*EOFFSET(188)*/

  "Raft mission job miss paras in the request body",                          /*EOFFSET(189)*/
  "Raft mission job miss task_type in the request body",                          /*EOFFSET(190)*/
  "Raft mission job task_type is not supported",                          /*EOFFSET(191)*/
  "Raft mission job miss target_leader in the request body",              /*EOFFSET(192)*/
  "Raft mission job assign target_leader is not in configuration",        /*EOFFSET(193)*/
  "Raft mission job miss peer in the request body",                          /*EOFFSET(194)*/
  "Raft mission job parse current config failed",                          /*EOFFSET(195)*/
  "Raft mission job conv target_leader to peerid failed",                /*EOFFSET(196)*/
  "Raft mission job transfer leader failed",                          /*EOFFSET(197)*/

  "Raft mission job assign peer failed",                          /*EOFFSET(198)*/
  "Raft mission job conv peer failed",                          /*EOFFSET(199)*/
  "Raft mission job add peer failed",                          /*EOFFSET(200)*/
  "Raft mission job remove peer failed",                          /*EOFFSET(201)*/
  "Add node_exporter to prometheus failed",                          /*EOFFSET(202)*/
  "Delete node_exporter to prometheus failed",                          /*EOFFSET(203)*/
  //"Add shard job get first shard master node failed",                   /*EOFFSET(204)*/

  // New item should add above
  "Undefined Error Number and Relating Readable Information" /*undefined*/
};

#define ERROR_STR_DEFINED_NUM (sizeof(g_error_num_str) / sizeof(char *))
const char *GlobalErrorNum::get_err_num_str() {
  if(err_num_ == 0)
    return g_error_num_str[0];

  if ((err_num_ + 1 - EERROR_NUM_START) > ERROR_STR_DEFINED_NUM) {
    return g_error_num_str[ERROR_STR_DEFINED_NUM - 1];
  }
  return g_error_num_str[err_num_ - EERROR_NUM_START];
}

}; // namespace kunlun
