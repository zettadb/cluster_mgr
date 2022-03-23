#!/bin/bash

cnfpath=`realpath ../conf/cluster_mgr.cnf`
./cluster_mgr ${cnfpath} >../log/std.log 2>&1

ps -ef | grep ${cnfpath} | grep -v grep
