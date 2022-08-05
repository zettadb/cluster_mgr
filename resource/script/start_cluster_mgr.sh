#!/bin/bash

cnfpath=`realpath ../conf/cluster_mgr.cnf`

num=`ps -ef | grep ${cnfpath} | grep -v grep | grep -v vim | wc -l`
[ $num -gt 0 ] && {
        echo `pwd`"/cluster_mgr is running, so quit to start again" 
        exit 0
}

./cluster_mgr ${cnfpath} >../log/std.log 2>&1

ps -ef | grep ${cnfpath} | grep -v grep
