#!/bin/bash

cnfpath=`realpath ../conf/cluster_mgr.cnf`
./cluster_mgr ${cnfpath}

ps -ef | grep ${cnfpath} | grep -v grep
