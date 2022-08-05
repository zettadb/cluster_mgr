#!/bin/bash

#killall -9 cluster_mgr;
BINPATH=`pwd`
SBINPATH=`eval echo "${BINPATH}"`
FPATH=${SBINPATH%/*}

ps -ef | grep cluster_mgr | grep ${FPATH} | grep -v grep | grep -v vim | awk '{print $2}' | xargs kill -9
