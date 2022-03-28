/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "sys_config.h"
#include "global.h"
#include "log.h"
#include "hdfs_client.h"
#include "hdfs/hdfs.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>

// export CLASSPATH=$CLASSPATH:$($HADOOP_HOME/bin/hadoop classpath --glob)
#define BUFSIZE 4096		//4K

int Hdfs_client::do_exit = 0;
Hdfs_client* Hdfs_client::m_inst = NULL;

std::string hdfs_server_ip;
int64_t hdfs_server_port;
int64_t hdfs_replication;

Hdfs_client::Hdfs_client()
{

}

Hdfs_client::~Hdfs_client()
{

}

void Hdfs_client::hdfs_init()
{

}

bool Hdfs_client::hdfs_pull_file(std::string &local_file, std::string &hdfs_file)
{
	bool ret = false;
	hdfsFS pfs = NULL;
	hdfsFile pfile = NULL;
	int iRet = 0;
	FILE *fp;
	
	pfs = hdfsConnect(hdfs_server_ip.c_str(), hdfs_server_port);
	if (NULL == pfs)
	{
		syslog(Logger::ERROR, "hdfsConnect failed! errno=%d", errno);
		goto end;
	}

	pfile = hdfsOpenFile(pfs, hdfs_file.c_str(), O_RDONLY, 0, 0, 0); 
	if (NULL == pfile)
	{
		syslog(Logger::ERROR, "hdfsOpenFile failed! szFilePath=%s,errno=%d\n", hdfs_file.c_str(), errno);
		goto end;
	}

	fp = fopen(local_file.c_str(), "wb");
	if(fp == NULL)
	{
		syslog(Logger::ERROR, "fopen failed! local_file=%s,errno=%d\n", local_file.c_str(), errno);
		goto end;
	}

	char file_buf[BUFSIZE];
	while(true)
	{
		iRet = hdfsRead(pfs, pfile, file_buf, BUFSIZE); 
		if (-1 == iRet)
		{
			syslog(Logger::ERROR, "hdfsRead failed! ret=%d,errno=%d\n", iRet, errno);
			goto end;
		}
		else if(0 == iRet)
			break;

		fwrite(file_buf, 1, iRet, fp);
	}

	fflush(fp);
	fclose(fp);
	fp = NULL;

	iRet = hdfsCloseFile(pfs, pfile);
	if (-1 == iRet)
	{
		syslog(Logger::ERROR, "hdfsCloseFile failed! ret=%d,errno=%d\n", iRet, errno);
		goto end;
	}
	pfile = NULL;

	ret = true;

end:
	if(fp != NULL)
	{
		fclose(fp);
		fp = NULL;
	}
	
	if(pfile != NULL)
	{
		hdfsCloseFile(pfs, pfile);
		pfile = NULL;
	}

	if (pfs != NULL)
	{
		iRet = hdfsDisconnect(pfs);
		if (-1 == iRet)
		{
			syslog(Logger::ERROR, "hdfsDisconnect failed! errno=%d",  errno);
		}
		pfs = NULL;
	}
	
	return ret;
}

bool Hdfs_client::hdfs_push_file(std::string &local_file, std::string &hdfs_file)
{
	bool ret = false;
	hdfsFS pfs = NULL;
	hdfsFile pfile = NULL;
	int iRet = 0;
	FILE *fp;
	
	pfs = hdfsConnect(hdfs_server_ip.c_str(), hdfs_server_port);
	if (NULL == pfs)
	{
		syslog(Logger::ERROR, "hdfsConnect failed! errno=%d", errno);
		goto end;
	}

	pfile = hdfsOpenFile(pfs, hdfs_file.c_str(), O_WRONLY | O_CREAT, 0, hdfs_replication, 0); 
	if (NULL == pfile)
	{
		syslog(Logger::ERROR, "hdfsOpenFile failed! szFilePath=%s,errno=%d\n", hdfs_file.c_str(), errno);
		goto end;
	}

	fp = fopen(local_file.c_str(), "rb");
	if(fp == NULL)
	{
		syslog(Logger::ERROR, "fopen failed! local_file=%s,errno=%d\n", local_file.c_str(), errno);
		goto end;
	}

	char file_buf[BUFSIZE];
	while(true)
	{
		int rlen = fread(file_buf, 1, BUFSIZE, fp);
		if(rlen<=0)
			break;
			
		iRet = hdfsWrite(pfs, pfile, file_buf, rlen); 
		if (-1 == iRet)
		{
			syslog(Logger::ERROR, "hdfsWrite failed! ret=%d,errno=%d\n", iRet, errno);
			goto end;
		}
	}

	fclose(fp);
	fp = NULL;

	iRet = hdfsHFlush(pfs, pfile);
	if (-1 == iRet)
	{
		syslog(Logger::ERROR, "hdfsFlush failed! ret=%d,errno=%d\n", iRet, errno);
		goto end;
	}

	iRet = hdfsCloseFile(pfs, pfile);
	if (-1 == iRet)
	{
		syslog(Logger::ERROR, "hdfsCloseFile failed! ret=%d,errno=%d\n", iRet, errno);
		goto end;
	}
	pfile = NULL;

	ret = true;

end:
	if(fp != NULL)
	{
		fclose(fp);
		fp = NULL;
	}
	
	if(pfile != NULL)
	{
		hdfsCloseFile(pfs, pfile);
		pfile = NULL;
	}

	if (pfs != NULL)
	{
		iRet = hdfsDisconnect(pfs);
		if (-1 == iRet)
		{
			syslog(Logger::ERROR, "hdfsDisconnect failed! errno=%d",  errno);
		}
		pfs = NULL;
	}
	
	return ret;
}

bool Hdfs_client::hdfs_record_file(std::string &record_file, std::string &hdfs_file)
{
	bool ret = false;
	hdfsFS pfs = NULL;
	hdfsFile pfile = NULL;
	int iRet = 0;
	
	pfs = hdfsConnect(hdfs_server_ip.c_str(), hdfs_server_port);
	if (NULL == pfs)
	{
		syslog(Logger::ERROR, "hdfsConnect failed! errno=%d", errno);
		goto end;
	}

	iRet = hdfsExists(pfs, record_file.c_str());
	if(iRet == 0)
		pfile = hdfsOpenFile(pfs, record_file.c_str(), O_WRONLY | O_APPEND, 0, 0, 0); 
	else
		pfile = hdfsOpenFile(pfs, record_file.c_str(), O_WRONLY | O_CREAT, 0, hdfs_replication, 0); 
	
	if (NULL == pfile)
	{
		syslog(Logger::ERROR, "hdfsOpenFile failed! szFilePath=%s,errno=%d\n", hdfs_file.c_str(), errno);
		goto end;
	}

	iRet = hdfsWrite(pfs, pfile, hdfs_file.c_str(), hdfs_file.length()); 
	if (-1 == iRet)
	{
		syslog(Logger::ERROR, "hdfsWrite failed! ret=%d,errno=%d\n", iRet, errno);
		goto end;
	}

	iRet = hdfsHFlush(pfs, pfile);
	if (-1 == iRet)
	{
		syslog(Logger::ERROR, "hdfsFlush failed! ret=%d,errno=%d\n", iRet, errno);
		goto end;
	}

	iRet = hdfsCloseFile(pfs, pfile);
	if (-1 == iRet)
	{
		syslog(Logger::ERROR, "hdfsCloseFile failed! ret=%d,errno=%d\n", iRet, errno);
		goto end;
	}
	pfile = NULL;

	ret = true;

end:	
	if(pfile != NULL)
	{
		hdfsCloseFile(pfs, pfile);
		pfile = NULL;
	}

	if (pfs != NULL)
	{
		iRet = hdfsDisconnect(pfs);
		if (-1 == iRet)
		{
			syslog(Logger::ERROR, "hdfsDisconnect failed! errno=%d",  errno);
		}
		pfs = NULL;
	}
	
	return ret;
}

