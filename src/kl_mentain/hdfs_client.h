/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef HDFS_CLIENT_H
#define HDFS_CLIENT_H
#include "sys_config.h"
#include <errno.h>
#include "global.h"

#include <pthread.h>
#include <vector>
#include <string>
#include <algorithm>

class Hdfs_client
{
public:
	static int do_exit;
private:
	static Hdfs_client *m_inst;
	Hdfs_client();
public:
	~Hdfs_client();
	static Hdfs_client *get_instance()
	{
		if (!m_inst) m_inst = new Hdfs_client();
		return m_inst;
	}
	void hdfs_init();
	bool hdfs_pull_file(std::string &local_file, std::string &hdfs_file);
	bool hdfs_push_file(std::string &local_file, std::string &hdfs_file);
	bool hdfs_record_file(std::string &record_file, std::string &hdfs_file);
};

#endif // !HDFS_CLIENT_H
