/*
   Copyright (c) 2019 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef LOG_H
#define LOG_H
#include "sys_config.h"
#include "global.h"
#include <atomic>
#include <pthread.h>
#include <string>

class Logger
{
public:
	enum enum_log_verbosity
	{
	  ERROR, WARNING, INFO, LOG, DEBUG1, DEBUG2, DEBUG3
	};
	
	static const char *log_verbosity_options[7];
private:
	std::atomic<bool> doing_rotation;
	pthread_mutex_t mtx;
	int log_fd;
	size_t cur_bytes;
	std::string log_file_path;
	int rotate();
	Logger() : doing_rotation(false),
		log_fd(-1), cur_bytes(0)
	{
		pthread_mutex_init(&mtx, NULL);
	}
	static Logger*m_inst;
	int generate_log_fn(const std::string&lfp, std::string&fn);
public:
	~Logger() { pthread_mutex_destroy(&mtx); }
	static int create_instance();
	int init(const std::string&lfp);
	static Logger*get_instance() { return m_inst; }
	ssize_t do_syslog(const char *file, const char *func, int lineno,
		int elevel, const char *fmt,...);
};

extern std::string log_file_path;
extern int64_t max_log_file_size;
extern Logger::enum_log_verbosity log_verbosity;
#define syslog(elevel, ...) Logger::get_instance()->do_syslog(__FILE__, __func__, __LINE__, elevel, __VA_ARGS__)
#endif // !LOG_H
