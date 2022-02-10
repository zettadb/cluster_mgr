/*
	 Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

	 This source code is licensed under Apache 2.0 License,
	 combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "global.h"
#include "log.h"
#include "sys.h"
#include "os.h"
#include <time.h>
#include <cstring>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

TLS_VAR char *log_buf = NULL;
TLS_VAR size_t log_buf_len = 0;
Logger::enum_log_verbosity log_verbosity = Logger::INFO;
std::string log_file_path;

int64_t max_log_file_size = 100; // in MBs
inline static size_t get_max_log_file_size()
{
	return max_log_file_size * 1024 * 1024;
}

// must match enum_log_verbosity.
const char *Logger::log_verbosity_options[] = {"ERROR", "WARNING", "INFO", "LOG", "DEBUG1", "DEBUG2", "DEBUG3"};
Logger *Logger::m_inst = NULL;

ssize_t Logger::do_syslog(const char *file, const char *func, int lineno, int elevel, const char *fmt, ...)
{
	if (elevel > log_verbosity)
		return 0;
	if (log_buf_len == 0 || log_buf == NULL)
	{
		Assert(log_buf_len == 0 && log_buf == NULL);
		log_buf = (char *)malloc(log_buf_len = 2048);
	}

	time_t now = time(NULL);
	int len = 0;
	int slen = 0;

	char timebuf[64]; // doc says 26 bytes needed.
	ctime_r(&now, timebuf);
	size_t tblen = strlen(timebuf);
	if (tblen > 0 && timebuf[tblen - 1] == '\n')
		timebuf[tblen - 1] = ' ';

	if (log_verbosity > LOG)
		len = snprintf(log_buf, log_buf_len, "%s [%s] [%s:%d %s]: ", timebuf,
									 log_verbosity_options[elevel], file, lineno, func);
	else
		len = snprintf(log_buf, log_buf_len, "%s [%s] : ", timebuf,
									 log_verbosity_options[elevel]);
	Assert(len < log_buf_len);
	slen = len;

	// log timestamp, log verbosity string; only log file/func/lineno if log_verbosity>LOG
	va_list args;

	va_start(args, fmt);
	len = vsnprintf(log_buf + len, log_buf_len - len - 1, fmt, args);
	va_end(args);
	if (len >= log_buf_len - len - 1)
	{
		// ignore the rest for this time.
		log_buf = (char *)realloc(log_buf, log_buf_len *= 2);
	}

	slen += len;
	log_buf[slen++] = '\n';

	int ret = 0;
	// At initialization, we may need to print
	// error messages before log file path config item is read.
	{
		Scopped_mutex sm(mtx);
		ret = my_write(log_fd > 0 ? log_fd : 2, log_buf, slen);
	}

	if (ret < 0)
	{
		char errbuf[256];
		/*
			If we can't write into the log file we'd better write to stderr instead.
		*/
		dprintf(2, "Failed to write to error log file, error is %d, %s.",
						errno, strerror_r(errno, errbuf, 256));
		return ret;
	}

	cur_bytes += slen;
	bool is_rotating = false;
	if (cur_bytes > get_max_log_file_size() &&
			doing_rotation.compare_exchange_strong(is_rotating, true) &&
			!is_rotating)
		rotate();
	return slen;
}

int Logger::create_instance()
{
	m_inst = new Logger();
	return 0;
}

int Logger::init(const std::string &lfp)
{
	this->log_file_path = lfp;
	return rotate();
}

/*
	@retval: -1: invalid log file path, it must be of this form:
	PATH/FILE or FILE
	PATH must be of this form:
	/log/file/path
	log/file/path

	FILE must be of this form:
	file
	file.suffix
*/
int Logger::generate_log_fn(const std::string &lfp, std::string &fn)
{
	const char *s = lfp.c_str();
	const char *p = strrchr(s, '.');
	// FILE can't be of this form: file.  or .suffix
	if (s >= p || *(p + 1) == '\0')
		return -1;
	if (p == NULL)
		fn = lfp;
	else
	{
		for (const char *i = s; i < p; i++)
			fn.push_back(*i);
	}

	char buf[64];
	struct tm tmnow;
	time_t now = time(NULL);
	localtime_r(&now, &tmnow);

	int len = snprintf(buf, sizeof(buf), "-%d-%.2d-%.2d_%.2d%.2d%.2d",
										 tmnow.tm_year + 1900, tmnow.tm_mon + 1, tmnow.tm_mday, tmnow.tm_hour,
										 tmnow.tm_min, tmnow.tm_sec);
	Assert(len < sizeof(buf));
	fn += buf;

	if (p)
		fn += p;

	return 0;
}

int Logger::rotate()
{
	std::string fn;
	if (generate_log_fn(log_file_path, fn))
		return -1;

	int ret = 0, fd = -1;

	doing_rotation.store(true);
	Scopped_mutex sm(mtx);

	// At initialization log_fd is -1 and cur_bytes is 0, and we need to
	// open the file.
	if (cur_bytes < get_max_log_file_size() && log_fd > 0)
		goto end;

	fd = open(fn.c_str(),
						O_APPEND | O_NOATIME | O_CREAT | O_CLOEXEC | O_RDWR, 0644);
	if (fd < 0)
	{
		char buf[256];
		dprintf(log_fd > 0 ? log_fd : 1,
						"Failed to open file %s for rotation, got error (%d: %s)", fn.c_str(),
						errno, strerror_r(errno, buf, sizeof(buf)));
		ret = -1;
		// keep using this log file.
	}
	else
	{
		if (log_fd > 0)
			close(log_fd);
		log_fd = -1;
		this->log_fd = fd;
		cur_bytes = 0;
	}

end:
	doing_rotation.store(false);

	return ret;
}
