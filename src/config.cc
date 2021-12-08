/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "sys_config.h"
#include "global.h"
#include "config.h"
#include <utility>
#include "log.h"
#include "sys.h"
#include <sys/types.h>
#include <unistd.h>
#include <limits.h>

extern int64_t num_worker_threads;
extern int64_t thread_work_interval;
extern int64_t storage_sync_interval;
extern int64_t commit_log_retention_hours;

extern int64_t num_job_threads;
extern int64_t num_http_threads;
extern int64_t cluster_mgr_http_port;
extern std::string http_web_path;
extern std::string http_upload_path;

extern int64_t node_mgr_http_port;
std::string hdfs_server_ip;
int64_t hdfs_server_port;
int64_t hdfs_replication;


Configs *Configs::get_instance()
{
	if (m_inst == NULL)
		m_inst = new Configs(System::get_instance()->get_config_path());
	return m_inst;
}

Configs::Configs(const std::string&cfg_path)
{
	define_configs();
}

/*
  Return if 'name' is already defined as a config parameter.
*/
bool Configs::check_name_conflicts(const char *name)
{
	std::string sname(name);

	if (bool_cfgs.find(sname) != bool_cfgs.end())
		return true;
	if (int_cfgs.find(sname) != int_cfgs.end())
		return true;
	if (uint_cfgs.find(sname) != uint_cfgs.end())
		return true;
	if (str_cfgs.find(sname) != str_cfgs.end())
		return true;
	if (enum_cfgs.find(sname) != enum_cfgs.end())
		return true;
	return false;
}

void Configs::define_bool_config(const char *name, bool &holder, bool def,
	const char *comment)
{
	if (check_name_conflicts(name))
		return;

	Config_entry_bool *cei = new Config_entry_bool;
	cei->name = name;
	cei->comment = comment;
	cei->value_holder = &(holder);
	*cei->value_holder = def;
	bool_cfgs.insert(std::make_pair(cei->name, cei));
}

void Configs::define_str_config(const char *name, std::string &holder,
	const char*def, const char*comment)
{
	if (check_name_conflicts(name))
		return;
	Config_entry_str *cei = new Config_entry_str;
	cei->name = name;
	cei->comment = comment;
	cei->value_holder = &(holder);
	*cei->value_holder = def;
	str_cfgs.insert(std::make_pair(cei->name, cei));
}


void Configs::define_uint_config(const char *name, uint64_t &holder, uint64_t min,
	uint64_t max, uint64_t def, const char *comment)
{
	if (check_name_conflicts(name))
		return;
	Config_entry_uint *cei = new Config_entry_uint;
	cei->name = name;
	cei->min = min;
	cei->max = max;
	cei->comment = comment;
	cei->value_holder = &(holder);
	*cei->value_holder = def;
	uint_cfgs.insert(std::make_pair(cei->name, cei));
}

void Configs::define_int_config(const char *name, int64_t &holder, int64_t min,
	int64_t max, int64_t def, const char *comment)
{
	if (check_name_conflicts(name))
		return;
	Config_entry_int *cei = new Config_entry_int;
	cei->name = name;
	cei->min = min;
	cei->max = max;
	cei->comment = comment;
	cei->value_holder = &(holder);
	*cei->value_holder = def;
	int_cfgs.insert(std::make_pair(cei->name, cei));
}

void Configs::define_enum_config(const char *name, int &holder,
	const char **options, long unsigned int num_opts,
	const char *def_option, const char *comment)
{
	if (check_name_conflicts(name))
		return;
	Config_entry_enum *cei = new Config_entry_enum;
	cei->name = name;
	cei->comment = comment;
	cei->value_holder = &(holder);
	cei->options = options;
	cei->num_options = num_opts;

	int def = -1;
	for (int i = 0; i < num_opts; i++)
	{
		if (strcasecmp(def_option, options[i]) == 0)
		{
			def = i;
			break;
		}
	}
	Assert(def >= 0);

	*cei->value_holder = def;
	enum_cfgs.insert(std::make_pair(cei->name, cei));
}

/*
  Define the set of config parameters and their default values.
*/
void Configs::define_configs()
{
	define_int_config("mysql_connect_timeout", mysql_connect_timeout,
		1, 1000000, 10,
		"MySQL client connection timeout in seconds.");
	define_int_config("mysql_read_timeout", mysql_read_timeout, 1, 1000000, 20,
		"MySQL client read timeout in seconds.");
	define_int_config("mysql_write_timeout", mysql_write_timeout, 1, 1000000, 20,
		"MySQL client write timeout in seconds.");
	define_int_config("mysql_max_packet_size", mysql_max_packet_size,
		65535, 1024*1024*1024, 1024*1024*1024,
		"MySQL client max packet size in bytes.");
	define_enum_config("log_verbosity", (int&)log_verbosity,
		Logger::log_verbosity_options,
		sizeof(Logger::log_verbosity_options)/sizeof(char*), "INFO",
		"Log verbosity or amount of details, options are(in increasing details):{ERROR, WARNING, INFO, LOG, DEBUG1, DEBUG2, DEBUG3}.");
	define_int_config("max_log_file_size", max_log_file_size, 10, 10000, 100,
		"Max log file size in mega bytes(MB). When a log file grows larger than this, a new one is created and used.");
	define_int_config("num_worker_threads", num_worker_threads, 1, 100, 3,
		"Number of worker threads to create.");

	define_int_config("meta_port", meta_svr_port, 0, 65535, 0,
		"meta data server listening port");
	define_str_config("meta_host", meta_svr_ip, "localhost",
		"meta data server ip address");
	define_str_config("meta_user", meta_svr_user, "",
		"meta data server user account");
	define_str_config("meta_pwd", meta_svr_pwd, "",
		"meta data server user's password");
	define_int_config("check_shard_interval", check_shard_interval, 1, 100, 3,
		"Interval in seconds a shard's two checks should be apart.");
	define_int_config("thread_work_interval", thread_work_interval, 1, 100, 3,
		"Interval in seconds a thread waits after it finds no work to do.");
	define_int_config("storage_sync_interval", storage_sync_interval, 1, 300, 60,
		"Interval in seconds a thread waits next storage stats sync.");
	define_int_config("commit_log_retention_hours", commit_log_retention_hours, 24, 24*30, 24,
		"Interval in hours a thread waits next commit_log clear.");
	define_int_config("statement_retries", stmt_retries, 1, 10000, 3,
		"NO. of times a SQL statement is resent for execution when MySQL connection broken.");
	define_int_config("statement_retry_interval_ms", stmt_retry_interval_ms, 1, 1000000, 100,
		"Interval in milli-seconds a statement is resent for execution when it fails and we believe MySQL node will be ready in a while.");

	define_int_config("num_job_threads", num_job_threads, 1, 10, 3,
		"Number of job work threads to create.");
	define_int_config("num_http_threads", num_http_threads, 1, 10, 3,
		"Number of http server threads to create.");
	define_int_config("cluster_mgr_http_port", cluster_mgr_http_port, 1000, 65535, 7878,
		"http server listen port.");

	char def_log_path[64];
	int slen = snprintf(def_log_path, sizeof(def_log_path),
						"./cluster_mgr-%d.log", getpid());
	Assert(slen < sizeof(def_log_path));

	define_str_config("log_file", log_file_path, def_log_path,
		"log file path");

	define_str_config("http_web_path", http_web_path, "../web",
		"http_web_path");
	define_str_config("http_upload_path", http_upload_path, "../upload",
		"http_upload_path");

	define_int_config("node_mgr_http_port", node_mgr_http_port, 0, 65535, 7879,
		"node_mgr_http_port");

	define_int_config("hdfs_server_port", hdfs_server_port, 0, 65535, 0,
		"hdfs_server_port");
	define_str_config("hdfs_server_ip", hdfs_server_ip, "localhost",
		"hdfs_server_ip");
	define_int_config("hdfs_replication", hdfs_replication, 0, 65535, 2,
		"hdfs_replication");


	/*
	  There is no practical way we can prevent multiple cluster_mgr processes
	  from working on the same (set of) clusters, at least the pid file approach
	  deosn't work, since user can start other processes on other machines.
	  We don't want to set flags on metadata server to do so because that could
	  cause no cluster_mgr working at all.
	define_str_config("pid_file_path", pid_file, ".",
		"The directory path to store cluster_mgr's pid file to guarantee only a process is running.");
	*/
}


/*
Set config parameter values.
The setters return 0 if successful;
-1 if name doesn't exist;
-2 value uninterpretable;
-3 if value and type mismatch
-4 value overflow
-5 if value out of bounds;

@param name the parameter to set
@param val the value string to assign to the parameter, it must be a
valid value of its type, and must be in valid range for uint/int params.
*/
int Configs::set_cfg(const std::string &name, const char * val)
{
	int ret = 0;
	if (bool_cfgs.find(name) != bool_cfgs.end())
		ret = set_bool_cfg(name, val);
	else if (int_cfgs.find(name) != int_cfgs.end())
		ret = set_int_cfg(name, val);
	else if (uint_cfgs.find(name) != uint_cfgs.end())
		ret = set_uint_cfg(name, val);
	else if (str_cfgs.find(name) != str_cfgs.end())
		ret = set_str_cfg(name, val);
	else if (enum_cfgs.find(name) != enum_cfgs.end())
		ret = set_enum_cfg(name, val);
	else
		ret = -1;
	switch (ret)
	{
	case 0:
		break;
	case -1:
		syslog(Logger::ERROR, "Variable name %s does not exist", name.c_str());
		break;
	case -2:
		syslog(Logger::ERROR,
			"Variable %s value %s not valid or can not be interpreted.",
			name.c_str(), val);
		break;
	case -3:
		syslog(Logger::ERROR, "Variable %s value %s does not match its type.",
			name.c_str(), val);
		break;
	case -4:
		syslog(Logger::ERROR,
			"Variable %s value %s is not in its type's valid range.",
			name.c_str(), val);
		break;
	case -5:
		// reported in specific functions already
		break;
	default:
		Assert(false);
		break;
	}
	return ret;
}

int Configs::set_uint_cfg(const std::string &name, const char* val)
{
	char *endptr = NULL;
	bool is_neg = false;
	uint64_t ival = strtoull(val, &endptr, 0);
	for (const char *p = val; *p; p++)
	{
		if (!isspace(*p))
		{
			if (*p == '-')
				is_neg = true;
			break;
		}
	}

	if (endptr && *endptr != '\0')
		return -2;
	if (is_neg)
		return -3;
	if (ival == ULLONG_MAX and errno == ERANGE)
		return -4;
	Config_entry_uint *ceu = uint_cfgs.find(name)->second;
	if (ceu->min > ival || ival > ceu->max)
	{
		syslog(Logger::ERROR,
			"Variable %s value %s is not in its defined valid range [%lu, %lu].",
			name.c_str(), val, ceu->min, ceu->max);
		return -5;
	}
	*(ceu->value_holder) = ival;
	return 0;
}

int Configs::set_int_cfg(const std::string &name, const char* val)
{
	char *endptr = NULL;
	uint64_t ival = strtoll(val, &endptr, 0);

	if (endptr && *endptr != '\0')
		return -2;
	if (ival == ULLONG_MAX and errno == ERANGE)
		return -4;
	Config_entry_int *cei = int_cfgs.find(name)->second;
	if (cei->min > ival || ival > cei->max)
	{
		syslog(Logger::ERROR,
			"Variable %s value %s is not in its defined valid range [%ld, %ld].",
			name.c_str(), val, cei->min, cei->max);
		return -5;
	}
	*(cei->value_holder) = ival;
	return 0;
}


int Configs::set_str_cfg(const std::string &name, const char* val)
{
	Config_entry_str *cei = str_cfgs.find(name)->second;
	*(cei->value_holder) = val;
	return 0;
}


int Configs::set_bool_cfg(const std::string &name, const char* val)
{
	Config_entry_bool *cei = bool_cfgs.find(name)->second;
	bool v = false;
	if (strcasecmp(val, "true") == 0 || strcasecmp(val, "on") == 0 ||
		strcasecmp(val, "yes") == 0)
		v = true;
	else if (strcasecmp(val, "false") == 0 || strcasecmp(val, "off") == 0 ||
		strcasecmp(val, "no") == 0)
		v = false;
	else
		return -2;
	*(cei->value_holder) = v;
	return 0;
}

int Configs::set_enum_cfg(const std::string &name, const char* val)
{
	char *endptr = NULL;
	std::string sval;

	bool started = false;

	// fetch valid enum value
	for (const char *p = val; *p; p++)
	{
		if (!started && !isspace(*p))
		{
			started = true;
		}
		else if (started && isspace(*p))
		{
			started = false;
			break;
		}

		if (started)
			sval.push_back(*p);
	}

	Config_entry_enum *cei = enum_cfgs.find(name)->second;
	int eval = -1;
	for (int i = 0; i < cei->num_options; i++)
		if (strcasecmp(sval.c_str(), cei->options[i]) == 0)
		{
			eval = i;
			break;
		}

	if (eval < 0)
		return -2;

	*(cei->value_holder) = eval;
	return 0;
}

/*
  Return the NO. of all the key variables/configs are NOT assigned a value.
  'vars' takes back the names of such variables.

  Such vars don't have default values to use, so they must be assigned.
*/
int Configs::check_key_vars_set(std::string &vars)
{
	int cnt = 0;
	if (meta_svr_port == 0) { cnt++; vars = "port"; }
	if (meta_svr_ip.length() == 0) { cnt++; vars += "; host"; }
	if (meta_svr_user.length() == 0) { cnt++; vars += "; user"; }
	if (meta_svr_pwd.length() == 0) { cnt++; vars += "; password"; }
	if (log_file_path.length() == 0) { cnt++; vars += "; log_file"; }
	return cnt;
}

class FILE_closer
{
	FILE *_fp;
public:
	FILE_closer(FILE *fp) : _fp(fp)
	{}
	~FILE_closer() { if (_fp) fclose(_fp); }
};

/*
  Return 0 on success;
  -9 on log entry format error
  -8 if there are vars that must be assigned a value are not so.
*/
int Configs::process_config_file(const std::string &fn)
{
	char line[1024];
	FILE *fp = fopen(fn.c_str(), "r");
	int ret = 0;
	FILE_closer fcloser(fp);

	while (fgets(line, sizeof(line), fp))
	{
		char *p;
		/* trim spaces from start of log entry (and also name string). */
		for (p = line; *p; p++)
			if (!isspace(*p))
				break;

		if (!*p || *p == '#')
			continue;

		char *s = strchr(p, '=');
		if (!s || s <= p)
		{
			syslog(Logger::ERROR, "Invalid log file format in line '%s'", line);
			return -9;
		}
		
		/* trim spaces from end of name string. */
		char *q;
		for (q = s - 1; q > p; q--)
			if (!isspace(*q))
				break;
		q++;
		*q = '\0';

		std::string varname(p);

		/* trim spaces from both sides of value string. */
		for (q = s + 1; *q; q++)
			if (!isspace(*q))
				break;
		char *r;
		for (r = q; *r; r++)
			if (isspace(*r))
				break;
		*r = '\0';

		if (q >= r) // no value part
		{
			syslog(Logger::ERROR, "Invalid log file format in line '%s'", line);
			return -9;
		}

		if ((ret = set_cfg(varname, q)) < 0)
			return ret;
	}

	std::string bad_vars;
	int nbad = 0;
	if ((nbad = check_key_vars_set(bad_vars)) > 0)
	{
		syslog(Logger::ERROR,
			"Some (%d) critical variables in config file not assigned a value : %s",
			nbad, bad_vars.c_str());
		return -8;
	}
	return 0;
}

Configs *Configs::m_inst = NULL;
