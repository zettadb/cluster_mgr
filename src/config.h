/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef CONFIG_H
#define CONFIG_H
#include "sys_config.h"
#include "global.h"
#include <string>
#include <map>

class Configs
{
	struct Config_entry_str
	{
		std::string name, comment, *value_holder;
	};
	struct Config_entry_int
	{
		std::string name, comment;
		int64_t min, max, *value_holder;
	};
	struct Config_entry_uint
	{
		std::string name, comment;
		uint64_t min, max, *value_holder;
	};
	struct Config_entry_bool
	{
		std::string name, comment;
		bool *value_holder;
	};
	struct Config_entry_enum
	{
		std::string name, comment;
		int *value_holder;
		const char **options;
		int num_options;
	};

	std::map<std::string, Config_entry_int*> int_cfgs;
	std::map<std::string, Config_entry_uint*> uint_cfgs;
	std::map<std::string, Config_entry_str*> str_cfgs;
	std::map<std::string, Config_entry_bool*> bool_cfgs;
	std::map<std::string, Config_entry_enum*> enum_cfgs;
	Configs(const Configs&);
	Configs&operator=(const Configs&);
	Configs();

	void define_bool_config(const char *name, bool &holder, bool def,
		const char *comment);
	void define_str_config(const char *name, std::string &holder,
		const char*def, const char*comment);
	void define_uint_config(const char *name, uint64_t &holder, uint64_t min,
		uint64_t max, uint64_t def, const char *comment);
	void define_int_config(const char *name, int64_t &holder, int64_t min,
		int64_t max, int64_t def, const char *comment);
	void define_enum_config(const char *name, int &holder, const char **options,
		long unsigned int num_opts, const char *def_option, const char *comment);
	bool check_name_conflicts(const char *name);
	
	Configs(const std::string&cfg_path);
	
	void define_configs();
	
	static Configs *m_inst;

	int set_uint_cfg(const std::string &name, const char * val);
	int set_int_cfg(const std::string &name, const char * val);
	int set_str_cfg(const std::string &name, const char * val);
	int set_bool_cfg(const std::string &name, const char * val);
	int set_enum_cfg(const std::string &name, const char * val);
	int check_key_vars_set(std::string &vars);
public:
	static Configs *get_instance();
	int process_config_file(const std::string &fn);
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
	int set_cfg(const std::string &name, const char * val);
};

#endif // !CONFIG_H
