/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef GLOBAL_INCLUDED
#define GLOBAL_INCLUDED
#include <cstdint>
#include <cstdlib>
#include <cstdio>
#include <cassert>
#include <strings.h>
#include <cstring>
#include <pthread.h>
#include <string>
#include <vector>
#include <tuple>

typedef uint32_t uint;
#ifdef ENABLE_DEBUG
#define Assert(expr) assert(expr)
#else
#define Assert(expr)
#endif

//some sql buf strlen(const_str) < sizeof(const_str)-1
//#define CONST_STR_PTR_LEN(const_str) const_str,(sizeof(const_str) - 1)
#define CONST_STR_PTR_LEN(const_str) const_str,strlen(const_str)
#define ABS(a,b) (a>b?(a-b):(b-a))

#define KUNLUN_METADATA_DBNAME "Kunlun_Metadata_DB"

typedef std::tuple<std::string, int, int, int, int, int, int, int> Tpye_cluster_info;
typedef std::tuple<std::string, int, std::string, std::string> Tpye_Ip_Port_User_Pwd;
typedef std::tuple<std::string, int, std::vector<std::string>> Tpye_Ip_Port_Paths;
typedef std::tuple<std::string, std::string, int> Tpye_Shard_Ip_Port;
typedef std::tuple<std::string, int, int> Tpye_Path_Used_Free;

typedef std::tuple<std::string, std::string> Tpye_string2;
typedef std::tuple<std::string, std::string, std::string> Tpye_string3;
typedef std::tuple<std::string, std::string, std::string, std::string> Tpye_string4;
typedef std::tuple<std::string, std::string, std::string, std::string, std::string> Tpye_string5;

typedef std::pair<std::string, int> Tpye_Ip_Port;
typedef std::pair<std::string, int> Tpye_Path_Space;
typedef std::pair<std::string, std::vector<std::string>> Tpye_Ip_Paths;


#if __GLIBC__ == 2 && __GLIBC_MINOR__ < 30
#include <sys/syscall.h>
#define gettid() syscall(SYS_gettid)
#endif

inline bool likely(bool expr) { return __builtin_expect(expr, true); }
inline bool unlikely(bool expr) { return __builtin_expect(expr, false); }

class Scopped_mutex
{
	pthread_mutex_t &mtx;
public:
	Scopped_mutex(pthread_mutex_t&m) : mtx(m)
	{
		pthread_mutex_lock(&mtx);
	}

	~Scopped_mutex()
	{
		pthread_mutex_unlock(&mtx);
	}
};

#endif // !GLOBAL_INCLUDED
