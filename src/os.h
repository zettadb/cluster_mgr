/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef OS_H
#define OS_H
#include "sys_config.h"
#include <errno.h>
#include "global.h"

#define TLS_VAR __thread 

typedef void (*sigfunc_t) (int signo);
sigfunc_t handle_signal(int signo, sigfunc_t func);

ssize_t my_write(int fd, const void *buf, size_t count);
#endif // !OS_H
