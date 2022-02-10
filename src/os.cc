/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "sys_config.h"
#include "global.h"
#include "os.h"
#include <signal.h>
#include <unistd.h>

/*
  Make sure count bytes have been written, when write() is interrupted
  by a signal.
*/
ssize_t my_write(int fd, const void *buf, size_t count)
{
	int ret = 0;
retry:
	ret = write(fd, buf, count);
	if (ret > 0 && ret < count)
	{
		buf = ((char*)buf) + ret;
		count -= ret;
		goto retry;
	}
	if (ret < 0 && errno == EINTR)
		goto retry;
	return ret;
}

/*
 * Set up a signal handler, with SA_RESTART, for signal "signo"
 *
 * Returns the previous handler.
 */
sigfunc_t handle_signal(int signo, sigfunc_t func)
{
    struct sigaction act, oact;

    act.sa_handler = func;
    sigemptyset(&act.sa_mask);
    act.sa_flags = SA_RESTART;
//#ifdef SA_NOCLDSTOP
    if (signo == SIGCHLD)
        act.sa_flags |= SA_NOCLDSTOP;
//#endif
    if (sigaction(signo, &act, &oact) < 0)
        return SIG_ERR;
    return oact.sa_handler;
}

