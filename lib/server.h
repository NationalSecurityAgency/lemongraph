#ifndef _SERVER_H
#define _SERVER_H

#include<sys/types.h>

/*
 * forking tcp server
 * returns:
 *	0 in master process when complete
 *	[-workers .. -1] for worker processes, which should
 *		monitor socks[0] and exit when it is closed
 *		use xfd_recv() to receive client connections via socks[0]
 *		on completion, write '\0' to socks[0] to keep the connection alive
 *			or a non-zero byte to close it
 *	[1 .. extras] for extra processes, which should
 *		monitor socks[0] and exit when it is closed
 * also spawns a process for accept()ing new and monitoring idle connections
 */
int server(int *socks, size_t n, ssize_t workers, char **extras, size_t ecount, int level, int sockd_level);

#endif
