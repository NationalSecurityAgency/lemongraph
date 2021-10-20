#ifndef _XFD_H
#define _XFD_H

#include<sys/types.h>
#include<sys/socket.h>

int xfd_send(int sock, int fd, void *byte);
int xfd_recv(int sock, int *fd, void *byte);

#endif
