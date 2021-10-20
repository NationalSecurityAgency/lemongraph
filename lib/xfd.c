#include<errno.h>
#include<string.h>

#include "xfd.h"

int xfd_send(int sock, int fd, void *byte){
	static char z = 0;
	struct iovec io = {
		.iov_len = 1,
		.iov_base = byte ? byte : &z,
	};
	union {
		struct cmsghdr cmsg;
		char buffer[CMSG_LEN(sizeof(int))];
	} cmsg = {
		.cmsg = {
			.cmsg_level = SOL_SOCKET,
			.cmsg_type = SCM_RIGHTS,
			.cmsg_len = sizeof(cmsg.buffer),
		}
	};
	struct msghdr msg = {
		.msg_iov = &io,
		.msg_iovlen = 1,
		.msg_name = NULL,
		.msg_namelen = 0,
		.msg_control = &cmsg.cmsg,
		.msg_controllen = sizeof(cmsg.buffer),
		.msg_flags = 0,
	};

	memcpy(CMSG_DATA(&cmsg.cmsg), &fd, sizeof(int));
	return sendmsg(sock, &msg, 0);
}

int xfd_recv(int sock, int *fd, void *byte){
	static char z = 0;
	struct iovec io = {
		.iov_len = 1,
		.iov_base = byte ? byte : &z,
	};
	union {
		struct cmsghdr cmsg;
		char buffer[CMSG_LEN(sizeof(int))];
	} cmsg = {
		.cmsg = {
			.cmsg_level = SOL_SOCKET,
			.cmsg_type = SCM_RIGHTS,
			.cmsg_len = sizeof(cmsg.buffer),
		}
	};
	struct msghdr msg = {
		.msg_iov = &io,
		.msg_iovlen = 1,
		.msg_name = NULL,
		.msg_namelen = 0,
		.msg_control = &cmsg.cmsg,
		.msg_controllen = sizeof(cmsg.buffer),
		.msg_flags = 0,
	};
	int r = recvmsg(sock, &msg, 0);
	if(1 == r){
		if(SOL_SOCKET == cmsg.cmsg.cmsg_level && SCM_RIGHTS == cmsg.cmsg.cmsg_type && CMSG_LEN(sizeof(int)) ==  cmsg.cmsg.cmsg_len){
			memcpy(fd, CMSG_DATA(&cmsg.cmsg), sizeof(int));
			return 1;
		}else{
			errno = EBADMSG;
			r = -1;
		}
	}
	return r;
}
