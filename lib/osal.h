#ifndef _OSAL_H
#define _OSAL_H

#include<unistd.h>

#ifndef HAVE_FDATASYNC
#if _POSIX_SYNCHRONIZED_IO > 0
#define HAVE_FDATASYNC 1
#endif
#endif

#if HAVE_FDATASYNC
#define FDATASYNC(fd) fdatasync((fd))
#else

// macOS
#ifdef F_FULLFSYNC
#warning fdatasync(fd) => fcntl(fd, F_FULLSYNC)
#define FDATASYNC(fd) fcntl((fd), F_FULLFSYNC)
#else
// fallback to fsync
#warning fdatasync(fd) => fsync(fd)
#define FDATASYNC(fd) fsync((fd))
#endif

#endif

#endif
