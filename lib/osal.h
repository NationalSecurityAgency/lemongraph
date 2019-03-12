/* https://en.wikipedia.org/wiki/Operating_system_abstraction_layer */

// Inspired by MDBX's abstraction setup, this is just a first step towards platform-specific encapsulation with enough needed to extend support to MacOS. With a bit of copy-paste, LMDB source provides necessary details to add Windows, but current linux binary should run atop WSL.

#ifndef _OSAL_H
#define _OSAL_H

#ifndef _BSD_SOURCE
#define _BSD_SOURCE
#endif

#include<dirent.h>

/*  HANDLE
    An abstraction for a file handle.
    On POSIX systems file handles are small integers. On Windows
    they're opaque pointers.
*/
#define	HANDLE int

int osal_fdatasync(HANDLE fd);
void osal_set_pdeathsig(int sig);
char *_readdir(DIR *dirp);

#endif