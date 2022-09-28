/* https://en.wikipedia.org/wiki/Operating_system_abstraction_layer*/

#include <unistd.h>

#if defined(__linux__)
#include <prctl.h>
#endif

#include"osal.h"

int
osal_fdatasync(HANDLE fd)
{
#if defined(__linux__)
    return fdatasync(fd);
#else
    return fsync(fd);
#endif
}

void
osal_set_pdeathsig(int sig)
{
#if defined(__linux__)
	prctl(PR_SET_PDEATHSIG, sig);
#else
    // TODO:
#endif
}

char *
_readdir(DIR *dirp){
    struct dirent *de = readdir(dirp);
    return de ? de->d_name : NULL;
}