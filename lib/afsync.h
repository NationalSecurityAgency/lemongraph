#ifndef _AFSYNC_H
#define _AFSYNC_H

int afsync_receiver(char *dir, unsigned int threads, int sfd);
int afsync_queue(int sfd, char *uuid);
int afsync_unqueue(int sfd, char *uuid);

#endif
