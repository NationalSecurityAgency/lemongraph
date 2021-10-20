#ifdef NDEBUG
#undef NDEBUG
#endif

#include<assert.h>
#include<errno.h>
#include<fcntl.h>
#include<limits.h>
#include<pthread.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<sys/time.h>
#include<unistd.h>

#include"avl.h"
#include"afsync.h"
#include"logging.h"
#include"osal.h"

#define MAX(a, b) ((a) > (b) ? (a) : (b))

static unsigned char idx_uuid[16] = {0};

// intentionally does not null terminate output buffer
static inline void uuid_b2a(char *out, const unsigned char *in){
	static const char tbl[] = "0123456789abcdef";
	const char *dash = "\x04\x06\x08\x0a";
	unsigned char ch, i = 0;
	while(i < 16){
		ch = in[i];
		*(out++) = tbl[ch >> 4];
		*(out++) = tbl[ch & 0xf];
		// output needs hyphens at input byte offsets 4, 6, 8, and 10
		if(*dash == ++i){
			*(out++) = '-';
			dash++;
		}
	}
}

static inline int uuid_a2b(unsigned char *out, const char *in){
	int i = 0;
	int ch;
	unsigned char hi;
	int flag = 1;
	while(i < 16){
		ch = *(in++);
		if(ch >= '0' && ch <= '9'){
			ch -= '0';
		}else if(ch >= 'a' && ch <= 'f'){
			ch = 10 + ch - 'a';
		}else if(ch >= 'A' && ch <= 'F'){
			ch = 10 + ch - 'A';
		}else if('-' == ch){
			continue;
		}else{
			return ~i;
		}
		if(flag)
			hi = ch << 4;
		else
			out[i++] = hi | ch;
		flag = !flag;
	}
	return 0;
}


static inline int recv_msg(int fd, unsigned char *id){
	ssize_t rem = 17;
	ssize_t off = 0;
	ssize_t len;

	while(1){
		len = read(fd, id + off, rem);
		if(len == rem){
			break;
		}else if(-1 == len){
			if(EINTR == errno || EAGAIN == errno)
				continue;
			break;
		}else if(0 == len){
			break;
		}
		rem -= len;
		off += len;
	}
	return (int)len;
}

typedef struct _msg_t * msg_t;
struct _msg_t {
	msg_t next, prev;
	// first and last seen timestamps
	time_t ts0, ts1;
	// code byte + 16 packed uuid bytes
	unsigned char msg[17];
};

typedef struct _shared_t * shared_t;
struct _shared_t {
	const char *dir;
	pthread_mutex_t *mutex;
	pthread_cond_t *cond_tick, *cond_empty;
	msg_t head, cursor, end, trash;
	unsigned int active, threads;
	int min_delta, max_delta;
	time_t now, min, max, start;
	avl_tree_t idx;
	int syncs, dupes;
	int sfd;
};

static inline void _afs_tick_thread(shared_t s){
	const msg_t head = s->head;
	const msg_t end = s->end;
	int active = 0, active2;
	time_t now;
	int age, syncs, dupes, elapsed;
	const char *fmt = "backlog %d, syncs %d, dupes %d, age %ds, elapsed %ds";
	struct timespec tick = { 0 };
	pthread_mutex_lock(s->mutex);
	do{
		active2 = s->active;
		s->now = now = time(0);
		s->min = now - s->min_delta;
		s->max = now - s->max_delta;
		s->cursor = head->next;

		age = (end == s->cursor) ? 0 : now - s->cursor->ts0;
		elapsed = now - s->start;
		syncs = s->syncs;
		dupes = s->dupes;
		pthread_cond_signal(s->cond_tick);
		pthread_mutex_unlock(s->mutex);
		if(active || active2)
			logmsg(LOG_INFO, fmt, active2, syncs, dupes, age, elapsed);
		active = active2;
		tick.tv_sec = now + 1;
		pthread_mutex_lock(s->mutex);
		do{
			pthread_cond_timedwait(s->cond_tick, s->mutex, &tick);
		}while(s->cursor && time(0) == now);
	}while(s->cursor);
	if(active2)
		logmsg(LOG_INFO, fmt, s->active, s->syncs, s->dupes, 0, now - s->start);
	s->threads--;
	pthread_cond_signal(s->cond_tick);
	pthread_mutex_unlock(s->mutex);
}

static inline void _afs_sync_thread(shared_t s){
	msg_t c, trash = NULL;
	const msg_t end = s->end;
	const int fdc = 1;
	unsigned char *bin_uuids[fdc];
	int i, fd, dirlen;
	char path[PATH_MAX];

	dirlen = strlen(s->dir);
	strcpy(path, s->dir);

	pthread_mutex_lock(s->mutex);
	while((c = s->cursor)){
		// list is sorted by ts0, ascending
		if(c == end || (c->ts0 > s->max)){
			pthread_cond_wait(s->cond_tick, s->mutex);
			continue;
		}
		for(i = 0; i < fdc && c != end && c->ts0 <= s->max; c = c->next){
			if(c->ts0 <= s->min || c->ts1 <= s->max){
				bin_uuids[i++] = c->msg + 1;
				c->prev->next = c->next;
				c->next->prev = c->prev;
				c->prev = trash;
				trash = c;
				avl_delete(s->idx, c);
			}
		}
		s->cursor = c;

		if(fdc == i)
			pthread_cond_signal(s->cond_tick);
		else if(0 == i)
			continue;

		const int syncs = i;
		pthread_mutex_unlock(s->mutex);

		strcpy(path + dirlen + 37, ".db");
		for(i = 0; i < syncs; i++){
			if(memcmp(bin_uuids[i], idx_uuid, 16) == 0){
				// target: path/to/graphs.idx
				strcpy(path + dirlen, ".idx");
			}else{
				// target: path/to/graphs/<uuid>.idx
				path[dirlen] = '/';
				uuid_b2a(path + dirlen + 1, bin_uuids[i]);
			}
			fd = open(path, O_RDONLY);
			if(-1 == fd){
				perror(path);
			}else{
				FDATASYNC(fd);
				close(fd);
			}
		}

		pthread_mutex_lock(s->mutex);
		while((c = trash)){
			c->next = s->trash;
			s->trash = c;
			trash = c->prev;
		}
		s->syncs += syncs;
		s->active -= syncs;
		if(!s->active)
			pthread_cond_signal(s->cond_empty);
	}
	s->threads--;
	pthread_cond_signal(s->cond_tick);
	pthread_mutex_unlock(s->mutex);
}

static void *_afs_sync_thread_wrapper(void *arg){
	_afs_sync_thread((shared_t)arg);
	return NULL;
}

static void *_afs_tick_thread_wrapper(void *arg){
	_afs_tick_thread((shared_t)arg);
	return NULL;
}

static int _afs_id_cmp(void *a, void *b, void *data){
	return memcmp(((msg_t)a)->msg + 1, ((msg_t)b)->msg + 1, 16);
}

// fork and run this in a child process
int afsync_receiver(char *dir, unsigned int threads, int sfd){
	pthread_t thr;
	int r, ret = 1;
	unsigned int i;
	msg_t m, *mp;
	struct _msg_t qhead = {0}, qtail = {0};

	avl_tree_t idx = avl_new(_afs_id_cmp, NULL);
	if(!idx)
		goto error0;

	// init empty linked list
	qhead.prev = qtail.next = NULL;
	qhead.next = &qtail;
	qtail.prev = &qhead;

	pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
	pthread_cond_t cond_tick = PTHREAD_COND_INITIALIZER;
	pthread_cond_t cond_empty = PTHREAD_COND_INITIALIZER;

	struct _shared_t s = {
		.dir = dir,
		.head = &qhead,
		.cursor = &qtail,
		.end = &qtail,
		.mutex = &mutex,
		.cond_tick = &cond_tick,
		.cond_empty = &cond_empty,
		.idx = idx,
		.threads = 0,
		.trash = NULL,
		.min_delta = 15,
		.max_delta = 5,
		.sfd = sfd,
	};

	logmsg(LOG_INFO, "%u threads", threads);

	// spawn ticker thread and wait for first tick
	pthread_mutex_lock(&mutex);
	r = pthread_create(&thr, NULL, _afs_tick_thread_wrapper, &s);
	if(0 == r)
		pthread_cond_wait(&cond_tick, &mutex);
	pthread_mutex_unlock(&mutex);
	if(r)
		goto error1;
	s.threads = 1;

	// spawn sync threads
	for(i = 0; i < threads; i++){
		r = pthread_create(&thr, NULL, _afs_sync_thread_wrapper, &s);
		if(r)
			break;
	}
	if(!i){
		pthread_mutex_lock(&mutex);
		goto error2;
	}
	s.threads += i;

	m = NULL;
	pthread_mutex_lock(&mutex);
	while(1){
		pthread_mutex_unlock(&mutex);

		// allocate new buffer if trash was empty
		if(!m)
			m = malloc(sizeof(*m));

		// wait for a message
		if(!recv_msg(sfd, m->msg))
			break;

		pthread_mutex_lock(&mutex);
		if(m->msg[0] == '+'){
			// upsert into index - first attempt insert
			mp = (msg_t *)avl_insert(s.idx, m);
			if(*mp == m){
				// if successful, set timestamps
				m->ts0 = m->ts1 = s.now;

				// bookkeeping
				if(0 == s.active++){
					s.dupes = s.syncs = 0;
					s.start = s.now;
				}

				// insert at tail
				m->prev = qtail.prev;
				m->prev->next = m;
				m->next = &qtail;
				qtail.prev = m;
				// attempt to pop buffer off of trash stack
				if((m = s.trash))
					s.trash = m->next;
			}else{
				// else if already present
				msg_t found = *mp;
				// update last seen timestamp
				found->ts1 = s.now;
				s.dupes++;
			}
		}else if(m->msg[0] == '-'){
			mp = (msg_t *)avl_delete(s.idx, m);
			if(mp){
				msg_t tmp = *mp;
				if(s.cursor == tmp)
					s.cursor = tmp->next;
				// if found, remove from list
				tmp->prev->next = tmp->next;
				tmp->next->prev = tmp->prev;
				// push push onto trash stack
				tmp->next = s.trash;
				s.trash = tmp;
				if(!--s.active)
					pthread_cond_signal(s.cond_empty);
			}
		}else{
			assert(0);
		}
	}

	ret = 0;
	s.min_delta = 0;
	s.max_delta = 0;

	// wait for fds to be synced
	while(s.active)
		pthread_cond_wait(&cond_empty, &mutex);

error2:
	// tell threads to stop
	s.cursor = NULL;
	pthread_cond_broadcast(&cond_tick);

	// wait for threads to exit
	while(s.threads)
		pthread_cond_wait(&cond_tick, &mutex);
	pthread_mutex_unlock(&mutex);

	// free trash stack
	msg_t tmp;
	while((tmp = s.trash)){
		s.trash = s.trash->next;
		free(tmp);
	}

error1:
	avl_free(idx);
error0:
	logmsg(LOG_INFO, "exit");
	return ret;
};

static inline int _afsync_send(int sfd, int code, char *uuid){
	unsigned char msg[17];
	ssize_t len;
	msg[0] = code;
	len = uuid_a2b(msg+1, uuid);
	assert(!len);

	while(1){
		len = write(sfd, msg, sizeof(msg));
		if(sizeof(msg) == len){
			break;
		}else if(-1 != len){
			assert(0);
			return 0;
		}
	}
	return 1;
}

int afsync_queue(int sfd, char *uuid){
	return _afsync_send(sfd, '+', uuid);
}

int afsync_unqueue(int sfd, char *uuid){
	return _afsync_send(sfd, '-', uuid);
}
