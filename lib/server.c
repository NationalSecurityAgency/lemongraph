#include<assert.h>
#include<arpa/inet.h>
#include<errno.h>
#include<fcntl.h>
#include<limits.h>
#include<poll.h>
#include<signal.h>
#include<stdlib.h>
#include<string.h>
#include<sys/ioctl.h>
#include<sys/resource.h>
#include<sys/socket.h>
#include<sys/un.h>
#include<sys/wait.h>
#include<unistd.h>

#include"server.h"
#include"logging.h"
#include"list.h"
#include"xfd.h"


/*
static int in_list(list_t ls, void *t){
	void *chk = list_peek(ls, 0);
	while(chk){
		if(t == chk)
			return 1;
		chk = list_next(ls, chk, 0);
	}
	return 0;
}
#define list_remove(a, b) do{ assert(in_list(a, b)); list_remove(a, b); } while(0)
*/

// binary search to find max number of file descriptors
// assumes valid fds must be consecutively numbered - if we are
// allowed 1024 fds, then dup2() cannot return larger than 1023
static int maxfds(void){
	int fd = socket(AF_UNIX, SOCK_STREAM, 0);
	int lo = fd + 1;
	int hi = INT_MAX;
	int mid;
	while(lo < hi){
		mid = lo + (hi - lo)/2;
		if(fcntl(mid, F_GETFD) != -1 || errno != EBADF){
			lo = mid + 1;
		}else if(dup2(fd, mid) == mid){
			lo = mid + 1;
			close(mid);
		}else{
			hi = mid;
		}
	}
	close(fd);
	return lo;
}

static int cmp_ints(const void *a, const void *b){
	return (*(int *)a > *(int *)b) - (*(int *)a < *(int *)b);
}

static int pack_fds(int *fds, int n, int maxfd){
	// keep stdin/stdout/stderr
	// pack server sockets into next n descriptors
	int fd;
	qsort(fds, n, sizeof(*fds), cmp_ints);
	for(fd = 3; n; fd++, fds++, n--)
		if(*fds != fd){
			*fds = dup2(*fds, fd);
			if(*fds != fd)
				abort();
		}
	// save number of fds in use
	n = fd;
	// close remaining fds
	while(fd < maxfd)
		close(fd++);
	return n;
}

typedef struct {
	list_t ls;
	int fd;
	char byte;
} wc_t;

static int sockd(int *sfds, int nsfds, int sockd_level){
	loginit(sockd_level, "LemonGraph.sockd(%d)", (int)getpid());

	// all of the file descriptors
	struct rlimit rlim;
	if(getrlimit(RLIMIT_NOFILE, &rlim) == 0 && rlim.rlim_cur != rlim.rlim_max){
		rlim.rlim_cur = rlim.rlim_max;
		setrlimit(RLIMIT_NOFILE, &rlim);
	}
	// find upper bound
	const int max = maxfds();

	// pack server sockets starting at fd 3
	int avail = max - pack_fds(sfds, nsfds, max);
	int nworkers = 0;

	logmsg(LOG_INFO, "max workers/connections: %d", avail);

	// 32 (x86_64) or 24 (x86) bytes per fd
	struct pollfd *pfds;
	wc_t *cfds;

	// allocate in one chunk
	cfds = malloc((sizeof(*pfds) + sizeof(*cfds)) * max);
	pfds = (struct pollfd *)(cfds + max);

	list_t spare;          // pool of connection/worker objects
	list_t conn_idle;      // idle connected client sockets
	list_t conn_ready;     // connected client sockets waiting to be serviced
	list_t workers_idle;   // idle worker sockets
	list_t workers_active; // pairs of worker + connected client sockets

	list_init(spare);
	list_init(conn_idle);
	list_init(conn_ready);
	list_init(workers_idle);
	list_init(workers_active);

	int i, one = 1;
	for(i = 0; i < max; i++)
		list_push(spare, cfds + i, 0);

	for(i = 0; i < nsfds; i++)
		ioctl(sfds[i], FIONBIO, &one);

	struct pollfd *pfd;
	int n, listening = 1;
	int poll_master;
	int poll_accept;
	int poll_active;
	int poll_idle;
	wc_t *worker, *conn, *next;
	while(listening || !list_empty(workers_active)){
		pfd = pfds;
		poll_master = poll_accept = poll_active = poll_idle = 0;

		if(avail && listening){
			// look for new workers
			poll_master++;
			pfd->fd = 0;
			pfd->events = POLLIN;
			pfd++;

			// look for new connections
			for(i = 0; i < nsfds; i++){
				poll_accept++;
				pfd->fd = nworkers ? sfds[i] : ~sfds[i];
				pfd->events = POLLIN;
				pfd++;
			}
		}else if(!listening){
			conn = list_pop(conn_idle, 0);
			while(conn){
				list_push(spare, conn, 0);
				logmsg(LOG_DEBUG, "conn_close(%d)", conn->fd);
				close(conn->fd);
				avail++;
				conn = list_pop(conn_idle, 0);
			}
		}

		// look for completed or dead workers
		worker = list_peek(workers_active, 0);
		while(worker){
			poll_active++;
			pfd->fd = worker->fd;
			pfd->events = POLLIN;
			pfd++;
			// next item is the connection that is being worked
			conn = list_next(workers_active, worker, 0);
			if(!conn)
				abort();
			// grab next running worker
			worker = list_next(workers_active, conn, 0);
		}

		// look for idle connections w/ new activity
		conn = list_peek(conn_idle, 0);

		while(conn){
			poll_idle++;
			pfd->fd = conn->fd;
			pfd->events = POLLIN;
			pfd++;
			conn = list_next(conn_idle, conn, 0);
		}

		if(pfd == pfds)
			break;

		// poll on the collected fds
		do{
			n = poll(pfds, pfd - pfds, -1);
		}while(n < 1);

		pfd = pfds;

		// receive new worker sockets
		for(; n && poll_master && avail; poll_master--, pfd++){
			if(pfd->revents){
				n--;
				worker = list_peek(spare, 0);
				if(!worker)
					abort();
				int r = xfd_recv(0, &worker->fd, NULL);
				if(1 == r){
					list_remove(spare, worker);
					list_push(workers_idle, worker, 1);
					logmsg(LOG_DEBUG, "worker_add(%d) => %d", worker->fd, ++nworkers);
					avail--;
				}else if(0 == r || EINTR != errno){
					listening = 0;
				}
			}
		}

		// accept new client connections
		for(; n && poll_accept && avail; poll_accept--, pfd++){
			if(pfd->revents){
				n--;
				conn = list_peek(spare, 0);
				if(!conn)
					abort();
				conn->fd = accept(pfd->fd, NULL, NULL);
				if(conn->fd != -1){
					logmsg(LOG_DEBUG, "accept(%d) => %d", pfd->fd, conn->fd);
					ioctl(conn->fd, FIONBIO, &one);
					list_remove(spare, conn);
					list_push(conn_idle, conn, 1);
					avail--;
				}
			}
		}
		pfd += poll_accept;

		// get response from workers
		worker = list_peek(workers_active, 0);
		for(; n && poll_active; poll_active--, pfd++, worker = next){
			if(!worker)
				abort();
			conn = list_next(workers_active, worker, 0);
			if(!conn)
				abort();
			next = list_next(workers_active, conn, 0);
			if(pfd->revents){
				n--;
				ssize_t r = recv(worker->fd, &conn->byte, 1, 0);
				// ignore interrupted read
				if(-1 == r && EINTR == errno)
					continue;
				list_remove(workers_active, worker);
				list_remove(workers_active, conn);
				if(0 == r)
					conn->byte = 2;
				if(conn->byte & 2){
					list_push(spare, worker, 0);
					close(worker->fd);
					logmsg(LOG_DEBUG, "worker_del(%d) => %d", worker->fd, --nworkers);
					avail++;
				}else{
					list_push(workers_idle, worker, 0);
				}
				if(conn->byte & 1){
					logmsg(LOG_DEBUG, "conn_idle(%d)", conn->fd);
					list_push(conn_idle, conn, 1);
				}else{
					logmsg(LOG_DEBUG, "conn_close(%d)", conn->fd);
					list_push(spare, conn, 0);
					close(conn->fd);
					avail++;
				}
			}
		}
		pfd += poll_active;

		// try to read from idle connections w/ activity
		conn = list_peek(conn_idle, 0);
		for(; n && poll_idle; poll_idle--, pfd++, conn = next){
			if(!conn)
				abort();
			next = list_next(conn_idle, conn, 0);
			if(pfd->revents){
				n--;
				ssize_t r = recv(conn->fd, &conn->byte, 1, 0);
				if(1 == r){
					// move to ready
					list_remove(conn_idle, conn);
					list_push(conn_ready, conn, 1);
					logmsg(LOG_DEBUG, "conn_ready(%d)", conn->fd);
				}else if(0 == r || (EINTR != errno && EAGAIN != errno && EWOULDBLOCK != errno)){
					// close on eof or error
					list_remove(conn_idle, conn);
					list_push(spare, conn, 0);
					logmsg(LOG_DEBUG, "conn_close(%d)", conn->fd);
					close(conn->fd);
					avail++;
				}
			}
		}
//		pfd += poll_idle;
		worker = list_peek(workers_idle, 0);
		while(worker){
			conn = list_peek(conn_ready, 0);
			if(!conn)
				break;
			ssize_t r;
			while(worker){
				do{
					r = xfd_send(worker->fd, conn->fd, &conn->byte);
				}while(-1 == r && EINTR == errno);
				if(1 == r){
					logmsg(LOG_DEBUG, "conn(%d) => worker(%d)", conn->fd, worker->fd);
					list_remove(workers_idle, worker);
					list_remove(conn_ready, conn);
					list_push(workers_active, worker, 1);
					list_push(workers_active, conn, 1);
					break;
				}
//				assert(-1 == r && EPIPE == errno);
				list_remove(workers_idle, worker);
				list_push(spare, worker, 0);
				close(worker->fd);
				logmsg(LOG_DEBUG, "worker_del(%d) => %d", worker->fd, --nworkers);
				avail++;
			}
			worker = list_peek(workers_idle, 0);
		}
	}
	worker = list_pop(workers_idle, 0);
	while(worker){
//		avail++;
		logmsg(LOG_DEBUG, "worker_del(%d) => %d", worker->fd, --nworkers);
		close(worker->fd);
//		list_push(spare, worker, 0);
		worker = list_pop(workers_idle, 0);
	}
	free(cfds);
	logmsg(LOG_INFO, "exit");
	_exit(0);
}

static volatile int go = 1;
void halt(int sig){
	go = 0;
}

int server(int *socks, size_t n, ssize_t workers, char **extras, size_t ecount, int level, int sockd_level){
	const pid_t self = getpid();
	loginit(level, "LemonGraph.proc(%d)", (int)self);
	const long cores = sysconf(_SC_NPROCESSORS_ONLN);
	if(workers < 1)
		workers = (workers ? workers : -1) * -cores;
	logmsg(LOG_INFO, "+master(%d), cores=%ld, workers=%ld", (int)self, cores, (long)workers);
	const size_t spawns = 1 + workers + ecount;
	int rc, s, efd[2], cfd[2];
	size_t r, i;
	if(level <= LOG_INFO){
		struct sockaddr_storage ss;
		union {
			struct sockaddr *sa;
			struct sockaddr_un *sun;
			struct sockaddr_in *sin;
			struct sockaddr_in6 *sin6;
		} m = { (struct sockaddr *)&ss };

		socklen_t slen;
		char taddr[INET_ADDRSTRLEN];
		for(i = 0; i  < n; i++){
			slen = sizeof(ss);
			rc = getsockname(socks[i], m.sa, &slen);
			if(rc)
				abort();
			if(ss.ss_family == AF_UNIX){
				slen -= m.sun->sun_path - (char *)m.sun;
				if(!m.sun->sun_path[0])
					m.sun->sun_path[0] = '@';
				logmsg(LOG_INFO, "listening on UNIX socket %.*s", (int)slen, m.sun->sun_path);
			}else if(ss.ss_family == AF_INET && inet_ntop(AF_INET, &m.sin->sin_addr, taddr, sizeof(taddr))){
				logmsg(LOG_INFO, "listening on IPv4 %s, port %d", taddr, (int)ntohs(m.sin->sin_port));
			}else if(ss.ss_family == AF_INET6 && inet_ntop(AF_INET6, &m.sin6->sin6_addr, taddr, sizeof(taddr))){
				logmsg(LOG_INFO, "listening on IPv6 %s, port %d", taddr, (int)ntohs(m.sin6->sin6_port));
			}
		}
	}
	pid_t pid = 0, pids[spawns];
	rc = pipe(efd);
	if(rc)
		abort();
	rc = socketpair(AF_UNIX, SOCK_STREAM, 0, cfd);
	if(rc)
		abort();
	shutdown(cfd[0], SHUT_WR);
	shutdown(cfd[1], SHUT_RD);
	sigset_t signew, sigold;

	rc = sigemptyset(&signew);
	if(rc)
		abort();
	rc = sigaddset(&signew, SIGINT);
	if(rc)
		abort();
	rc = sigaddset(&signew, SIGTERM);
	if(rc)
		abort();
	rc = sigaddset(&signew, SIGQUIT);
	if(rc)
		abort();
	rc = sigprocmask(SIG_UNBLOCK, &signew, &sigold);
	if(rc)
		abort();

	struct sigaction oldsa[3], newsa = { .sa_flags = 0 };
	newsa.sa_handler = halt;
	sigfillset(&newsa.sa_mask);
	rc = sigaction(SIGINT, &newsa, oldsa);
	if(rc)
		abort();
	rc = sigaction(SIGTERM, &newsa, oldsa + 1);
	if(rc)
		abort();
	rc = sigaction(SIGQUIT, &newsa, oldsa + 2);
	if(rc)
		abort();

	for(i = 0; i < spawns; i++){
		goto spawn;
spawn_init:
		while(0);
	}

#define LABEL(i) (i > (size_t)workers ? extras[i-workers-1] : i ? "worker" : "sockd")
wait:
	while(go){
		logmsg(LOG_DEBUG, "wait...");
		pid = wait(&s);
		if(-1 == pid){
			if(EINTR == errno && go)
				continue;
			break;
		}
		for(i = 0; i < spawns; i++)
			if(pid == pids[i]){
				logmsg(LOG_INFO, "-%s(%d): exit: %d", LABEL(i), (int)pid, s);
				if(go)
					goto spawn;
				goto shutdown;
			}
		abort();
	}

shutdown:
	// eat the ^C that shows up on the console
	if(level <= LOG_INFO)
		(void)write(STDERR_FILENO, "\r", 1);
	logmsg(LOG_INFO, "shutting down...");
	rc = sigaction(SIGINT, oldsa, NULL);
	if(rc)
		abort();
	rc = sigaction(SIGTERM, oldsa + 1, NULL);
	if(rc)
		abort();
	rc = sigaction(SIGQUIT, oldsa + 2, NULL);
	if(rc)
		abort();
	rc = sigprocmask(SIG_SETMASK, &sigold, NULL);
	if(rc)
		abort();

	close(efd[0]);
	close(efd[1]);
	close(cfd[0]);
	close(cfd[1]);

	for(i = 0; i <= (size_t)workers; i++){
		do{
			logmsg(LOG_DEBUG, "waitpid %d", (int)pids[i]);
			pid = waitpid(pids[i], &s, 0);
		}while(-1 == pid && EINTR == errno);
		if(pid != -1)
			logmsg(LOG_INFO, "-%s(%d): exit: %d", LABEL(i), (int)pid, s);
	}
	logmsg(LOG_INFO, "exit");
	return 0;

spawn:
	pids[i] = fork();
	if(-1 == pids[i])
		abort();
	else if(pids[i]){
		logmsg(LOG_INFO, "+%s(%d): spawned", LABEL(i), (int)pids[i]);
		if(pid)
			goto wait;
		goto spawn_init;
	}
	setsid();
	signal(SIGINT,  SIG_IGN);
	signal(SIGTERM, SIG_IGN);
	signal(SIGQUIT, SIG_IGN);
	sigprocmask(SIG_SETMASK, &sigold, NULL);

	if(i > (size_t)workers){ // extras
		s = dup2(efd[0], socks[0]);
		if(s != socks[0])
			abort();
		r = i - workers;
	}else if(i){ // workers
		int wfd[2];
		rc = socketpair(AF_UNIX, SOCK_STREAM, 0, wfd);
		if(rc)
			abort();
		r = xfd_send(cfd[1], wfd[1], NULL);
		if(1 != r)
			abort();
		if(wfd[0] == socks[0])
			abort();
		s = dup2(wfd[0], socks[0]);
		if(s != socks[0])
			abort();
		close(wfd[0]);
		close(wfd[1]);
		r = -i;
	}else{
		s = dup2(cfd[0], 0);
		if(s)
			abort();
		return sockd(socks, n, sockd_level);
	}

	close(efd[0]);
	close(efd[1]);
	close(cfd[0]);
	close(cfd[1]);
	for(i = 1; i < n; i++)
		close(socks[i]);
	return r;
}
