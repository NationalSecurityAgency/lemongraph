#include<dirent.h>
#include<fcntl.h>
#include<netinet/in.h>
#include<stdarg.h>
#include<stdlib.h>
#include<sys/socket.h>

#include<db.h>
#include<lemongraph.h>
#include<afsync.h>
#include<logging.h>
#include<server.h>
#include<xfd.h>

char *_readdir(DIR *dirp){
    struct dirent *de = readdir(dirp);
    return de ? de->d_name : NULL;
}

db_snapshot_t graph_snapshot_new(graph_t g, int compact){
    return db_snapshot_new((db_t)g, compact);
}

int graph_set_mapsize(graph_t g, size_t mapsize){
    return db_set_mapsize((db_t)g, mapsize);
}

size_t graph_get_mapsize(graph_t g){
	size_t size;
	int r = db_get_mapsize((db_t)g, &size);
	return r ? 0 : size;
}

size_t graph_get_disksize(graph_t g){
	size_t size;
    int r = db_get_disksize((db_t)g, &size);
    return r ? 0 : size;
}

node_t asNode(entry_t e){
    return (node_t)e;
}

edge_t asEdge(entry_t e){
    return (edge_t)e;
}

prop_t asProp(entry_t e){
    return (prop_t)e;
}

deletion_t asDel(entry_t e){
    return (deletion_t)e;
}

node_t iter_next_node(graph_iter_t iter){
    return (node_t) graph_iter_next(iter);
}

edge_t iter_next_edge(graph_iter_t iter){
    return (edge_t) graph_iter_next(iter);
}

prop_t iter_next_prop(graph_iter_t iter){
    return (prop_t) graph_iter_next(iter);
}

void graph_node_delete(graph_txn_t txn, node_t e){
    graph_delete(txn, (entry_t)e);
}

void graph_edge_delete(graph_txn_t txn, edge_t e){
    graph_delete(txn, (entry_t)e);
}

void graph_prop_delete(graph_txn_t txn, prop_t e){
    graph_delete(txn, (entry_t)e);
}

#define LG_WORKER_KEEPALIVE  1
#define LG_WORKER_EXIT       2

typedef struct {
	int sock, conn, family, type, proto;
	char byte;
} lg_worker_t;

lg_worker_t *lg_worker_new(int sock){
	lg_worker_t *w = malloc(sizeof(*w));
	if(w)
		w->sock = sock;
	return w;
}

int lg_worker_accept(lg_worker_t *w){
	struct sockaddr_storage ss;
	socklen_t optlen;
	int rc, r = xfd_recv(w->sock, &w->conn, &w->byte);
	if(1 != r)
		return r;

	optlen = sizeof(ss);
	rc = getpeername(w->conn, (struct sockaddr *)&ss, &optlen);
	if(rc) goto fail;

	w->family = ss.ss_family;
	w->type = SOCK_STREAM;
	w->proto = (AF_UNIX == ss.ss_family) ? 0 : IPPROTO_TCP;

	return r;

fail:
	close(w->conn);
	return -1;
}

int lg_worker_finish(lg_worker_t *w, unsigned char flags){
	int r;
	do{
		r = send(w->sock, &flags, 1, 0);
		if(1 == r)
			break;
	}while(EINTR == errno);
	close(w->conn);
	if(flags & LG_WORKER_EXIT){
		close(w->sock);
		w->sock = -1;
		free(w);
	}
	return r;
}

void lg_log_init(int level, char *name){
	loginit(level, "LemonGraph.%s(%d)", name, getpid());
}

void lg_log_msg(int level, char *fmt, ...){
	va_list ap;
	va_start(ap, fmt);
	vlogmsg(level, fmt, ap);
	va_end(ap);
}
