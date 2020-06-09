/*
TODO:
        + graph_set_mapsize(g, (1<<30) * 10); // as in bench.py, but not working here...
        + graph_close is seg faulting...
*/
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <time.h>
#include "lemongraph.h"
#define	O_RDWR	0x0002
#define	O_CREAT 0x0200
#define CATCH_ERRNO \
    if (errno) {\
        fprintf(stderr, "ERR: %s\n", graph_strerror(errno));\
        exit(-1);\
    }
void bench();
void bench_fast();
int bench_nodes(graph_t);
int bench_nodes_fast(graph_t);
int bench_props(graph_t);
int bench_edges(graph_t);
int commit(graph_txn_t, logID_t*);

#include <ftw.h>
#include <unistd.h>
int rmrf(char *);

#define COUNT 1000000
node_t NODES[COUNT];
logID_t NODE_IDS[COUNT];
char dirtmp[] = "/tmp/bench.XXXXXX";

int main(int argc, const char *argv[]) {
    bench();
    bench_fast();
    return 0;
}

void bench() {
    char name[] = "bench";    
    char *dir = mkdtemp(dirtmp);    
    char *path = malloc(strlen(dir) + strlen(name) + 2);
    strcpy(path, dir);
    strcat(path, "/");
    strcat(path, name);
    printf("==================\n%s\n",path);

    graph_t g = graph_open(path, O_RDWR|O_CREAT, 0760, DB_NOSYNC|DB_NORDAHEAD);
    CATCH_ERRNO

    bench_nodes(g);
    bench_props(g);
    bench_edges(g);
    
    graph_close(g);

    rmrf(dir);
    free(path);
    int i = 0;
    while (i < COUNT)
        free(NODES[i++]);
}
void bench_fast() {
    char name[] = "bench_fast";    
    char *dir = mkdtemp(dirtmp);    
    char *path = malloc(strlen(dir) + strlen(name) + 2);
    strcpy(path, dir);
    strcat(path, "/");
    strcat(path, name);
    printf("==================\n%s\n",path);

    graph_t g = graph_open(path, O_RDWR|O_CREAT, 0760, DB_NOSYNC|DB_NORDAHEAD);
    CATCH_ERRNO

    bench_nodes_fast(g);    
    bench_props_fast(g);    
    bench_edges_fast(g);        
    
    graph_close(g);

    rmrf(dir);
    free(path);    
}

#define BENCH_START(name) \
    graph_txn_t txn = graph_txn_begin(g, NULL, 0); \
    CATCH_ERRNO \
    logID_t logid = graph_log_nextID(txn); \
    printf("* %s\n", name); \
    clock_t _t = clock();

#define BENCH_FINISH(name) \
    double _elapsed = ((double)(clock() - _t))/CLOCKS_PER_SEC; \
    printf("    - total %s insert time: %f sec \n", name, _elapsed); \
    printf("    - total %s insert rate: %f / sec \n", name, (COUNT / _elapsed)); \
    commit(txn, &logid); \

int bench_nodes(graph_t g) {
    BENCH_START("nodes")
    int i = 0; while (i < COUNT)
        NODES[i++] = graph_node_resolve(txn, "node",4, &i,sizeof(int));
    BENCH_FINISH("nodes")
    return 0;
}
int bench_props(graph_t g) {
    BENCH_START("props")
    int i = 0; while (i < COUNT)
        free(graph_node_set(txn, NODES[i++], "prop",4, &i,sizeof(int)));
    BENCH_FINISH("props")
    return 0;
}
int bench_edges(graph_t g) {
    BENCH_START("edges")
    int i = 0; while (i < COUNT)
        free(graph_edge_resolve(txn, NODES[i++], NODES[i++], "edge",4, &i,sizeof(int)));
    BENCH_FINISH("edges");
    return 0;
}

int bench_nodes_fast(graph_t g) {
    BENCH_START("nodes")

    // node_t e = malloc(sizeof(*e));    
    // if (!graph_string_resolve(txn, &e->type, "node",4)) 
        // exit(1);        
    strID_t type;    
    if (!graph_string_resolve(txn, &type, "node",4)) 
        exit(1);        
    uint64_t i = 0; while (i < COUNT) {        
        // e->val = i;
        NODE_IDS[i++] = graph_nodeID_resolve(txn, type, i);    
    }
	// free(e);

    BENCH_FINISH("nodes")
    return 0;
}
int bench_props_fast(graph_t g) {
    BENCH_START("props")

    strID_t key;       
    if (!graph_string_resolve(txn, &key, "prop",4)) 
        exit(1);        
    uint64_t i = 0; while (i < COUNT)         
        graph_ID_set(txn, NODE_IDS[i++], key, i);        

    BENCH_FINISH("props")
    return 0;
}
int bench_edges_fast(graph_t g) {
    BENCH_START("edges")

    strID_t type;       
    if (!graph_string_resolve(txn, &type, "edge",4)) 
        exit(1);             
    uint64_t i = 0; while (i < COUNT)         
        graph_edgeID_resolve(txn, NODES[i++], NODES[i++], type, i);    

    BENCH_FINISH("edges");
    return 0;
}

int commit(graph_txn_t txn, logID_t *logid) {
    CATCH_ERRNO
    if (graph_txn_updated(txn)) {
        logID_t updates = graph_log_nextID(txn) - *logid;
        *logid += updates;
        printf("    - commit updates: %lli\n", updates);
        graph_txn_commit(txn);
        return 0;
    }
    graph_txn_abort(txn);
    return 1;
}

int _rmrf_cb(const char *fpath, const struct stat *sb, int typeflag, struct FTW *ftwbuf) {
    int rv = remove(fpath);
    if (rv) {
        perror(fpath);
    }
    return rv;
}

int rmrf(char *path) {
    return nftw(path, _rmrf_cb, 64, FTW_DEPTH | FTW_PHYS);
}