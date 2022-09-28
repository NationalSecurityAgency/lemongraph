import cffi

C_HEADER_SRC = '''
#include "lemongraph-cffi.c"
'''

CDEF = '''
#define DB_NOSYNC      ...
#define DB_RDONLY      ...
#define DB_NOMETASYNC  ...
#define DB_NOTLS       ...
#define DB_NOLOCK      ...
#define DB_NORDAHEAD   ...
#define DB_NOMEMINIT   ...
#define DB_PREVMETA    ...

#define DB_NOOVERWRITE ...
#define DB_NODUPDATA   ...
#define DB_CURRENT     ...
#define DB_RESERVE     ...
#define DB_APPEND      ...
#define DB_APPENDDUP   ...
#define DB_MULTIPLE    ...

#define GRAPH_DELETION ...
#define GRAPH_NODE     ...
#define GRAPH_EDGE     ...
#define GRAPH_PROP     ...

#define GRAPH_DIR_IN   ...
#define GRAPH_DIR_OUT  ...
#define GRAPH_DIR_BOTH ...

#define LG_KV_RO       ...
#define LG_KV_MAP_KEYS ...
#define LG_KV_MAP_DATA ...

#define O_RDWR   ...
#define O_RDONLY ...
#define O_CREAT  ...
#define O_EXCL   ...

typedef uint64_t logID_t;
typedef uint64_t strID_t;

typedef struct graph_t * graph_t;
typedef struct graph_txn_t * graph_txn_t;
typedef struct graph_iter_t * graph_iter_t;

typedef struct entry_t   * entry_t;
typedef struct entry_t   * deletion_t;
typedef struct node_t    * node_t;
typedef struct edge_t    * edge_t;
typedef struct prop_t    * prop_t;
typedef struct kv_t      * kv_t;
typedef struct kv_iter_t * kv_iter_t;

typedef struct db_t * db_t;
typedef struct txn_t * txn_t;
typedef struct cursor_t * cursor_t;
typedef struct iter_t * iter_t;
typedef struct db_snapshot_t * db_snapshot_t;

struct entry_t{
    logID_t id;
    uint8_t is_new:1;
    uint8_t rectype:7;
    logID_t next;
};

struct node_t {
    logID_t id;
    uint8_t is_new:1;
    uint8_t rectype:7;
    logID_t next;
    strID_t type;
    strID_t val;
};

struct edge_t {
    logID_t id;
    uint8_t is_new:1;
    uint8_t rectype:7;
    logID_t next;
    strID_t type;
    strID_t val;
    logID_t src;
    logID_t tgt;
};

struct prop_t {
    logID_t id;
    uint8_t is_new:1;
    uint8_t rectype:7;
    logID_t next;
    logID_t pid;
    strID_t key;
    strID_t val;
};


char *graph_strerror(int err);

graph_t graph_open(const char * const path, const int flags, const int mode, int db_flags);
graph_txn_t graph_txn_begin(graph_t g, graph_txn_t parent, unsigned int flags);
int graph_txn_updated(graph_txn_t txn);
int graph_txn_reset(graph_txn_t txn);
int graph_txn_commit(graph_txn_t txn);
void graph_txn_abort(graph_txn_t txn);
void graph_sync(graph_t g, int force);
int graph_updated(graph_t g);
size_t graph_size(graph_t g);
void graph_remap(graph_t g);
void graph_close(graph_t g);

// fetch entities by logID
entry_t graph_entry(graph_txn_t txn, const logID_t id);
prop_t graph_prop(graph_txn_t txn, const logID_t id);
node_t graph_node(graph_txn_t txn, const logID_t id);
edge_t graph_edge(graph_txn_t txn, const logID_t id);

// returns highest logID affecting an entry - self, deletion, or latest property (or prop deletion)
logID_t graph_updateID(graph_txn_t txn, logID_t beforeID);
logID_t graph_entry_updateID(graph_txn_t txn, entry_t e, logID_t beforeID);
logID_t graph_node_updateID(graph_txn_t txn, node_t n, logID_t beforeID);
logID_t graph_edge_updateID(graph_txn_t txn, edge_t e, logID_t beforeID);
logID_t graph_prop_updateID(graph_txn_t txn, prop_t p, logID_t beforeID);

// get properties
prop_t graph_get(graph_txn_t txn, void *key, size_t klen, logID_t beforeID);
prop_t graph_node_get(graph_txn_t txn, node_t node, void *key, size_t klen, logID_t beforeID);
prop_t graph_edge_get(graph_txn_t txn, edge_t edge, void *key, size_t klen, logID_t beforeID);
prop_t graph_prop_get(graph_txn_t txn, prop_t prop, void *key, size_t klen, logID_t beforeID);

// set properties
prop_t graph_set(graph_txn_t txn, void *key, size_t klen, void *val, size_t vlen);
prop_t graph_node_set(graph_txn_t txn, node_t node, void *key, size_t klen, void *val, size_t vlen);
prop_t graph_edge_set(graph_txn_t txn, edge_t edge, void *key, size_t klen, void *val, size_t vlen);
prop_t graph_prop_set(graph_txn_t txn, prop_t prop, void *key, size_t klen, void *val, size_t vlen);

// unset properties
void graph_unset(graph_txn_t txn, void *key, size_t klen);
void graph_node_unset(graph_txn_t txn, node_t e, void *key, size_t klen);
void graph_edge_unset(graph_txn_t txn, edge_t e, void *key, size_t klen);
void graph_prop_unset(graph_txn_t txn, prop_t e, void *key, size_t klen);

// query node/edge
node_t graph_node_lookup(graph_txn_t txn, void *type, size_t tlen, void *val, size_t vlen, logID_t beforeID);
edge_t graph_edge_lookup(graph_txn_t txn, node_t src, node_t tgt, void *type, size_t tlen, void *val, size_t vlen, logID_t beforeID);

// resolve node/edge
node_t graph_node_resolve(graph_txn_t txn, void *type, size_t tlen, void *val, size_t vlen);
edge_t graph_edge_resolve(graph_txn_t txn, node_t src, node_t tgt, void *type, size_t tlen, void *val, size_t vlen);

// count nodes/edges
size_t graph_nodes_count(graph_txn_t txn, logID_t beforeID);
size_t graph_edges_count(graph_txn_t txn, logID_t beforeID);

// delete any type of graph entity
logID_t graph_delete(graph_txn_t txn, entry_t e);

// iterator foo - be sure to close them before aborting or committing a txn
graph_iter_t graph_nodes(graph_txn_t txn, logID_t beforeID);
graph_iter_t graph_edges(graph_txn_t txn, logID_t beforeID);
graph_iter_t graph_nodes_type(graph_txn_t txn, void *type, size_t tlen, logID_t beforeID);
graph_iter_t graph_edges_type(graph_txn_t txn, void *type, size_t tlen, logID_t beforeID);
graph_iter_t graph_node_edges_in(graph_txn_t txn, node_t node, logID_t beforeID);
graph_iter_t graph_node_edges_out(graph_txn_t txn, node_t node, logID_t beforeID);
graph_iter_t graph_node_edges(graph_txn_t txn, node_t node, logID_t beforeID);
graph_iter_t graph_node_edges_dir(graph_txn_t txn, node_t node, unsigned int direction, logID_t beforeID);
graph_iter_t graph_node_edges_type_in(graph_txn_t txn, node_t node, void *type, size_t tlen, logID_t beforeID);
graph_iter_t graph_node_edges_type_out(graph_txn_t txn, node_t node, void *type, size_t tlen, logID_t beforeID);
graph_iter_t graph_node_edges_type(graph_txn_t txn, node_t node, void *type, size_t tlen, logID_t beforeID);
graph_iter_t graph_node_edges_dir_type(graph_txn_t txn, node_t node, unsigned int direction, void *type, size_t tlen, logID_t beforeID);
graph_iter_t graph_props(graph_txn_t txn, logID_t beforeID);
graph_iter_t graph_node_props(graph_txn_t txn, node_t node, logID_t beforeID);
graph_iter_t graph_edge_props(graph_txn_t txn, edge_t edge, logID_t beforeID);
graph_iter_t graph_prop_props(graph_txn_t txn, prop_t prop, logID_t beforeID);
entry_t graph_iter_next(graph_iter_t iter);
void graph_iter_close(graph_iter_t iter);

char *graph_string(graph_txn_t txn, strID_t id, size_t *len);
int graph_string_lookup(graph_txn_t txn, strID_t *id, void const *data, const size_t len);
logID_t graph_log_nextID(graph_txn_t txn);

// kv storage api - domains get mapped to stringIDs via the string storage layer
// so do keys and values if LG_KV_MAP_KEYS or LG_KV_MAP_DATA are set
// non-mapped keys must be fairly short (less than 500 bytes is safe)
// flags are not stored internally - client must know per domain
// note - related kv & kv_iter objects share buffers - do not use concurrently from multiple threads

kv_t graph_kv(graph_txn_t txn, const void *domain, const size_t dlen, const int flags);
void *kv_get(kv_t kv, void *key, size_t klen, size_t *dlen);
void *kv_last_key(kv_t kv, size_t *len);
int kv_del(kv_t kv, void *key, size_t klen);
int kv_put(kv_t kv, void *key, size_t klen, void *data, size_t dlen);
void kv_deref(kv_t kv);

kv_iter_t kv_iter(kv_t kv);
kv_iter_t kv_iter_pfx(kv_t kv, uint8_t *pfx, unsigned int len);
int kv_iter_next(kv_iter_t iter, void **key, size_t *klen, void **data, size_t *dlen);
void kv_iter_close(kv_iter_t iter);

// snapshot foo
int db_snapshot_to_fd(db_t db, int fd, int compact);
db_snapshot_t db_snapshot_new(db_t db, int compact);
ssize_t db_snapshot_read(db_snapshot_t snap, void *buffer, size_t len);
int db_snapshot_close(db_snapshot_t snap);
int db_snapshot_fd(db_snapshot_t snap);

// helpers for serializing/unserializing tuples of non-negative integers
int pack_uints(int count, uint64_t *ints, void *buffer);
int unpack_uints(int count, uint64_t *ints, void *buffer);
int pack_uint(uint64_t i, char *buffer);
uint64_t unpack_uint(char *buffer);

typedef ... DIR;

// custom extras
void free(void *);
DIR *opendir(const char *name);
char *_readdir(DIR *dirp);
int closedir(DIR *dirp);
node_t asNode(entry_t e);
edge_t asEdge(entry_t e);
prop_t asProp(entry_t e);
deletion_t asDel(entry_t e);
node_t iter_next_node(graph_iter_t iter);
edge_t iter_next_edge(graph_iter_t iter);
prop_t iter_next_prop(graph_iter_t iter);
void graph_node_delete(graph_txn_t txn, node_t e);
void graph_edge_delete(graph_txn_t txn, edge_t e);
void graph_prop_delete(graph_txn_t txn, prop_t e);
int graph_set_mapsize(graph_t g, size_t mapsize);
size_t graph_get_mapsize(graph_t g);
size_t graph_get_disksize(graph_t g);
db_snapshot_t graph_snapshot_new(graph_t g, int compact);

typedef int HANDLE;
int osal_fdatasync(HANDLE fd);
void osal_set_pdeathsig(int sig);

'''

C_KEYWORDS = dict(
    sources=['deps/lmdb/libraries/liblmdb/mdb.c', 'deps/lmdb/libraries/liblmdb/midl.c', 'lib/lemongraph.c', 'lib/db.c', 'lib/osal.c'],
    include_dirs=['lib','deps/lmdb/libraries/liblmdb'],
    libraries=['z'],
)

ffi = cffi.FFI()

if hasattr(ffi, 'set_source'):
    ffi.set_source(
        "LemonGraph._lemongraph_cffi",
        C_HEADER_SRC,
        **C_KEYWORDS)

ffi.cdef(CDEF)

if __name__ == '__main__':
    ffi.compile()
