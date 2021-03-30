#ifndef _LEMONGRAPH_H
#define _LEMONGRAPH_H

#include<inttypes.h>
#include<sys/types.h>

#include"db.h"

// log entry types
#define GRAPH_DELETION 0x0
#define GRAPH_NODE     0x1
#define GRAPH_EDGE     0x2
#define GRAPH_PROP     0x3

// edge directions
#define GRAPH_DIR_IN   0x1
#define GRAPH_DIR_OUT  0x2
#define GRAPH_DIR_BOTH 0x3

// kv flags
#define LG_KV_RO       0x1
#define LG_KV_MAP_KEYS 0x2
#define LG_KV_MAP_DATA 0x4

#define PRI_LOGID PRIu64
#define PRI_STRID PRIu64

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
// For deletions, the 'next' field points to the top-level entry that was the target of the delete.
// As a deletion may cascade to multiple children, I don't think it makes any sense to reserve it for pointing to a future entry.

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
int graph_sync(graph_t g, int force);
int graph_updated(graph_t g);
size_t graph_size(graph_t g);
void graph_remap(graph_t g);
void graph_close(graph_t g);
int graph_fd(graph_t g);

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

// returns beforeID for entire transaction that given id was written in
logID_t graph_snap_id(graph_txn_t txn, logID_t id);

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

// iterator foo - be sure to close them before aborting or commiting a txn
graph_iter_t graph_nodes(graph_txn_t txn, logID_t beforeID);
graph_iter_t graph_edges(graph_txn_t txn, logID_t beforeID);
graph_iter_t graph_nodes_type(graph_txn_t txn, void *type, size_t tlen, logID_t beforeID);
graph_iter_t graph_edges_type(graph_txn_t txn, void *type, size_t tlen, logID_t beforeID);
graph_iter_t graph_edges_type_value(graph_txn_t txn, void *type, size_t tlen, void *value, size_t vlen, logID_t beforeID);
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
int graph_string_resolve(graph_txn_t txn, strID_t *id, void const *data, const size_t len);
logID_t graph_log_nextID(graph_txn_t txn);

// kv storage api - domains get mapped to stringIDs via the string storage layer
// so do keys and values if LG_KV_MAP_KEYS or LG_KV_MAP_DATA are set
// non-mapped keys must be fairly short (less than 500 bytes is safe)
// flags are not stored internally - client must know per domain
// note - related kv & kv_iter objects share buffers - do not use concurrently from multiple threads

kv_t graph_kv(graph_txn_t txn, const void *domain, const size_t dlen, const int flags);
void *kv_get(kv_t kv, void *key, size_t klen, size_t *dlen);
void *kv_first_key(kv_t kv, size_t *klen);
void *kv_last_key(kv_t kv, size_t *len);
int kv_del(kv_t kv, void *key, size_t klen);
int kv_put(kv_t kv, void *key, size_t klen, void *data, size_t dlen);
int kv_next(kv_t kv, void **key, size_t *klen, void **data, size_t *dlen);
int kv_next_reset(kv_t kv);
int kv_clear_pfx(kv_t kv, uint8_t *pfx, unsigned int len);
int kv_clear(kv_t kv);
void kv_deref(kv_t kv);

kv_iter_t kv_iter(kv_t kv);
kv_iter_t kv_iter_pfx(kv_t kv, uint8_t *pfx, unsigned int len);
int kv_iter_next(kv_iter_t iter, void **key, size_t *klen, void **data, size_t *dlen);
int kv_iter_seek(kv_iter_t iter, void *key, size_t klen);
void kv_iter_close(kv_iter_t iter);

// fifos
int kv_fifo_push_n(kv_t kv, void **datas, size_t *lens, const int count);
int kv_fifo_push(kv_t kv, void *data, size_t len);
int kv_fifo_peek_n(kv_t kv, void **datas, size_t *lens, const int count);
int kv_fifo_peek(kv_t kv, void **data, size_t *size);
int kv_fifo_delete(kv_t kv, const int count);
int kv_fifo_len(kv_t kv, uint64_t *len);

// priority queues
int kv_pq_add(kv_t kv, void *key, size_t klen, uint8_t priority);
int kv_pq_get(kv_t kv, void *key, size_t klen);
int kv_pq_del(kv_t kv, void *key, size_t klen);
kv_iter_t kv_pq_iter(kv_t kv);
int kv_pq_iter_next(kv_iter_t iter, void **data, size_t *dlen);
uint8_t *kv_pq_cursor(kv_t kv, uint8_t priority);
int kv_pq_cursor_next(graph_txn_t txn, uint8_t *cursor, void **key, size_t *klen);
void kv_pq_cursor_close(uint8_t *cursor);

// helpers for serializing/unserializing tuples of non-negative integers
int pack_uints(int count, uint64_t *ints, void *buffer);
int unpack_uints(int count, uint64_t *ints, void *buffer);
int unpack_uints2(int count, uint64_t *ints, void *buffer, size_t buflen);
int pack_uint(uint64_t i, char *buffer);
uint64_t unpack_uint(char *buffer);
int pack_uuid(char *uuid, char *bin);
int unpack_uuid(char *bin, char *uuid);

#endif
