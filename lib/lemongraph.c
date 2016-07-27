#ifndef _BSD_SOURCE
#define _BSD_SOURCE
#endif

// suppress assert() elimination, as we are currently heavily relying on it
#ifdef NDEBUG
#undef NDEBUG
#endif

#include<assert.h>


#include<errno.h>
#include<dirent.h>
#include<fcntl.h>
#include<inttypes.h>
#include<limits.h>
#include<pthread.h>
#include<stdarg.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<sys/stat.h>
#include<zlib.h>

#include<lmdb.h>
#include<lemongraph.h>

#include"db-private.h"

#define MAX(x, y) ((x) > (y) ? (x) : (y))

//#define debug(args...) do{ fprintf(stderr, "%d: ", __LINE__); fprintf(stderr, args); }while(0)
//#define debug(args...) while(0);

// provide type-agnostic clz wrapper, and return a more useful value for clz(0)
#define __clz_wrapper(x) (int)((x) ? (sizeof(x) == sizeof(long) ? __builtin_clzl(x) : (sizeof(x) == sizeof(long long) ? __builtin_clzll(x) : __builtin_clz((int)(x)))) : sizeof(x) * 8)

// quickly take unsigned numeric types and count minimum number of bytes needed to represent - for varint encoding
#define intbytes(x) (sizeof(x) - __clz_wrapper(x) / 8)


// encode unsigned values into buffer, advancing iter
// ensure you have a least 9 bytes per call
#define encode(x, buffer, iter) do{ \
	int _shift; \
	((uint8_t *)(buffer))[iter] = intbytes(x); \
	for(_shift = (((uint8_t *)(buffer))[iter++] - 1) * 8; _shift >= 0; iter++, _shift -= 8) \
		((uint8_t *)(buffer))[iter] = ((x) >> _shift) & 0xff; \
}while(0)

#define esizeof(x) (sizeof(x)+1)

// corresponding decode
#define decode(x, buffer, iter) do{ \
	uint8_t count = ((uint8_t *)(buffer))[iter++]; \
	assert(sizeof(x) >= count); \
	x = 0; \
	while(count--) \
		x = (x<<8) + ((uint8_t *)(buffer))[iter++]; \
}while(0)

int pack_uints(int count, uint64_t *ints, void *buffer){
	int i, len = 0;
	for(i = 0; i < count; i++)
		encode(ints[i], buffer, len);
	return len;
}

int unpack_uints(int count, uint64_t *ints, void *buffer){
	int i, len = 0;
	for(i = 0; i < count; i++)
		decode(ints[i], buffer, len);
	return len;
}

int pack_uint(uint64_t i, char *buffer){
	int len = 0;
	encode(i, buffer, len);
	return len;
}

uint64_t unpack_uint(char *buffer){
	int len = 0;
	uint64_t i;
	decode(i, buffer, len);
	return i;
}

char *graph_strerror(int err){
	return db_strerror(err);
}

#define DBS (sizeof(DB_INFO)/sizeof(*DB_INFO))

#define DB_SCALAR      0
#define DB_SCALAR_IDX  1
#define DB_LOG         2
#define DB_NODE_IDX    3
#define DB_EDGE_IDX    4
#define DB_PROP_IDX    5
#define DB_SRCNODE_IDX 6
#define DB_TGTNODE_IDX 7
#define DB_KV          8

static dbi_t DB_INFO[] = {
	// strID_t strID => bytes (append-only)
	[DB_SCALAR] = { "scalar", MDB_INTEGERKEY },

	// uint32_t crc => strID_t strIDs[]
	[DB_SCALAR_IDX] = { "scalar_idx", MDB_DUPSORT|MDB_INTEGERKEY|MDB_DUPFIXED|MDB_INTEGERDUP },

	// varint_t logID => entry_t (appends & updates)
	[DB_LOG] = { "log", 0 },

	// varint_t [type, val, logID] => ''
	[DB_NODE_IDX] = { "node_idx", 0 },

	// varint_t [type, val, src, tgt, logID]
	[DB_EDGE_IDX] = { "edge_idx", 0 },

	// varint_t pid, key, logID => ''
	[DB_PROP_IDX] = { "prop_idx", 0 },

	// varint_t node, type, edge => ''
	[DB_SRCNODE_IDX] = { "srcnode_idx", 0 },

	// varint_t node, type, edge => ''
	[DB_TGTNODE_IDX] = { "tgtnode_idx", 0 },

	// varint_t domain, key => varint_t val
	[DB_KV] = { "kv", 0 },
};

struct graph_t{
	struct db_t db;
};

#define TXN_DB(txn) ((txn_t)(txn))->db
#define TXN_RW(txn) ((txn)->txn.rw)
#define TXN_RO(txn) ((txn)->txn.ro)

struct graph_txn_t{
	struct txn_t txn;
	strID_t next_strID;
	logID_t next_logID;
};

static strID_t graph_string_nextID(graph_txn_t txn, int consume){
	strID_t id = txn->next_strID;
	if(!id){
		cursor_t scalar = txn_cursor_new((txn_t)txn, DB_SCALAR);
		int r = cursor_get(scalar, NULL, NULL, MDB_LAST);
		if(MDB_SUCCESS == r){
			MDB_val str, key = { sizeof(id), &id };
			r = cursor_get(scalar, &key, &str, MDB_GET_CURRENT);
			assert(MDB_SUCCESS == r);
			memcpy(&id, key.mv_data, sizeof(id));
			id++;
			// make sure we didn't wrap around
			assert(id);
		}else if(MDB_NOTFOUND == r){
			id = 1;
		}else{
			assert(MDB_SUCCESS == r);
		}
		txn->next_strID = id;
		cursor_close(scalar);
	}
	if(consume){
		assert(TXN_RW(txn));
		txn->next_strID++;
		assert(txn->next_strID);
	}
	return id;
}

static logID_t _graph_log_nextID(graph_txn_t txn, int consume){
	logID_t id = txn->next_logID;
	if(!id){
		cursor_t scalar = txn_cursor_new((txn_t)txn, DB_LOG);
		int r = cursor_get(scalar, NULL, NULL, MDB_LAST);
		if(MDB_SUCCESS == r){
			MDB_val val, key = { sizeof(id), &id };
			r = cursor_get(scalar, &key, &val, MDB_GET_CURRENT);
			assert(MDB_SUCCESS == r);
			int i = 0;
			decode(id, key.mv_data, i);
			id++;
			// make sure we didn't wrap around
			assert(id);
		}else if(MDB_NOTFOUND == r){
			id = 1;
		}else{
			assert(MDB_SUCCESS == r);
		}
		txn->next_logID = id;
		cursor_close(scalar);
	}
	if(consume){
		assert(TXN_RW(txn));
		txn->next_logID++;
		assert(txn->next_logID);
	}
	return id;
}

// returns 1 for success, 0 for failure (only for readonly)
static int _string_resolve(graph_txn_t txn, strID_t *ret, void const *data, const size_t len, int readonly){
	uint32_t chk;
	int r;
	size_t count;
	MDB_val str = { 0, NULL }, key1 = { 0, NULL }, key2 = { .mv_data = &chk, .mv_size = sizeof(chk) };

	if(NULL == data){
		assert(0 == len);
		*ret = 0;
		return 1;
	}

	cursor_t scalar = txn_cursor_new((txn_t)txn, DB_SCALAR);
	cursor_t idx = txn_cursor_new((txn_t)txn, DB_SCALAR_IDX);

	// fill in checksum
	chk = crc32(0, data, len);

	int retval = 1;

	r = cursor_get(idx, &key2, &key1, MDB_SET_KEY);
	if(MDB_SUCCESS == r){
		r = cursor_count(idx, &count);
		assert(MDB_SUCCESS == r);
		while(1){
			// query main db
			r = cursor_get(scalar, &key1, &str, MDB_SET_KEY);
			assert(MDB_SUCCESS == r);
			if(str.mv_size == len && memcmp(str.mv_data, data, len) == 0){
				assert(sizeof(*ret) == key1.mv_size);
				memcpy(ret, key1.mv_data, sizeof(*ret));
				goto done;
			}
			if(0 == --count)
				break;
			key1.mv_data = NULL;
			r = cursor_get(idx, &key2, &key1, MDB_NEXT_DUP);
			assert(MDB_SUCCESS == r);
		}
		r = MDB_NOTFOUND;
	}
	assert(MDB_NOTFOUND == r);

	// no key at all, or no matching strings

	// bail out now for read-only requests
	if(readonly){
		retval = 0;
		goto done;
	}

	// figure out next ID to use
	*ret = graph_string_nextID(txn, 1);
	// store new string in db
	key1.mv_size = sizeof(*ret);
	key1.mv_data = ret;
	str.mv_size = len;
	str.mv_data = (void *)data;
	r = cursor_put(scalar, &key1, &str, MDB_APPEND);
	assert(MDB_SUCCESS == r);

	// and add index entry
	key2.mv_data = &chk;
	key2.mv_size = sizeof(chk);
	r = cursor_put(idx, &key2, &key1, MDB_APPENDDUP);
	assert(MDB_SUCCESS == r);

done:
	cursor_close(scalar);
	cursor_close(idx);
	return retval;
}

static logID_t _cleanse_beforeID(graph_txn_t txn, logID_t beforeID){
	return (beforeID && _graph_log_nextID(txn, 0) > beforeID) ? beforeID : 0;
}


static uint8_t *__lookup(graph_txn_t txn, entry_t e, const int db_idx, uint8_t *kbuf, size_t klen, const logID_t beforeID){
	cursor_t idx = txn_cursor_new((txn_t)txn, db_idx);
	MDB_val key = { klen, kbuf }, data = { 0, NULL };
	MDB_cursor_op op = -1;
	int r;
	uint8_t *logbuf = NULL;

	e->id = 0;

	// use beforeID to seek just past our target
	if(beforeID)
		encode(beforeID, kbuf, key.mv_size);
	else
		kbuf[key.mv_size++] = 0xff;
	r = cursor_get(idx, &key, &data, MDB_SET_RANGE);
	if(MDB_SUCCESS == r){ // back up one record
		op = MDB_PREV;
	}else if(MDB_NOTFOUND == r){ // no records larger than target - try last record
		op = MDB_LAST;
	}else{
		assert(MDB_SUCCESS == r);
	}

	r = cursor_get(idx, &key, &data, op);
	if(MDB_SUCCESS == r){
		r = cursor_get(idx, &key, &data, MDB_GET_CURRENT);
		assert(MDB_SUCCESS == r);
		if(memcmp(key.mv_data, kbuf, klen) == 0){
			uint8_t buf[esizeof(e->id)];

			// harvest id
			decode(e->id, key.mv_data, klen);

			// now pull log entry to fill in .next
			key.mv_size = 0;
			key.mv_data = buf;
			encode(e->id, buf, key.mv_size);
			r = db_get((txn_t)txn, DB_LOG, &key, &data);
			assert(MDB_SUCCESS == r);
			assert(e->rectype == *(uint8_t *)data.mv_data);
			klen = 1;
			decode(e->next, data.mv_data, klen);
			if(e->next && (0 == beforeID || e->next < beforeID)){
				e->id = 0;
			}else{
				logbuf = &((uint8_t *)data.mv_data)[klen];
				e->is_new = 0;
			}
		}
	}

	cursor_close(idx);
	return logbuf;
}

static logID_t _node_lookup(graph_txn_t txn, node_t e, logID_t beforeID){
	uint8_t kbuf[esizeof(e->type) + esizeof(e->val) + esizeof(e->id)];
	size_t klen = 0;
	encode(e->type, kbuf, klen);
	encode(e->val,  kbuf, klen);
	__lookup(txn, (entry_t)e, DB_NODE_IDX, kbuf, klen, beforeID);
	return e->id;
}

static logID_t _edge_lookup(graph_txn_t txn, edge_t e, logID_t beforeID){
	uint8_t kbuf[esizeof(e->type) + esizeof(e->val) + esizeof(e->src) + esizeof(e->tgt) + esizeof(e->id)];
	size_t klen = 0;
	encode(e->type, kbuf, klen);
	encode(e->val,  kbuf, klen);
	encode(e->src,  kbuf, klen);
	encode(e->tgt,  kbuf, klen);
	__lookup(txn, (entry_t)e, DB_EDGE_IDX, kbuf, klen, beforeID);
	return e->id;
}

static logID_t _prop_lookup(graph_txn_t txn, prop_t e, logID_t beforeID){
	uint8_t *logbuf, kbuf[esizeof(e->pid) + esizeof(e->key) + esizeof(e->id)];
	size_t klen = 0;
	encode(e->pid, kbuf, klen);
	encode(e->key, kbuf, klen);
	logbuf = __lookup(txn, (entry_t)e, DB_PROP_IDX, kbuf, klen, beforeID);
	if(logbuf){
		klen = 1 + logbuf[0];      // skip pid
		klen += 1 + logbuf[klen];  // skip key
		decode(e->val, logbuf, klen); // pull current value
	}
	return e->id;
}

static graph_iter_t _graph_entry_idx(graph_txn_t txn, int dbi, logID_t id, logID_t beforeID);
graph_iter_t graph_iter_concat(unsigned int count, ...);

static void _delete(graph_txn_t txn, const logID_t newrecID, const logID_t oldrecID){
	uint8_t kbuf[esizeof(newrecID)], buf[esizeof(newrecID)];
	MDB_val key = { 0, kbuf }, olddata, newdata;
	int r, oldsize, newsize;
	graph_iter_t iter;
	entry_t child;
	uint8_t *mem;

	// update existing log entry
	encode(oldrecID, kbuf, key.mv_size);
	r = db_get((txn_t)txn, DB_LOG, &key, &olddata);
	assert(MDB_SUCCESS == r);

	oldsize = 1 + ((uint8_t *)olddata.mv_data)[1];
	newsize = 0;
	encode(newrecID, buf, newsize);

	newdata.mv_data = NULL;
	newdata.mv_size = olddata.mv_size + (newsize - oldsize);

	mem = newdata.mv_data = malloc(newdata.mv_size);
	memcpy(mem, olddata.mv_data, 1);
	memcpy(&mem[1], buf, newsize);
	memcpy(&mem[newsize+1], &((uint8_t *)olddata.mv_data)[oldsize+1], 1 + olddata.mv_size - oldsize);
	r = db_put((txn_t)txn, DB_LOG, &key, &newdata, 0);
	assert(MDB_SUCCESS == r);
	free(mem);

	// recursively delete item properties, and edges if item is a node
	if(GRAPH_NODE == *(uint8_t *)olddata.mv_data){
		iter = graph_iter_concat(3,
			_graph_entry_idx(txn, DB_PROP_IDX, oldrecID, 0),
			_graph_entry_idx(txn, DB_SRCNODE_IDX, oldrecID, 0),
			_graph_entry_idx(txn, DB_TGTNODE_IDX, oldrecID, 0));
	}else{
		iter = _graph_entry_idx(txn, DB_PROP_IDX, oldrecID, 0);
	}
	while((child = graph_iter_next(iter))){
		_delete(txn, newrecID, child->id);
		free(child);
	}

	graph_iter_close(iter);
}

static logID_t _log_append(graph_txn_t txn, uint8_t *dbuf, size_t dlen, logID_t delID){
	int r;
	logID_t id;
	uint8_t kbuf[esizeof(id)];
	MDB_val key = { 0, kbuf }, data = { dlen, dbuf };

	id = _graph_log_nextID(txn, 1);

	if(delID)
		_delete(txn, id, delID);

	encode(id, kbuf, key.mv_size);

	r = db_put((txn_t)txn, DB_LOG, &key, &data, MDB_APPEND);
	if(MDB_SUCCESS != r)
		fprintf(stderr, "err: %s\n", mdb_strerror(r));
	assert(MDB_SUCCESS == r);
	return id;
}

static void _entry_unset(graph_txn_t txn, logID_t id, void *key, size_t klen){
	struct prop_t p = { .pid = id, .rectype = GRAPH_PROP };
	if(_string_resolve(txn, &p.key, key, klen, 1)){
		if(_prop_lookup(txn, &p, 0))
			graph_delete(txn, (entry_t)&p);
	}
}

static logID_t _entry_delete(graph_txn_t txn, logID_t delID){
	uint8_t dbuf[1 + esizeof(delID)];
	size_t dlen = 0;

	dbuf[dlen++] = GRAPH_DELETION;
	encode(delID, dbuf, dlen);

	return _log_append(txn, dbuf, dlen, delID);
}

static logID_t _node_append(graph_txn_t txn, node_t e, logID_t delID){
	uint8_t dbuf[1 + esizeof(e->next) + esizeof(e->type) + esizeof(e->val)];
	size_t dlen = 0;

	dbuf[dlen++] = e->rectype;
	encode(e->next, dbuf, dlen);
	encode(e->type, dbuf, dlen);
	encode(e->val,  dbuf, dlen);

	return e->id = _log_append(txn, dbuf, dlen, delID);
}

static logID_t _edge_append(graph_txn_t txn, edge_t e, logID_t delID){
	uint8_t dbuf[1 + esizeof(e->next) + esizeof(e->type) + esizeof(e->val) + esizeof(e->src) + esizeof(e->tgt)];
	size_t dlen = 0;

	dbuf[dlen++] = e->rectype;
	encode(e->next, dbuf, dlen);
	encode(e->type, dbuf, dlen);
	encode(e->val,  dbuf, dlen);
	encode(e->src,  dbuf, dlen);
	encode(e->tgt,  dbuf, dlen);

	return e->id = _log_append(txn, dbuf, dlen, delID);
}

static logID_t _prop_append(graph_txn_t txn, prop_t e, logID_t delID){
	uint8_t dbuf[1 + esizeof(e->next) + esizeof(e->pid) + esizeof(e->key) + esizeof(e->val)];
	size_t dlen = 0;

	dbuf[dlen++] = e->rectype;
	encode(e->next, dbuf, dlen);
	encode(e->pid,  dbuf, dlen);
	encode(e->key,  dbuf, dlen);
	encode(e->val,  dbuf, dlen);

	return e->id = _log_append(txn, dbuf, dlen, delID);
}


static void _node_index(graph_txn_t txn, node_t e){
	uint8_t kbuf[esizeof(e->type) + esizeof(e->val) + esizeof(e->id)];
	MDB_val key = { 0, kbuf };
	MDB_val data = { 0, NULL };

	cursor_t idx = txn_cursor_new((txn_t)txn, DB_NODE_IDX);
	encode(e->type, kbuf, key.mv_size);
	encode(e->val,  kbuf, key.mv_size);
	encode(e->id,   kbuf, key.mv_size);
	int r = cursor_put(idx, &key, &data, 0);
	assert(MDB_SUCCESS == r);
	cursor_close(idx);
}

static void _edge_index(graph_txn_t txn, edge_t e){
	uint8_t kbuf[esizeof(e->type) + esizeof(e->val) + esizeof(e->src) + esizeof(e->tgt) + esizeof(e->id)];
	MDB_val key = { 0, kbuf };
	MDB_val data = { 0, NULL };
	cursor_t idx;
	int r;

	idx = txn_cursor_new((txn_t)txn, DB_EDGE_IDX);
	encode(e->type, kbuf, key.mv_size);
	encode(e->val,  kbuf, key.mv_size);
	encode(e->src,  kbuf, key.mv_size);
	encode(e->tgt,  kbuf, key.mv_size);
	encode(e->id,   kbuf, key.mv_size);
	r = cursor_put(idx, &key, &data, 0);
	assert(MDB_SUCCESS == r);
	cursor_close(idx);

	idx = txn_cursor_new((txn_t)txn, DB_SRCNODE_IDX);
	key.mv_size = 0;
	encode(e->src, kbuf, key.mv_size);
	encode(e->type, kbuf, key.mv_size);
	encode(e->id,  kbuf, key.mv_size);
	r = cursor_put(idx, &key, &data, 0);
	assert(MDB_SUCCESS == r);
	cursor_close(idx);

	idx = txn_cursor_new((txn_t)txn, DB_TGTNODE_IDX);
	key.mv_size = 0;
	encode(e->tgt, kbuf, key.mv_size);
	encode(e->type, kbuf, key.mv_size);
	encode(e->id,  kbuf, key.mv_size);
	r = cursor_put(idx, &key, &data, 0);
	assert(MDB_SUCCESS == r);
	cursor_close(idx);
}

static void _prop_index(graph_txn_t txn, prop_t e){
	uint8_t kbuf[esizeof(e->pid) + esizeof(e->key) + esizeof(e->id)];
	MDB_val key = { 0, kbuf };
	MDB_val data = { 0, NULL };

	cursor_t idx = txn_cursor_new((txn_t)txn, DB_PROP_IDX);
	encode(e->pid, kbuf, key.mv_size);
	encode(e->key, kbuf, key.mv_size);
	encode(e->id,  kbuf, key.mv_size);
	int r = cursor_put(idx, &key, &data, 0);
	assert(MDB_SUCCESS == r);
	cursor_close(idx);
}


static void _node_unpack(graph_txn_t txn, node_t e, const uint8_t *buf){
	int i = 0;
	decode(e->type, buf, i);
	decode(e->val,  buf, i);
}

static void _edge_unpack(graph_txn_t txn, edge_t e, const uint8_t *buf){
	int i = 0;
	decode(e->type, buf, i);
	decode(e->val,  buf, i);
	decode(e->src,  buf, i);
	decode(e->tgt,  buf, i);
}

static void _prop_unpack(graph_txn_t txn, prop_t e, const uint8_t *buf){
	int i = 0;
	decode(e->pid, buf, i);
	decode(e->key, buf, i);
	decode(e->val, buf, i);
}

static void _deletion_unpack(graph_txn_t txn, deletion_t e, const uint8_t *buf){
	return;
}

static logID_t __prop_resolve(graph_txn_t txn, prop_t e, logID_t beforeID, int readonly){
	// stash the old value, in case we cared
	strID_t val = e->val;

	// stomps e->val
	if((_prop_lookup(txn, e, beforeID) && val == e->val) || readonly)
		return e->id;

	assert(0 == beforeID);
	e->val = val;
	e->next = 0;
	e->is_new = 1;
	_prop_append(txn, e, e->id);
	_prop_index(txn, e);
	return e->id;
}

static logID_t __node_resolve(graph_txn_t txn, node_t e, logID_t beforeID, int readonly){
	if(_node_lookup(txn, e, beforeID) || readonly)
		return e->id;

	assert(0 == beforeID);
	e->next = 0;
	e->is_new = 1;
	_node_append(txn, e, e->id);
	_node_index(txn, e);
	return e->id;
}

static logID_t __edge_resolve(graph_txn_t txn, edge_t e, logID_t beforeID, int readonly){
	if(_edge_lookup(txn, e, beforeID) || readonly)
		return e->id;

	assert(0 == beforeID);
	e->next = 0;
	e->is_new = 1;
	_edge_append(txn, e, e->id);
	_edge_index(txn, e);
	return e->id;
}


typedef logID_t (*resolve_func)(graph_txn_t txn, edge_t e, logID_t beforeID, int readonly);
typedef logID_t (*lookup_func)(graph_txn_t txn, entry_t e, logID_t beforeID);
typedef logID_t (*append_func)(graph_txn_t txn, entry_t e, logID_t delID);
typedef void (*index_func)(graph_txn_t txn, entry_t e);
typedef void (*unpack_func)(graph_txn_t txn, entry_t e, const uint8_t *buf);

static node_t _node_resolve(graph_txn_t txn, void *type, size_t tlen, void *val, size_t vlen, logID_t beforeID, int readonly){
	node_t e = (node_t) malloc(sizeof(*e));
	e->rectype = GRAPH_NODE;
	if(_string_resolve(txn, &e->type, type, tlen, readonly) &&
	   _string_resolve(txn, &e->val, val, vlen, readonly) &&
	   __node_resolve(txn, e, beforeID, readonly) ){
		return e;
	}
	free(e);
	return NULL;
}

static edge_t _edge_resolve(graph_txn_t txn, node_t src, node_t tgt, void *type, size_t tlen, void *val, size_t vlen, logID_t beforeID, int readonly){
	edge_t e = (edge_t) malloc(sizeof(*e));
	e->rectype = GRAPH_EDGE;
	assert(src && tgt);
	e->src = src->id;
	e->tgt = tgt->id;
	if(_string_resolve(txn, &e->type, type, tlen, readonly) &&
	   _string_resolve(txn, &e->val, val, vlen, readonly) &&
	   __edge_resolve(txn, e, beforeID, readonly) ){
		return e;
	}
	free(e);
	return NULL;
}

static prop_t _prop_resolve(graph_txn_t txn, entry_t parent, void *key, size_t klen, void *val, size_t vlen, logID_t beforeID, int readonly){
	prop_t e = (prop_t) malloc(sizeof(*e));
	e->rectype = GRAPH_PROP;
	e->pid = parent->id;
	if(_string_resolve(txn, &e->key, key, klen, readonly) &&
	   _string_resolve(txn, &e->val, val, vlen, readonly) &&
	   __prop_resolve(txn, e, beforeID, readonly)){
		return e;
	}
	free(e);
	return NULL;
}

entry_t graph_entry(graph_txn_t txn, const logID_t id){
	static const  unpack_func unpack[] = {
		[GRAPH_DELETION] = (unpack_func)_deletion_unpack,
		[GRAPH_NODE]     = (unpack_func)_node_unpack,
		[GRAPH_EDGE]     = (unpack_func)_edge_unpack,
		[GRAPH_PROP]     = (unpack_func)_prop_unpack,
	};
	static const size_t recsizes[] = {
		[GRAPH_DELETION] = sizeof(struct entry_t),
		[GRAPH_NODE]     = sizeof(struct node_t),
		[GRAPH_EDGE]     = sizeof(struct edge_t),
		[GRAPH_PROP]     = sizeof(struct prop_t),
	};
	uint8_t buf[esizeof(id)];
	MDB_val key = { 0, buf }, data;
	entry_t e = NULL;
	int r;
	encode(id, buf, key.mv_size);
	r = db_get((txn_t)txn, DB_LOG, &key, &data);
	if(MDB_SUCCESS == r){
		int klen = 1, rectype = *(uint8_t *)data.mv_data;
		e = (entry_t) malloc(recsizes[rectype]);
		e->id = id;
		e->rectype = rectype;
		decode(e->next, data.mv_data, klen);
		unpack[rectype](txn, e, &((uint8_t *)data.mv_data)[klen]);
	}
	return e;
}


/*static int graph_string_resolve(graph_txn_t txn, strID_t *id, void const *data, const size_t len){
	return _string_resolve(txn, id, data, len, 0);
}*/


static logID_t _iter_idx_nextID(graph_iter_t iter);

logID_t graph_entry_updateID(graph_txn_t txn, entry_t e, logID_t beforeID){
	logID_t id, maxID;
	graph_iter_t iter = _graph_entry_idx(txn, DB_PROP_IDX, e->id, beforeID);
	if(beforeID){
		maxID = (e->next && e->next < beforeID) ? e->next : e->id;
		while((id = _iter_idx_nextID(iter))){
			if(id >= beforeID)
				continue;
			entry_t e = graph_entry(txn, id);
			if(e->next){
				if(e->next < beforeID && e->next > maxID)
					maxID = e->next;
			}else if(e->id > maxID){
				maxID = e->id;
			}
			free(e);
		}
	}else{
		maxID = e->next ? e->next : e->id;
		while((id = _iter_idx_nextID(iter))){
			entry_t e = graph_entry(txn, id);
			if(e->next){
				if(e->next > maxID)
					maxID = e->next;
			}else if(e->id > maxID){
				maxID = e->id;
			}
			free(e);
		}
	}
	graph_iter_close(iter);
	return maxID;
}

logID_t graph_updateID(graph_txn_t txn, logID_t beforeID){
	static struct entry_t top = { .id = 0, .next = 0 };
	return graph_entry_updateID(txn, &top, beforeID);
}

logID_t graph_node_updateID(graph_txn_t txn, node_t n, logID_t beforeID){
	return graph_entry_updateID(txn, (entry_t)n, beforeID);
}

logID_t graph_edge_updateID(graph_txn_t txn, edge_t e, logID_t beforeID){
	return graph_entry_updateID(txn, (entry_t)e, beforeID);
}

logID_t graph_prop_updateID(graph_txn_t txn, prop_t p, logID_t beforeID){
	return graph_entry_updateID(txn, (entry_t)p, beforeID);
}

int graph_string_lookup(graph_txn_t txn, strID_t *id, void const *data, const size_t len){
	return _string_resolve(txn, id, data, len, 1);
}

logID_t graph_log_nextID(graph_txn_t txn){
	return _graph_log_nextID(txn, 0);
}


logID_t graph_delete(graph_txn_t txn, entry_t e){
	return _entry_delete(txn, e->id);
}

prop_t graph_prop(graph_txn_t txn, const logID_t id){
	prop_t e = (prop_t) graph_entry(txn, id);
	if(e && GRAPH_PROP != e->rectype){
		free(e);
		e = NULL;
	}
	return e;
}

prop_t graph_prop_get(graph_txn_t txn, prop_t prop, void *key, size_t klen, logID_t beforeID){
	return _prop_resolve(txn, (entry_t)prop, key, klen, NULL, 0, beforeID, 1);
}

prop_t graph_prop_set(graph_txn_t txn, prop_t prop, void *key, size_t klen, void *val, size_t vlen){
	return _prop_resolve(txn, (entry_t)prop, key, klen, val, vlen, 0, 0);
}

void graph_prop_unset(graph_txn_t txn, prop_t e, void *key, size_t klen){
	_entry_unset(txn, e->id, key, klen);
}

prop_t graph_get(graph_txn_t txn, void *key, size_t klen, logID_t beforeID){
	static struct entry_t parent = { .id = 0 };
	return _prop_resolve(txn, &parent, key, klen, NULL, 0, beforeID, 1);
}

prop_t graph_set(graph_txn_t txn, void *key, size_t klen, void *val, size_t vlen){
	static struct entry_t parent = { .id = 0 };
	return _prop_resolve(txn, &parent, key, klen, val, vlen, 0, 0);
}

void graph_unset(graph_txn_t txn, void *key, size_t klen){
	_entry_unset(txn, 0, key, klen);
}


node_t graph_node(graph_txn_t txn, const logID_t id){
	node_t e = (node_t) graph_entry(txn, id);
	if(e && GRAPH_NODE != e->rectype){
		free(e);
		e = NULL;
	}
	return e;
}

node_t graph_node_lookup(graph_txn_t txn, void *type, size_t tlen, void *val, size_t vlen, logID_t beforeID){
	return _node_resolve(txn, type, tlen, val, vlen, beforeID, 1);
}

node_t graph_node_resolve(graph_txn_t txn, void *type, size_t tlen, void *val, size_t vlen){
	return _node_resolve(txn, type, tlen, val, vlen, 0, 0);
}

prop_t graph_node_get(graph_txn_t txn, node_t node, void *key, size_t klen, logID_t beforeID){
	return _prop_resolve(txn, (entry_t)node, key, klen, NULL, 0, beforeID, 1);
}

prop_t graph_node_set(graph_txn_t txn, node_t node, void *key, size_t klen, void *val, size_t vlen){
	return _prop_resolve(txn, (entry_t)node, key, klen, val, vlen, 0, 0);
}

void graph_node_unset(graph_txn_t txn, node_t e, void *key, size_t klen){
	_entry_unset(txn, e->id, key, klen);
}


edge_t graph_edge(graph_txn_t txn, const logID_t id){
	edge_t e = (edge_t) graph_entry(txn, id);
	if(e && GRAPH_EDGE != e->rectype){
		free(e);
		e = NULL;
	}
	return e;
}

edge_t graph_edge_lookup(graph_txn_t txn, node_t src, node_t tgt, void *type, size_t tlen, void *val, size_t vlen, logID_t beforeID){
	return _edge_resolve(txn, src, tgt, type, tlen, val, vlen, beforeID, 1);
}

edge_t graph_edge_resolve(graph_txn_t txn, node_t src, node_t tgt, void *type, size_t tlen, void *val, size_t vlen){
	return _edge_resolve(txn, src, tgt, type, tlen, val, vlen, 0, 0);
}

prop_t graph_edge_get(graph_txn_t txn, edge_t edge, void *key, size_t klen, logID_t beforeID){
	return _prop_resolve(txn, (entry_t)edge, key, klen, NULL, 0, beforeID, 1);
}

prop_t graph_edge_set(graph_txn_t txn, edge_t edge, void *key, size_t klen, void *val, size_t vlen){
	return _prop_resolve(txn, (entry_t)edge, key, klen, val, vlen, 0, 0);
}

void graph_edge_unset(graph_txn_t txn, edge_t e, void *key, size_t klen){
	_entry_unset(txn, e->id, key, klen);
}

struct kv_t {
	graph_txn_t txn;
	MDB_val key, data;
	int flags;
	unsigned int refs, klen;
	uint8_t kbuf[511];
};

struct kv_iter_t {
	struct iter_t iter;
	kv_t kv;
};

kv_t graph_kv(graph_txn_t txn, const void *domain, const size_t dlen, const int flags){
	const int readonly = (TXN_RO(txn) || (flags & LG_KV_RO));
	kv_t kv = NULL;

	strID_t domainID;
	if(!_string_resolve(txn, &domainID, domain, dlen, readonly))
		goto fail;

	kv = (kv_t) malloc(sizeof(*kv));
	if(!kv)
		goto fail;

	kv->txn = txn;
	kv->flags = flags;
	kv->refs = 1;
	kv->klen = 0;
	encode(domainID, kv->kbuf, kv->klen);

	return kv;
fail:
	if(kv)
		free(kv);
	return NULL;
}

static int _kv_setup_key(kv_t kv, void *key, size_t klen, int query){
	strID_t id;
	kv->key.mv_data = kv->kbuf;
	kv->key.mv_size = kv->klen;
	if(kv->flags & LG_KV_MAP_KEYS){
		if(!_string_resolve(kv->txn, &id, key, klen, query))
			return 0;
		encode(id, kv->kbuf, kv->key.mv_size);
	}else{
		assert(klen <= sizeof(kv->kbuf) - kv->klen);
		memcpy(&kv->kbuf[kv->klen], key, klen);
		kv->key.mv_size += klen;
	}
	return 1;
}

void *kv_get(kv_t kv, void *key, size_t klen, size_t *dlen){
	void *data = NULL;
	if(!_kv_setup_key(kv, key, klen, 1))
		goto done;
	if(db_get((txn_t)kv->txn, DB_KV, &kv->key, &kv->data) != MDB_SUCCESS)
		goto done;
	if(kv->flags & LG_KV_MAP_DATA){
		strID_t id;
		int len = 0;
		decode(id, kv->data.mv_data, len);
		data = graph_string(kv->txn, id, dlen);
	}else{
		data = kv->data.mv_data;
		*dlen = kv->data.mv_size;
	}
done:
	return data;
}

int kv_del(kv_t kv, void *key, size_t klen){
	int ret = 0;
	if(_kv_setup_key(kv, key, klen, 1))
		ret = (db_del((txn_t)kv->txn, DB_KV, &kv->key, NULL) == MDB_SUCCESS);
	return ret;
}

int kv_put(kv_t kv, void *key, size_t klen, void *data, size_t dlen){
	int ret = 0;
	uint8_t dbuf[esizeof(strID_t)];
	if(!_kv_setup_key(kv, key, klen, 0))
		goto done;
	if(kv->flags & LG_KV_MAP_DATA){
		strID_t id;
		if(!_string_resolve(kv->txn, &id, data, dlen, 0))
			goto done;
		kv->data.mv_data = dbuf;
		kv->data.mv_size = 0;
		encode(id, dbuf, kv->data.mv_size);
	}else{
		kv->data.mv_data = data;
		kv->data.mv_size = dlen;
	}
	ret = (db_put((txn_t)kv->txn, DB_KV, &kv->key, &kv->data, 0) == MDB_SUCCESS);
done:
	return ret;
}

void *kv_last_key(kv_t kv, size_t *len){
	cursor_t cursor = txn_cursor_new((txn_t)kv->txn, DB_KV);
	assert(cursor);
	MDB_val key;
	void *ret = NULL;
	if(cursor_last_key(cursor, &key, kv->kbuf, kv->klen) == MDB_SUCCESS){
		ret = key.mv_data + kv->klen;
		*len = key.mv_size - kv->klen;
	}
	cursor_close(cursor);
	return ret;
}

void kv_deref(kv_t kv){
	if(!kv || !kv->refs)
		return;
	if(!--kv->refs)
		free(kv);
}


kv_iter_t kv_iter_pfx(kv_t kv, uint8_t *pfx, unsigned int len){
	kv_iter_t iter;
	if(pfx){
		uint8_t buf[kv->klen + len];
		memcpy(buf, kv->kbuf, kv->klen);
		memcpy(buf + kv->klen, pfx, len);
		iter = (kv_iter_t) txn_iter_init(sizeof(*iter), (txn_t)kv->txn, DB_KV, buf, kv->klen + len);
	}else{
		iter = (kv_iter_t) txn_iter_init(sizeof(*iter), (txn_t)kv->txn, DB_KV, kv->kbuf, kv->klen);
	}
	iter->kv = kv;
	kv->refs++;
	return iter;
}

kv_iter_t kv_iter(kv_t kv){
	return kv_iter_pfx(kv, NULL, 0);
}

int kv_iter_next(kv_iter_t iter, void **key, size_t *klen, void **data, size_t *dlen){
	int r = iter_next((iter_t)iter);
	strID_t id;
	int len;
	const int ret = (MDB_SUCCESS == r);
	if(ret){
		if(iter->kv->flags & LG_KV_MAP_KEYS){
			len = iter->kv->klen;
			decode(id, ((iter_t)iter)->key.mv_data, len);
			*key = graph_string(iter->kv->txn, id, klen);
		}else{
			*key = ((iter_t)iter)->key.mv_data + iter->kv->klen;
			*klen = ((iter_t)iter)->key.mv_size - iter->kv->klen;
		}
		if(iter->kv->flags & LG_KV_MAP_DATA){
			len = 0;
			decode(id, ((iter_t)iter)->data.mv_data, len);
			*data = graph_string(iter->kv->txn, id, dlen);
		}else{
			*data = ((iter_t)iter)->data.mv_data;
			*dlen = ((iter_t)iter)->data.mv_size;
		}
	}
	return ret;
}

void kv_iter_close(kv_iter_t iter){
	kv_deref(iter->kv);
	iter_close((iter_t)iter);
}


struct graph_iter_t{
	struct iter_t iter;
	graph_txn_t txn;
	logID_t beforeID;
	graph_iter_t next;
	int head_active;
};


graph_iter_t graph_iter_new(graph_txn_t txn, int dbi, void *pfx, size_t pfxlen, logID_t beforeID){
	graph_iter_t gi = (graph_iter_t)txn_iter_init(sizeof(*gi), (txn_t)txn, dbi, pfx, pfxlen);
	gi->beforeID = _cleanse_beforeID(txn, beforeID);
	gi->txn = txn;
	gi->next = NULL;
	gi->head_active = 1;
	return gi;
}

graph_iter_t graph_iter_concat(unsigned int count, ...){
	graph_iter_t head = NULL, tail = NULL;
	va_list ap;
	va_start(ap, count);
	while(count--){
		graph_iter_t current = va_arg(ap, graph_iter_t);
		if(!current)
			continue;
		if(tail)
			tail->next = current;
		else
			head = tail = current;
		while(tail->next)
			tail = tail->next;
	}
	va_end(ap);
	return head;
}

static logID_t _parse_idx_logID(uint8_t *buf, size_t buflen){
	size_t i = 0;
	logID_t id;

	while(i + buf[i] + 1 != buflen)
		i += 1 + buf[i];
	decode(id, buf, i);
	return id;
}

static logID_t _blarf(graph_iter_t iter){
	logID_t ret = 0;
	while(iter_next_key((iter_t)iter) == MDB_SUCCESS){
		logID_t id = _parse_idx_logID(((iter_t)iter)->key.mv_data, ((iter_t)iter)->key.mv_size);
		if(0 == iter->beforeID || id < iter->beforeID){
			ret = id;
			goto done;
		}
	}
done:
	return ret;
}

// scans index and returns logIDs < beforeID (if beforeID applies)
// caller is responsible for filtering out overwritten IDs
static logID_t _iter_idx_nextID(graph_iter_t gi){
	logID_t id = 0;
	if(gi->head_active){
		// head is still active - try it
		if((id = _blarf(gi)))
			goto done;

		// exhaused - deactivate head
		gi->head_active = 0;
		gi->txn = gi->next ? gi->next->txn : NULL;
	}
	while(gi->next){
		if((id = _blarf(gi->next)))
			goto done;

		// exhausted - remove chained iterator
		graph_iter_t tmp = gi->next;
		gi->next = tmp->next;
		iter_close((iter_t)tmp);
		gi->txn = gi->next ? gi->next->txn : NULL;
	}

done:
	return id;
}

entry_t graph_iter_next(graph_iter_t gi){
	if(gi){
		logID_t id;
		while((id = _iter_idx_nextID(gi))){
			entry_t e = graph_entry(gi->txn, id);
			if(e->next == 0 || (gi->beforeID && e->next >= gi->beforeID))
				return e;
			free(e);
		}
	}
	return NULL;
}

void graph_iter_close(graph_iter_t gi){
	while(gi){
		graph_iter_t next = gi->next;
		iter_close((iter_t)gi);
		gi = next;
	}
}

static graph_iter_t _graph_entry_idx(graph_txn_t txn, int dbi, logID_t id, logID_t beforeID){
	uint8_t buf[esizeof(id)];
	size_t buflen = 0;
	encode(id, buf, buflen);
	return graph_iter_new(txn, dbi, buf, buflen, beforeID);
}

graph_iter_t graph_nodes(graph_txn_t txn, logID_t beforeID){
	return graph_iter_new(txn, DB_NODE_IDX, "", 0, beforeID);
}

graph_iter_t graph_edges(graph_txn_t txn, logID_t beforeID){
	return graph_iter_new(txn, DB_EDGE_IDX, "", 0, beforeID);
}

static graph_iter_t _graph_nodes_edges_type(graph_txn_t txn, int dbi, void *type, size_t tlen, logID_t beforeID){
	strID_t typeID;
	uint8_t kbuf[esizeof(typeID)];
	size_t klen = 0;
	graph_iter_t iter = NULL;
	if(graph_string_lookup(txn, &typeID, type, tlen)){
		encode(typeID, kbuf, klen);
		iter = graph_iter_new(txn, dbi, kbuf, klen, beforeID);
	}
	return iter;
}

graph_iter_t graph_nodes_type(graph_txn_t txn, void *type, size_t tlen, logID_t beforeID){
	return _graph_nodes_edges_type(txn, DB_NODE_IDX, type, tlen, beforeID);
}

graph_iter_t graph_edges_type(graph_txn_t txn, void *type, size_t tlen, logID_t beforeID){
	return _graph_nodes_edges_type(txn, DB_EDGE_IDX, type, tlen, beforeID);
}


graph_iter_t graph_node_edges_in(graph_txn_t txn, node_t node, logID_t beforeID){
	return _graph_entry_idx(txn, DB_TGTNODE_IDX, node->id, beforeID);
}

graph_iter_t graph_node_edges_out(graph_txn_t txn, node_t node, logID_t beforeID){
	return _graph_entry_idx(txn, DB_SRCNODE_IDX, node->id, beforeID);
}

graph_iter_t graph_node_edges(graph_txn_t txn, node_t node, logID_t beforeID){
	graph_iter_t in = graph_node_edges_in(txn, node, beforeID);
	graph_iter_t out = graph_node_edges_out(txn, node, beforeID);
	return graph_iter_concat(2, in, out);
	return graph_iter_concat(2,
		graph_node_edges_in(txn, node, beforeID),
		graph_node_edges_out(txn, node, beforeID));
}

/*
graph_iter_t graph_node_edges_dir(graph_txn_t txn, node_t node, unsigned int direction, logID_t beforeID){
	assert(direction <= GRAPH_DIR_BOTH);
	switch(direction){
		case GRAPH_DIR_IN:
			return graph_node_edges_in(txn, node, beforeID);
		case GRAPH_DIR_OUT:
			return graph_node_edges_out(txn, node, beforeID);
		default:
			return graph_node_edges(txn, node, beforeID);
	}
}*/

graph_iter_t graph_node_edges_dir(graph_txn_t txn, node_t node, unsigned int direction, logID_t beforeID){
	typedef graph_iter_t (*edges_func)(graph_txn_t, node_t, logID_t);
	const static edges_func edges_x[] = {
		[0]              = graph_node_edges,
		[GRAPH_DIR_IN]   = graph_node_edges_in,
		[GRAPH_DIR_OUT]  = graph_node_edges_out,
		[GRAPH_DIR_BOTH] = graph_node_edges,
	};
	assert(direction <= GRAPH_DIR_BOTH);
	return edges_x[direction](txn, node, beforeID);
}

// lookup edges within a node by type
static graph_iter_t _graph_node_edges_type(graph_txn_t txn, int dbi, logID_t id, strID_t typeID, logID_t beforeID){
	uint8_t kbuf[esizeof(id) + esizeof(typeID)];
	size_t klen = 0;
	encode(id, kbuf, klen);
	encode(typeID, kbuf, klen);
	return graph_iter_new(txn, dbi, kbuf, klen, beforeID);
}

graph_iter_t graph_node_edges_type_in(graph_txn_t txn, node_t node, void *type, size_t tlen, logID_t beforeID){
	strID_t typeID;
	if(graph_string_lookup(txn, &typeID, type, tlen))
		return _graph_node_edges_type(txn, DB_TGTNODE_IDX, node->id, typeID, beforeID);
	return NULL;
}

graph_iter_t graph_node_edges_type_out(graph_txn_t txn, node_t node, void *type, size_t tlen, logID_t beforeID){
	strID_t typeID;
	if(graph_string_lookup(txn, &typeID, type, tlen))
		return _graph_node_edges_type(txn, DB_SRCNODE_IDX, node->id, typeID, beforeID);
	return NULL;
}

graph_iter_t graph_node_edges_type(graph_txn_t txn, node_t node, void *type, size_t tlen, logID_t beforeID){
	strID_t typeID;
	if(graph_string_lookup(txn, &typeID, type, tlen))
		return graph_iter_concat(2,
			_graph_node_edges_type(txn, DB_TGTNODE_IDX, node->id, typeID, beforeID),
			_graph_node_edges_type(txn, DB_SRCNODE_IDX, node->id, typeID, beforeID));
	return NULL;
}

graph_iter_t graph_node_edges_dir_type(graph_txn_t txn, node_t node, unsigned int direction, void *type, size_t tlen, logID_t beforeID){
	typedef graph_iter_t (*edges_func)(graph_txn_t, node_t, void *, size_t, logID_t);
	const static edges_func edges_x[] = {
		[0]              = graph_node_edges_type,
		[GRAPH_DIR_IN]   = graph_node_edges_type_in,
		[GRAPH_DIR_OUT]  = graph_node_edges_type_out,
		[GRAPH_DIR_BOTH] = graph_node_edges_type,
	};
	assert(direction <= GRAPH_DIR_BOTH);
	return edges_x[direction](txn, node, type, tlen, beforeID);
}

graph_iter_t graph_props(graph_txn_t txn, logID_t beforeID){
	return _graph_entry_idx(txn, DB_PROP_IDX, 0, beforeID);
}

graph_iter_t graph_node_props(graph_txn_t txn, node_t node, logID_t beforeID){
	return _graph_entry_idx(txn, DB_PROP_IDX, node->id, beforeID);
}

graph_iter_t graph_edge_props(graph_txn_t txn, edge_t edge, logID_t beforeID){
	return _graph_entry_idx(txn, DB_PROP_IDX, edge->id, beforeID);
}

graph_iter_t graph_prop_props(graph_txn_t txn, prop_t prop, logID_t beforeID){
	return _graph_entry_idx(txn, DB_PROP_IDX, prop->id, beforeID);
}

graph_t graph_open(const char * const path, const int flags, const int mode, int mdb_flags){
	// fixme? padsize hardcoded to 1gb
	return (graph_t) db_init(sizeof(struct graph_t), path, flags, mode, mdb_flags, DBS, DB_INFO, 1<<30);
}

graph_txn_t graph_txn_begin(graph_t g, graph_txn_t parent, unsigned int flags){
	graph_txn_t txn = (graph_txn_t) db_txn_init(sizeof(*txn), (db_t)g, (txn_t)parent, flags);
	if(txn)
		txn->next_strID = txn->next_logID = 0;
	return txn;
}

int graph_txn_updated(graph_txn_t txn){
	return txn_updated((txn_t)txn);
}

int graph_txn_commit(graph_txn_t txn){
	return txn_commit((txn_t)txn);
}

void graph_txn_abort(graph_txn_t txn){
	txn_abort((txn_t)txn);
}

void graph_sync(graph_t g, int force){
	db_sync((db_t)g, force);
}

int graph_updated(graph_t g){
	return db_updated((db_t)g);
}

size_t graph_size(graph_t g){
	return db_size((db_t)g);
}

void graph_close(graph_t g){
	if(g)
		db_close((db_t)g);
}

/*
static int _graph_string_fetchID(graph_txn_t txn, strID_t id, void **buf, size_t *len){
	int found = 0;
	if(id){
		MDB_val key = { sizeof(id), &id }, data;
		MDB_cursor *scalar = graph_txn_cursor(txn, DB_SCALAR);
		int r = mdb_cursor_get(scalar, &key, &data, MDB_SET_KEY);
		if(0 == r){
			*len = data.mv_size;
			*buf = data.mv_data;
			found = 1;
		}
	}else{
		*len = 0;
		*buf = NULL;
		found = 1;
	}
	return found;
}
*/

char *graph_string(graph_txn_t txn, strID_t id, size_t *len){
	char *s = NULL;
	if(id){
		MDB_val key = { sizeof(id), &id }, data;
		int r = db_get((txn_t)txn, DB_SCALAR, &key, &data);
		assert(MDB_SUCCESS == r);
		if(len)
			*len = data.mv_size;
		s = data.mv_data;
	}else if(len){
		*len = 0;
	}
	return s;
}

// end public api
