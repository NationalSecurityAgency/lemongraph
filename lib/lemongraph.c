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

#include<lemongraph.h>

#include"static_assert.h"

typedef uint64_t txnID_t;

STATIC_ASSERT(sizeof(uint64_t) == sizeof(txnID_t), "");
STATIC_ASSERT(sizeof(uint64_t) == sizeof(logID_t), "");
STATIC_ASSERT(sizeof(uint64_t) == sizeof(strID_t), "");

#define INLINE __attribute__((always_inline)) inline

#define MAX(x, y) ((x) > (y) ? (x) : (y))

// max log entry size is for edge_t
#define MAX_LOGBUF (1 + esizeof(strID_t) * 2 + esizeof(logID_t) * 3)

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

// corresponding decode
#define decode(x, buffer, iter) do{ \
	uint8_t _count = ((uint8_t *)(buffer))[iter++]; \
	assert(sizeof(x) >= _count); \
	x = 0; \
	while(_count--) \
		x = (x<<8) + ((uint8_t *)(buffer))[iter++]; \
}while(0)

#define enclen(buffer, offset) (1 + ((uint8_t *)(buffer))[offset])

#define esizeof(x) (sizeof(x)+1)

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

// here's the deal - the actual key in the db is comprised of 3 serialized uints:
//	txnID: incrementing sequence of transaction numbers, starting at 1
//	       each write txn that caused the log to grow will get it's own txnID
//	start: first logID in the txn, will be >= 1
//	count: number of logIDs accumulated in the txn, will be >= 1
//
// when this function is called, at least one of the params will be an actual key as above
// if the txnID for the other decodes to zero, then it is a query operation. Decoding the second uint determines
//	whether the 3rd uint should be tested against (0) the txnID or (non-zero) the start/count range
//
// with one btree, this lets us quickly:
//	map txnID to logID range
//	map logID to containing txnID
static int magic_txnlog_cmp(const buffer_t *a, const buffer_t *b){
	int ia = 0, ib = 0;
	txnID_t ta, tb;
	decode(ta, a->data, ia);
	decode(tb, b->data, ib);
	if(!ta){
		assert(tb);
		// a is query, b is actual key in db
		decode(ta, a->data, ia);
		if(ta){
			decode(ta, a->data, ia);
			uint64_t start, count;
			decode(start, b->data, ib);
			if(ta < start)
				return -1;
			decode(count, b->data, ib);
			return ta >= (start + count);
		}
		// txnID query
		decode(ta, a->data, ia);
	}else if(!tb){
		// I don't believe this happens today, but just in case ...
		return - magic_txnlog_cmp(b, a);
	}
	return ta < tb ? -1 : ta > tb ? 1 : 0;
}

#define DBS (sizeof(DB_INFO)/sizeof(*DB_INFO))

#define DB_LOG          0
#define DB_KEY          1
#define DB_KEY_IDX      2
#define DB_SCALAR       3
#define DB_SCALAR_IDX   4
#define DB_NODE_IDX     5
#define DB_EDGE_IDX     6
#define DB_PROP_IDX     7
#define DB_SRCNODE_IDX  8
#define DB_TGTNODE_IDX  9
#define DB_KV          10
#define DB_TXNLOG      11

static dbi_t DB_INFO[] = {
	// strID_t strID => bytes (append-only)
	[DB_SCALAR] = { "scalar", DB_INTEGERKEY, NULL },

	// uint32_t crc => strID_t strIDs[]
	[DB_SCALAR_IDX] = { "scalar_idx", DB_DUPSORT|DB_INTEGERKEY|DB_DUPFIXED|DB_INTEGERDUP, NULL },

	// varint_t logID => entry_t (appends & updates)
	[DB_LOG] = { "log", 0, NULL },

	// varint_t [type, val, logID] => ''
	[DB_NODE_IDX] = { "node_idx", 0, NULL },

	// varint_t [type, val, src, tgt, logID]
	[DB_EDGE_IDX] = { "edge_idx", 0, NULL },

	// varint_t pid, key, logID => ''
	[DB_PROP_IDX] = { "prop_idx", 0, NULL },

	// varint_t node, type, edge => ''
	[DB_SRCNODE_IDX] = { "srcnode_idx", 0, NULL },

	// varint_t node, type, edge => ''
	[DB_TGTNODE_IDX] = { "tgtnode_idx", 0, NULL },

	// varint_t domain, key => varint_t val
	[DB_KV] = { "kv", 0, NULL },

	// varint_t [txnID, start, count] => varint_t [node_count, edge_count] (append only)
	[DB_TXNLOG] = { "txnlog", 0, magic_txnlog_cmp }
};

struct graph_t{
	struct db_t db;
};

#define TXN_DB(txn) ((txn_t)(txn))->db
#define TXN_RW(txn) ((txn)->txn.rw)
#define TXN_RO(txn) ((txn)->txn.ro)
#define TXN_PARENT(txn) ((graph_txn_t)(((txn_t)(txn))->parent))

struct graph_txn_t{
	// everything after 'txn' is copied to a parent txn on commit success
	struct txn_t txn;

	strID_t next_strID;
	logID_t next_logID;
	logID_t begin_nextID;
	int64_t node_delta;
	int64_t edge_delta;

	// everything from prev_id down may be copied to a parent txn on commit fail/abort
	// (iff the parent didn't already have it)
	txnID_t prev_id;
	logID_t prev_start;
	logID_t prev_count;
	uint64_t prev_nodes;
	uint64_t prev_edges;
};

typedef struct txn_info_t * txn_info_t;
struct txn_info_t{
	txnID_t id;
	logID_t start;
	logID_t count;
	uint64_t nodes;
	uint64_t edges;
};

// return 0 on error
static INLINE uint64_t _nextID(graph_txn_t txn, const int consume, uint64_t * const cache, const int db1, const int venc){
	if(consume && TXN_RO(txn)){
		errno = EINVAL;
		return 0;
	}

	int r, i;
	uint64_t id = *cache;
	if(0 == id){
		struct cursor_t c;
		buffer_t key;
		r = txn_cursor_init(&c, (txn_t)txn, db1);
		if(DB_SUCCESS == r)
			r = cursor_last_key(&c, &key, NULL, 0);
		cursor_close(&c);
		switch(r){
			case DB_SUCCESS:
				if(venc){
					i = 0;
					decode(id, key.data, i);
					assert(i == key.size);
				}else{
					assert(sizeof(id) == key.size);
					memcpy(&id, key.data, sizeof(id));
				}
				// passthrough
			case DB_NOTFOUND:
				*cache = ++id;
				break;
			default:
				errno = r;
				return 0;
		}
	}
	if(consume && id && 0 == ++(*cache)){
		*cache = id;
		errno = EOVERFLOW;
		id = 0;
	}
	return id;
}

static INLINE logID_t _graph_log_nextID(graph_txn_t txn, int consume){
	return _nextID(txn, consume, &txn->next_logID, DB_LOG, 1);
}

// returns 1 for success, 0 for failure (only for readonly)
static INLINE int __resolve_blob(graph_txn_t txn, uint64_t *ret, char const *data, const size_t len, const int readonly, uint64_t * const cache, int db1, int db2){
	assert(data);

	int r;
	size_t count;
	uint64_t id;
	uint32_t chk;

	struct cursor_t c, idx;
	buffer_t val, vkey, ival, ikey = { .data = &chk, .size = sizeof(chk) };

	r = txn_cursor_init(&c, (txn_t)txn, db1);
	assert(DB_SUCCESS == r);
	r = txn_cursor_init(&idx, (txn_t)txn, db2);
	assert(DB_SUCCESS == r);

	// fill in checksum
	chk = crc32(0, (void *)data, len);

	int retval = 1;

	r = cursor_get(&idx, &ikey, &ival, DB_SET_KEY);
	if(DB_SUCCESS == r){
		r = cursor_count(&idx, &count);
		assert(DB_SUCCESS == r);
		while(1){
			memcpy(&vkey, &ival, sizeof(ival));
			// query main db
			r = cursor_get(&c, &vkey, &val, DB_SET_KEY);
			assert(DB_SUCCESS == r);
			if(val.size == len && memcmp(val.data, data, len) == 0){
				assert(sizeof(*ret) == vkey.size);
				memcpy(ret, vkey.data, sizeof(*ret));
				goto done;
			}
			if(0 == --count)
				break;
			r = cursor_get(&idx, &ikey, &vkey, DB_NEXT_DUP);
			assert(DB_SUCCESS == r);
		}
		r = DB_NOTFOUND;
	}
	assert(DB_NOTFOUND == r);

	// no key at all, or no matching strings

	// bail out now for read-only requests
	if(readonly){
		retval = 0;
		*ret = 0;
		goto done;
	}

	// figure out next ID to use
	*ret = id = _nextID(txn, 1, cache, db1, 0);
	assert(id);

	// store new string in db
	vkey.size = sizeof(id);
	vkey.data = &id;
	val.size = len;
	r = cursor_put(&c, &vkey, &val, DB_APPEND|DB_RESERVE);
	assert(DB_SUCCESS == r);
	memcpy(val.data, data, len);

	// and add index entry
	ikey.data = &chk;
	ikey.size = sizeof(chk);
	assert(&id == vkey.data);
	r = cursor_put(&idx, &ikey, &vkey, DB_APPENDDUP);
	assert(DB_SUCCESS == r);

done:
	cursor_close(&c);
	cursor_close(&idx);
	return retval;
}

static INLINE int _string_resolve(graph_txn_t txn, strID_t *ret, void const *data, const size_t len, int readonly){
	if(NULL == data){
		assert(0 == len);
		*ret = 0;
		return 1;
	}
	return __resolve_blob(txn, ret, data, len, readonly, &txn->next_strID, DB_SCALAR, DB_SCALAR_IDX);
}

static INLINE logID_t _cleanse_beforeID(graph_txn_t txn, logID_t beforeID){
	return (beforeID && _graph_log_nextID(txn, 0) > beforeID) ? beforeID : 0;
}


static INLINE uint8_t *__lookup(graph_txn_t txn, entry_t e, const int db_idx, uint8_t *kbuf, size_t klen, const logID_t beforeID){
	struct cursor_t idx;
	int r = txn_cursor_init(&idx, (txn_t)txn, db_idx);
	assert(DB_SUCCESS == r);

	buffer_t key = { klen, kbuf }, data = { 0, NULL };
	db_cursor_op op = -1;
	uint8_t *logbuf = NULL;

	e->id = 0;

	// use beforeID to seek just past our target
	if(beforeID)
		encode(beforeID, kbuf, key.size);
	else
		kbuf[key.size++] = 0xff;
	r = cursor_get(&idx, &key, &data, DB_SET_RANGE);
	if(DB_SUCCESS == r){ // back up one record
		op = DB_PREV;
	}else if(DB_NOTFOUND == r){ // no records larger than target - try last record
		op = DB_LAST;
	}else{
		assert(DB_SUCCESS == r);
	}

	r = cursor_get(&idx, &key, &data, op);
	if(DB_SUCCESS == r){
		r = cursor_get(&idx, &key, &data, DB_GET_CURRENT);
		assert(DB_SUCCESS == r);
		if(memcmp(key.data, kbuf, klen) == 0){
			uint8_t buf[esizeof(e->id)];

			// harvest id
			decode(e->id, key.data, klen);

			// now pull log entry to fill in .next
			key.size = 0;
			key.data = buf;
			encode(e->id, buf, key.size);
			r = db_get((txn_t)txn, DB_LOG, &key, &data);
			assert(DB_SUCCESS == r);
			assert(e->rectype == *(uint8_t *)data.data);
			klen = 1;
			decode(e->next, data.data, klen);
			if(e->next && (0 == beforeID || e->next < beforeID)){
				e->id = 0;
			}else{
				logbuf = &((uint8_t *)data.data)[klen];
				e->is_new = 0;
			}
		}
	}

	cursor_close(&idx);
	return logbuf;
}

static INLINE logID_t _node_lookup(graph_txn_t txn, node_t e, logID_t beforeID){
	uint8_t kbuf[esizeof(e->type) + esizeof(e->val) + esizeof(e->id)];
	size_t klen = 0;
	encode(e->type, kbuf, klen);
	encode(e->val,  kbuf, klen);
	__lookup(txn, (entry_t)e, DB_NODE_IDX, kbuf, klen, beforeID);
	return e->id;
}

static INLINE logID_t _edge_lookup(graph_txn_t txn, edge_t e, logID_t beforeID){
	uint8_t kbuf[esizeof(e->type) + esizeof(e->val) + esizeof(e->src) + esizeof(e->tgt) + esizeof(e->id)];
	size_t klen = 0;
	encode(e->type, kbuf, klen);
	encode(e->val,  kbuf, klen);
	encode(e->src,  kbuf, klen);
	encode(e->tgt,  kbuf, klen);
	__lookup(txn, (entry_t)e, DB_EDGE_IDX, kbuf, klen, beforeID);
	return e->id;
}

static INLINE logID_t _prop_lookup(graph_txn_t txn, prop_t e, logID_t beforeID){
	uint8_t *logbuf, kbuf[esizeof(e->pid) + esizeof(e->key) + esizeof(e->id)];
	size_t klen = 0;
	encode(e->pid, kbuf, klen);
	encode(e->key, kbuf, klen);
	logbuf = __lookup(txn, (entry_t)e, DB_PROP_IDX, kbuf, klen, beforeID);
	if(logbuf){
		klen = 0;
		klen += enclen(logbuf, klen); // skip pid
		klen += enclen(logbuf, klen); // skip key
		decode(e->val, logbuf, klen); // pull current value
	}
	return e->id;
}

static INLINE graph_iter_t _graph_entry_idx(graph_txn_t txn, int dbi, logID_t id, logID_t beforeID);
graph_iter_t graph_iter_concat(unsigned int count, ...);

static void _delete(graph_txn_t txn, const logID_t newrecID, const logID_t oldrecID, uint8_t *mem){
	uint8_t kbuf[esizeof(newrecID)];
	buffer_t key = { 0, kbuf }, olddata, newdata = { 1, mem };
	int r, tail, tlen;
	graph_iter_t iter;
	entry_t child;

	// update existing log entry - first fetch current
	encode(oldrecID, kbuf, key.size);
	r = db_get((txn_t)txn, DB_LOG, &key, &olddata);
	assert(DB_SUCCESS == r);

	// copy rectype (size already set to 1)
	const uint8_t rectype = *mem = *(uint8_t *)olddata.data;

	// fill in new nextID
	encode(newrecID, mem, newdata.size);

	// append remainder of original record
	tail = 1 + enclen(olddata.data, 1);
	tlen = olddata.size - tail;
	memcpy(&mem[newdata.size], &((uint8_t *)olddata.data)[tail], tlen);
	newdata.size += tlen;

	// store
	r = db_put((txn_t)txn, DB_LOG, &key, &newdata, 0);
	assert(DB_SUCCESS == r);

	// recursively delete item properties, and edges if item is a node
	if(GRAPH_NODE == rectype){
		iter = graph_iter_concat(3,
			_graph_entry_idx(txn, DB_PROP_IDX, oldrecID, 0),
			_graph_entry_idx(txn, DB_SRCNODE_IDX, oldrecID, 0),
			_graph_entry_idx(txn, DB_TGTNODE_IDX, oldrecID, 0));
		txn->node_delta--;
	}else{
		if(GRAPH_EDGE == rectype)
			txn->edge_delta--;
		iter = _graph_entry_idx(txn, DB_PROP_IDX, oldrecID, 0);
	}
	while((child = graph_iter_next(iter))){
		_delete(txn, newrecID, child->id, mem);
		free(child);
	}

	graph_iter_close(iter);
}

static INLINE logID_t _log_append(graph_txn_t txn, uint8_t *dbuf, size_t dlen, logID_t delID){
	int r;
	logID_t id;
	uint8_t kbuf[esizeof(id)];
	buffer_t key = { 0, kbuf }, data = { dlen, dbuf };

	id = _graph_log_nextID(txn, 1);

	if(delID){
		uint8_t tmp[MAX_LOGBUF];
		_delete(txn, id, delID, tmp);
	}

	encode(id, kbuf, key.size);

	r = db_put((txn_t)txn, DB_LOG, &key, &data, DB_APPEND);
	if(DB_SUCCESS != r)
		fprintf(stderr, "err: %s\n", db_strerror(r));
	assert(DB_SUCCESS == r);
	return id;
}

static INLINE void _entry_unset(graph_txn_t txn, logID_t id, void *key, size_t klen){
	struct prop_t p = { .pid = id, .rectype = GRAPH_PROP };
	if(_string_resolve(txn, &p.key, key, klen, 1)){
		if(_prop_lookup(txn, &p, 0))
			graph_delete(txn, (entry_t)&p);
	}
}

static INLINE logID_t _entry_delete(graph_txn_t txn, logID_t delID){
	uint8_t dbuf[1 + esizeof(delID)];
	size_t dlen = 0;

	dbuf[dlen++] = GRAPH_DELETION;
	encode(delID, dbuf, dlen);

	return _log_append(txn, dbuf, dlen, delID);
}

static INLINE logID_t _node_append(graph_txn_t txn, node_t e, logID_t delID){
	uint8_t dbuf[1 + esizeof(e->next) + esizeof(e->type) + esizeof(e->val)];
	size_t dlen = 0;

	dbuf[dlen++] = e->rectype;
	encode(e->next, dbuf, dlen);
	encode(e->type, dbuf, dlen);
	encode(e->val,  dbuf, dlen);

	return e->id = _log_append(txn, dbuf, dlen, delID);
}

static INLINE logID_t _edge_append(graph_txn_t txn, edge_t e, logID_t delID){
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

static INLINE logID_t _prop_append(graph_txn_t txn, prop_t e, logID_t delID){
	uint8_t dbuf[1 + esizeof(e->next) + esizeof(e->pid) + esizeof(e->key) + esizeof(e->val)];
	size_t dlen = 0;

	dbuf[dlen++] = e->rectype;
	encode(e->next, dbuf, dlen);
	encode(e->pid,  dbuf, dlen);
	encode(e->key,  dbuf, dlen);
	encode(e->val,  dbuf, dlen);

	return e->id = _log_append(txn, dbuf, dlen, delID);
}


static INLINE void _node_index(graph_txn_t txn, node_t e){
	uint8_t kbuf[esizeof(e->type) + esizeof(e->val) + esizeof(e->id)];
	buffer_t key = { 0, kbuf };
	buffer_t data = { 0, NULL };

	encode(e->type, kbuf, key.size);
	encode(e->val,  kbuf, key.size);
	encode(e->id,   kbuf, key.size);
	int r = db_put((txn_t)txn, DB_NODE_IDX, &key, &data, 0);
	assert(DB_SUCCESS == r);
}

static INLINE void _edge_index(graph_txn_t txn, edge_t e){
	uint8_t kbuf[esizeof(e->type) + esizeof(e->val) + esizeof(e->src) + esizeof(e->tgt) + esizeof(e->id)];
	buffer_t key = { 0, kbuf };
	buffer_t data = { 0, NULL };
	int r;

	encode(e->type, kbuf, key.size);
	encode(e->val,  kbuf, key.size);
	encode(e->src,  kbuf, key.size);
	encode(e->tgt,  kbuf, key.size);
	encode(e->id,   kbuf, key.size);
	r = db_put((txn_t)txn, DB_EDGE_IDX, &key, &data, 0);
	assert(DB_SUCCESS == r);

	key.size = 0;
	encode(e->src, kbuf, key.size);
	encode(e->type, kbuf, key.size);
	encode(e->id,  kbuf, key.size);
	r = db_put((txn_t)txn, DB_SRCNODE_IDX, &key, &data, 0);
	assert(DB_SUCCESS == r);

	key.size = 0;
	encode(e->tgt, kbuf, key.size);
	encode(e->type, kbuf, key.size);
	encode(e->id,  kbuf, key.size);
	r = db_put((txn_t)txn, DB_TGTNODE_IDX, &key, &data, 0);
	assert(DB_SUCCESS == r);
}

static INLINE void _prop_index(graph_txn_t txn, prop_t e){
	uint8_t kbuf[esizeof(e->pid) + esizeof(e->key) + esizeof(e->id)];
	buffer_t key = { 0, kbuf };
	buffer_t data = { 0, NULL };

	encode(e->pid, kbuf, key.size);
	encode(e->key, kbuf, key.size);
	encode(e->id,  kbuf, key.size);
	int r = db_put((txn_t)txn, DB_PROP_IDX, &key, &data, 0);
	assert(DB_SUCCESS == r);
}

static INLINE logID_t __prop_resolve(graph_txn_t txn, prop_t e, logID_t beforeID, int readonly){
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

static INLINE logID_t __node_resolve(graph_txn_t txn, node_t e, logID_t beforeID, int readonly){
	if(_node_lookup(txn, e, beforeID) || readonly)
		return e->id;

	assert(0 == beforeID);
	e->next = 0;
	e->is_new = 1;
	txn->node_delta++;
	_node_append(txn, e, e->id);
	_node_index(txn, e);
	return e->id;
}

static INLINE logID_t __edge_resolve(graph_txn_t txn, edge_t e, logID_t beforeID, int readonly){
	if(_edge_lookup(txn, e, beforeID) || readonly)
		return e->id;

	assert(0 == beforeID);
	e->next = 0;
	e->is_new = 1;
	txn->edge_delta++;
	_edge_append(txn, e, e->id);
	_edge_index(txn, e);
	return e->id;
}


static INLINE node_t _node_resolve(graph_txn_t txn, void *type, size_t tlen, void *val, size_t vlen, logID_t beforeID, int readonly){
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

static INLINE edge_t _edge_resolve(graph_txn_t txn, node_t src, node_t tgt, void *type, size_t tlen, void *val, size_t vlen, logID_t beforeID, int readonly){
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

static INLINE prop_t _prop_resolve(graph_txn_t txn, entry_t parent, void *key, size_t klen, void *val, size_t vlen, logID_t beforeID, int readonly){
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
	static const int recsizes[] = {
		[GRAPH_DELETION] = sizeof(struct entry_t),
		[GRAPH_NODE]     = sizeof(struct node_t),
		[GRAPH_EDGE]     = sizeof(struct edge_t),
		[GRAPH_PROP]     = sizeof(struct prop_t),
	};
	uint8_t buf[esizeof(id)];
	buffer_t key = { 0, buf }, data;
	entry_t e = NULL;
	int r;
	encode(id, buf, key.size);
	r = db_get((txn_t)txn, DB_LOG, &key, &data);
	if(DB_SUCCESS == r){
		const int rectype = *(uint8_t *)data.data;
		assert(rectype < sizeof(recsizes) / sizeof(*recsizes));
		int klen = 1;
		e = (entry_t) malloc(recsizes[rectype]);
		e->id = id;
		e->rectype = rectype;
		decode(e->next, data.data, klen);
		switch(rectype){
			case GRAPH_NODE:
				decode(((node_t)e)->type, data.data, klen);
				decode(((node_t)e)->val,  data.data, klen);
				break;
			case GRAPH_EDGE:
				decode(((edge_t)e)->type, data.data, klen);
				decode(((edge_t)e)->val,  data.data, klen);
				decode(((edge_t)e)->src,  data.data, klen);
				decode(((edge_t)e)->tgt,  data.data, klen);
				break;
			case GRAPH_PROP:
				decode(((prop_t)e)->pid,  data.data, klen);
				decode(((prop_t)e)->key,  data.data, klen);
				decode(((prop_t)e)->val,  data.data, klen);
				break;
		}
	}
	return e;
}


/*static int graph_string_resolve(graph_txn_t txn, strID_t *id, void const *data, const size_t len){
	return _string_resolve(txn, id, data, len, 0);
}*/


static INLINE logID_t _iter_idx_nextID(graph_iter_t iter);

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
	buffer_t key, data;
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

static INLINE int _kv_setup_key(kv_t kv, void *key, size_t klen, int query){
	strID_t id;
	kv->key.data = kv->kbuf;
	kv->key.size = kv->klen;
	if(kv->flags & LG_KV_MAP_KEYS){
		if(!_string_resolve(kv->txn, &id, key, klen, query))
			return 0;
		encode(id, kv->kbuf, kv->key.size);
	}else{
		assert(klen <= sizeof(kv->kbuf) - kv->klen);
		memcpy(&kv->kbuf[kv->klen], key, klen);
		kv->key.size += klen;
	}
	return 1;
}

void *kv_get(kv_t kv, void *key, size_t klen, size_t *dlen){
	void *data = NULL;
	if(!_kv_setup_key(kv, key, klen, 1))
		goto done;
	if(db_get((txn_t)kv->txn, DB_KV, &kv->key, &kv->data) != DB_SUCCESS)
		goto done;
	if(kv->flags & LG_KV_MAP_DATA){
		strID_t id;
		int len = 0;
		decode(id, kv->data.data, len);
		data = graph_string(kv->txn, id, dlen);
	}else{
		data = kv->data.data;
		*dlen = kv->data.size;
	}
done:
	return data;
}

int kv_del(kv_t kv, void *key, size_t klen){
	int ret = 0;
	if(_kv_setup_key(kv, key, klen, 1))
		ret = (db_del((txn_t)kv->txn, DB_KV, &kv->key, NULL) == DB_SUCCESS);
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
		kv->data.data = dbuf;
		kv->data.size = 0;
		encode(id, dbuf, kv->data.size);
	}else{
		kv->data.data = data;
		kv->data.size = dlen;
	}
	ret = (db_put((txn_t)kv->txn, DB_KV, &kv->key, &kv->data, 0) == DB_SUCCESS);
done:
	return ret;
}

void *kv_last_key(kv_t kv, size_t *len){
	struct cursor_t cursor;
	int r = txn_cursor_init(&cursor, (txn_t)kv->txn, DB_KV);
	assert(DB_SUCCESS == r);
	buffer_t key;
	void *ret = NULL;
	if(cursor_last_key(&cursor, &key, kv->kbuf, kv->klen) == DB_SUCCESS){
		ret = key.data + kv->klen;
		*len = key.size - kv->klen;
	}
	cursor_close(&cursor);
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
	iter = malloc(sizeof(*iter));
	if(iter){
		int r;
		if(pfx){
			uint8_t buf[kv->klen + len];
			memcpy(buf, kv->kbuf, kv->klen);
			memcpy(buf + kv->klen, pfx, len);
			r = txn_iter_init((iter_t)iter, (txn_t)kv->txn, DB_KV, buf, kv->klen + len);
		}else{
			r = txn_iter_init((iter_t)iter, (txn_t)kv->txn, DB_KV, kv->kbuf, kv->klen);
		}
		if(DB_SUCCESS == r){
			iter->kv = kv;
			kv->refs++;
		}else{
			free(iter);
			iter = NULL;
			errno = r;
		}
	}
	return iter;
}

kv_iter_t kv_iter(kv_t kv){
	return kv_iter_pfx(kv, NULL, 0);
}

int kv_iter_next(kv_iter_t iter, void **key, size_t *klen, void **data, size_t *dlen){
	int r = iter_next((iter_t)iter);
	strID_t id;
	int len;
	const int ret = (DB_SUCCESS == r);
	if(ret){
		if(iter->kv->flags & LG_KV_MAP_KEYS){
			len = iter->kv->klen;
			decode(id, ((iter_t)iter)->key.data, len);
			*key = graph_string(iter->kv->txn, id, klen);
		}else{
			*key = ((iter_t)iter)->key.data + iter->kv->klen;
			*klen = ((iter_t)iter)->key.size - iter->kv->klen;
		}
		if(iter->kv->flags & LG_KV_MAP_DATA){
			len = 0;
			decode(id, ((iter_t)iter)->data.data, len);
			*data = graph_string(iter->kv->txn, id, dlen);
		}else{
			*data = ((iter_t)iter)->data.data;
			*dlen = ((iter_t)iter)->data.size;
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
	graph_iter_t gi = malloc(sizeof(*gi));
	if(gi){
		int r = txn_iter_init((iter_t)gi, (txn_t)txn, dbi, pfx, pfxlen);
		if(DB_SUCCESS == r){
			gi->beforeID = _cleanse_beforeID(txn, beforeID);
			gi->txn = txn;
			gi->next = NULL;
			gi->head_active = 1;
		}else{
			free(gi);
			gi = NULL;
			errno = r;
		}
	}
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

static INLINE logID_t _parse_idx_logID(uint8_t *buf, size_t buflen){
	size_t i = 0, len = 0;
	logID_t id;

	do{
		i += len;
		len = enclen(buf, i);
	}while(i + len < buflen);
	assert(i + len == buflen);
	decode(id, buf, i);
	return id;
}

static INLINE logID_t _blarf(graph_iter_t iter){
	logID_t ret = 0;
	while(iter_next_key((iter_t)iter) == DB_SUCCESS){
		logID_t id = _parse_idx_logID(((iter_t)iter)->key.data, ((iter_t)iter)->key.size);
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
static INLINE logID_t _iter_idx_nextID(graph_iter_t gi){
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

static INLINE graph_iter_t _graph_entry_idx(graph_txn_t txn, int dbi, logID_t id, logID_t beforeID){
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

static INLINE graph_iter_t _graph_nodes_edges_type(graph_txn_t txn, int dbi, void *type, size_t tlen, logID_t beforeID){
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

graph_iter_t graph_node_edges_dir(graph_txn_t txn, node_t node, unsigned int direction, logID_t beforeID){
	graph_iter_t it;
	switch(direction){
		case GRAPH_DIR_IN:
			it = graph_node_edges_in(txn, node, beforeID);
			break;
		case GRAPH_DIR_OUT:
			it = graph_node_edges_out(txn, node, beforeID);
			break;
		default:
			it = graph_node_edges(txn, node, beforeID);
	}
	return it;
}

// lookup edges within a node by type
static INLINE graph_iter_t _graph_node_edges_type(graph_txn_t txn, int dbi, logID_t id, strID_t typeID, logID_t beforeID){
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
	graph_iter_t it;
	switch(direction){
		case GRAPH_DIR_IN:
			it = graph_node_edges_type_in(txn, node, type, tlen, beforeID);
			break;
		case GRAPH_DIR_OUT:
			it = graph_node_edges_type_out(txn, node, type, tlen, beforeID);
			break;
		default:
			it = graph_node_edges_type(txn, node, type, tlen, beforeID);
	}
	return it;
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

graph_t graph_open(const char * const path, const int flags, const int mode, const int db_flags){
	int r;
	graph_t g = malloc(sizeof(*g));
	if(g){
		// fixme? padsize hardcoded to 1gb
		// explicitly disable DB_WRITEMAP - graph_txn_reset current depends on nested write txns
		r = db_init((db_t)g, path, flags, mode, db_flags & ~DB_WRITEMAP, DBS, DB_INFO, 1<<30);
		if(r){
			free(g);
			g = NULL;
			errno = r;
		}
	}
	return g;
}

graph_txn_t graph_txn_begin(graph_t g, graph_txn_t parent, unsigned int flags){
	graph_txn_t txn = malloc(sizeof(*txn));
	int r = errno;
	if(txn){
		r = db_txn_init((txn_t)txn, (db_t)g, (txn_t)parent, flags);
		if(DB_SUCCESS == r){
			if(parent){
				// for child write txns, take snapshot of parent data
				memcpy(sizeof(txn->txn) + (unsigned char *)txn,
				       sizeof(txn->txn) + (unsigned char *)parent, sizeof(*txn) - sizeof(txn->txn));
			}else{
				// for parent write txns, we need to harvest the nextID
				txn->next_strID = txn->next_logID = txn->node_delta = txn->edge_delta = 0;
				txn->begin_nextID = TXN_RW(txn) ? _graph_log_nextID(txn, 0) : 0;

				// other prev_* fields are only valid if prev_start is non-zero
				txn->prev_start = 0;
			}
		}else{
			free(txn);
			errno = r;
			txn = NULL;
		}
	}
	return txn;
}

static INLINE int _fetch_info(graph_txn_t txn){
	if(!txn->prev_start){
		struct cursor_t c;
		int r = txn_cursor_init(&c, (txn_t)txn, DB_TXNLOG);
		assert(DB_SUCCESS == r);
		r = cursor_get(&c, NULL, NULL, DB_LAST);
		if(DB_SUCCESS == r){
			buffer_t data, key;
			r = cursor_get(&c, &key, &data, DB_GET_CURRENT);
			assert(DB_SUCCESS == r);
			int i = 0;
			decode(txn->prev_id, key.data, i);
			decode(txn->prev_start, key.data, i);
			decode(txn->prev_count, key.data, i);
			assert(i == key.size);

			i = 0;
			decode(txn->prev_nodes, data.data, i);
			decode(txn->prev_edges, data.data, i);
			assert(i == data.size);
		}else if(DB_NOTFOUND == r){
			txn->prev_start = 1; // fudged to make the return statement easy
			txn->prev_id = txn->prev_count = 0;
			txn->prev_nodes = txn->prev_edges = 0;
		}else{
			assert(DB_SUCCESS == r);
		}
		cursor_close(&c);
	}
	return txn->prev_start + txn->prev_count == txn->begin_nextID;
}

int graph_txn_commit(graph_txn_t txn){
	int r;
	graph_txn_t parent;
	txnID_t txnID = 0;
	if(!txn->txn.updated){
		// nothing happened
		graph_txn_abort(txn);
		r = DB_SUCCESS;
	}else if((parent = TXN_PARENT(txn))){
		// nested write txn
		r = txn_commit((txn_t)txn);
		if(DB_SUCCESS == r){
			memcpy(sizeof(txn->txn) + (unsigned char *)parent,
			       sizeof(txn->txn) + (unsigned char *)txn, sizeof(*txn) - sizeof(txn->txn));
		}else if(txn->prev_start != parent->prev_start){
			memcpy(&parent->prev_id, &txn->prev_id, sizeof(*txn) - (intptr_t)&((graph_txn_t)NULL)->prev_id);
		}
		memset(txn, 0, sizeof(*txn));
	}else if(_fetch_info(txn) && txn->next_logID > txn->begin_nextID){
		// write txn w/ valid txnlog table
		logID_t nextID = txn->begin_nextID;
		logID_t count = txn->next_logID - nextID;
		uint64_t nodes = txn->prev_nodes + txn->node_delta;
		uint64_t edges = txn->prev_edges + txn->edge_delta;
		uint8_t kbuf[esizeof(txnID) + esizeof(nextID) + esizeof(count)];
		uint8_t dbuf[esizeof(nodes) + esizeof(edges)];
		buffer_t key = { 0, kbuf }, data = { 0, dbuf };

		txnID = txn->prev_id + 1;

		encode(txnID,  kbuf, key.size);
		encode(nextID, kbuf, key.size);
		encode(count,  kbuf, key.size);
		encode(nodes,  dbuf, data.size);
		encode(edges,  dbuf, data.size);

		r = db_put((txn_t)txn, DB_TXNLOG, &key, &data, DB_APPEND);
		if(DB_SUCCESS == r){
			r = txn_commit((txn_t)txn);
		}else{
			txn_abort((txn_t)txn);
		}
	}else{
		// write txn w/ invalid txnlog table
		r = txn_commit((txn_t)txn);
	}

	free(txn);

	return r;
}

void graph_txn_abort(graph_txn_t txn){
	graph_txn_t parent = TXN_PARENT(txn);
	if(parent)
		memcpy(&parent->prev_id, &txn->prev_id, sizeof(*txn) - (intptr_t)&((graph_txn_t)NULL)->prev_id);
	txn_abort((txn_t)txn);
}

int graph_txn_reset(graph_txn_t txn){
	int i, r = 1;
	graph_txn_t sub_txn = graph_txn_begin((graph_t)(((txn_t)txn)->db), txn, 0);
	if(sub_txn){
		// truncate all tables
		for(i = 0, r = DB_SUCCESS; i < DBS && DB_SUCCESS == r; i++)
			r = db_drop((txn_t) sub_txn, i, 0);
		if(DB_SUCCESS == r){
			r = graph_txn_commit(sub_txn);
			if(DB_SUCCESS == r){
				txn->begin_nextID = 1;
				txn->next_strID = txn->next_logID = txn->node_delta = txn->edge_delta = txn->prev_start = 0;
			}
		}else{
			graph_txn_abort(sub_txn);
		}
	}
	return r;
}

int graph_txn_updated(graph_txn_t txn){
	return txn_updated((txn_t)txn);
}

int graph_sync(graph_t g, int force){
	int r = db_sync((db_t)g, force);
	if(DB_SUCCESS != r){
		fprintf(stderr, "%d: mdb_env_sync(): %s (%d)\n", (int)getpid(), db_strerror(r), r);
		assert(DB_SUCCESS == r);
	}
	return r;
}

int graph_updated(graph_t g){
	return db_updated((db_t)g);
}

size_t graph_size(graph_t g){
	size_t size;
	int r = db_size((db_t)g, &size);
	return r ? 0 : size;
}

void graph_remap(graph_t g){
	db_remap((db_t)g);
}

void graph_close(graph_t g){
	if(g)
		db_close((db_t)g);
}

static INLINE int _find_txn(graph_txn_t txn, txn_info_t info, logID_t beforeID){
	assert(beforeID && beforeID <= txn->next_logID);
	const logID_t stopID = beforeID - 1;
	int ret = 0;

	uint8_t kbuf[esizeof(txnID_t) + esizeof(logID_t) + esizeof(logID_t)];
	buffer_t data, key = { 0, kbuf };

	// encode magic to query by logID
	encode(0, kbuf, key.size);
	encode(1, kbuf, key.size);
	encode(stopID, kbuf, key.size);

	struct cursor_t c;
	int r =  txn_cursor_init(&c, (txn_t)txn, DB_TXNLOG);
	assert(DB_SUCCESS == r);
	r = cursor_get(&c, &key, &data, DB_SET_KEY);
	int i;

	if(DB_SUCCESS == r){
again:
		i = 0;
		decode(info->id, key.data, i);
		decode(info->start, key.data, i);
		decode(info->count, key.data, i);
		assert(key.size == i);

		if(info->start + info->count <= beforeID){
			i = 0;
			decode(info->nodes, data.data, i);
			decode(info->edges, data.data, i);
			assert(data.size == i);
			info->start = info->start + info->count;
		}else if(info->id > 1){
			r = cursor_get(&c, &key, &data, DB_PREV);
			assert(DB_SUCCESS == r);
			goto again;
		}else{
			info->start = 1;
			info->count = 0;
			info->nodes = 0;
			info->edges = 0;
		}
		ret = 1;
	}else if(_fetch_info(txn)){
//		info->id = txn->prev_id;
		info->start = txn->prev_start + txn->prev_count;
//		info->count = txn->next_logID - info->start;
		info->nodes = txn->prev_nodes;
		info->edges = txn->prev_edges;
		ret = 1;
	}

	cursor_close(&c);

	return ret;
}

static INLINE void _nodes_edges_delta(graph_txn_t txn, txn_info_t info, logID_t beforeID){
	uint64_t nodes = info->nodes, edges = info->edges;
	logID_t id = info->start;

	if(id == beforeID)
		return;

	struct cursor_t c;
	int r = txn_cursor_init(&c, (txn_t)txn, DB_LOG);
	assert(DB_SUCCESS == r);

	uint8_t kbuf[esizeof(id)];
	buffer_t data, key = { 0, &kbuf };
	encode(id, kbuf, key.size);

	r = cursor_get(&c, &key, &data, DB_SET_KEY);
	assert(DB_SUCCESS == r);
	while(1){
		uint8_t rectype = *(uint8_t *)data.data;
		int i = 0;

		decode(id, key.data, i);

		if(GRAPH_NODE == rectype){
			nodes++;
		}else if(GRAPH_EDGE == rectype){
			edges++;
		}else if(GRAPH_DELETION == rectype){
			buffer_t d2, k2 = { enclen((uint8_t *)data.data, 1), 1 + (uint8_t *)data.data };
			r = db_get((txn_t)txn, DB_LOG, &k2, &d2);
			assert(DB_SUCCESS == r);
			rectype = *(uint8_t *)d2.data;
			if(GRAPH_NODE == rectype){
				graph_iter_t it = graph_edges(txn, id);
				entry_t e;
				while((e = graph_iter_next(it))){
					free(e);
					edges--;
				}
				nodes--;
			}else if(GRAPH_EDGE == rectype){
				edges--;
			}
		}

		if(++id == beforeID)
			break;

		r = cursor_get(&c, &key, &data, DB_NEXT);
		assert(DB_SUCCESS == r);
	}
	cursor_close(&c);
	info->nodes = nodes;
	info->edges = edges;
}

size_t graph_nodes_count(graph_txn_t txn, logID_t beforeID){
	size_t count = 0;

	const logID_t nextID = _graph_log_nextID(txn, 0);
	if(!beforeID || beforeID > nextID)
		beforeID = nextID;

	if(1 == beforeID)
		goto done;

	struct txn_info_t info;
	if(_find_txn(txn, &info, beforeID)){
		_nodes_edges_delta(txn, &info, beforeID);
		count = info.nodes;
		goto done;
	}

	// fall back to scanning the nodes index
	graph_iter_t iter = graph_nodes(txn, beforeID);
	entry_t e;
	while((e = graph_iter_next(iter))){
		free(e);
		count++;
	}

done:
	return count;
}

size_t graph_edges_count(graph_txn_t txn, logID_t beforeID){
	size_t count = 0;

	const logID_t nextID = _graph_log_nextID(txn, 0);
	if(!beforeID || beforeID > nextID)
		beforeID = nextID;

	if(1 == beforeID)
		goto done;

	struct txn_info_t info;
	if(_find_txn(txn, &info, beforeID)){
		_nodes_edges_delta(txn, &info, beforeID);
		count = info.edges;
		goto done;
	}

	// fall back to scanning the edges index for old graphs
	graph_iter_t iter = graph_edges(txn, beforeID);
	entry_t e;
	while((e = graph_iter_next(iter))){
		free(e);
		count++;
	}

done:
	return count;
}

char *__blob(graph_txn_t txn, uint64_t id, size_t *len, int db1){
	assert(id);
	buffer_t key = { sizeof(id), &id }, data;
	int r = db_get((txn_t)txn, db1, &key, &data);
	assert(DB_SUCCESS == r);
	if(len)
		*len = data.size;
	return data.data;
}

char *graph_string(graph_txn_t txn, strID_t id, size_t *len){
	return id ? __blob(txn, id, len, DB_SCALAR) : ((*len = 0), NULL);
}
