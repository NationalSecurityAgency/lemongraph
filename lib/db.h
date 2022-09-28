#ifndef _DB_H
#define _DB_H

#ifndef _BSD_SOURCE
#define _BSD_SOURCE
#endif

#include<stddef.h>
#include<stdarg.h>
#include<pthread.h>

#include"lmdb.h"

// status codes
#define DB_SUCCESS 0
#define DB_NOTFOUND (-30798)

// env/txn flags
#define DB_FIXEDMAP    0x0000001
#define DB_NOSYNC      0x0010000
#define DB_RDONLY      0x0020000
#define DB_NOMETASYNC  0x0040000
#define DB_WRITEMAP    0x0080000
#define DB_MAPASYNC    0x0100000
#define DB_NOTLS       0x0200000
#define DB_NOLOCK      0x0400000
#define DB_NORDAHEAD   0x0800000
#define DB_NOMEMINIT   0x1000000
#define DB_PREVMETA    0x2000000

// db flags
#define DB_REVERSEKEY  0x00002
#define DB_DUPSORT     0x00004
#define DB_INTEGERKEY  0x00008
#define DB_DUPFIXED    0x00010
#define DB_INTEGERDUP  0x00020
#define DB_REVERSEDUP  0x00040
#define DB_CREATE      0x40000

// write flags
#define DB_NOOVERWRITE 0x00010
#define DB_NODUPDATA   0x00020
#define DB_CURRENT     0x00040
#define DB_RESERVE     0x10000
#define DB_APPEND      0x20000
#define DB_APPENDDUP   0x40000
#define DB_MULTIPLE    0x80000

typedef enum db_cursor_op {
    DB_FIRST,
    DB_FIRST_DUP,
    DB_GET_BOTH,
    DB_GET_BOTH_RANGE,
    DB_GET_CURRENT,
    DB_GET_MULTIPLE,
    DB_LAST,
    DB_LAST_DUP,
    DB_NEXT,
    DB_NEXT_DUP,
    DB_NEXT_MULTIPLE,
    DB_NEXT_NODUP,
    DB_PREV,
    DB_PREV_DUP,
    DB_PREV_NODUP,
    DB_SET,
    DB_SET_KEY,
    DB_SET_RANGE,
    DB_PREV_MULTIPLE,
} db_cursor_op;

// txn_end flags
#define DB_TXN_ABORT  1

typedef struct db_t * db_t;
typedef struct txn_t * txn_t;
typedef struct cursor_t * cursor_t;
typedef struct iter_t * iter_t;
typedef struct db_snapshot_t * db_snapshot_t;
typedef struct buffer_t buffer_t;
typedef int (*db_cmp_func)(const buffer_t *a, const buffer_t *b);
typedef unsigned int db_dbi;

typedef const struct {
	const char *const name;
	const unsigned int flags;
	db_cmp_func cmp;
} dbi_t;

typedef struct buffer_t {
	size_t size;   // size of the data item
	void  *data;   // address of the data item
} buffer_t;

// base database object
struct db_t {
	void *env;
	db_dbi *handles;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	size_t padsize;
	int fd, txns;
	int updated : 1;
	int release : 1;
};

// base txn object
struct txn_t {
	cursor_t head; // must be first
	db_t db;
	txn_t parent;
	void *txn;
	int ro : 1;
	int rw : 1;
	int updated : 1;
	int release : 1;
};

// base cursor object
struct cursor_t {
	cursor_t next;  // must be first
	cursor_t prev;
	void *cursor;
	txn_t txn;
	int release : 1;
};

// base iterator object
struct iter_t {
	struct cursor_t cursor;
	buffer_t key, data;
	void *pfx;
	unsigned int pfxlen;
	db_cursor_op op;
	int r;
	int release : 1;
};

char *db_strerror(int err);

int db_new(db_t *db, const char * const path, const int flags, const int mode, int mdb_flags, int ndbi, dbi_t *dbi, size_t padsize);
int db_init(db_t db, const char * const path, const int flags, const int mode, int mdb_flags, int ndbi, dbi_t *dbi, size_t padsize);
int db_get(txn_t txn, int dbi, buffer_t *key, buffer_t *data);
int db_put(txn_t txn, int dbi, buffer_t *key, buffer_t *data, unsigned int flags);
int db_del(txn_t txn, int dbi, buffer_t *key, buffer_t *data);
int db_drop(txn_t txn, int dbi, int del);
int db_sync(db_t db, int force);
int db_updated(db_t db);
void db_close(db_t db);
int db_size(db_t db, size_t *size);
int db_remap(db_t db);

// will fail if this process has active txns/snapshots
// supplied mapsize must be a multiple of the OS pagesize
int db_set_mapsize(db_t db, size_t mapsize);

// these are always safe
int db_get_mapsize(db_t db, size_t *size);
int db_get_disksize(db_t db, size_t *size);

// snapshot foo
int db_snapshot_to_fd(db_t db, int fd, int compact);
db_snapshot_t db_snapshot_new(db_t db, int compact);
ssize_t db_snapshot_read(db_snapshot_t snap, void *buffer, size_t len);
int db_snapshot_close(db_snapshot_t snap);
int db_snapshot_fd(db_snapshot_t snap);

int db_txn_new(txn_t *txn, db_t db, txn_t parent, int flags);
int db_txn_init(txn_t txn, db_t db, txn_t parent, int flags);
int txn_updated(txn_t txn);
void txn_abort(txn_t txn);
int txn_commit(txn_t txn);
int txn_end(txn_t txn, int flags);

int txn_cursor_new(cursor_t *cursor, txn_t txn, int dbi);
int txn_cursor_init(cursor_t cursor, txn_t txn, int dbi);

int cursor_get(cursor_t cursor, buffer_t *key, buffer_t *data, db_cursor_op op);
int cursor_put(cursor_t cursor, buffer_t *key, buffer_t *data, unsigned int flags);
int cursor_del(cursor_t cursor, unsigned int flags);
int cursor_count(cursor_t cursor, size_t *count);
int cursor_last_key(cursor_t cursor, buffer_t *key, uint8_t *pfx, const unsigned int pfxlen);
void cursor_close(cursor_t cursor);

int txn_iter_new(iter_t *iter, txn_t txn, int dbi, void *pfx, const unsigned int len);
int txn_iter_init(iter_t iter, txn_t txn, int dbi, void *pfx, const unsigned int len);
int iter_next(iter_t iter);
int iter_next_key(iter_t iter);
void iter_close(iter_t iter);

#endif
