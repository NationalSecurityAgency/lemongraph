#ifndef _DB_H
#define _DB_H

#ifndef _BSD_SOURCE
#define _BSD_SOURCE
#endif

#include<stddef.h>
#include<stdarg.h>

#include"lmdb.h"

typedef struct db_t * db_t;
typedef struct txn_t * txn_t;
typedef struct cursor_t * cursor_t;
typedef struct iter_t * iter_t;
typedef struct db_snapshot_t * db_snapshot_t;

typedef const struct {
	const char *const name;
	const unsigned int flags;
} dbi_t;

char *db_strerror(int err);

db_t db_new(const char * const path, const int flags, const int mode, int mdb_flags, int ndbi, dbi_t *dbi, size_t padsize);
db_t db_init(const size_t size, const char * const path, const int flags, const int mode, int mdb_flags, int ndbi, dbi_t *dbi, size_t padsize);
int db_get(txn_t txn, int dbi, MDB_val *key, MDB_val *data);
int db_put(txn_t txn, int dbi, MDB_val *key, MDB_val *data, unsigned int flags);
int db_del(txn_t txn, int dbi, MDB_val *key, MDB_val *data);
int db_drop(txn_t txn, int dbi, int del);
void db_sync(db_t db, int force);
int db_updated(db_t db);
void db_close(db_t db);
size_t db_size(db_t db);

// will fail if this process has active txns/snapshots
// supplied mapsize must be a multiple of the OS pagesize
int db_set_mapsize(db_t db, size_t mapsize);

// these are always safe
size_t db_get_mapsize(db_t db);
size_t db_get_disksize(db_t db);

// snapshot foo
int db_snapshot_to_fd(db_t db, int fd, int compact);
db_snapshot_t db_snapshot_new(db_t db, int compact);
ssize_t db_snapshot_read(db_snapshot_t snap, void *buffer, size_t len);
int db_snapshot_close(db_snapshot_t snap);
int db_snapshot_fd(db_snapshot_t snap);

txn_t db_txn_new(db_t db, txn_t parent, int flags);
txn_t db_txn_init(const size_t size, db_t db, txn_t parent, int flags);
txn_t db_txn_rw(db_t db, txn_t parent);
txn_t db_txn_ro(db_t db);
int txn_updated(txn_t txn);
void txn_abort(txn_t txn);
int txn_commit(txn_t txn);

cursor_t txn_cursor_new(txn_t txn, int dbi);
cursor_t txn_cursor_init(const size_t size, txn_t txn, int dbi);

int cursor_get(cursor_t cursor, MDB_val *key, MDB_val *data, MDB_cursor_op op);
int cursor_put(cursor_t cursor, MDB_val *key, MDB_val *data, unsigned int flags);
int cursor_del(cursor_t cursor, unsigned int flags);
int cursor_count(cursor_t cursor, size_t *count);
int cursor_last_key(cursor_t cursor, MDB_val *key, uint8_t *pfx, const unsigned int pfxlen);
void cursor_close(cursor_t cursor);

iter_t txn_iter_new(txn_t txn, int dbi, void *pfx, const unsigned int len);
iter_t txn_iter_init(const size_t size, txn_t txn, int dbi, void *pfx, const unsigned int len);
int iter_next(iter_t iter);
int iter_next_key(iter_t iter);
void iter_close(iter_t iter);

#endif
