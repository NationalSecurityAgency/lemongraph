#ifndef _DB_PRIVATE_H
#define _DB_PRIVATE_H

// base database object
struct db_t{
	MDB_env *env;
	MDB_dbi *handles;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	size_t padsize;
	int fd, txns;
	int updated : 1;
};

struct txn_t {
	cursor_t head; // must be first
	db_t db;
	MDB_txn *txn;
	int ro : 1;
	int rw : 1;
	int updated : 1;
};

struct cursor_t {
	cursor_t next;  // must be first
	cursor_t prev;
	MDB_cursor *cursor;
	txn_t txn;
};

struct iter_t {
	struct cursor_t cursor;
	MDB_val key, data;
	void *pfx;
	unsigned int pfxlen;
	MDB_cursor_op op;
	int r;
};

#endif
