#ifndef _BSD_SOURCE
#define _BSD_SOURCE
#endif

#include<errno.h>
#include<fcntl.h>
#include<inttypes.h>
#include<pthread.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<sys/mman.h>
#include<sys/stat.h>
#include<unistd.h>

#include"lmdb.h"
#include"db.h"
#include"osal.h"

#include"static_assert.h"

#ifndef offsetof
#define offsetof(type, member) (size_t)(&((type *)NULL)->member - NULL)
#endif

#define sizeof_member(type, member) sizeof(((type *)NULL)->member)

#define INLINE __attribute__((always_inline)) inline

// attempt to verify that buffer_t & MDB_val structs match
STATIC_ASSERT(sizeof(buffer_t) == sizeof(MDB_val), "mismatched buffer_t & MDB_val objects");
STATIC_ASSERT(offsetof(buffer_t, size) == offsetof(MDB_val, mv_size), "mismatched buffer_t & MDB_val objects");
STATIC_ASSERT(offsetof(buffer_t, data) == offsetof(MDB_val, mv_data), "mismatched buffer_t & MDB_val objects");
STATIC_ASSERT(sizeof_member(buffer_t, size) == sizeof_member(MDB_val, mv_size), "mismatched buffer_t & MDB_val objects");
STATIC_ASSERT(sizeof_member(buffer_t, data) == sizeof_member(MDB_val, mv_data), "mismatched buffer_t & MDB_val objects");

// verify MDB_dbi matches
STATIC_ASSERT(sizeof(db_dbi) == sizeof(MDB_dbi), "mismatched MDB_dbi");

// duplicate LMDB's compare func typedef, so that things break noisily if it ever changes
typedef int (*_db_cmp_func)(const MDB_val *a, const MDB_val *b);

// check status codes
STATIC_ASSERT(DB_SUCCESS  == MDB_SUCCESS,  "mismatched MDB_SUCCESS");
STATIC_ASSERT(DB_NOTFOUND == MDB_NOTFOUND, "mismatched MDB_NOTFOUND");

// check env flags
STATIC_ASSERT(DB_FIXEDMAP   == MDB_FIXEDMAP,   "mismatched MDB_FIXEDMAP");
STATIC_ASSERT(DB_NOSYNC     == MDB_NOSYNC,     "mismatched MDB_NOSYNC");
STATIC_ASSERT(DB_RDONLY     == MDB_RDONLY,     "mismatched MDB_RDONLY");
STATIC_ASSERT(DB_NOMETASYNC == MDB_NOMETASYNC, "mismatched MDB_NOMETASYNC");
STATIC_ASSERT(DB_WRITEMAP   == MDB_WRITEMAP,   "mismatched MDB_WRITEMAP");
STATIC_ASSERT(DB_MAPASYNC   == MDB_MAPASYNC,   "mismatched MDB_MAPASYNC");
STATIC_ASSERT(DB_NOTLS      == MDB_NOTLS,      "mismatched MDB_NOTLS");
STATIC_ASSERT(DB_NOLOCK     == MDB_NOLOCK,     "mismatched MDB_NOLOCK");
STATIC_ASSERT(DB_NORDAHEAD  == MDB_NORDAHEAD,  "mismatched MDB_NORDAHEAD");
STATIC_ASSERT(DB_NOMEMINIT  == MDB_NOMEMINIT,  "mismatched MDB_NOMEMINIT");
STATIC_ASSERT(DB_PREVMETA   == MDB_PREVMETA,   "mismatched MDB_PREVMETA");

// check db flags
STATIC_ASSERT(DB_REVERSEKEY == MDB_REVERSEKEY, "mismatched MDB_REVERSEKEY");
STATIC_ASSERT(DB_DUPSORT    == MDB_DUPSORT,    "mismatched MDB_DUPSORT");
STATIC_ASSERT(DB_INTEGERKEY == MDB_INTEGERKEY, "mismatched MDB_INTEGERKEY");
STATIC_ASSERT(DB_DUPFIXED   == MDB_DUPFIXED,   "mismatched MDB_DUPFIXED");
STATIC_ASSERT(DB_INTEGERDUP == MDB_INTEGERDUP, "mismatched MDB_INTEGERDUP");
STATIC_ASSERT(DB_REVERSEDUP == MDB_REVERSEDUP, "mismatched MDB_REVERSEDUP");
STATIC_ASSERT(DB_CREATE     == MDB_CREATE,     "mismatched MDB_CREATE");

// check that db_cursor_op
STATIC_ASSERT((int) DB_FIRST          == (int) MDB_FIRST,          "mismatched MDB_FIRST");
STATIC_ASSERT((int) DB_FIRST_DUP      == (int) MDB_FIRST_DUP,      "mismatched MDB_FIRST_DUP");
STATIC_ASSERT((int) DB_GET_BOTH       == (int) MDB_GET_BOTH,       "mismatched MDB_GET_BOTH");
STATIC_ASSERT((int) DB_GET_BOTH_RANGE == (int) MDB_GET_BOTH_RANGE, "mismatched MDB_GET_BOTH_RANGE");
STATIC_ASSERT((int) DB_GET_CURRENT    == (int) MDB_GET_CURRENT,    "mismatched MDB_GET_CURRENT");
STATIC_ASSERT((int) DB_GET_MULTIPLE   == (int) MDB_GET_MULTIPLE,   "mismatched MDB_GET_MULTIPLE");
STATIC_ASSERT((int) DB_LAST           == (int) MDB_LAST,           "mismatched MDB_LAST");
STATIC_ASSERT((int) DB_LAST_DUP       == (int) MDB_LAST_DUP,       "mismatched MDB_LAST_DUP");
STATIC_ASSERT((int) DB_NEXT           == (int) MDB_NEXT,           "mismatched MDB_NEXT");
STATIC_ASSERT((int) DB_NEXT_DUP       == (int) MDB_NEXT_DUP,       "mismatched MDB_NEXT_DUP");
STATIC_ASSERT((int) DB_NEXT_MULTIPLE  == (int) MDB_NEXT_MULTIPLE,  "mismatched MDB_NEXT_MULTIPLE");
STATIC_ASSERT((int) DB_NEXT_NODUP     == (int) MDB_NEXT_NODUP,     "mismatched MDB_NEXT_NODUP");
STATIC_ASSERT((int) DB_PREV           == (int) MDB_PREV,           "mismatched MDB_PREV");
STATIC_ASSERT((int) DB_PREV_DUP       == (int) MDB_PREV_DUP,       "mismatched MDB_PREV_DUP");
STATIC_ASSERT((int) DB_PREV_NODUP     == (int) MDB_PREV_NODUP,     "mismatched MDB_PREV_NODUP");
STATIC_ASSERT((int) DB_SET            == (int) MDB_SET,            "mismatched MDB_SET");
STATIC_ASSERT((int) DB_SET_KEY        == (int) MDB_SET_KEY,        "mismatched MDB_SET_KEY");
STATIC_ASSERT((int) DB_SET_RANGE      == (int) MDB_SET_RANGE,      "mismatched MDB_SET_RANGE");
STATIC_ASSERT((int) DB_PREV_MULTIPLE  == (int) MDB_PREV_MULTIPLE,  "mismatched MDB_PREV_MULTIPLE");

//#define FAIL(cond, err, val, label) if(cond) do{ fprintf(stderr, "%s:%d: %d:%s\n", __FILE__, __LINE__, val, mdb_strerror(val)); err=val; goto label; }while(0)
#define FAIL(cond, stash, val, label) if(cond) do{ stash=val; goto label; }while(0)

// handy helpers
#define TXN_dbi(txn, dbi) (MDB_dbi)txn->db->handles[dbi]
#define TXN(txn) (MDB_txn *)txn->txn
#define TXNP(txn) (MDB_txn **)&txn->txn
#define CURSOR(cursor) (MDB_cursor *)cursor->cursor
#define CURSORP(cursor) (MDB_cursor **)&cursor->cursor

char *db_strerror(int err){
	return mdb_strerror(err);
}

int cursor_get(cursor_t cursor, buffer_t *key, buffer_t *data, db_cursor_op op){
	return cursor->prev ? mdb_cursor_get(CURSOR(cursor), (MDB_val *)key, (MDB_val *)data, (MDB_cursor_op)op) : MDB_BAD_TXN;
}

int cursor_put(cursor_t cursor, buffer_t *key, buffer_t *data, unsigned int flags){
	int ret = cursor->prev ? mdb_cursor_put(CURSOR(cursor), (MDB_val *)key, (MDB_val *)data, flags) : MDB_BAD_TXN;
	if(DB_SUCCESS == ret)
		cursor->txn->updated = 1;
	return ret;
}

int cursor_del(cursor_t cursor, unsigned int flags){
	int ret = cursor->prev ? mdb_cursor_del(CURSOR(cursor), flags) : MDB_BAD_TXN;
	if(DB_SUCCESS == ret)
		cursor->txn->updated = 1;
	return ret;
}

int cursor_count(cursor_t cursor, size_t *count){
	return cursor->prev ? mdb_cursor_count(CURSOR(cursor), count) : MDB_BAD_TXN;
}

int cursor_last_key(cursor_t cursor, buffer_t *key, uint8_t *pfx, const unsigned int pfxlen){
	if(!cursor->prev)
		return MDB_BAD_TXN;

	if(!pfx || !pfxlen)
		return mdb_cursor_get(CURSOR(cursor), (MDB_val *)key, NULL, MDB_LAST);

	// clone key
	uint8_t knext[pfxlen];
	memcpy(knext, pfx, pfxlen);
	int r;
	unsigned int i = pfxlen;
	while(i){
		// increase to very next prefix
		if(++knext[--i])
			goto increased;
	}
	// but if we wrapped all of the way around, examine last item
	goto check_last;

increased:
	// seek to increased key
	key->size = pfxlen;
	key->data = knext;
	r = mdb_cursor_get(CURSOR(cursor), (MDB_val *)key, NULL, MDB_SET_RANGE);
	if(DB_SUCCESS == r){
		// and back up one
		r = mdb_cursor_get(CURSOR(cursor), (MDB_val *)key, NULL, MDB_PREV);
	}else if(MDB_NOTFOUND == r){
check_last:
		r = mdb_cursor_get(CURSOR(cursor), (MDB_val *)key, NULL, MDB_LAST);
	}

	if(DB_SUCCESS == r)
		if(key->size < pfxlen || memcmp(pfx, key->data, pfxlen))
			r = MDB_NOTFOUND;
	return r;
}

int db_get(txn_t txn, int dbi, buffer_t *key, buffer_t *data){
	return mdb_get(TXN(txn), TXN_dbi(txn, dbi), (MDB_val *)key, (MDB_val *)data);
}

int db_put(txn_t txn, int dbi, buffer_t *key, buffer_t *data, unsigned int flags){
	int ret = mdb_put(TXN(txn), TXN_dbi(txn, dbi), (MDB_val *)key, (MDB_val *)data, flags);
	if(DB_SUCCESS == ret)
		txn->updated = 1;
	return ret;
}

int db_del(txn_t txn, int dbi, buffer_t *key, buffer_t *data){
	int ret = mdb_del(TXN(txn), TXN_dbi(txn, dbi), (MDB_val *)key, (MDB_val *)data);
	if(DB_SUCCESS == ret)
		txn->updated = 1;
	return ret;
}

int db_drop(txn_t txn, int dbi, int del){
	int ret = mdb_drop(TXN(txn), TXN_dbi(txn, dbi), del);
	if(DB_SUCCESS == ret)
		txn->updated = 1;
	return ret;
}

int txn_cursor_new(cursor_t *cursor, txn_t txn, int dbi){
	*cursor = malloc(sizeof(**cursor));
	int r = errno;
	if(*cursor){
		r = txn_cursor_init(*cursor, txn, dbi);
		if(DB_SUCCESS == r){
			(*cursor)->release = 1;
		}else{
			free(*cursor);
			*cursor = NULL;
		}
	}
	return r;
}

int txn_cursor_init(cursor_t cursor, txn_t txn, int dbi){
	int r = mdb_cursor_open(TXN(txn), TXN_dbi(txn, dbi), CURSORP(cursor));
	if(DB_SUCCESS == r){
		cursor->release = 0;
		// db txn object acts as it's own cursor list head
		cursor->next = txn->head;
		if(cursor->next)
			cursor->next->prev = cursor;
		cursor->prev = (cursor_t) txn;
		txn->head = cursor;
		cursor->txn = txn;
	}
	return r;
}

static INLINE void _cursor_cancel(cursor_t cursor){
	cursor_t next = cursor->next;
	if(next)
		next->prev = cursor->prev;
	// we always have a prev
	cursor->prev->next = next;

	mdb_cursor_close(CURSOR(cursor));
	cursor->cursor = NULL;

	// mark as cancelled
	cursor->prev = NULL;
}

void cursor_close(cursor_t cursor){
	if(cursor->prev)
		_cursor_cancel(cursor);
	if(cursor->release)
		free(cursor);
}

int txn_end(txn_t txn, int flags){
	int r = DB_SUCCESS;
	while(txn->head)
		_cursor_cancel(txn->head);

	if(flags & DB_TXN_ABORT){
		mdb_txn_abort(TXN(txn));
	}else{
		r = mdb_txn_commit(TXN(txn));
		if(txn->updated && DB_SUCCESS == r){
			if(txn->parent)
				txn->parent->updated = 1;
			else
				txn->db->updated = 1;
		}
	}

	pthread_mutex_lock(&txn->db->mutex);
	if(1 == txn->db->txns--)
		pthread_cond_signal(&txn->db->cond);
	pthread_mutex_unlock(&txn->db->mutex);

	((volatile txn_t)txn)->txn = NULL;

	if(txn->release)
		free(txn);

	return r;
}

int txn_updated(txn_t txn){
	return txn->updated;
}

void txn_abort(txn_t txn){
	txn_end(txn, DB_TXN_ABORT);
}

int txn_commit(txn_t txn){
	return txn_end(txn, 0);
}

int db_txn_new(txn_t *txn, db_t db, txn_t parent, int flags){
	*txn = malloc(sizeof(**txn));
	int r = errno;
	if(*txn){
		r = db_txn_init(*txn, db, parent, flags);
		if(DB_SUCCESS == r){
			(*txn)->release = 1;
		}else{
			free(*txn);
			*txn = NULL;
		}
	}
	return r;
}

static INLINE int _txn_begin(db_t db, MDB_txn *parent, unsigned int flags, MDB_txn **txn);

int db_txn_init(txn_t txn, db_t db, txn_t parent, int flags){
	txn->db = db;
	txn->parent = parent;
	txn->head = NULL;
	txn->ro = (flags & MDB_RDONLY) ? 1 : 0;
	txn->rw = !txn->ro;
	txn->updated = 0;
	txn->release = 0;
	return _txn_begin(db, parent ? parent->txn : NULL, flags, TXNP(txn));
}

int db_sync(db_t db, int force){
	int r = mdb_env_sync((MDB_env *)db->env, force);
	// lmdb refuses to sync envs opened with mdb_readonly
	// I've begun bothering with figuring out cross-platform fdatasync
	if(EACCES == r)
		r = osal_fdatasync(db->fd);
	return r;
}

int db_updated(db_t db){
	return db->updated;
}

int db_size(db_t db, size_t *size){
	struct stat st;
	int r = fstat(db->fd, &st);
	*size = st.st_size;
	return r;
}

static INLINE int _mdb_txn_begin(MDB_env *env, MDB_txn *parent, unsigned int flags, MDB_txn **txn){
	int r = mdb_txn_begin(env, parent, flags, txn);
	if(MDB_READERS_FULL == r){
		int r2, dead;
		r2 = mdb_reader_check(env, &dead);
		if(DB_SUCCESS == r2)
			fprintf(stderr, "%d: mdb_reader_check released %d stale entries\n", (int)getpid(), dead);
		else
			fprintf(stderr, "%d: mdb_reader_check: %s (%d)\n", (int)getpid(), mdb_strerror(r2), r2);
		r = mdb_txn_begin(env, parent, flags, txn);
	}
	return r;
}

static INLINE int _do_resize(db_t db, size_t size){
	while(db->txns)
		pthread_cond_wait(&db->cond, &db->mutex);
	return mdb_env_set_mapsize((MDB_env *)db->env, size);
}

static INLINE int _auto_resize(db_t db){
	MDB_envinfo info;
	size_t size;

	int r = db_size(db, &size);
	if(DB_SUCCESS != r)
		goto done;

	r = mdb_env_info((MDB_env *)db->env, &info);
	if(DB_SUCCESS != r)
		goto done;

	if(size + db->padsize > info.me_mapsize){
		size = ((size / (db->padsize)) + 2) * db->padsize;
		r = _do_resize(db, size);
	}
done:
	return r;
}

static INLINE int _txn_begin(db_t db, MDB_txn *parent, unsigned int flags, MDB_txn **txn){
	int r;

	// Here's the deal - we need to grow the mapsize if:
	//  * we are a write transaction, and there is less than 1gb overhead
	//  * txn fails with MDB_MAP_RESIZED
	// However, mapsize may only be altered if there are no active txns in this process.
	pthread_mutex_lock(&db->mutex);
	if(flags & MDB_RDONLY){
		// don't care about overhead for readonly txns
		r = _mdb_txn_begin((MDB_env *)db->env, parent, flags, txn);
		while(MDB_MAP_RESIZED == r){
			r = _do_resize(db, 0);
			if(DB_SUCCESS == r)
				r = _mdb_txn_begin((MDB_env *)db->env, parent, flags, txn);
		}
	}else if(parent){
		// cannot resize map for nested txns
		r = _mdb_txn_begin((MDB_env *)db->env, parent, flags, txn);
	}else{
		// ensure we have room for growth prior to opening write txns
		do{
			r = _auto_resize(db);
			if(DB_SUCCESS == r)
				r = _mdb_txn_begin((MDB_env *)db->env, parent, flags, txn);
		}while(MDB_MAP_RESIZED == r);
	}
	if(DB_SUCCESS == r)
		db->txns++;
	else if(0 == db->txns)
		pthread_cond_signal(&db->cond);
	pthread_mutex_unlock(&db->mutex);

	return r;
}

int db_remap(db_t db){
	MDB_envinfo info;
	pthread_mutex_lock(&db->mutex);
	while(db->txns)
		pthread_cond_wait(&db->cond, &db->mutex);
	int r = mdb_env_info((MDB_env *)db->env, &info);
	if(DB_SUCCESS == r)
		r = mdb_env_set_mapsize((MDB_env *)db->env, info.me_mapsize);
	pthread_mutex_unlock(&db->mutex);
	return r;
}

int db_new(db_t *db, const char * const path, const int flags, const int mode, int mdb_flags, int ndbi, dbi_t *dbis, size_t padsize){
	*db = malloc(sizeof(**db));
	int r = errno;
	if(*db){
		r = db_init(*db, path, flags, mode, mdb_flags, ndbi, dbis, padsize);
		if(DB_SUCCESS == r){
			(*db)->release = 1;
		}else{
			free(*db);
			*db = NULL;
		}
	}
	return r;
}

int db_init(db_t db, const char * const path, const int flags, const int mode, int mdb_flags, int ndbi, dbi_t *dbis, size_t padsize){
	int init_status = 0;
	int fd = -1, err = 0xdeadbeef;
	struct stat st;

	// always do this
	mdb_flags |= MDB_NOSUBDIR;

	int r;
	// ignore MDB_RDONLY - key off of OS flags
	// see docs for open() - O_RDONLY is not a bit!
	if((flags & (O_RDONLY|O_WRONLY|O_RDWR)) == O_RDONLY){
		mdb_flags |= MDB_RDONLY;
		// disable NOSYNC/NOMETASYNC for readonly, as that burns another file descriptor
		mdb_flags &= ~(MDB_NOSYNC|MDB_NOMETASYNC);
	}else{
		mdb_flags &= ~MDB_RDONLY;
	}


	// default to 100mb minimum pad
	db->padsize = padsize ? padsize : (1<<27);

	db->env = NULL;
	db->txns = 0;
	db->handles = NULL;
	db->updated = 0;
	db->release = 0;

	r = pthread_mutex_init(&db->mutex, NULL);
	FAIL(r, err, errno, fail);
	init_status++;

	r = pthread_cond_init(&db->cond, NULL);
	FAIL(r, err, errno, fail);
	init_status++;

	// unless MDB_RDONLY is specified, lmdb will automatically create non-existant databases,
	// which is not what I want. Try to emulate standard unix open() flags:
	fd = open(path, flags, mode);
	FAIL(-1 == fd, err, errno, fail);

	r = mdb_env_create((MDB_env **)&db->env);
	FAIL(r, err, r, fail);

	r = mdb_env_set_maxdbs((MDB_env *)db->env, ndbi);
	FAIL(r, err, r, fail);

	size_t mapsize;
	do{
		r = fstat(fd, &st);
		FAIL(r, err, errno, fail);

		if(!st.st_size &&( flags & O_CREAT))
			db->updated = 1;

		// pad out such that we have at least 1gb of map overhead
		mapsize = (1 + st.st_size / db->padsize) * db->padsize;

		r = mdb_env_set_mapsize((MDB_env *)db->env, mapsize);
	}while(DB_SUCCESS != r);
	close(fd);
	fd = -1;

	r = mdb_env_open((MDB_env *)db->env, path, mdb_flags, mode);

	// mdb_env_open can return EAGAIN somehow, but I think it really means:
	if(EAGAIN == r)
		r = EMFILE;
	FAIL(r, err, r, fail);

	r = mdb_env_get_fd((MDB_env *)db->env, &db->fd);
	FAIL(r, err, r, fail);
	init_status++;

	if(dbis && ndbi){
		struct txn_t _txn;
		txn_t txn = &_txn;

		db->handles = malloc(sizeof(db->handles[0]) * ndbi);
		FAIL(!db->handles, err, errno, fail);
		init_status++;

		// open the indexes - try read-only first
		unsigned int txn_flags = MDB_RDONLY;
		r = db_txn_init(txn, db, NULL, txn_flags);
		FAIL(r, err, r, fail);

		int i;
		for(i = 0; i < ndbi; i++){
			int dbflags = dbis[i].flags;
retry:
			dbflags = (txn_flags & MDB_RDONLY) ? (dbflags & ~MDB_CREATE) : (dbflags | MDB_CREATE);
			r = mdb_dbi_open(TXN(txn), dbis[i].name, dbflags, (MDB_dbi *)&db->handles[i]);
			if(DB_SUCCESS != r){
				if(MDB_NOTFOUND == r && (txn_flags & MDB_RDONLY)){
					// we were in read-only and a sub-db was missing
					// end txn
					txn_commit(txn);
					// switch to read-write
					txn_flags &= ~MDB_RDONLY;
					r = db_txn_init(txn, db, NULL, txn_flags);
					FAIL(r, err, r, fail);

					// and pick up where we left off
					goto retry;
				}else{
					txn_abort(txn);
				}
				FAIL(r, err, r, fail);
			}
			if(dbis[i].cmp){
				r = mdb_set_compare(TXN(txn), db->handles[i], (_db_cmp_func)dbis[i].cmp);
				FAIL(r, err, r, fail);
			}
		}
		r = txn_commit(txn);
		FAIL(r, err, r, fail);
	}

	return DB_SUCCESS;

fail:
	switch(init_status){
		case 4:  free(db->handles);
		case 3:  mdb_env_close((MDB_env *)db->env);
		case 2:  pthread_cond_destroy(&db->cond);
		case 1:  pthread_mutex_destroy(&db->mutex);
		default: break;
	}
	if(fd != -1){
		if((flags & (O_CREAT|O_EXCL)) == (O_CREAT|O_EXCL))
			unlink(path);
		close(fd);
	}
	errno = err;
	return err;
}

void db_close(db_t db){
	free(db->handles);
	mdb_env_close((MDB_env *)db->env);
	((volatile db_t) db)->env = NULL;
	pthread_cond_destroy(&db->cond);
	pthread_mutex_destroy(&db->mutex);
	if(db->release)
		free(db);
}

// will fail if this process has active txns/snapshots
// supplied mapsize must be a multiple of the OS pagesize
int db_set_mapsize(db_t db, size_t mapsize){
	int r = -1;
	pthread_mutex_lock(&db->mutex);
	if(0 == db->txns)
		r = mdb_env_set_mapsize((MDB_env *)db->env, mapsize);
	pthread_mutex_unlock(&db->mutex);
	return r;
}

int db_get_mapsize(db_t db, size_t *size){
	MDB_envinfo info;
	int r = mdb_env_info((MDB_env *)db->env, &info);
	*size = info.me_mapsize;
	return r;
}

int db_get_disksize(db_t db, size_t *size){
	struct stat st;
	int r = fstat(db->fd, &st);
	*size = st.st_size;
	return r;
}


/*
 * cursor-based iterators
 */

int txn_iter_new(iter_t *iter, txn_t txn, int dbi, void *pfx, const unsigned int len){
	*iter = malloc(sizeof(**iter));
	int r = errno;
	if(*iter){
		r = txn_iter_init(*iter, txn, dbi, pfx, len);
		if(DB_SUCCESS == r){
			(*iter)->release = 1;
		}else{
			free(*iter);
			*iter = NULL;
		}
	}
	return r;
}

int txn_iter_init(iter_t iter, txn_t txn, int dbi, void *pfx, const unsigned int len){
	int r = txn_cursor_init((cursor_t)iter, txn, dbi);
	if(DB_SUCCESS == r){
		iter->pfxlen = len;
		iter->release = 0;
		if(len){
			iter->pfx = malloc(len);
			if(!iter->pfx)
				return errno;

			memcpy(iter->pfx, pfx, len);
			iter->key.data = iter->pfx;
			iter->key.size = len;
			iter->op = DB_SET_RANGE;
		}else{
			iter->pfx = NULL;
			iter->op = DB_FIRST;
		}
	}
	return r;
}

static INLINE int _iter_next(iter_t iter, const int data){
	// set/advance key
	iter->r = cursor_get((cursor_t)iter, &(iter->key), &(iter->data), iter->op);
	if(DB_NEXT != iter->op)
		iter->op = DB_NEXT;
	if(DB_SUCCESS != iter->r)
		return iter->r;

	// possibly check pfx on updated key
	if(iter->pfx && memcmp(iter->key.data, iter->pfx, iter->pfxlen))
		return (iter->r = MDB_NOTFOUND);

	// maybe grab data too
	if(data)
		iter->r = cursor_get((cursor_t)iter, &(iter->key), &(iter->data), DB_GET_CURRENT);
	return iter->r;
}

int iter_next(iter_t iter){
	return _iter_next(iter, 1);
}

int iter_next_key(iter_t iter){
	return _iter_next(iter, 0);
}

void iter_close(iter_t iter){
	if(iter->pfx)
		free(iter->pfx);
	cursor_close((cursor_t)iter);
	if(iter->release)
		free(iter);
}



struct db_snapshot_t{
	db_t db;
	int compact, ret, fds[2];
	pthread_t thread;
};

int db_snapshot_to_fd(db_t db, int fd, int compact){
	int r;
	pthread_mutex_lock(&db->mutex);
	db->txns++;
	pthread_mutex_unlock(&db->mutex);

	r = mdb_env_copyfd2((MDB_env *)db->env, fd, compact ? MDB_CP_COMPACT : 0);
	while(MDB_MAP_RESIZED == r){
		pthread_mutex_lock(&db->mutex);
		while(db->txns > 1)
			pthread_cond_wait(&db->cond, &db->mutex);
		mdb_env_set_mapsize((MDB_env *)db->env, 0);
		pthread_mutex_unlock(&db->mutex);
		r = mdb_env_copyfd2((MDB_env *)db->env, fd, compact ? MDB_CP_COMPACT : 0);
	}

	pthread_mutex_lock(&db->mutex);
	if(1 == db->txns--)
		pthread_cond_signal(&db->cond);
	pthread_mutex_unlock(&db->mutex);
	return r;
}

static INLINE void *__snapshot_thread(db_snapshot_t snap){
	snap->ret = db_snapshot_to_fd(snap->db, snap->fds[1], snap->compact);
	close(snap->fds[1]);
	return NULL;
}

static void *_snapshot_thread(void *snap){
	return __snapshot_thread((db_snapshot_t)snap);
}

ssize_t db_snapshot_read(db_snapshot_t snap, void *buffer, size_t len){
	ssize_t red, total = 0;
	while(len){
		red = read(snap->fds[0], buffer, len);
		if(red > 0){
			total += red;
			len -= red;
			buffer += red;
		}else if(red && EINTR == errno){
			continue;
		}else{
			break;
		}
	}
	return total;
}

int db_snapshot_close(db_snapshot_t snap){
	int r;
	close(snap->fds[0]);
	r = pthread_join(snap->thread, NULL);
	if(!r)
		r = snap->ret;
	free(snap);
	return r;
}

int db_snapshot_fd(db_snapshot_t snap){
	return snap->fds[0];
}

db_snapshot_t db_snapshot_new(db_t db, int compact){
	db_snapshot_t snap = malloc(sizeof(*snap));
	if(!snap)
		goto err_c;

	if(pipe(snap->fds))
		goto err_b;

	// spawn thread
	snap->db = db;
	snap->compact = compact;
	if(pthread_create(&snap->thread, NULL, _snapshot_thread, (void *)snap))
		goto err_a;

	return snap;

err_a:
	close(snap->fds[0]);
	close(snap->fds[1]);
err_b:
	free(snap);
err_c:
	return NULL;
}

