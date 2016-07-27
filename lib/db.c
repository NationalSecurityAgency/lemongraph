#ifndef _BSD_SOURCE
#define _BSD_SOURCE
#endif

// suppress assert() elimination, as we are currently heavily relying on it
#ifdef NDEBUG
#undef NDEBUG
#endif

#include<assert.h>

#include<errno.h>
#include<fcntl.h>
#include<inttypes.h>
#include<pthread.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<sys/stat.h>
#include<unistd.h>

#include"lmdb.h"
#include"db.h"
#include"db-private.h"

//#define debug(args...) do{ fprintf(stderr, "%d: ", __LINE__); fprintf(stderr, args); }while(0)
//#define debug(args...) while(0);

//#define FAIL(cond, err, val, label) if(cond) do{ fprintf(stderr, "%s:%d: %d:%s\n", __FILE__, __LINE__, val, mdb_strerror(val)); err=val; goto label; }while(0)
#define FAIL(cond, stash, val, label) if(cond) do{ stash=val; goto label; }while(0)

char *db_strerror(int err){
	return mdb_strerror(err);
}

int cursor_get(cursor_t cursor, MDB_val *key, MDB_val *data, MDB_cursor_op op){
	return cursor->prev ? mdb_cursor_get(cursor->cursor, key, data, op) : MDB_BAD_TXN;
}

int cursor_put(cursor_t cursor, MDB_val *key, MDB_val *data, unsigned int flags){
	int ret = cursor->prev ? mdb_cursor_put(cursor->cursor, key, data, flags) : MDB_BAD_TXN;
	if(MDB_SUCCESS == ret)
		cursor->txn->updated = 1;
	return ret;
}

int cursor_del(cursor_t cursor, unsigned int flags){
	int ret = cursor->prev ? mdb_cursor_del(cursor->cursor, flags) : MDB_BAD_TXN;
	if(MDB_SUCCESS == ret)
		cursor->txn->updated = 1;
	return ret;
}

int cursor_count(cursor_t cursor, size_t *count){
	return cursor->prev ? mdb_cursor_count(cursor->cursor, count) : MDB_BAD_TXN;
}

int cursor_last_key(cursor_t cursor, MDB_val *key, uint8_t *pfx, const unsigned int pfxlen){
	if(!cursor->prev)
		return MDB_BAD_TXN;

	if(!pfx || !pfxlen)
		return mdb_cursor_get(cursor->cursor, key, NULL, MDB_LAST);

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
	key->mv_size = pfxlen;
	key->mv_data = knext;
	r = mdb_cursor_get(cursor->cursor, key, NULL, MDB_SET_RANGE);
	if(MDB_SUCCESS == r){
		// and back up one
		r = mdb_cursor_get(cursor->cursor, key, NULL, MDB_PREV);
	}else if(MDB_NOTFOUND == r){
check_last:
		r = mdb_cursor_get(cursor->cursor, key, NULL, MDB_LAST);
	}

	if(MDB_SUCCESS == r)
		if(key->mv_size < pfxlen || memcmp(pfx, key->mv_data, pfxlen))
			r = MDB_NOTFOUND;
	return r;
}

int db_get(txn_t txn, int dbi, MDB_val *key, MDB_val *data){
	return mdb_get(txn->txn, txn->db->handles[dbi], key, data);
}

int db_put(txn_t txn, int dbi, MDB_val *key, MDB_val *data, unsigned int flags){
	int ret = mdb_put(txn->txn, txn->db->handles[dbi], key, data, flags);
	if(MDB_SUCCESS == ret)
		txn->updated = 1;
	return ret;
}

int db_del(txn_t txn, int dbi, MDB_val *key, MDB_val *data){
	int ret = mdb_del(txn->txn, txn->db->handles[dbi], key, data);
	if(MDB_SUCCESS == ret)
		txn->updated = 1;
	return ret;
}

cursor_t txn_cursor_new(txn_t txn, int dbi){
	return txn_cursor_init(0, txn, dbi);
}

cursor_t txn_cursor_init(const size_t size, txn_t txn, int dbi){
	cursor_t cursor = malloc(size < sizeof(*cursor) ? sizeof(*cursor) : size);
	assert(cursor);

	int r = mdb_cursor_open(txn->txn, txn->db->handles[dbi], &cursor->cursor);
	if(MDB_SUCCESS != r)
		fprintf(stderr, "%d: mdb_cursor_open(): %s (%d)\n", (int)getpid(), mdb_strerror(r), r);
	assert(MDB_SUCCESS == r);

	// db txn object acts as it's own cursor list head
	cursor->next = txn->head;
	if(cursor->next)
		cursor->next->prev = cursor;
	cursor->prev = (cursor_t) txn;
	txn->head = cursor;
	cursor->txn = txn;
	return cursor;
}

static inline void _cursor_cancel(cursor_t cursor){
	cursor_t next = cursor->next;
	if(next)
		next->prev = cursor->prev;
	// we always have a prev
	cursor->prev->next = next;

	mdb_cursor_close(cursor->cursor);
	cursor->cursor = NULL;

	// mark as cancelled
	cursor->prev = NULL;
}

void cursor_close(cursor_t cursor){
	if(cursor->prev)
		_cursor_cancel(cursor);
	free(cursor);
}

static inline int _txn_end(txn_t txn, int abort){
	while(txn->head)
		_cursor_cancel(txn->head);

	int r = MDB_SUCCESS;
	if(abort){
		mdb_txn_abort(txn->txn);
	}else{
		r = mdb_txn_commit(txn->txn);
	}

	pthread_mutex_lock(&txn->db->mutex);
	txn->db->updated |= txn->updated;
	if(1 == txn->db->txns--)
		pthread_cond_signal(&txn->db->cond);
	pthread_mutex_unlock(&txn->db->mutex);
	free(txn);
	return r;
}

int txn_updated(txn_t txn){
	return txn->updated;
}

void txn_abort(txn_t txn){
	_txn_end(txn, 1);
}

int txn_commit(txn_t txn){
	return _txn_end(txn, 0);
}

txn_t db_txn_new(db_t db, txn_t parent, int flags){
	return db_txn_init(0, db, parent, flags);
}

static inline int _txn_begin(db_t db, MDB_txn *parent, unsigned int flags, MDB_txn **txn);

txn_t db_txn_init(const size_t size, db_t db, txn_t parent, int flags){
	assert(db);
	txn_t txn = malloc(size < sizeof(*txn) ? sizeof(*txn) : size);
	txn->db = db;
	txn->head = NULL;
	txn->ro = (flags & MDB_RDONLY) ? 1 : 0;
	txn->rw = !txn->ro;
	txn->updated = 0;
	int r = _txn_begin(db, parent ? parent->txn : NULL, flags, &txn->txn);
	if(r != MDB_SUCCESS){
//		fprintf(stderr, "%d: txn_begin(): %s (%d)\n", (int)getpid(), mdb_strerror(r), r);
		free(txn);
		txn = NULL;
	}
	errno = r;
	return txn;
}

txn_t db_txn_rw(db_t db, txn_t parent){
	return db_txn_new(db, parent, 0);
}

txn_t db_txn_ro(db_t db){
	return db_txn_new(db, NULL, MDB_RDONLY);
}

void db_sync(db_t db, int force){
	int r = mdb_env_sync(db->env, force);
	// lmdb refuses to sync envs opened with mdb_readonly
	// I am not bothering with figuring out if fdatasync is broken on your platform
	if(EACCES == r)
		r = fdatasync(db->fd);
	if(MDB_SUCCESS != r)
		fprintf(stderr, "%d: mdb_env_sync(): %s (%d)\n", (int)getpid(), mdb_strerror(r), r);
	assert(MDB_SUCCESS == r);
}

int db_updated(db_t db){
	return db->updated;
}

void db_close(db_t db){
	if(db){
		if(db->env)
			mdb_env_close(db->env);
		if(db->handles)
			free(db->handles);
		pthread_mutex_destroy(&db->mutex);
		pthread_cond_destroy(&db->cond);
		free(db);
	}
}

size_t db_size(db_t db){
	struct stat st;
	int r = fstat(db->fd, &st);
	assert(0 == r);
	return st.st_size;
}

static inline size_t _env_newsize(db_t db, int flags){
	MDB_envinfo info;
	size_t ret = 0;
	int r;

	if(flags & MDB_RDONLY)
		return 0;

	size_t size = db_size(db);
	r = mdb_env_info(db->env, &info);
	assert(MDB_SUCCESS == r);

	if(size + db->padsize > info.me_mapsize)
		ret = ((size / (db->padsize)) + 2) * db->padsize;

	return ret;
}

static inline int _mdb_txn_begin(MDB_env *env, MDB_txn *parent, unsigned int flags, MDB_txn **txn){
	int r = mdb_txn_begin(env, parent, flags, txn);
	if(MDB_READERS_FULL == r){
		int r2, dead;
		r2 = mdb_reader_check(env, &dead);
		if(MDB_SUCCESS == r2)
			fprintf(stderr, "%d: mdb_reader_check released %d stale entries\n", (int)getpid(), dead);
		else
			fprintf(stderr, "%d: mdb_reader_check: %s (%d)\n", (int)getpid(), mdb_strerror(r2), r2);
		r = mdb_txn_begin(env, parent, flags, txn);
	}
	return r;
}

static inline int _txn_begin(db_t db, MDB_txn *parent, unsigned int flags, MDB_txn **txn){
	int r;

	// here's the deal - we need to grow the mapsize if:
	//  * we are a write transaction, and there is less than 1gb overhead
	//  * txn fails with MDB_MAP_RESIZED
	// However, mapsize may only be altered if there are no active txns, including
	// snapshots.
	// Additional
	pthread_mutex_lock(&db->mutex);
	size_t newsize = parent ? 0 : _env_newsize(db, flags);
	if(!newsize)
		r = _mdb_txn_begin(db->env, parent, flags, txn);
	if(!parent && (newsize || MDB_MAP_RESIZED == r)){
		while(db->txns)
			pthread_cond_wait(&db->cond, &db->mutex);
		do{
			mdb_env_set_mapsize(db->env, newsize);
			newsize = _env_newsize(db, flags);
			r = _mdb_txn_begin(db->env, parent, flags, txn);
		}while(newsize || MDB_MAP_RESIZED == r);
	}
	if(MDB_SUCCESS == r)
		db->txns++;
	else if(0 == db->txns)
		pthread_cond_signal(&db->cond);
	pthread_mutex_unlock(&db->mutex);

	return r;
}

db_t db_new(const char * const path, const int flags, const int mode, int mdb_flags, int ndbi, dbi_t *dbis, size_t padsize){
	return db_init(0, path, flags, mode, mdb_flags, ndbi, dbis, padsize);
}

db_t db_init(const size_t size, const char * const path, const int flags, const int mode, int mdb_flags, int ndbi, dbi_t *dbis, size_t padsize){
	int fd = -1, err = 0xdeadbeef;
	struct stat st;

	db_t db;
	db = malloc(size < sizeof(*db) ? sizeof(*db) : size);
	assert(db);

	// always do this
	mdb_flags |= MDB_NOSUBDIR;

	int r;
	// ignore MDB_RDONLY - key off of OS flags
	// see docs for open() - O_RDONLY is not a bit!
	if((flags & 0x3) == O_RDONLY){
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

	r = pthread_mutex_init(&db->mutex, NULL);
	FAIL(r, err, errno, fail);

	r = pthread_cond_init(&db->cond, NULL);
	FAIL(r, err, errno, fail);

	// unless MDB_RDONLY is specified, lmdb will automatically create non-existant databases,
	// which is not what I want. Try to emulate standard unix open() flags:
	fd = open(path, flags, mode);
	FAIL(-1 == fd, err, errno, fail);

	r = mdb_env_create(&db->env);
	FAIL(r, err, r, fail);

	r = mdb_env_set_maxdbs(db->env, ndbi);
	FAIL(r, err, r, fail);

	size_t mapsize;
	do{
		r = fstat(fd, &st);
		FAIL(r, err, errno, fail);

		if(!st.st_size &&( flags & O_CREAT))
			db->updated = 1;

		// pad out such that we have at least 1gb of map overhead
		mapsize = (1 + st.st_size / db->padsize) * db->padsize;

		r = mdb_env_set_mapsize(db->env, mapsize);
	}while(MDB_SUCCESS != r);
	close(fd);
	fd = -1;

	r = mdb_env_open(db->env, path, mdb_flags, mode);

	// mdb_env_open can return EAGAIN somehow, but I think it really means:
	if(EAGAIN == r)
		r = EMFILE;
	FAIL(r, err, r, fail);

	r = mdb_env_get_fd(db->env, &db->fd);
	FAIL(r, err, r, fail);

	if(dbis && ndbi){
		db->handles = malloc(sizeof(db->handles[0]) * ndbi);
		FAIL(!db->handles, err, errno, fail);

		// open the indexes - try read-only first
		unsigned int txn_flags = MDB_RDONLY;
		txn_t txn = db_txn_new(db, NULL, txn_flags);

		int i;
		for(i = 0; i < ndbi; i++){
			int dbflags = dbis[i].flags;
retry:
			dbflags = (txn_flags & MDB_RDONLY) ? (dbflags & ~MDB_CREATE) : (dbflags | MDB_CREATE);
			r = mdb_dbi_open(txn->txn, dbis[i].name, dbflags, &db->handles[i]);
			if(MDB_SUCCESS != r){
				if(MDB_NOTFOUND == r && (txn_flags & MDB_RDONLY)){
					// we were in read-only and a sub-db was missing
					// end txn
					txn_commit(txn);
					// switch to read-write
					txn_flags &= ~MDB_RDONLY;
					txn = db_txn_new(db, NULL, txn_flags);
					// and pick up where we left off
					goto retry;
				}else{
					txn_abort(txn);
				}
				FAIL(r, err, r, fail);
			}
		}
		r = txn_commit(txn);
		FAIL(r, err, r, fail);
	}

	return db;

fail:
	db_close(db);
	if(fd != -1){
		if((flags & (O_CREAT|O_EXCL)) == (O_CREAT|O_EXCL))
			unlink(path);
		close(fd);
	}
	errno = err;
	return NULL;
}

// will fail if this process has active txns/snapshots
// supplied mapsize must be a multiple of the OS pagesize
int db_set_mapsize(db_t db, size_t mapsize){
	int r = -1;
	pthread_mutex_lock(&db->mutex);
	if(0 == db->txns)
		r = mdb_env_set_mapsize(db->env, mapsize);
	pthread_mutex_unlock(&db->mutex);
	return r;
}

size_t db_get_mapsize(db_t db){
	MDB_envinfo info;
	int r = mdb_env_info(db->env, &info);
	assert(MDB_SUCCESS == r);
	return info.me_mapsize;
}

size_t db_get_disksize(db_t db){
	size_t ret;
	struct stat st;
	int r = fstat(db->fd, &st);
	assert(0 == r);
	ret = st.st_size;
	assert(ret == st.st_size);
	return ret;
}


/*
 * cursor-based iterators
 */

iter_t txn_iter_new(txn_t txn, int dbi, void *pfx, const unsigned int len){
	return txn_iter_init(0, txn, dbi, pfx, len);
}

iter_t txn_iter_init(const size_t size, txn_t txn, int dbi, void *pfx, const unsigned int len){
	iter_t iter = (iter_t) txn_cursor_init(size < sizeof(*iter) ? sizeof(*iter) : size, txn, dbi);
	iter->pfxlen = len;
	if(len){
		iter->pfx = malloc(len);
		assert(iter->pfx);
		memcpy(iter->pfx, pfx, len);
		iter->key.mv_data = iter->pfx;
		iter->key.mv_size = len;
		iter->op = MDB_SET_RANGE;
	}else{
		iter->pfx = NULL;
		iter->op = MDB_FIRST;
	}
	return iter;
}

static int _iter_next(iter_t iter, const int data){
	// set/advance key
	iter->r = cursor_get((cursor_t)iter, &(iter->key), &(iter->data), iter->op);
	if(MDB_NEXT != iter->op)
		iter->op = MDB_NEXT;
	if(MDB_SUCCESS != iter->r)
		return iter->r;

	// possibly check pfx on updated key
	if(iter->pfx && memcmp(iter->key.mv_data, iter->pfx, iter->pfxlen))
		return (iter->r = MDB_NOTFOUND);

	// maybe grab data too
	if(data)
		iter->r = cursor_get((cursor_t)iter, &(iter->key), &(iter->data), MDB_GET_CURRENT);
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

	r = mdb_env_copyfd2(db->env, fd, compact ? MDB_CP_COMPACT : 0);
	while(MDB_MAP_RESIZED == r){
		pthread_mutex_lock(&db->mutex);
		while(db->txns > 1)
			pthread_cond_wait(&db->cond, &db->mutex);
		mdb_env_set_mapsize(db->env, 0);
		pthread_mutex_unlock(&db->mutex);
		r = mdb_env_copyfd2(db->env, fd, compact ? MDB_CP_COMPACT : 0);
	}

	pthread_mutex_lock(&db->mutex);
	if(1 == db->txns--)
		pthread_cond_signal(&db->cond);
	pthread_mutex_unlock(&db->mutex);
	return r;
}

static void *__snapshot_thread(db_snapshot_t snap){
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
		}else if(red){
			if(EINTR == errno)
				continue;
			fprintf(stderr,"%d: read(): errno=%d: %s", (int)getpid(), errno, strerror(errno));
			assert(red);
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

