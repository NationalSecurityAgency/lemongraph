from collections import deque
import datetime
import errno
import logging
import os
import re
import resource
import signal
from six import iteritems, iterkeys
import sys
from time import sleep, time
import uuid


from lazy import lazy
from pysigset import suspended_signals

from . import Graph, Serializer, Hooks, dirlist, Indexer, Query

try:
    xrange          # Python 2
except NameError:
    xrange = range  # Python 3

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

def uuidgen():
    return str(uuid.uuid1())

def uuid_to_utc_ts(u):
    return (uuid.UUID('{%s}' % u).time - 0x01b21dd213814000) // 1e7

def uuid_to_utc(u):
    return datetime.datetime.utcfromtimestamp(uuid_to_utc_ts(u)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

class CollectionHooks(Hooks):
    def __init__(self, uuid, collection):
        self.uuid = uuid
        self.collection = collection

    def opened(self, g):
        self.collection.sync(self.uuid, g)

    def updated(self, g, nextID, updates):
        self.collection.sync_qflush(self.uuid, g)

    def deleted(self):
        self.collection.remove(self.uuid)


class StatusIndexer(Indexer):
    def idx_user_roles(self, obj):
        try:
            user_roles = obj['roles']
        except KeyError:
            return
        for user, roles in iteritems(user_roles):
            for role in roles:
                yield '%s\0%s' % (user, role)

    def idx_users(self, obj):
        try:
            return iterkeys(obj['roles'])
        except (KeyError, AttributeError):
            return ()

class StatusIndex(object):
    domain = 'status'
    null = Serializer.null()

    def __init__(self, ctx):
        self.ctx = ctx
        self.indexer = StatusIndexer()
        self._indexes = {}

    def update(self, uuid, old, new):
        oldkeys = self.indexer.index(old)
        newkeys = self.indexer.index(new)
        uuid = uuid.encode()
        for name, crc in oldkeys.difference(newkeys):
            keys = self._index(name)
            try:
                keys.remove(crc + uuid)
            except KeyError:
                pass
        for name, crc in newkeys.difference(oldkeys):
            keys = self._index(name)
            keys.add(crc + uuid)

    def _index(self, idx):
        try:
            return self._indexes[idx]
        except KeyError:
            self._indexes[idx] = self.ctx.txn.sset('lg.collection.idx.%s.%s' % (self.domain, idx), serialize_value=self.null)
        return self._indexes[idx]

    def search(self, idx, value):
        idx, crc, check = self.indexer.prequery(idx, value)
        crclen = len(crc)
        try:
            index = self._index(idx)
        except KeyError:
            return
        for key in index.iterpfx(pfx=crc):
            uuid = key[crclen:].decode()
            status = self.ctx.statusDB[uuid]
            if check(status):
                yield uuid, status

class Context(object):
    def __init__(self, collection, write=True):
        self.db = collection.db
        self._graph = collection.graph
        self.msgpack = collection.msgpack
        self.uint = collection.uint
        self.user_roles = collection.user_roles
        self.write = write

    def __enter__(self):
        self.txn = self.db.transaction(write=self.write)
        self.txn.__enter__()
        return self

    def __exit__(self, type, value, traceback):
        ret = self.txn.__exit__(type, value, traceback)
        self.txn = None
        return ret

    def _graphs(self, user, roles):
        if roles:
            seen = set()
            for role in roles:
                for uuid, status in self.status_index.search('user_roles', '%s\0%s' % (user, role)):
                    if uuid not in seen:
                        seen.add(uuid)
                        yield uuid, status
        elif user:
            for uuid, status in self.status_index.search('users', user):
                yield uuid, status
        else:
            try:
                all = iteritems(self.statusDB)
            except KeyError:
                return
            for uuid, status in all:
                yield uuid, status

    def _filter_objs(self, gen, filters):
        filters = map(lambda pat: 'n(%s)' % pat, filters)
        qf = Query(filters)
        for output in gen:
            vgen = qf.validate((output,))
            for f, _ in vgen:
                yield output
                vgen.close()

    def graphs(self, enabled=None, user=None, roles=None, created_before=None, created_after=None, filters=None):
        gen = self._graphs(user, None if user is None else roles)
        if created_before is not None:
            gen = ((uuid, status) for uuid, status in gen if uuid_to_utc_ts(uuid) < created_before)
        if created_after is not None:
            gen = ((uuid, status) for uuid, status in gen if uuid_to_utc_ts(uuid) > created_after)
        if enabled is not None:
            gen = ((uuid, status) for uuid, status in gen if status['enabled'] is enabled)
        gen = (self._status_enrich(status, uuid) for uuid, status in gen)
        if filters:
            gen = self._filter_objs(gen, filters)
        return gen

    def graph(self, *args, **kwargs):
        kwargs['ctx'] = self
        return self._graph(*args, **kwargs)

    def _status_enrich(self, status, uuid):
        output = { 'graph': uuid, 'id': uuid }
        try:
            output['meta'] = self.metaDB[uuid]
        except KeyError:
            output['meta'] = {}
        for field in ('size', 'nodes_count', 'edges_count'):
            output[field] = status[field]
        output['maxID'] = status['nextID'] - 1
        output['created'] = uuid_to_utc(uuid)
        return output

    def status(self, uuid):
        try:
            return self._status_enrich(self.statusDB[uuid], uuid)
        except KeyError:
            pass

    def sync(self, uuid, txn):
        old_status, new_status = self._sync_status(uuid, txn)
        self.status_index.update(uuid, old_status, new_status)
        self.metaDB[uuid] = txn.as_dict()

    @lazy
    def status_index(self):
        return StatusIndex(self)

    def _sync_status(self, uuid, txn):
        status = {'nextID': txn.nextID,
                  'size': txn.graph.size,
                  'nodes_count': txn.nodes_count(),
                  'edges_count': txn.edges_count()}

        try:
            status['enabled'] = bool(txn['enabled'])
        except KeyError:
            status['enabled'] = True

        try:
            status['priority'] = sorted((0, int(txn['priority']), 255))[1]
        except (KeyError, ValueError):
            status['priority'] = 100

        try:
            roles_graph = txn['roles']
            if isinstance(roles_graph, dict):
                status['roles'] = cache = {}
                for user in roles_graph:
                    user_roles = self.user_roles(txn, user)
                    if user_roles:
                        cache[user] = sorted(user_roles)
        except: # fixme
            pass

        try:
            old_status = self.statusDB[uuid]
        except KeyError:
            old_status = None
        self.statusDB[uuid] = status

        return old_status, status

    def remove(self, uuid):
        try:
            status = self.statusDB.pop(uuid)
            self.status_index.update(uuid, status, None)
        except KeyError:
            pass
        try:
            del self.metaDB[uuid]
        except KeyError:
            pass

    @lazy
    def updatedDB(self):
        return self.txn.fifo('lg.collection.updated')

    @lazy
    def updatedDB_idx(self):
        return self.txn.kv('lg.collection.updated_idx', serialize_value=self.msgpack)

    @lazy
    def statusDB(self):
        return self.txn.kv('lg.collection.status', serialize_value=self.msgpack)

    @lazy
    def metaDB(self):
        return self.txn.kv('lg.collection.meta', serialize_value=self.msgpack)


class Collection(object):
    # increment on index structure changes
    VERSION = 2

    def __init__(self, dir, graph_opts=None, create=True, rebuild=False, **kwargs):
        self.db = None
        if create:
            try:
                os.mkdir(dir)
            except OSError as e:
                if e.errno != errno.EEXIST or not os.path.isdir(dir):
                    raise
        self.dir = os.path.abspath(dir)
        idx = "%s.idx" % self.dir
        self.graph_opts = {} if graph_opts is None else graph_opts
        kwargs['serialize_property_value'] = self.msgpack
        kwargs['create'] = create
        self.db = Graph(idx, **kwargs)
        if not rebuild:
            with self.context(write=False) as ctx:
                found = ctx.txn.get('version', 0)
                if found == self.VERSION:
                    return
                log.info("upgrading index version: %d => %d", found, self.VERSION)

        log.info("rebuilding collection index ...")
        with self.context(write=True) as ctx:
            ctx.txn.reset()

        uuids = []
        count = 0
        for u in self._fs_dbs():
            uuids.append(u)
            count += 1
            if count % 1000:
                continue

            self._sync_uuids(uuids)
            log.debug("updated: %d", count)
            uuids = []

        if uuids:
            self._sync_uuids(uuids)
            log.debug("updated: %d", count)

        with self.context(write=True) as ctx:
            ctx.txn['version'] = self.VERSION
        log.info("indexed %d graphs", count)

    def _sync_uuids(self, uuids):
        with self.context(write=True) as ctx:
            for uuid in uuids:
                try:
#                    with ctx.graph(uuid, readonly=True, create=False, hook=False) as g:
                    with ctx.graph(uuid, create=False, hook=False) as g:
                        with g.transaction(write=False) as txn:
                            ctx.sync(uuid, txn)
                except IOError as e:
                    log.warning('error syncing graph %s: %s', uuid, str(e))

        self.db.sync(force=True)

    def _fs_dbs(self):
        UUID = re.compile(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$')
        for x in dirlist(self.dir):
            if len(x) != 39 or x[-3:] != '.db':
                continue
            uuid = x[0:36]
            if UUID.match(uuid):
                yield uuid

    def sync(self, uuid, g):
        with self.context(write=True) as ctx:
            with g.transaction(write=False) as txn:
                ctx.sync(uuid, txn)

    def sync_qflush(self, uuid, g):
        with self.context(write=True) as ctx:
            with g.transaction(write=False) as txn:
                try:
                    ctx.sync(uuid, txn)
                finally:
                    try:
                        if uuid in ctx.updatedDB_idx:
                            return
                    except KeyError:
                        pass
                    ctx.updatedDB.push(uuid)
                    ctx.updatedDB_idx[uuid] = time()

    def remove(self, uuid):
        with self.context(write=True) as ctx:
            ctx.remove(uuid)

    def drop(self, uuid):
        path = self.graph_path(uuid)
        for x in (path, '%s-lock' % path):
            try:
                os.unlink(x)
            except:
                pass
        self.remove(uuid)

    def __del__(self):
        self.close()

    def close(self):
        if self.db is not None:
            self.db.close()
            self.db = None

    # opens a graph
    def graph(self, uuid=None, hook=True, ctx=None, user=None, roles=None, **kwargs):
        if uuid is None and kwargs.get('create', False):
            uuid = uuidgen()
        for k,v in iteritems(self.graph_opts):
            if k not in kwargs:
                kwargs[k] = v
        if hook:
            kwargs['hooks'] = CollectionHooks(uuid, self)
        try:
            g = Graph(self.graph_path(uuid), **kwargs)
        except:
            self.remove(uuid) if ctx is None else ctx.remove(uuid)
            raise
        if user is not None:
            # new graph - do not check creds
            if g.updated:
                pass
            else:
                if not self.user_allowed(g, user, roles):
                    g.close()
                    raise OSError(errno.EPERM, 'Permission denied', uuid)
        return g

    def user_allowed(self, g, user, roles):
        with g.transaction(write=False) as txn:
            user_roles = self.user_roles(txn, user)
            return bool(user_roles if roles is None else user_roles.intersection(roles))

    def user_roles(self, txn, user):
        try:
            user_roles = txn['roles'][user]
        except KeyError:
            return frozenset()
        if isinstance(user_roles, dict):
            user_roles = frozenset(role for role, val in iteritems(user_roles) if val)
        else:
            user_roles = frozenset(role for role in self._words.findall(str(user_roles)))
        return user_roles

    @lazy
    def _words(self):
        return re.compile('\w+')

    def status(self, uuid):
        with self.context(write=False) as ctx:
            return ctx.status(uuid)

    def graphs(self, enabled=None):
        with self.context(write=False) as ctx:
            for x in ctx.graphs(enabled=enabled):
                yield x

    def graph_path(self, uuid):
        return "%s%s%s.db" % (self.dir, os.path.sep, uuid)

    def context(self, write=True):
        return Context(self, write=write)

    def daemon(self, poll=250, maxopen=1000):
        poll /= 1000.0
        ticker = self.ticker()
        todo = deque()

        # count fds in use - just check first 100 or so
        pad = 0
        for fd in xrange(0, 100):
            try:
                os.fstat(fd)
                pad += 1
            except:
                pass

        # check limits
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        if maxopen + pad > soft:
            soft = min(maxopen + pad, hard)
            resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))
            maxopen = soft - pad

        map_age = 0
        log.info('using %d max open graphs' % maxopen)
        while True:
            next(ticker)
            sleep(poll)
            map_age += poll
            if map_age >= 60:
                self.db.remap()
                map_age = 0
            self.db.sync(force=True)
            with self.context(write=False) as ctx:
                try:
                    if ctx.updatedDB.empty:
                        continue
                except KeyError:
                    continue

            # Note - we assume user is not using DB_WRITEMAP which is reasonable because:
            #   LemonGraph explicitly disables that
            # Otherwise, we might have to mmap the whole region and msync it - maybe?
            # Opening it via LemonGraph adds overhead, burns double the file descriptors, and
            # currently explodes if I try to set RLIMIT_NOFILE > 2050. I know not why.
            # fdatasync() is used for Linux and assumed good v >= 3.6. The more portable fsync() used for Mac & Win should work, it's effectively fdatasync + additional guarantee to update the file's modification time - hence slight perf hit. But, testing wouldn't hurt.
            count = 0
            backlog = True
            while backlog:
                with suspended_signals(signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
                    try:
                        log.debug("syncing")
                        with self.context(write=True) as ctx:
                            uuids = ctx.updatedDB.pop(n=maxopen)
                            for uuid in uuids:
                                age = ctx.updatedDB_idx.pop(uuid)
                                try:
                                    fd = os.open(self.graph_path(uuid), os.O_RDONLY)
                                    todo.append(fd)
                                except OSError as e:
                                    # may have been legitimately deleted already
                                    if e.errno != errno.ENOENT:
                                        log.warning('error syncing graph %s: %s', uuid, str(e))
                            count += len(uuids)
                            backlog = len(ctx.updatedDB)
                        for fd in todo:
                            lib.osal_fdatasync(fd)
                    finally:
                        for fd in todo:
                            os.close(fd)
                        todo.clear()
                log.info("synced %d, backlog %d, age %.1fs", count, backlog, time() - age)

    @lazy
    def msgpack(self):
        return Serializer.msgpack()

    @lazy
    def uint(self):
        return Serializer.uint()

    def ticker(self, ticker='/-\\|', fh=sys.stderr):
        if not fh.isatty():
            while True:
                yield

        strings = tuple("\r%s\r" % x for x in ticker)
        while True:
            for x in strings:
                fh.write(x)
                yield
