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

from . import Graph, Serializer, Hooks, dirlist, Indexer, BaseIndexer, Query
from .uuidgen import uuidgen

try:
    xrange          # Python 2
except NameError:
    xrange = range  # Python 3

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

idx_uuid = '00000000-0000-0000-0000-000000000000'

def uuid_to_utc_ts(u):
    return (uuid.UUID('{%s}' % u).time - 0x01b21dd213814000) / 1e7

def uuid_to_utc(u):
    return datetime.datetime.utcfromtimestamp(uuid_to_utc_ts(u)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

def intersects(list1, list2):
    if len(list2) > len(list1):
        a = frozenset(list2)
        b = list1
    else:
        a = frozenset(list1)
        b = list2
    for x in b:
        if x in a:
            return True

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

class BaseIndex(object):
    def __init__(self, ctx):
        self.ctx = ctx
        self._indexes = {}

class StatusIndex(BaseIndex):
    indexer = StatusIndexer()
    domain = 'status'
    null = Serializer.null()
    uuid = Serializer.uuid()

    def index(self, idx):
        try:
            return self._indexes[idx]
        except KeyError:
            index = self._indexes[idx] = self.ctx.txn.sset('lg.collection.idx.%s.%s' % (self.domain, idx), serialize_value=self.null)
        return index

    def update(self, uuid, old, new):
        oldkeys = self.indexer.index(old)
        newkeys = self.indexer.index(new)
        uuid = self.uuid.encode(uuid)
        for name, crc in oldkeys.difference(newkeys):
            keys = self.index(name)
            try:
                keys.remove(crc + uuid)
            except KeyError:
                pass
        for name, crc in newkeys.difference(oldkeys):
            keys = self.index(name)
            keys.add(crc + uuid)

    def search(self, idx, value):
        idx, crc, check = self.indexer.prequery(idx, value)
        crclen = len(crc)
        try:
            index = self.index(idx)
        except KeyError:
            return
        for key in index.iterpfx(pfx=crc):
            uuid = self.uuid.decode(key[crclen:])
            status = self.ctx.statusDB[uuid]
            if check(status):
                yield uuid, status

class LG_LiteIndexer(BaseIndexer):
    def idx_adapters(self, obj):
        try:
            return obj['adapters']
        except KeyError:
            return ()

    def key(self, name, value):
        if isinstance(value, list):
            value = tuple(value)
        return (name,) + value

class LG_LiteIndex(BaseIndex):
    indexer = LG_LiteIndexer()
    domain = "lg_lite"
    uuid = Serializer.uuid()

    def update(self, uuid, old, new):
        oldkeys = self.indexer.index(old)
        newkeys = self.indexer.index(new)

        # keys should be ('adapters', <adaptername>, <query>, <priority>)
        for _, adapter, query, pri in oldkeys.difference(newkeys):
            assert _ == 'adapters'
            aqb = '%s:%s' % (adapter, query)
            pq = self.jobs(adapter, query)
            pq.remove(uuid)
            if pq.empty:
                self.queries(adapter).remove(query)
                del self.adapters[aqb]
            else:
                self.adapters[aqb] -= 1

        for _, adapter, query, pri in newkeys.difference(oldkeys):
            assert _ == 'adapters'
            aqb = '%s:%s' % (adapter, query)
            pq = self.jobs(adapter, query)
            try:
                self.adapters[aqb] += 1
            except KeyError:
                self.queries(adapter).add(query)
                self.adapters[aqb] = 1
            pq.add(uuid, priority=pri)

    # list of active queries per adapter
    def queries(self, adapter):
        try:
            return self._queries[adapter]
        except KeyError:
            ret = self.ctx.txn.sset('lg.collection.idx.%s.%s' % (self.domain, adapter), map_values=True)
            self._queries[adapter] = ret
            return ret

    # priority queue of jobs per adapter/query pair
    def jobs(self, adapter, query):
        try:
            return self._jobs[adapter, query]
        except KeyError:
            ret = self.ctx.txn.pqueue('lg.collection.pq.%s.%s.%s' % (self.domain, adapter, query), serialize_value=self.uuid)
            self._jobs[adapter, query] = ret
            return ret

    @lazy
    def _queries(self):
        return {}

    @lazy
    def _jobs(self):
        return {}

    @lazy
    def adapters(self):
        return self.ctx.txn.kv('lg.collection.idx.adapters', map_keys=True, serialize_value=self.ctx.uint)

class Context(object):
    uuid = Serializer.uuid()

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

    def _status_enrich(self, status, uuid, user=None, roles=None):
        if user is not None:
            user_roles = status['roles'][user]
            if roles is not None and not intersects(user_roles, roles):
                raise KeyError
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

    def status(self, uuid, **kwargs):
        try:
            return self._status_enrich(self.statusDB[uuid], uuid, **kwargs)
        except KeyError:
            pass

    def sync(self, uuid, txn):
        old_status, new_status = self._sync_status(uuid, txn)
        self.status_index.update(uuid, old_status, new_status)
        self.lg_lite_index.update(uuid, old_status, new_status)
        self.metaDB[uuid] = txn.as_dict()

    @lazy
    def status_index(self):
        return StatusIndex(self)

    @lazy
    def lg_lite_index(self):
        return LG_LiteIndex(self)

    def _sync_status(self, uuid, txn):
        status = {
            'nextID': txn.nextID,
            'size': txn.graph.size,
            'nodes_count': txn.nodes_count(),
            'edges_count': txn.edges_count(),
            'enabled': txn.enabled,
            'priority': txn.priority,
        }

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

        status['adapters'] = adapters = []
        if status['enabled']:
            priority = status['priority']
            for flow in txn.lg_lite.flows(active=True):
                adapters.append((flow.adapter, flow.query, priority))

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
            self.lg_lite_index.update(uuid, status, None)
        except KeyError:
            pass
        try:
            del self.metaDB[uuid]
        except KeyError:
            pass

    @lazy
    def statusDB(self):
        return self.txn.kv('lg.collection.status', serialize_key=self.uuid, serialize_value=self.msgpack)

    @lazy
    def metaDB(self):
        return self.txn.kv('lg.collection.meta', serialize_key=self.uuid, serialize_value=self.msgpack)


class Collection(object):
    # increment on index structure changes
    VERSION = 2

    def __init__(self, dir, graph_opts=None, create=True, rebuild=False, syncd=None, **kwargs):
        self.db = None
        if create:
            try:
                os.mkdir(dir)
            except OSError as e:
                if e.errno != errno.EEXIST or not os.path.isdir(dir):
                    raise
        self.dir = os.path.abspath(dir)
        self.syncd = syncd
        idx = "%s.idx" % self.dir
        self.graph_opts = {} if graph_opts is None else graph_opts
        self.notls = kwargs.get('notls', False)
        kwargs['serialize_property_value'] = self.msgpack
        kwargs['create'] = create
        if rebuild:
            os.remove(idx)
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
                    if self.syncd is not None:
                        self.syncd.queue(uuid)
                        self.syncd.queue(idx_uuid)

    def remove(self, uuid):
        with self.context(write=True) as ctx:
            ctx.remove(uuid)
            if self.syncd is not None:
                self.syncd.unqueue(uuid)
                self.syncd.queue(idx_uuid)

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
        if 'notls' not in kwargs:
            kwargs['notls'] = self.notls
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
        return re.compile(r'\w+')

    def status(self, uuid, **kwargs):
        with self.context(write=False) as ctx:
            return ctx.status(uuid, **kwargs)

    def graphs(self, enabled=None):
        with self.context(write=False) as ctx:
            for x in ctx.graphs(enabled=enabled):
                yield x

    def graph_path(self, uuid):
        return "%s%s%s.db" % (self.dir, os.path.sep, uuid)

    def context(self, write=True):
        return Context(self, write=write)

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

