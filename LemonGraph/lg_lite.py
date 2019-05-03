from __future__ import print_function

from lazy import lazy
from six import iteritems, string_types
from time import time
from uuid import uuid1 as uuidgen

from . import wire, lib, ffi
from .MatchLGQL import MatchLGQL
from .serializer import Serializer

msgpack = Serializer.msgpack()
vuints = Serializer.vuints(decode_type=list)
uint = Serializer.uint()
uints2d = Serializer.uints2d()
null = Serializer.null()

string_or_none = string_types + (type(None),)

def as_uint(x):
    ret = int(x)
    if ret < 0:
        raise ValueError(x)
    return ret

class LG_Lite(object):
    def __init__(self, txn):
        self.txn = txn

    @lazy # holds current query for each adapter
    def adapterdb(self):
        return self.txn.kv('lg_lite.adapters', map_keys=True, map_data=True)

    def update_adapters(self, data):
        if not isinstance(data, dict):
            raise ValueError
        for name, config in iteritems(data):
            self.update_adapter(name, config)

    def update_adapter(self, name, data):
        if isinstance(data, dict):
            return self._update_flows(name, [data], primary=True)
        if isinstance(data, (list, tuple)):
            return self._update_flows(name, data)
        raise ValueError

    def _update_flows(self, name, configs, primary=False):
        adapter = self.adapter(name)
        for config in configs:
            if primary:
                query = config.pop('query', None) or self.adapterdb[name]
            else:
                query = config.pop('query')
            with adapter.flow(query=query, create=True) as flow:
                default = flow
                disable = None
                if primary:
                    try:
                        if self.adapterdb[name] != query:
                            default = disable = adapter.flow()
                    except KeyError:
                        self.adapterdb[name] = query
                flow.pos      = config.pop('pos',      default.pos)
                flow.enabled  = config.pop('enabled',  default.enabled)
                flow.autotask = config.pop('autotask', default.autotask)
                flow.filter   = config.pop('filter',   default.filter)
            if config:
                raise ValueError
            if disable:
                with disable as flow:
                    flow.autotask = False
                self.adapterdb[name] = query

    def adapter(self, adapter):
        assert(self.txn is not None)
        return Adapter(self, adapter)

    def flow(self, adapter, **kwargs):
        return self.adapter(adapter).flow(**kwargs)

    def task(self, adapter, **kwargs):
        return self.adapter(adapter).task(**kwargs)

    def inject(self, adapter, **kwargs):
        return self.adapter(adapter).inject(**kwargs)

    def adapters(self):
        assert(self.txn is not None)
        try:
            adapterdb = self.adapterdb
        except KeyError:
            return
        for adapter in adapterdb.iterkeys():
            yield self.adapter(adapter)

    # return all defined flows
    def flows(self, adapter=None, **kwargs):
        assert(self.txn is not None)
        adapters = self.adapters() if adapter is None else (Adapter(self, adapter),)
        for adapter in adapters:
            for flow in adapter.flows(**kwargs):
                yield flow

    def ffwd(self, start=None):
        flows = self.flows(active=True)
        if start is None:
            flows = (f for f in flows if f._pos < self.txn.nextID)
        else:
            flows = (f for f in flows if f._pos == start)
        for f in flows:
            with f as flow:
                flow.ffwd()


class Adapter(object):

    def __init__(self, lg, adapter):
        self.lg = lg
        self.txn = lg.txn
        assert(self.txn is not None)
        self.adapter = adapter

    def flow(self, query=None, **kwargs):
        if query is None:
            query = self.adapterdb[self.adapter]
        return Flow(self, query, **kwargs)

    def flows(self, active=None):
        flows = (Flow(self, query) for query in self.flowdb)
        if active:
            flows = (flow for flow in flows if flow.active)
        return flows

    def inject(self, query=None, **kwargs):
        with self.flow(query=query) as flow:
            return flow.inject(**kwargs)

    # find/generate a task from active flows
    # raises KeyError or IndexError
    def task(self, query=None, **kwargs):
        if query is not None:
            with self.flow(query) as flow:
                try:
                    return flow.task(**kwargs)
                except IndexError:
                    pass
            raise IndexError

        seen = set()
        query = self.flowdb.next_key()
        while query not in seen:
            with self.flow(query) as flow:
                try:
                    return flow.task(**kwargs)
                except (KeyError, IndexError):
                    pass
            seen.add(query)
            query = self.flowdb.next_key()
        raise IndexError

    @lazy
    def adapterdb(self):
        return self.lg.adapterdb

    @lazy # holds [flags, pos, task_count, queue_length] for each adapter query
    def flowdb(self):
        return self.txn.kv('lg_lite.flows.%s' % self.adapter, map_keys=True, serialize_value=vuints)

    @lazy # holds autotask-ing filter for each adapter query
    def filterdb(self):
        return self.txn.kv('lg_lite.filters.%s' % self.adapter, map_keys=True, map_data=True)


ENABLE = 1
AUTOTASK = 2

class Flow(object):
    def __init__(self, adapter, query, create=False):
        if not isinstance(query, string_types):
            raise ValueError
        self.update = False
        self.txn = adapter.txn
        self.adapter = adapter
        self.query = query
        # adapter name + query base
        self.aqb = (adapter.adapter, query)
        self.refs = 0
        try:
            self._flags, self._pos, self._tasks, _qlen = adapter.flowdb[self.query]
        except KeyError:
            if not create:
                raise
            MatchLGQL(query)
            # set up defaults
            self._tasks = 0
            self._flags = AUTOTASK|ENABLE
            # default pos to current nextID for new flows
            self.pos = 0
            self.update = True

    def __del__(self):
        if self.update:
            raise RuntimeError

    def __enter__(self):
        self.refs += 1
        return self

    def __exit__(self, type, value, traceback):
        self.refs -= 1
        if self.update and not self.refs:
            self.adapter.flowdb[self.query] = self._flags, self._pos, self._tasks, self.qlen
            self.update = False

    @lazy
    def queue(self):
        return self.txn.fifo('lg_lite.queue.%s.%s' % self.aqb, serialize_value=vuints)

    @lazy
    def taskdb(self):
        return self.txn.kv('lg_lite.tasks.%s.%s' % self.aqb, serialize_value=uints2d)

    @lazy # map task uuid to timestamp
    def ts(self):
        return self.txn.kv('lg_lite.ts.%s.%s' % self.aqb, serialize_key=null, serialize_value=null)

    @lazy # enc(timestamp) + task uuid
    def ts_idx(self):
        return self.txn.sset('lg_lite.ts_idx.%s.%s' % self.aqb, serialize_value=null)

    @property
    def filter(self):
        return self.adapter.filterdb.get(self.query, None)

    @filter.setter
    def filter(self, filter):
        if filter is None:
            self.adapter.filterdb.pop(self.query, None)
        else:
            MatchLGQL(self.query + ',' + filter)
            self.adapter.filterdb[self.query] = filter

    @property
    def qfull(self):
        try:
            return self.query + ',' + self.filter
        except TypeError:
            return self.query

    @property
    def pos(self):
        return self._pos

    # set pos=1 to make flow start at beginning of job
    #   pos > 0: absolute
    #   pos < 1: relative to current nextID
    @pos.setter
    def pos(self, p):
        p = int(p)
        if p < 1:
            p += self.txn.nextID
            if p < 1:
                p = 1
        elif p > self.txn.nextID:
            p = self.txn.nextID
        self._pos = p
        self.update = True

    @property
    def qlen(self):
        return len(self.queue)

    @property
    def tasks(self):
        return self._tasks

    @tasks.setter
    def tasks(self, x):
        self._tasks = x
        self.update = True

    @property
    def active(self):
        return self.txn.enabled and self.enabled and (self.tasks > 0 or self.qlen > 0 or (self.pos < self.txn.nextID and self.autotask))

    @property
    def flags(self):
        return self._flags

    @flags.setter
    def flags(self, flags):
        flags = int(flags)
        if self._flags != flags:
            self._flags = flags
            self.update = True

    @property
    def enabled(self):
        return bool(self._flags & ENABLE)

    @enabled.setter
    def enabled(self, x):
        if x:
            self.flags |= ENABLE
        else:
            self.flags &= ~ENABLE

    @property
    def autotask(self):
        return bool(self.flags & AUTOTASK)

    @autotask.setter
    def autotask(self, x):
        if x:
            self.flags |= AUTOTASK
        else:
            self.flags &= ~AUTOTASK

    def _find_task(self, min_age, blacklist):
        max_ts = time() - as_uint(min_age)
        ts = ffi.new('uint64_t[]', 1)
        for enc in self.ts_idx:
            pfxlen = lib.unpack_uints(1, ts, enc)
            if ts[0] > max_ts:
                break
            uuid = wire.decode(enc[pfxlen:])
            if uuid not in blacklist:
                return Task(self, uuid=uuid)
        raise IndexError

    def ffwd(self):
        pos = self._pos
        def scanner(entry):
            pos = entry.ID

        for chain in self.txn.query(self.qfull, start=pos, scanner=scanner, snap=True):
            self.pos = pos
            return
        self.pos = self.txn.nextID

    def _create_task(self, limit, default_limit=200):
        try:
            limit = int(limit)
            if limit < 1:
                limit = default_limit
        except ValueError:
            limit = default_limit

        # pull from queue first
        records = self.queue.pop(limit)
        more = limit - len(records)
        if more and self.autotask and self._pos < self.txn.nextID:
            records = list(records)

            full = False
            rec = [self._pos - 1]
            def scanner(entry):
                if not full:
                    rec[0] = entry.ID
                return full

            for chain in self.txn.query(self.qfull, start=self._pos, scanner=scanner, snap=True):
                rec[1::] = tuple(x.ID for x in chain)
                if full:
                    self.queue.push(rec)
                else:
                    records.append(tuple(rec))
                    if len(records) == limit:
                        full = True

            records = tuple(records)

            # holds the logID for the last entry that we processed, stash nextID
            self.pos = rec[0] + 1

        if not records:
            raise IndexError

        self.update = True

        return Task(self, records=records)

    # fetch/create task
    def task(self, uuid=None, limit=200, min_age=60, blacklist=()):
        if uuid is not None:
            # if specified, ignore other args and fetch task by uuid
            # raises KeyError if does not exist
            return Task(self, uuid=uuid)

        if not self.txn.enabled:
            raise IndexError

        # grab oldest existing non-blacklisted task older than min_age seconds
        # failing that, pull from queue
        # raises IndexError if no tasks could be found/created
        try:
            return self._find_task(min_age, blacklist)
        except IndexError:
            return self._create_task(limit)

    # manually fill queue
    def inject(self, filter=None):
        query = self.query
        if filter is not None:
            query += ',' + filter
        count = 0
        rec = [self.txn.nextID]
        for chain in self.txn.query(query):
            rec[1::] = tuple(x.ID for x in chain)
            self.queue.push(rec)
            count += 1

        if count:
            self.update = True

        return count

class Task(object):
    def __init__(self, flow, uuid=None, records=None):
        if not flow.refs:
            raise RuntimeError

        self.flow = flow
        self.txn = flow.txn
        self._records = records

        if bool(uuid) is bool(records):
            raise RuntimeError

        if uuid is None:
            self.uuid = uuid = str(uuidgen())
            self.touch()
            flow.taskdb[uuid] = records
            with self.flow as flow:
                flow.tasks += 1
        else:
            self.uuid = uuid
            # ensure it exists
            self.timestamp

    @lazy
    def records(self):
        if self._records is not None:
            return self._records
        return self.flow.taskdb[self.uuid]

    def format(self):
        for rec in self.records:
            yield tuple(self.txn.entry(x, beforeID=rec[0]+1).as_dict() for x in rec[1::])

    def drop(self):
        del self.flow.taskdb[self.uuid]
        bid = wire.encode(self.uuid)
        try:
            ts_enc_prev = self.flow.ts.pop(bid)
            self.flow.ts_idx.remove(ts_enc_prev + bid)
        except KeyError:
            pass
        with self.flow as flow:
            self.flow.tasks -= 1

    @property
    def timestamp(self):
        return uint.decode(self.flow.ts[wire.encode(self.uuid)])

    @timestamp.setter
    def timestamp(self, ts):
        bid = wire.encode(self.uuid)
        ts = as_uint(time() if ts is None else ts)
        ts_enc = uint.encode(ts)
        try:
            ts_enc_prev = self.flow.ts[bid]
            self.flow.ts_idx.remove(ts_enc_prev + bid)
        except KeyError:
            pass
        self.flow.ts[bid] = ts_enc
        self.flow.ts_idx.add(ts_enc + bid)

    def touch(self, ts=None):
        self.timestamp = ts
