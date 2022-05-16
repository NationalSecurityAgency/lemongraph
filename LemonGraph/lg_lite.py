from lazy import lazy
import logging
from six import iteritems
from time import time
from weakref import WeakValueDictionary

from .wire import encode, decode
from .serializer import Serializer
from .uuidgen import uuidgen
from . import unspecified, lib, ffi
from .MatchLGQL import MatchLGQL, QueryCannotMatch, QuerySyntaxError
from .stringmap import StringMap
from . import cast

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

null = Serializer.null()
uuid = Serializer.uuid()
uint = Serializer.uint()
vuints = Serializer.vuints(decode_type=list)
uints2d = Serializer.uints2d()
msgpack = Serializer.msgpack()

def efilter(iterable, exceptions, cb):
    for x in iterable:
        try:
            yield cb(x)
        except exceptions:
            pass

TPS = 10
def to_ticks(ts=None, inv=1.0/TPS, round=True):
    if ts is None:
        ts = time() + (inv if round else 0)
    if ts < 0:
        raise ValueError(ts)
    return int(ts * TPS)

def from_ticks(val, fTPS=float(TPS)):
    return val / fTPS

ENABLE = 1
AUTOTASK = 2
DEFAULT_FLAGS = ENABLE|AUTOTASK

class LG_Lite(object):
    def __init__(self, txn):
        self.txn = txn
        self._flows = WeakValueDictionary()
        self._tasks = WeakValueDictionary()

    def flow(self, flowID=None, **kwargs):
        if flowID:
            # cache all flow objects
            try:
                flow = self._flows[flowID]
            except KeyError:
                flow = self._flows[flowID] = Flow(self, flowID=flowID)
            return flow
        return Flow.Get(self, **kwargs)

    def flows(self, **kwargs):
        return Flow.Find(self, **kwargs)

    def task(self, uuid=None, **kwargs):
        if uuid:
            # cache all task objects
            try:
                task = self._tasks[uuid]
            except KeyError:
                task = self._tasks[uuid] = Task(self, uuid=uuid, **kwargs)
            return task
        # pluck out flow identification
        adapter = kwargs.pop('adapter')
        query = kwargs.pop('query', None)
        with self.flow(adapter=adapter, query=query) as flow:
            # and find/create task as specified
            return flow.task(**kwargs)

    def tasks(self, uuids=unspecified, states=unspecified, adapters=unspecified):
        try:
            self.statusdb
        except KeyError:
            return ()

        if uuids is unspecified:
            # default to any task
            tasks = (self.task(uuid=u) for u in self.statusdb)
        else:
            tasks = (self.task(uuid=u) for u in uuids if u in self.statusdb)

        if states is unspecified:
            # default to any state
            pass
        elif states:
            stateIDs = set(efilter(states, KeyError, Task.states))
            tasks = (t for t in tasks if t.stateID in stateIDs)
        else:
            return ()

        if adapters is unspecified:
            # default to any adapter
            pass
        elif adapters:
            adapterIDs = set(efilter(adapters, KeyError, lambda a: self.txn.stringID(encode(a), update=False)))
            tasks = (t for t in tasks if t.adapterID in adapterIDs)
        else:
            return ()

        return tasks

    def inject(self, adapter=None, query=None, filter=None):
        with self.flow(adapter=adapter, query=query) as flow:
            return flow.inject(filter=filter)

    def ffwd(self, start=None):
        flows = self.flows(active=True)
        if start is None:
            flows = (f for f in flows if f.pos < self.txn.nextID)
        else:
            flows = (f for f in flows if f.pos == start)
        for f in flows:
            with f as flow:
                flow.ffwd()

    def update_adapters(self, configs):
        try:
            items = iteritems(configs)
        except AttributeError:
            raise ValueError
        for adapter, config in items:
            self.update_adapter(adapter, config)

    def update_adapter(self, adapter, data):
        try:
            # try it as a dictionary-ish object first
            return self._update_flow(adapter=adapter, primary=True, **data)
        except TypeError:
            raise

        # else try as a list of dictionary-ish objects
        for config in data:
            self._update_flow(adapter=adapter, **config)

    def _update_flow(self, adapter=None, query=unspecified, filter=unspecified, pos=unspecified, enabled=unspecified, autotask=unspecified, limit=unspecified, timeout=unspecified, primary=False):
        if query is unspecified:
            if primary:
                raise ValueError("<query> param must be supplied if setting as primary")
            flow = prev = self.flow(adapter=adapter)
        else:
            flow = prev = self.flow(adapter=adapter, query=query, create=True)
            # check if there is a previous primary to clone unspecified default values from
            if primary:
                try:
                    prev = self.flow(adapter=adapter)
                except KeyError:
                    pass
                flow.primary = True
        with flow:
            # set unspecified defaults from self or previous primary
            flow.pos      = prev.pos      if pos      is unspecified else pos
            flow.filter   = prev.filter   if filter   is unspecified else filter
            flow.enabled  = prev.enabled  if enabled  is unspecified else enabled
            flow.autotask = prev.autotask if autotask is unspecified else autotask
            flow.limit    = prev.limit    if limit    is unspecified else limit
            flow.timeout  = prev.timeout  if timeout  is unspecified else timeout
            if flow is not prev:
                # disable autotasking on previous primary
                with prev:
                    prev.autotask = False

    @lazy # flowID => [adapterID, queryID, chain length, flags, log position, active task count]
    def flowdb(self): # as well as (0, adapterID, queryID) => flowID
        return self.txn.kv('lg_lite.flow', serialize_key=null, serialize_value=null)
#        return self.txn.kv('lg_lite.flow', serialize_key=null, serialize_value=vuints)

    @lazy # flowID => query filter
    def filterdb(self):
        return self.txn.kv('lg_lite.filter', map_data=True)

    @lazy # adapterID => primary flowID
    def primarydb(self):
        return self.txn.kv('lg_lite.primary', serialize_key=uint, serialize_value=null)

    @lazy # task => [<records>]
    def recorddb(self):
        return self.txn.kv('lg_lite.records', serialize_key=uuid, serialize_value=uints2d)

    @lazy # task => [flowID, retries, <active|done|error|split>, timestamp]
    def statusdb(self):
        return self.txn.kv('lg_lite.status', serialize_key=uuid, serialize_value=vuints)

    @lazy # [flowID, timestamp, uuid]
    def retrydb(self):
        return self.txn.sset('lg_lite.retry', serialize_value=null)

    @lazy # task => message
    def detaildb(self):
        return self.txn.kv('lg_lite.details', serialize_key=uuid, serialize_value=msgpack)


class Flow(object):
    @staticmethod
    def Find(lg, adapter=None, active=None):
        try:
            flowdb = lg.flowdb
        except KeyError:
            return
        pfx = (0, lg.txn.stringID(encode(adapter), update=False)) if adapter else (0,)
        # crawl or filter secondary index to get primary key
        for pkey in flowdb.itervalues(pfx=vuints.encode(pfx)):
            flow = lg.flow(flowID=uint.decode(pkey))
            if active is None or flow.active is active:
                yield flow

    @staticmethod
    def Get(lg, adapter=None, query=None, create=False):
        adapterID = lg.txn.stringID(encode(adapter), update=create)
        # if query is unspecified, look up default adapter flow
        if query is None:
            return lg.flow(flowID=uint.decode(lg.primarydb[adapterID]))

        # next attempt look up by secondary index - adapterID/queryID
        queryID = lg.txn.stringID(encode(query), update=create)
        ikey = vuints.encode((0, adapterID, queryID))
        try:
            return lg.flow(flowID=uint.decode(lg.flowdb[ikey]))
        except KeyError:
            if not create:
                raise

        # flow is new - check query validity and save chain length
        m = MatchLGQL(query)
        chain_len = len(m.keep)

        # init data - remaining fields are: log position, active task count, default limit, and default timeout
        data = [adapterID, queryID, chain_len, DEFAULT_FLAGS, 0, 0, 200, to_ticks(60)]

        try: # grab, increment, and stash last flowID
            flowID = 1 + uint.decode(lg.flowdb[b''])
        except KeyError:
            flowID = 1

        # update bookmark, add index, and set primary key
        pkey = lg.flowdb[b''] = lg.flowdb[ikey] = uint.encode(flowID)
        lg.flowdb[pkey] = vuints.encode(data)
        return lg.flow(flowID=flowID)

    def __init__(self, lg, flowID=None):
        self.lg = lg
        self.txn = txn = lg.txn
        self.update = False
        self.refs = 0
        self._flowID = cast.uint(flowID)
        self._pkey = uint.encode(self._flowID)
        self._data = vuints.decode(lg.flowdb[self._pkey])

    def __del__(self):
        if self.update:
            raise RuntimeError("Flow() object updates should always be done via contexts!")

    def __enter__(self):
        self.refs += 1
        return self

    def __exit__(self, type, value, traceback):
        self.refs -= 1
        if self.update and not self.refs:
            self.lg.flowdb[self._pkey] = vuints.encode(self._data)
            self.update = False

    @lazy
    def queue(self):
        return self.txn.fifo('lg_lite.queue.%d' % self._flowID, serialize_value=vuints)

    @property
    def filter(self):
        try:
            return self.lg.filterdb[self._pkey]
        except KeyError:
            pass

    @filter.setter
    def filter(self, filter):
        if filter is None:
            self.lg.filterdb.pop(self._pkey, None)
        else:
            # ensure filter is valid for query
            MatchLGQL(self.query + ',' + filter)
            self.lg.filterdb[self._pkey] = filter

    @property
    def query_full(self):
        try:
            return self.query + ',' + self.filter
        except TypeError:
            return self.query

    @property
    def flowID(self):
        return self._flowID

    @property
    def adapterID(self):
        return self._data[0]

    @property
    def queryID(self):
        return self._data[1]

    @property
    def chain_len(self):
        return self._data[2]

    @property
    def _flags(self):
        return self._data[3]

    @_flags.setter
    def _flags(self, flags):
        flags = cast.uint(flags)
        if self._data[3] != flags:
            self._data[3] = flags
            self.update = True

    flags = _flags

    @property
    def enabled(self):
        return bool(self.flags & ENABLE)

    @enabled.setter
    def enabled(self, x):
        if x:
            self._flags |= ENABLE
        else:
            self._flags &= ~ENABLE

    @property
    def autotask(self):
        return bool(self.flags & AUTOTASK)

    @autotask.setter
    def autotask(self, x):
        if x:
            self._flags |= AUTOTASK
        else:
            self._flags &= ~AUTOTASK

    @property
    def pos(self):
        return self._data[4]

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
        self._data[4] = p
        self.update = True

    @property
    def primary(self):
        return self.lg.primarydb.get(self.adapterID, None) == self._pkey

    @primary.setter
    def primary(self, val):
        if val:
            self.lg.primarydb[self.adapterID] = self._pkey
        else:
            self.lg.primarydb.pop(self.adapterID, None)

    @property
    def _active_count(self):
        return self._data[5]
    active_count = _active_count

    @_active_count.setter
    def _active_count(self, n):
        n = cast.uint(n)
        if self._data[5] != n:
            self._data[5] = n
            self.update = True

    @property
    def qlen(self):
        return len(self.queue)

    @property
    def limit(self):
        return self._data[6]

    @limit.setter
    def limit(self, n):
        n = cast.uint(n) or 200
        if self._data[6] != n:
            self._data[6] = n
            self.update = True

    @property
    def timeout(self):
        return from_ticks(self._data[7])

    @timeout.setter
    def timeout(self, n):
        n = cast.unum(n)
        n = to_ticks(n)
        if self._data[7] != n:
            self._data[7] = n
            self.update = True

    def ffwd(self):
        pos = self.pos
        def scanner(entry):
            pos = entry.ID
        for chain in self.txn.query(self.query_full, start=pos, scanner=scanner, snap=True):
            self.pos = pos
            return
        self.pos = self.txn.nextID

    @property
    def adapter(self):
        return decode(self.txn.string(self.adapterID))

    @property
    def query(self):
        return decode(self.txn.string(self.queryID))

    # manually fill queue
    def inject(self, filter=None):
        query = self.query
        if filter is not None:
            query += ',' + filter
        count = 0
        rec = [self.txn.nextID]
        for chain in self.txn.query(query):
            rec[1:] = (x.ID for x in chain)
            self.queue.push(rec)
            count += 1

        if count:
            self.update = True

        return count

    def task(self, uuid=None, **kwargs):
        # always allow fetch by uuid
        if uuid:
            t = self.lg.task(self, uuid=uuid, **kwargs)
            # ensure task actually belongs to this flow
            if t.flowID != self.flowID:
                raise KeyError

        # fetch by other criteria if job is enabled
        if self.txn.enabled:
            return self._get_task(**kwargs)
        raise IndexError

    @property
    def active(self):
        return self.txn.enabled and self.enabled and (self._has_retries or self.qlen > 0 or (self.pos < self.txn.nextID and self.autotask))

    @property
    def _has_retries(self):
        for enc in self.lg.retrydb.iterpfx(pfx=self._pkey):
            return True
        return False

    def _get_task(self, ignore=(), limit=unspecified, **kwargs):
        # find oldest retry-able task or create new
        try:
            return Task._Find(self, ignore, **kwargs)
        except IndexError:
            pass
        return Task._Create(self, limit, **kwargs)

    def _get_records(self, limit):
        # first grab up to [limit] records from the queue
        records = self.queue.pop(limit)
        limit -= len(records)

        # unless full, resume streaming query where we left off
        if limit and self.autotask and self.pos < self.txn.nextID:
            rec = [self.pos - 1]

            # halt query when we've reached out limit
            def scanner(entry):
                if limit:
                    rec[0] = entry.ID
                else:
                    return True

            for chain in self.txn.query(self.query_full, start=self.pos, scanner=scanner, snap=True):
                rec[1:] = (x.ID for x in chain)
                # stash current log position
                p = rec[0]
                try: # harvest the snapped beforeID
                    rec[0] = chain[0].beforeID
                except IndexError:
                    pass
                if limit:
                    # clone record - it will get reused
                    records.append(tuple(rec))
                    limit -= 1
                else:
                    # send any overflow into the records queue for later
                    self.queue.push(rec)
                # restore current log position
                rec[0] = p

            # holds the logID for the last entry that we processed, stash nextID
            self.pos = rec[0] + 1

        if records:
            return records
        raise IndexError



class Task(object):
    states = StringMap(['active', 'done', 'error', 'deleted', 'retry', 'idle', 'void'])
    _ts = ffi.new('uint64_t[]', 1)

    @classmethod
    def _Retries(cls, flow, ts=None):
        # first part of retrydb enc val is flow pkey
        encs = flow.lg.retrydb.iterpfx(pfx=flow._pkey)
        flen = len(flow._pkey)
        try:
            enc = next(encs)
            ts = to_ticks(ts, round=False)
            while True:
                # second part is timestamp
                tlen = lib.unpack_uints(1, cls._ts, enc[flen:])
                if cls._ts[0] > ts:
                    raise StopIteration
                # third and final part is task uuid
                yield uuid.decode(enc[flen + tlen:])
                enc = next(encs)
        except StopIteration:
            pass

    @classmethod
    def _Find(cls, flow, ignore, timeout=unspecified):
        for u in cls._Retries(flow):
            if u not in ignore:
                return flow.lg.task(uuid=u, retry=True, state='active', timeout=flow.timeout if timeout is unspecified else timeout)
        raise IndexError

    @classmethod
    def _Create(cls, flow, limit, timeout=unspecified):
        if limit is unspecified:
            limit = flow.limit
        try:
            limit = cast.uint(limit) or flow.limit
        except (ValueError, KeyError):
            limit = flow.limit

        # attempt to pull some records from the flow
        records = flow._get_records(limit)

        # find an unused uuid
        uuid = uuidgen()
        while uuid in flow.lg.statusdb:
            uuid = uuidgen()

        # add initial status: flowID, retries, state, timestamp, and timeout
        flow.lg.statusdb[uuid] = (flow.flowID, 0, cls.states.error, 0, 0)
        flow.lg.recorddb[uuid] = records

        return flow.lg.task(uuid=uuid, state=cls.states.active, touch=True, timeout=flow.timeout if timeout is unspecified else timeout)

    def __init__(self, lg, uuid=None, **kwargs):
        self._status  = lg.statusdb[uuid]
        self.uuid = uuid
        self.txn = lg.txn
        self.lg = lg
        if kwargs:
            self.update(**kwargs)

    def chains(self, ids=None):
        for rec in self.records:
            chain = rec[1:]
            if ids and ids.isdisjoint(chain):
                continue
            yield tuple(self.txn.entry(x, beforeID=rec[0]) for x in chain)

    def format(self, chains=None):
        if chains is None:
            chains = self.chains()
        for chain in chains:
            yield tuple(x.as_dict() for x in chain)

    @property
    def status(self):
        return {
            'task':      self.uuid,
            'adapter':   self.adapter,
            'query':     self.query,
            'state':     self.state,
            'retries':   self.retries,
            'timestamp': self.timestamp,
            'timeout':   self.timeout,
            'details':   self.details,
            'length':    len(self.records),
        }

    @lazy
    def records(self):
        return self.lg.recorddb[self.uuid]

    @lazy
    def flow(self):
        return self.lg.flow(self.flowID)

    @property
    def flowID(self):
        return self._status[0]

    @property
    def retries(self):
        return self._status[1]

    @property
    def stateID(self):
        return self._status[2]

    @property
    def state(self):
        return self.states[self._status[2]]

    @property
    def timestamp(self):
        return from_ticks(self._status[3])

    @property
    def timeout(self):
        return from_ticks(self._status[4])

    @property
    def adapter(self):
        return self.flow.adapter

    @property
    def adapterID(self):
        return self.flow.adapterID

    @property
    def query(self):
        return self.flow.query

    @property
    def queryID(self):
        return self.flow.queryID

    @property
    def active(self):
        return self._status[2] == self.states.active

    def update(self, touch=None, state=None, retry=None, timeout=unspecified, details=unspecified):
        delete = False
        if state in ('deleted', 'delete'):
            # normalize 'delete' to 'deleted'
            state = 'deleted'
            # nuke details
            details = None
            # and flag to remove from statusdb & recorddb
            delete = True

        # clone current status
        _status = list(self._status)
        if retry is not None:
            # increment if true, else set to uint
            _status[1] = _status[1] + retry if isinstance(retry, bool) else cast.uint(retry)
        if state is not None:
            # normalize string/uint to valid uint
            _status[2] = self.states(state)
        if touch:
            # use current timestamp if true, else provided uint
            _status[3] = to_ticks(None if touch is True else touch)
        if timeout is not unspecified:
            # set or delete task timeout
            _status[4] = 0 if timeout is None else to_ticks(timeout)

        if _status != self._status:
            active_delta = bool(self._status[2]) - bool(_status[2])
            rk0 = self._rkey
            self.lg.statusdb[self.uuid] = self._status = _status
            rk1 = self._rkey
            if rk0 != rk1:
                if rk0:
                    self.lg.retrydb.pop(rk0)
                if rk1:
                    self.lg.retrydb.add(rk1)
            if active_delta:
                with self.flow as flow:
                    flow._active_count += active_delta
        if details is not unspecified:
            self.details = details
        if delete:
            # leave's self._status and pull records once so that self.status looks nice
            self.records
            del self.lg.statusdb[self.uuid]
            del self.lg.recorddb[self.uuid]

    def touch(self, retry=False, details=unspecified):
        self.update(touch=True, retry=retry, details=details)

    @property
    def details(self):
        return self.lg.detaildb.get(self.uuid, None)

    @details.setter
    def details(self, value):
        if value is None:
            self.lg.detaildb.pop(self.uuid, None)
        else:
            self.lg.detaildb[self.uuid] = value

    def drop(self):
        # delete all traces of a task
        self.update(state='deleted')

    @lazy
    def _bid(self):
        return uuid.encode(self.uuid)

    @property
    def _rkey(self):
        if self.active and self.timeout:
            timeout = self._status[3] + self._status[4]
        elif self._status[2] == self.states.retry:
            timeout = 0
        else:
            return
        return self.flow._pkey + uint.encode(timeout) + self._bid
