from . import lib, ffi, wire
from .serializer import Serializer

class PQueue(object):
    def __init__(self, txn, domain, map_values=False, serialize_domain=Serializer(), serialize_value=Serializer()):
        self.txn = txn
        self._txn = txn._txn
        self.domain = domain
        self.serialize_value  = serialize_value
        enc = serialize_domain.encode(domain)
        flags = 0
        if map_values:
            flags |= lib.LG_KV_MAP_KEYS
        self._dlen = ffi.new('size_t *')
        self._kv = lib.graph_kv(txn._txn, enc, len(enc), flags)
        if self._kv == ffi.NULL:
            self._kv = None
            raise KeyError(domain)

    # zero is low priority, 255 is high
    def add(self, value, priority=0):
        priority = int(sorted([0, priority, 255])[1])
        evalue = self.serialize_value.encode(value)
        # invert priority, because internally 0 is high, 255 is low
        ret = lib.kv_pq_add(self._kv, evalue, len(evalue), 255 - priority)
        if ret < 0:
            raise KeyError(value)

    def get(self, value):
        evalue = self.serialize_value.encode(value)
        ret = lib.kv_pq_get(self._kv, evalue, len(evalue))
        if ret < 0:
            raise KeyError(value)
        # invert priority
        return 255 - ret

    def remove(self, value):
        evalue = self.serialize_value.encode(value)
        r = lib.kv_pq_del(self._kv, evalue, len(evalue))
        if r:
            raise KeyError(value)

    @property
    def empty(self):
        for x in iter(self):
            return False
        return True

    def __getitem__(self, value):
        return self.get(value)

    def __contains__(self, value):
        try:
            self.get(value)
            return True
        except KeyError:
            return False

    def __iter__(self):
        return PQueueIterator(self)

    def __del__(self):
        if self._kv is not None:
            lib.kv_deref(self._kv)
            self._kv = None

    def clear(self):
        lib.kv_clear(self._kv)

    def cursor(self):
        return PQueueCursor(self)

class PQueueIterator(object):
    def __init__(self, kv):
        self.decode  = kv.serialize_value.decode
        self._data = ffi.new('void **')
        self._dlen = ffi.new('size_t *')
        self._iter = lib.kv_pq_iter(kv._kv)

    def __iter__(self):
        return self

    def __next__(self):
        if not lib.kv_pq_iter_next(self._iter, self._data, self._dlen):
            lib.kv_iter_close(self._iter)
            self._iter = None
            raise StopIteration
        return self.decode(ffi.buffer(self._data[0], self._dlen[0])[:])

    def __del__(self):
        if self._iter is not None:
            lib.kv_iter_close(self._iter)
            self._iter = None

    next = __next__

# PQ iterator that will work across transactions
class PQueueCursor(object):
    def __init__(self, kv):
        self.decode = kv.serialize_value.decode
        self._bookmark = lib.kv_pq_cursor(kv._kv, 0)
        if self._bookmark == ffi.NULL:
            raise RuntimeError
        self._data = ffi.new('void **')
        self._dlen = ffi.new('size_t *')

    def next(self, txn):
        priority = lib.kv_pq_cursor_next(txn._txn, self._bookmark, self._data, self._dlen)
        if priority < 0:
            raise IndexError
        val = self.decode(ffi.buffer(self._data[0], self._dlen[0])[:])
        return val, 255 - priority

    def __del__(self):
        if self._bookmark not in (None, ffi.NULL):
            lib.kv_pq_cursor_close(self._bookmark)
            self._bookmark = None
