from . import lib, ffi, lazy, unspecified
from .serializer import Serializer

from collections import deque

class Fifo(object):
    def __init__(self, txn, domain, map_values=False, serialize_domain=Serializer(), serialize_value=Serializer()):
        self.txn = txn
        self._txn = txn._txn
        self.domain = domain
        self.serialize_value  = serialize_value
        enc = serialize_domain.encode(domain)
        flags = lib.LG_KV_MAP_DATA if map_values else 0
        self._dlen = ffi.new('size_t *')
        self._kv = lib.graph_kv(txn._txn, enc, len(enc), flags)
        if self._kv == ffi.NULL:
            self._kv = None
            raise KeyError(domain)

    def push(self, *values):
        evals = tuple(self.serialize_value.encode(v) for v in values)
        data = tuple(ffi.from_buffer(e) for e in evals)
        dlen = tuple(len(e) for e in evals)
        r = lib.kv_fifo_push_n(self._kv, data, dlen, len(dlen))
        if len(dlen) != r:
            raise IOError()

    def pop(self, count=unspecified):
        n = 1 if count is unspecified else int(count)
        data = ffi.new('void *[]', n)
        dlen = ffi.new('size_t []', n)
        n2 = lib.kv_fifo_peek_n(self._kv, data, dlen, n)
        if n2 < 0:
            raise IOError()
        i = 0
        ret = []
        while i < n2:
            ret.append(self.serialize_value.decode(ffi.buffer(data[i], dlen[i])[:]))
            i += 1
        lib.kv_fifo_delete(self._kv, n2)
        return ret[0] if count is unspecified else ret

    @property
    def empty(self):
        return lib.kv_first_key(self._kv, self._dlen) == ffi.NULL

    def __len__(self):
        dlen = ffi.new('uint64_t *')
        r = lib.kv_fifo_len(self._kv, dlen)
        if r:
            raise IOError()
        return int(dlen[0])

    def __del__(self):
        if self._kv is not None:
            lib.kv_deref(self._kv)
            self._kv = None

    def __iter__(self):
        for key, data in  KVIterator(self):
            yield data

    def clear(self):
        return bool(lib.kv_clear(self._kv));


class KVIterator(object):
    def __init__(self, kv):
        self.serialize_value = kv.serialize_value
        self._key = ffi.new('void **')
        self._klen = ffi.new('size_t *')
        self._data = ffi.new('void **')
        self._dlen = ffi.new('size_t *')
        self._iter = lib.kv_iter(kv._kv)

    def __iter__(self):
        return self

    def __next__(self):
        if not lib.kv_iter_next(self._iter, self._key, self._klen, self._data, self._dlen):
            lib.kv_iter_close(self._iter)
            self._iter = None
            raise StopIteration
        return self.serialize_value.decode(ffi.buffer(self._data[0], self._dlen[0])[:])

    def __del__(self):
        if self._iter is not None:
            lib.kv_iter_close(self._iter)
            self._iter = None

    next = __next__
