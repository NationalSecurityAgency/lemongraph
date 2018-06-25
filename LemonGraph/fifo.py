from . import lib, ffi, lazy
from .serializer import Serializer

from collections import deque

UNSPECIFIED = object()

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
        self._idx = None
        if self._kv == ffi.NULL:
            self._kv = None
            raise KeyError(domain)

    @lazy
    def serialize_key(self):
        return Serializer.uint()

    def push(self, *data):
        if self._idx is None:
            key = lib.kv_last_key(self._kv, self._dlen)
            if key == ffi.NULL:
                self._idx = -1
            else:
                self._idx = self.serialize_key.decode(ffi.buffer(key, self._dlen[0]))
        for d in data:
            self._idx += 1
            try:
                key = self.serialize_key.encode(self._idx)
            except OverflowError:
                # rebase entries
                offset = None
                for idx, d2 in KVIterator(self):
                    try:
                        key = self.serialize_key.encode(idx - offset)
                    except TypeError:
                        offset = idx
                        key = self.serialize_key.encode(0)
                    value = self.serialize_value.encode(d2)
                    if not lib.kv_put(self._kv, key, len(key), value, len(value)):
                        raise IOError()

                # stash new target
                self._idx = 0 if offset is None else idx - offset + 1

                # remove leftovers
                idx = max(self._idx + 1, offset)
                try:
                    while True:
                        key = self.serialize_key.encode(idx)
                        lib.kv_del(self._kv, key, len(key))
                        idx += 1
                except OverflowError:
                    pass

                # encode the new key
                key = self.serialize_key.encode(self._idx)
            value = self.serialize_value.encode(d)
            if not lib.kv_put(self._kv, key, len(key), value, len(value)):
                raise IOError()

    def pop(self, n=UNSPECIFIED):
        count = 1 if n is UNSPECIFIED else int(n)
        ret = deque()
        nuke = deque()
        try:
            for idx, data in KVIterator(self):
                ret.append(data)
                nuke.append(idx)
                if len(ret) == count:
                    raise StopIteration
            self._idx = 0
        except StopIteration:
            pass

        for idx in nuke:
            enc = self.serialize_key.encode(idx)
            lib.kv_del(self._kv, enc, len(enc))
        return ret[0] if n is UNSPECIFIED else tuple(ret)

    @property
    def empty(self):
        return lib.kv_last_key(self._kv, self._dlen) == ffi.NULL

    def __len__(self):
        last = lib.kv_last_key(self._kv, self._dlen)
        if last == ffi.NULL:
            return 0
        last = self.serialize_key.decode(ffi.buffer(last, self._dlen[0]))
        for idx, _ in KVIterator(self):
            return 1+last-idx
        raise Exception

    def __del__(self):
        if self._kv is not None:
            lib.kv_deref(self._kv)
            self._kv = None

class KVIterator(object):
    def __init__(self, kv):
        self.serialize_key    = kv.serialize_key
        self.serialize_value  = kv.serialize_value
        self._key = ffi.new('void **')
        self._data = ffi.new('void **')
        self._klen = ffi.new('size_t *')
        self._dlen = ffi.new('size_t *')
        self._iter = lib.kv_iter(kv._kv)

    def __iter__(self):
        return self

    def next(self):
        if not lib.kv_iter_next(self._iter, self._key, self._klen, self._data, self._dlen):
            lib.kv_iter_close(self._iter)
            self._iter = None
            raise StopIteration
        return self.key, self.value

    @property
    def key(self):
        return self.serialize_key.decode(ffi.buffer(self._key[0], self._klen[0]))

    @property
    def value(self):
        return self.serialize_value.decode(ffi.buffer(self._data[0], self._dlen[0]))

    def __del__(self):
        if self._iter is not None:
            lib.kv_iter_close(self._iter)
            self._iter = None
