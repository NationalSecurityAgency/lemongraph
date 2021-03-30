from . import lib, ffi, wire, listify_py2, unspecified
from .serializer import Serializer

class KV(object):
    def __init__(self, txn, domain, map_data=False, map_keys=False, serialize_domain=Serializer(), serialize_key=Serializer(), serialize_value=Serializer()):
        self._kv = None
        self.txn = txn
        self._txn = txn._txn
        self.domain = domain
        self.serialize_key    = serialize_key
        self.serialize_value  = serialize_value
        enc = serialize_domain.encode(domain)
        flags = 0
        if map_keys:
            flags |= lib.LG_KV_MAP_KEYS
        if map_data:
            flags |= lib.LG_KV_MAP_DATA
        self._dlen = ffi.new('size_t *')
        self._kv = lib.graph_kv(txn._txn, enc, len(enc), flags)
        if self._kv == ffi.NULL:
            self._kv = None
            raise KeyError(domain)

    def __getitem__(self, key):
        ekey = self.serialize_key.encode(key)
        data = lib.kv_get(self._kv, ekey, len(ekey), self._dlen)
        if data == ffi.NULL:
            raise KeyError(key)
        return self.serialize_value.decode(ffi.buffer(data, self._dlen[0])[:])

    def __setitem__(self, key, value):
        key = self.serialize_key.encode(key)
        value = self.serialize_value.encode(value)
        lib.kv_put(self._kv, key, len(key), value, len(value))

    def __delitem__(self, key):
        key = self.serialize_key.encode(key)
        r = lib.kv_del(self._kv, key, len(key))
        if not r:
            raise KeyError(key)

    def __contains__(self, key):
        key = self.serialize_key.encode(key)
        data = lib.kv_get(self._kv, key, len(key), self._dlen)
        return False if data == ffi.NULL else True

    def get(self, key, default=unspecified):
        try:
            return self[key]
        except KeyError:
            if default is unspecified:
                raise
            return default

    def pop(self, key, default=unspecified):
        try:
            try:
                return self[key]
            finally:
                del self[key]
        except KeyError:
            if default is unspecified:
                raise
            return default

    def iterkeys(self, pfx=None):
        return KVIterator(self, lambda obj: obj.key, pfx=pfx)

    def itervalues(self, pfx=None):
        return KVIterator(self, lambda obj: obj.value, pfx=pfx)

    def iteritems(self, pfx=None):
        return KVIterator(self, lambda obj: (obj.key, obj.value), pfx=pfx)

    keys = listify_py2(iterkeys)
    items = listify_py2(iteritems)
    values = listify_py2(itervalues)

    def __iter__(self):
        return self.iterkeys()

    def __len__(self):
        return sum(1 for k in self.iterkeys())

    def __del__(self):
        if self._kv is not None:
            lib.kv_deref(self._kv)
            self._kv = None

    @property
    def empty(self):
        for k in self.iterkeys():
            return False
        return True

    def clear(self, pfx=None):
        if pfx is None:
            return bool(lib.kv_clear(self._kv))
        pfx = wire.encode(pfx)
        return bool(lib.kv_clear_pfx(self._kv, pfx, len(pfx)));

    def next(self):
        key  = ffi.new('void **')
        data = ffi.new('void **')
        klen = ffi.new('size_t *')
        dlen = ffi.new('size_t *')

        if lib.kv_next(self._kv, key, klen, data, dlen):
            return (self.serialize_key.decode(ffi.buffer(key[0],  klen[0])[:]),
                  self.serialize_value.decode(ffi.buffer(data[0], dlen[0])[:]))
        raise IndexError()

    def next_key(self):
        key  = ffi.new('void **')
        data = ffi.new('void **')
        klen = ffi.new('size_t *')
        dlen = ffi.new('size_t *')

        if lib.kv_next(self._kv, key, klen, data, dlen):
            return self.serialize_key.decode(ffi.buffer(key[0],  klen[0])[:])
        raise IndexError()

class KVIterator(object):
    def __init__(self, kv, handler, pfx=None):
        self.serialize_key    = kv.serialize_key
        self.serialize_value  = kv.serialize_value
        self.handler = handler
        self._key = ffi.new('void **')
        self._data = ffi.new('void **')
        self._klen = ffi.new('size_t *')
        self._dlen = ffi.new('size_t *')
        if pfx is None or len(pfx) == 0:
            pfx = ffi.NULL
            pfxlen = 0
        else:
            pfx = wire.encode(pfx)
            pfxlen = len(pfx)
        self._iter = lib.kv_iter_pfx(kv._kv, pfx, pfxlen)

    def __iter__(self):
        return self

    def __next__(self):
        if not lib.kv_iter_next(self._iter, self._key, self._klen, self._data, self._dlen):
            lib.kv_iter_close(self._iter)
            self._iter = None
            raise StopIteration
        return self.handler(self)

    @property
    def key(self):
        return self.serialize_key.decode(ffi.buffer(self._key[0], self._klen[0])[:])

    @property
    def value(self):
        return self.serialize_value.decode(ffi.buffer(self._data[0], self._dlen[0])[:])

    def __del__(self):
        if self._iter is not None:
            lib.kv_iter_close(self._iter)
            self._iter = None

    next = __next__
