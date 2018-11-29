from . import lib, ffi, wire
from .serializer import Serializer

UNSPECIFIED = object()

class SSet(object):
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

    def add(self, value):
        value = self.serialize_value.encode(value)
        return bool(lib.kv_put(self._kv, value, len(value), b'', 0))

    def remove(self, value):
        value = self.serialize_value.encode(value)
        r = lib.kv_del(self._kv, value, len(value))
        if not r:
            raise KeyError(value)

    def pop(self, n=UNSPECIFIED, default=UNSPECIFIED):
        if n is UNSPECIFIED:
            for ret in self:
                self.remove(ret)
                return ret
            if default is UNSPECIFIED:
                raise IndexError
            ret = default
        else:
            try:
                self.remove(n)
                ret = n
            except KeyError:
                if default is UNSPECIFIED:
                    raise
                ret = default
        return ret

    def __contains__(self, value):
        value = self.serialize_value.encode(value)
        data = lib.kv_get(self._kv, value, len(value), self._dlen)
        return False if data == ffi.NULL else True

    def __iter__(self):
        return SSetIterator(self, lambda obj: obj.key)

    def iterpfx(self, pfx=None):
        return SSetIterator(self, lambda obj: obj.key, pfx=pfx)

    def __len__(self):
        return sum(1 for k in iter(self))

    def __del__(self):
        if self._kv is not None:
            lib.kv_deref(self._kv)
            self._kv = None

    @property
    def empty(self):
        for x in self:
            return False
        return True

class SSetIterator(object):
    def __init__(self, kv, handler, pfx=None):
        self.serialize_value  = kv.serialize_value
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
        return self.serialize_value.decode(ffi.buffer(self._key[0], self._klen[0])[:])

    def __del__(self):
        if self._iter is not None:
            lib.kv_iter_close(self._iter)
            self._iter = None

    next = __next__
