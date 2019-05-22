import msgpack
from zlib import crc32
from struct import pack
from six import iteritems

class BaseIndexer(object):
    def __init__(self):
        idx = {}
        for name in dir(self):
            # discover subclass indexing methods
            if not name.startswith('idx_'):
                continue
            method = getattr(self, name)
            if callable(method):
                name = name[4:]
                idx[name] = method
        self._idx = idx
        self._index = {}

    def index(self, obj):
        keys = set()
        if obj is not None:
            for name, method in iteritems(self._idx):
                for value in method(obj):
                    key = self.key(name, value)
                    keys.add(key)
        return keys

    def key(self, name, value):
        return name, value

class Indexer(BaseIndexer):
    # python 2.7: crc32 returns a signed 32-bit int
    # python 3.4: crc32 returns an unsigned 32-bit int
    fmt = '=i' if crc32(b'foo') < 0 else '=I'

    def key(self, name, value):
        hash(value)
        return str(name), pack(self.fmt, crc32(msgpack.packb(value, use_bin_type=False)))

    def prequery(self, index, value):
        key = self.key(index, value)
        method = self._idx[key[0]]
        def check(obj):
            return value in method(obj)
        return tuple(key) + (check,)
