import msgpack
from zlib import crc32
from struct import pack
from six import iteritems

class Indexer(object):
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
                    try:
                        key = self.key(name, value)
                    except TypeError:
                        # fixme? silently ignore non-deterministic things
                        continue
                    keys.add(key)
        return keys

    def key(self, name, value, smash=True):
        hash(value)
        return str(name), pack('=i',crc32(msgpack.packb(value)))

    def prequery(self, index, value):
        key = self.key(index, value)
        method = self._idx[key[0]]
        def check(obj):
            return value in method(obj)
        return tuple(key) + (check,)
