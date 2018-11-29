from . import ffi, lib, wire
import msgpack as messagepack
import collections
import sys

try:
    xrange          # Python 2
except NameError:
    xrange = range  # Python 3

# Python 2.6 is generally angry about newer msgpack
# This is enough enough of a hack to make tests pass
try:
    memoryview
except NameError:
    import struct

    # bolt itemsize onto the string class to let
    # it look enough like memoryview for msgpack
    class memoryview_ish(str):
        itemsize = 1
    # and install it into msgpack's namespace
    messagepack.fallback.memoryview = memoryview_ish

    # in addition, looks like struct.unpack_from
    # gets angry when you feed it bytearrays, so
    # monkey patch that too
    def wrap_unpack_from():
        func = struct.unpack_from
        def unpack_from_wrapper(*args, **kwargs):
            if isinstance(args[1], bytearray):
                args = list(args)
                args[1] = str(args[1])
            return func(*args, **kwargs)
        return unpack_from_wrapper
    struct.unpack_from = wrap_unpack_from()

    # probably the right thing to do is to stop supporting 2.6...


# encode should support: arbitrary python object => bytes
# decode should support: python Buffer => object
# default for all encode/decode is 'str', except None maps to '' for encode
# for node/edge types/values as well as property keys, you should strive to make sure the encoder is deterministic
# if you plan to use complex values - if dicts are involved, msgpack is not so much.

def identity(x):
    return x

class Serializer(object):
    @staticmethod
    def str_encode(x):
        if x is None:
            return b''
        try:
            return wire.encode(x)
        except TypeError:
            return wire.encode(str(x))

    str_decode = staticmethod(wire.decode)

    def __init__(self, encode=None, decode=None):
        self.encode = encode or self.str_encode
        self.decode = decode or self.str_decode

    @staticmethod
    def msgpack(hashable=False):
        if hashable:
            def encode(x):
                if not isinstance(x, collections.Hashable):
                    raise ValueError(x)
                return messagepack.packb(x)

            def decode(x):
                return messagepack.unpackb(x, raw=False, use_list=False)

        else:
            def encode(x):
                return messagepack.packb(x)

            def decode(x):
                return messagepack.unpackb(x, raw=False)

        return Serializer(encode=encode, decode=decode)

    @staticmethod
    def uint():
        buffer = ffi.new('char[]', 9)
        buffers = {}

        def encode(n):
            size = lib.pack_uint(int(n), buffer)
            try:
                ret = buffers[size]
            except KeyError:
                ret = buffers[size] = ffi.buffer(buffer, size)
            return ret[:]

        def decode(b):
            return int(lib.unpack_uint(b[:]))

        return Serializer(encode=encode, decode=decode)

    @classmethod
    def null(cls):
        return Serializer(encode=identity, decode=identity)

    @classmethod
    def uints(cls, count, decode_type=tuple, string=False):
        if string:
            # last entry in tuple is a string - encode w/ length & data
            return cls._uints_string(count, decode_type=decode_type)
        count = int(count)
        if count < 1:
            raise ValueError(count)
        buffer = ffi.new('char[]', count*9)
        buffers = {}
        decoded = ffi.new('uint64_t[]', count)
        def encode(n):
            if len(n) != count:
                raise ValueError(n)
            size = lib.pack_uints(count, n, buffer)
            try:
                ret = buffers[size]
            except KeyError:
                ret = buffers[size] = ffi.buffer(buffer, size)
            return ret[:]

        def decode(b):
            lib.unpack_uints(count, decoded, b[:])
            return decode_type(int(decoded[i]) for i in xrange(0, count))

        return Serializer(encode=encode, decode=decode)

    @staticmethod
    def _uints_string(count, decode_type=tuple):
        count = int(count)
        if count < 1:
            raise ValueError(count)
        buffer = ffi.new('char[511]')
        decoded = ffi.new('uint64_t[]', count)
        def encode(n):
            if len(n) != count:
                raise ValueError(n)
            string = wire.encode(n[-1])
            n = list(n[0:-1])
            strlen = len(string)
            n.append(strlen)
            size = lib.pack_uints(count, n, buffer)

            if size + strlen > 511:
                raise ValueError()

            buffer[size:size+strlen] = string
            size += strlen
            return ffi.buffer(buffer, size)[:]

        def decode(b):
            size = lib.unpack_uints(count, decoded, b[:])
            buf = ffi.buffer(buffer, size + decoded[count-1])
            ret = list(int(decoded[i]) for i in xrange(0, count-1))
            ret.append(wire.decode(buf[size:]))
            return decode_type(ret)

        return Serializer(encode=encode, decode=decode)
