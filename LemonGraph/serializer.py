from . import ffi, lib
import msgpack as messagepack
import collections

def msgpack_encode_hashable(x):
    if not isinstance(x, collections.Hashable):
        raise ValueError(x)
    return messagepack.packb(x)

def msgpack_decode_hashable(x):
    return messagepack.unpackb(x, use_list=False)

# encode should support: arbitrary python object => bytes
# decode should support: python Buffer => object
# default for all encode/decode is 'str', except None maps to '' for encode
# for node/edge types/values as well as property keys, you should strive to make sure the encoder is deterministic
# if you plan to use complex values - if dicts are involved, msgpack is not so much.


class Serializer(object):
    @staticmethod
    def default(x):
        try:
            return '' if x is None else str(x)
        except UnicodeEncodeError:
            return x.encode('UTF-8')

    def __init__(self, encode=None, decode=None):
        self.encode = encode or self.default
        self.decode = decode or self.default

    @staticmethod
    def msgpack(hashable=False):
        return Serializer(encode=msgpack_encode_hashable, decode=msgpack_decode_hashable) if hashable else Serializer(encode=messagepack.packb, decode=messagepack.unpackb)

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
            return str(ret)

        def decode(b):
            return int(lib.unpack_uint(str(b)))

        return Serializer(encode=encode, decode=decode)

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
            return str(ret)

        def decode(b):
            lib.unpack_uints(count, decoded, str(b))
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
            string = str(n[-1])
            n = list(n[0:-1])
            strlen = len(string)
            n.append(strlen)
            size = lib.pack_uints(count, n, buffer)

            if size + strlen > 511:
                raise ValueError()

            buffer[size:size+strlen] = string
            size += strlen
            return str(ffi.buffer(buffer, size))

        def decode(b):
            size = lib.unpack_uints(count, decoded, str(b))
            strlen = decoded[count-1]
            buf = ffi.buffer(buffer, size + decoded[count-1])
            ret = list(int(decoded[i]) for i in xrange(0, count-1))
            ret.append(str(buf[size:]))
            return decode_type(ret)

        return Serializer(encode=encode, decode=decode)
