# stock
from collections import deque
import logging
import multiprocessing
import os
import re
import select
import signal
import socket
from six import itervalues
from six.moves.urllib_parse import urlsplit
import sys
import time
import traceback
import zlib

from . import ffi, lib, wire

try:
    import ujson
    def json_encode(x):
        return ujson.dumps(x, escape_forward_slashes=False, ensure_ascii=False)
    json_decode = ujson.loads
except ImportError:
    import json
    def json_encode(x):
        return json.dumps(x, separators=(',', ':'), ensure_ascii=False)
    json_decode = lambda x: json.loads(wire.decode(x))

# pypi
from lazy import lazy

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

log_proc = logging.getLogger('LemonGraph.proc')
log_proc.addHandler(logging.NullHandler())

loglevels = {
    'info': log.info,
    'warn': log.warning,
    'debug': log.debug,
}

def _generator():
    yield

generator = type(_generator())
iterator = type(iter(''))
string_bin = type(b'')
string_uni = type(u'')

class Disconnected(Exception):
    def __init__(self, why, level='warn'):
        self.why = why
        logger = loglevels.get(level, log.debug)
        logger(str(self))

    def __repr__(self):
        return 'Disconnected(%s)' % repr(self.why)

    def __str__(self):
        return 'disconnected: %s' % self.why

class ErrorCompleted(Exception):
    pass


class HTTPError(Exception):
    def __init__(self, code, message, headers=None):
        self.code = int(code)
        self.message = message
        self.headers = headers or ()

    def __repr__(self):
        return "HTTPError(%d, %s, headers=%s)" % (self.code, repr(self.message), repr(self.headers))

    def __str__(self):
        return "%d: %s" % (self.code, self.message)


class HTTPMethods(object):
    all_methods = ('CONNECT', 'DELETE', 'GET', 'HEAD', 'OPTIONS', 'POST', 'PUT', 'TRACE')

    @lazy
    def _methods(self):
        methods = {}
        for m in self.all_methods:
            lm = m.lower()
            try:
                h = getattr(self, lm)
                methods[m] = methods[lm] = methods[m.encode()] = h
            except AttributeError:
                pass
        return methods

    @lazy
    def methods(self):
        return frozenset(m.lower() for m in self.all_methods if m in self._methods)

    @lazy
    def allowed(self):
        return ', '.join(sorted(self.methods))

    def method(self, m):
        try:
            handler = self._methods[m]
        except KeyError:
            raise AttributeError('Unsupported http method for object: %s' % repr(m))
        return handler


class Chunk(object):
    def __init__(self, bs):
        self.bs = bs = int(bs)
        header ='%x\r\n' % bs
        self.hlen = hlen = len(header)
        self.ba = bytearray(hlen + bs + 2)
        self.mem = memoryview(self.ba)
        self.header = self.mem[0:hlen]
        self.payload = self.mem[hlen:hlen+bs]
        self.payload_plus = self.mem[hlen:hlen+bs+2]
        self.hoffset = None
        self.size = None

    def __call__(self, size):
        self.size = size
        return self

    def reset(self):
        # set up default full chunk wrapper
        self.size = self.bs
        self._wrap(self.bs)

    @property
    def chunk(self):
        if self.size == self.bs:
            return self.mem
        return self._wrap(self.size)

    @property
    def body(self):
        return self.mem[self.hlen : self.hlen + self.size]

    def _wrap(self, size):
        # update header
        header = wire.encode('%x\r\n' % size)
        hlen = len(header)
        self.hoffset = self.hlen - hlen
        self.header[self.hoffset : self.hoffset + hlen] = header

        # add footer
        self.payload_plus[size:size+2] = b'\r\n'
        if size == self.bs:
            return self.mem
        return self.mem[self.hoffset : self.hoffset + self.size + hlen + 2]


class Chunks(object):
    def __init__(self, bs=1048576):
        # clamp output buffer size to be between 1k and 10m
        self.bs = bs = sorted((1024, int(bs), 10485760))[1]
        self._chunks = (Chunk(bs), Chunk(bs))

    def chunks(self):
        for chunk in self._chunks:
            chunk.reset()
            yield chunk

        while True:
            for chunk in self._chunks:
                yield chunk

    def chunkify(self, gen):
        bs = self.bs
        chunks = self.chunks()
        chunk = next(chunks)

        pos = 0
        for src in gen:
            src = wire.encode(src)
            slen = len(src)
            try:
                # fast append
                chunk.payload[pos:pos + slen] = src
                pos += slen
            except ValueError:
                # oops - too big - slice & dice
                soff = bs - pos
                # pad buffer out to end using first n bytes from src
                chunk.payload[pos:bs] = src[0:soff]
                yield chunk
                chunk = next(chunks)
                pos = 0

                # then carve off full blocks directly from src
                while soff + bs <= slen:
                    chunk.payload[0:bs] = src[soff:soff+bs]
                    yield chunk
                    chunk = next(chunks)
                    soff += bs

                # and stash the remainder
                pos = slen - soff
                chunk.payload[0:pos] = src[soff:soff+pos]

        if pos:
            yield chunk(pos)


class Service(object):
    def __init__(self, handlers=None, spawn=1, maxreqs=500, sock=None, host=None, port=None, timeout=10, extra_procs=None, buflen=1048576):
        if sock is None:
            if not host:
                host = '127.0.0.1'
            if host[0] == '@': # '@' => abstract unix domain
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.bind('\0' + host[1:])
            elif '/' in host: # '/' => unix domain
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.bind(host)
            else: # assume ip or resolvable local hostname
                sock = socket.socket(socket.AF_INET6 if ':' in host else socket.AF_INET, socket.SOCK_STREAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, True)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
                try:
                    sock.bind((host, 8000 if port is None else port))
                except socket.gaierror:
                    sock.close()
                    # fall back to unix domain
                    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    sock.bind(host)

        sock.listen(socket.SOMAXCONN)
        self.spawn = spawn
        self.maxreqs = maxreqs
        self.chunks = Chunks(bs=buflen)
        self.socks = [sock]
        self.timeout = timeout
        self.extra_procs = extra_procs or {}
        self.handlers = handlers or ()
        self.root = Step()

        for h in self.handlers:
            cursor = self.root
            for p in h.path:
                cursor = cursor.add(p)
            for m in h.methods:
                try:
                    getattr(cursor, m)
                except AttributeError:
                    setattr(cursor, m, (h, h.method(m)))
                    continue
                # method handler already existed - bogus
                raise AttributeError("Duplicate %s handlers for endpoint: %s" % (m.upper(), repr(h.path)))

    def run(self):
        extras = dict(enumerate(self.extra_procs.values(), start=1))
        labels = tuple(x.encode() for x in self.extra_procs)
        socks = ffi.new('int[]', [s.fileno() for s in self.socks])
        proc = logging.getLogger('LemonGraph.proc')
        sockd = logging.getLogger('LemonGraph.sockd')
        code = lib.server(socks, len(socks), (self.spawn or -1), tuple(ffi.from_buffer(x) for x in labels), len(labels), proc.level, sockd.level)
        if not code:
            return
        try:
            if code < 0:
                self.worker(socks[0])
            else:
                extras[code]()
        finally:
            for h in self.handlers:
                h.close()
            os._exit(0)

    def worker(self, wsock):
        w = lib.lg_worker_new(wsock)
        byte = ffi.buffer(ffi.addressof(w, 'byte'), 1)
        go = True
        while go:
            r = lib.lg_worker_accept(w)
            if 0 == r:
                return
            elif -1 == r:
                continue
            conn = BufferedSocket(socket.fromfd(w.conn, w.family, w.type, w.proto), byte[:])
            addr = conn.getpeername() or ('UNIX', 0)
            log.debug('client %s:%d: connected', *addr)
            try:
                while go:
                    self.maxreqs -= 1
                    res = Response(conn)
                    try:
                        try:
                            req = Request(conn, res, timeout=self.timeout)
                            self.process(req, res)
                        except HTTPError as e:
                            if e.code >= 500:
                                log.error('HTTP error %s', e)
                            elif e.code >= 400:
                                log.info('HTTP error %s', e)
                            res.error(e.code, e.message, *e.headers)
                    except ErrorCompleted:
                        pass
                    ended = time.time()
                    log.debug('response/finished ms: %d/%d', res.delay_ms, int((ended - res.start) * 1000))
                    code = keepalive = 'HTTP/1.1' == req.version and not res.headers.contains('Connection', 'close')
                    if keepalive and conn:
                        continue
                    go = self.maxreqs > 0
                    if not go:
                        code |= 2
                    lib.lg_worker_finish(w, code)
                    break
            except Disconnected as e:
                lib.lg_worker_finish(w, 0)
            except socket.timeout:
                log.warning('client %s:%d: timed out', *addr)
                lib.lg_worker_finish(w, 0)
            except Exception as e:
                info = sys.exc_info()
                trace = ''.join(traceback.format_exception(*info))
                log.error('Unhandled exception: %s', trace)
                os._exit(1)
            finally:
                conn.close()
                log.debug('client %s:%d: finished', *addr)

    def process(self, req, res):
        cursor = self.root
        try:
            for p in req.components:
                cursor = cursor.child(p)
        except KeyError:
            raise HTTPError(400, "Invalid endpoint: " + str(req.path) + " " + repr(req.components))
        try:
            h, handler = cursor.method(req.method)
        except AttributeError:
            if cursor.methods:
                raise HTTPError(405, "Unsupported method: %s" % req.method, headers=[('Allow', cursor.allowed)])
            else:
                raise HTTPError(400, "Invalid endpoint: " + str(req.path))

        body = None
        try:
            offset = h.offset
        except AttributeError:
            offset = 0
        try:
            h.init(req, res)
            body = handler(*req.components[offset:])
            self.handle(body, req, res)
        except Exception as e:
            if isinstance(e, HTTPError):
                raise
            info = sys.exc_info()
            trace = ''.join(traceback.format_exception(*info))
            log.error('Unhandled exception: %s', trace)
            raise HTTPError(500, "Unhandled exception in handler: %s" % repr(e))

    def handle(self, body, req, res):
        self.req = req
        self.res = res
        if isinstance(body, string_bin):
            return self._fixed(body)
        if isinstance(body, string_uni):
            return self._fixed(body.encode('UTF-8'))
        if isinstance(body, (generator, iterator, list, tuple, deque)):
            try:
                return self._chunked(body)
            finally:
                if isinstance(body, generator):
                    body.close()
        if body is None:
            return self._fixed(b'')
        raise TypeError('Unknown body object type: %s' % repr(type(body)))

    def _fixed(self, body):
        clen = len(body)
        self.res.headers.set('Content-Length', clen)
        if clen:
            self.res.begin(default=200)
            self.res.send(body)
        else:
            self.res.begin(default=204)

    def _chunked(self, body):
        chunks = self.chunks.chunkify(body)
        for first in chunks:
            for chunk in chunks:
                self.res.headers.set('Transfer-Encoding','chunked')
                self.res.begin(default=200)
                self.res.send(first.chunk)
                self.res.send(chunk.chunk)
                for chunk in chunks:
                    self.res.send(chunk.chunk)
                # send chunked xfer trailer
                return self.res.send('0\r\n\r\n')
            # only one chunk - send just the raw data as fixed
            return self._fixed(first.body)
        # no chunks - send empty response
        return self._fixed(b'')

    def __del__(self):
        try:
            for sock in self.socks:
                sock.close()
        except AttributeError:
            pass


class Response(object):
    codes = {
        200: 'OK',
        201: 'Created',
        204: 'No Content',
        304: 'Not Modified',
        400: 'Bad Request',
        403: 'Forbidden',
        404: 'Not Found',
        405: 'Method Unavailable',
        406: 'Not Acceptable',
        409: 'Conflict',
        500: 'Internal Server Error',
        502: 'Bad Gateway',
        503: 'Service Unavailable',
        505: 'HTTP Version Not Supported',
        507: 'Insufficient Storage',
    }

    no_body = set([
        304,
    ])

    def __init__(self, sock):
        self._code = None
        self.sock = sock
        self.begun = False
        self.message = None
        self.version = 'HTTP/1.0'
        self.headers = Headers()
        self.start = time.time()

    def begin(self, code=None, default=None):
        if self.begun:
            return

        if code:
            self.code = code
        elif default and not self._code:
            self.code = default
        self.begun = time.time()
        self.delay_ms = int((self.begun - self.start) * 1000)
        self.headers.set('x-delay-ms', self.delay_ms)
        self.send('%s %d %s\r\n' % (self.version, self.code, self.codes[self.code]))
        self.send(str(self.headers))

    def error(self, code, message, *headers):
        body = {
            'code': code,
            'reason': self.codes[code],
            'message': message,
        }
        js = '' if code in self.no_body else json_encode(body) + '\n'
        if self.begun:
            # oops - log error to console and just disconnect
            raise Disconnected(str(body))
        close = self.headers.contains('Connection', 'close')
        self.headers.reset()
        for header in headers:
            self.headers.set(*header)
        if js:
            self.headers.set('Content-Type', 'application/json')
            self.headers.set('Content-Length', len(js))
        if close:
            self.headers.set('Connection', 'close')
        self.begin(code=code)
        self.send(js)
        raise ErrorCompleted()

    def send(self, *data):
        try:
            for d in data:
                self.sock.send(wire.encode(d))
        except Exception as e:
            info = sys.exc_info()
            trace = ''.join(traceback.format_exception(*info))
            log.error('Unhandled exception: %s', trace)
            raise Disconnected(str(e))

    @property
    def code(self):
        return self._code or 200

    @code.setter
    def code(self, newcode):
        if newcode not in self.codes:
            raise Exception('unsupported http error code: %d' % newcode)
        self._code = newcode

    def json(self, doc):
        return json_encode(doc)


class BufferedSocket(object):
    class EOF(Exception):
        pass

    def __init__(self, sock, *data):
        self.sock = sock
        self.chunks = deque(bytes(d) for d in data)
        self.size = sum(len(d) for d in data)

    def _chunks(self, bs):
        while self.size:
            chunk = self.chunks.popleft()
            self.size -= len(chunk)
            yield chunk
        while bs:
            chunk = self.recv(bs)
            if not chunk:
                raise self.EOF
            yield chunk

    def _readline(self, eol=b'\n', bs=1024):
        for chunk in self._chunks(bs):
            i = chunk.find(eol)
            if -1 == i:
                yield chunk
                continue
            i += 1
            yield chunk[0:i]
            rem = chunk[i:]
            if rem:
                self.chunks.appendleft(rem)
                self.size += len(rem)
            return

    def readline(self, eol=b'\n', bs=1024):
        return b''.join(self._readline(eol, bs))

    def read(self, size):
        if not size:
            raise RuntimeError
        ret = bytearray(size)
        off = 0
        for chunk in self._chunks(0):
            clen = len(chunk)
            if size >= clen:
                ret[off:off+clen] = chunk
                if size > clen:
                    off += clen
                    size -= clen
                    continue
            else: # size < clen
                ret[off:off+size] = chunk[0:size]
                self.chunks.appendleft(chunk[size:])
                self.size += len(self.chunks[0])
            return bytes(ret)
        view = memoryview(ret)
        while size:
            v = view[-size:]
            n = self.sock.recv_into(v, size)
            if 0 == n:
                raise self.EOF
            size -= n
        return bytes(ret)

    if sys.version_info[0] < 3:
        def __nonzero__(self):
            return bool(self.size)
    else:
        def __bool__(self):
            return bool(self.size)

    def __dir__(self):
        return dir(self.sock) + ('read', 'readline')

    def __getattr__(self, attr):
        return getattr(self.sock, attr)


class Request(object):
    req = re.compile('^(' + '|'.join(HTTPMethods.all_methods) + r') (.+?)(?: (HTTP/[0-9.]+))?(\r?\n)$')
    hsplit = re.compile(r':\s*')

    def __init__(self, sock, response, timeout=10):
        self.sock = sock
        self.headers = Headers()
        self.timeout = timeout
        self.method = None
        self.uri = None
        self.version = None
        self.path = None
        self.response = response

        self._load_request_headers()

    # returns generator for posted content
    @lazy
    def body(self):
        if self.method not in ('POST','PUT'):
            return tuple()

        # RFC 2616 says that if Transfer-Encoding is set to anything other than 'identity', than
        # it is to be treated as chunked, and Content-Length does not apply.
        #
        # Apparently, curl only checks for 'chunked' exactly - try this:
        #   cat foo | curl -H 'Transfer-Encoding: chunked' -XPOST --data-binary @/dev/stdin -i http://$host:$port
        try:
            chunked = self.headers.contains('Transfer-Encoding', 'chunked')
        except KeyError:
            chunked = False
        # if we are not using chunked - require a content-length.
        # fixme - is that kosher? or can we read until EOF for HTTP/1.0 connections??
        try:
            content_length = None if chunked else int(str(self.headers['Content-Length']))
        except KeyError:
            content_length = 0

        if self.headers.contains('Expect','100-continue'):
            self.sock.send(b'HTTP/1.1 100 Continue\r\n\r\n')
            log.debug('continued!')

        # fixme - I'm not super sure how Transfer-Encoding and Content-Encoding get used
        # in the wild - can you specify both chunked and gzip (or other) in TE? Does order matter?
        body = self._body_chunked() if chunked else self._body_raw(content_length)
        if self.headers.contains('Content-Encoding','gzip'):
            body = zcat(body)
        return body

    def _load_request_headers(self):
        deadline = time.time() + self.timeout
        self.sock.settimeout(self.timeout)
        try:
            line = wire.decode(self.sock.readline())
        except BufferedSocket.EOF:
            raise Disconnected('bad request') if self.sock else Disconnected('no request', level='debug')

        m = self.req.match(line)
        try:
            self.method = m.group(1)
        except AttributeError:
            raise Disconnected('bad request: ' + repr(line))
        self.uri = m.group(2)
        self.version = m.group(3) or 'HTTP/1.0'
        if self.version not in ('HTTP/1.0', 'HTTP/1.1'):
            raise HTTPError(505, self.version)
        self.response.version = self.version
        parts = urlsplit(self.uri)
        self.path = parts.path
        self.components = tuple(x for x in self.path.split('/') if x)
        self.query = parts.query

        log.info('%s\t%s\t%s', self.method, self.path, self.query)

        while True:
            now = time.time()
            if now >= deadline:
                raise socket.timeout()
            self.sock.settimeout(deadline - now)
            try:
                line = wire.decode(self.sock.readline())
            except BufferedSocket.EOF:
                raise Disconnected('bad request')
            if line == '\r\n':
                # done parsing headers
                self.sock.settimeout(None)
                return
            elif '\r\n' != line[-2:]:
                raise Disconnected('bad header line: ' + line)
            try:
                h, v = self.hsplit.split(line[0:-2], maxsplit=1)
            except ValueError:
                raise Disconnected('invalid header line: ' + line[0:-2])
            self.headers.add(h, v)

    def _body_chunked(self):
        while True:
            try:
                h = self.sock.readline()
            except BufferedSocket.EOF:
                raise Disconnected('bad chunk header')
            if len(h) < 3 or h[-2:] != b'\r\n':
                raise Disconnected('bad chunk header')
            chunklen = int(h.rstrip(), base=16) + 2
            try:
                chunk = self.sock.read(chunklen)
            except BufferedSocket.EOF:
                raise Disconnected('short data chunk')
            if chunklen > 2:
                yield chunk[0:-2]
            else:
                return

    def _body_raw(self, bytes, buflen=32768):
        while bytes > 0:
            r = buflen if bytes > buflen else bytes
            try:
                x = self.sock.read(r)
            except BufferedSocket.EOF:
                raise Disconnected('early eof')
            bytes -= len(x)
            yield x


class Header(object):
    @staticmethod
    def clean(*values):
        if len(values) == 1:
            values = list(s for s in str(values[0]).split(',') if len(s) > 0)
        else:
            values = list(s for s in map(str, values) if len(s) > 0)
        return values

    def __init__(self, label, *values):
        self.label = str(label)
        self.values = self.clean(*values)

    def __contains__(self, val):
        return str(val) in self.values

    def __repr__(self):
        return 'Header(%s, %s)' % (repr(self.label), repr(list(self.values)))

    def __iter__(self):
        return iter(self.values)

    def __getitem__(self, idx):
        return self.values[idx]

    def __str__(self):
        return ','.join(self.values)

    def __delitem__(self, value):
        v = str(value)
        self.values = list(val for val in self.values if val != v)

    def __len__(self):
        return len(self.values)

    def set(self, *values):
        self.values = self.clean(*values)

    def add(self, *values):
        self.values.extend(v for v in self.clean(*values) if v not in self)


class Headers(object):
    @staticmethod
    def norm(header):
        return str(header).lower()

    def __init__(self):
        self.data = {}

    def reset(self):
        self.data = {}

    def items(self):
        for h in itervalues(self.data):
            yield h.label, str(h)

    def __str__(self):
        lines = ['%s: %s' % h for h in self.items()]
        lines.extend(('', ''))
        return '\r\n'.join(lines)

    def __contains__(self, header):
        return self.norm(header) in self.data

    def __setitem__(self, header, value):
        if isinstance(value, (str, u'')):
            self.data[self.norm(header)] = Header(header, value)
        else:
            self.data[self.norm(header)] = Header(header, *value)

    def __getitem__(self, header):
        return self.data[self.norm(header)]

    def __delitem__(self, header):
        del self.data[self.norm(header)]

    def add(self, header, *values):
        try:
            self.data[self.norm(header)].add(*values)
        except KeyError:
            self.set(header, *values)

    def set(self, header, *values):
        if values:
            self.data[self.norm(header)] = Header(header, *values)
        else:
            del self.data[self.norm(header)]

    def put(self, header, *values):
        hl = self.norm(header)
        if values and hl not in self.data:
            self.data[hl] = Header(header, *values)

    def contains(self, header, value, icase=False):
        try:
            values = self.data[self.norm(header)].values
        except KeyError:
            return False
        if icase:
            value = value.lower()
            values = (v.lower() for v in values)
        return str(value) in values


def zcat(gen):
    dec = zlib.decompressobj(16 + zlib.MAX_WBITS)
    try:
        for chunk in gen:
            yield dec.decompress(chunk)
        final = dec.flush()
        if len(final):
            yield final
    except zlib.error as e:
        raise Disconnected('zlib error: ' + e.message)


class Step(HTTPMethods):
    re_type = type(re.compile('super lame'))

    def __init__(self):
        self.steps = {}
        self.regex = ()
        self.Handler = None

    def add(self, item):
        try:
            return self.steps[item]
        except KeyError:
            pass

        if isinstance(item, self.re_type):
            self.regex = self.regex + (item,)
        step = self.steps[item] = Step()
        return step

    def __repr__(self):
        return repr(self.steps)

    def child(self, p):
        try:
            return self.steps[p]
        except KeyError:
            pass

        for reg in self.regex:
            m = reg.search(p)
            if m is not None:
                return self.steps[reg]

        raise KeyError(p)


def httpd(**kwargs):
    service = Service(**kwargs)
    service.run()

if '__main__' == __name__:
    httpd()
