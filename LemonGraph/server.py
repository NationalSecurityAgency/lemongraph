from . import Serializer, Node, Edge, Adapters, QuerySyntaxError, merge_values
from .collection import Collection, uuid_to_utc
from .lock import Lock

import atexit
from collections import deque, defaultdict, namedtuple
import dateutil.parser
import errno
from getopt import GetoptError, gnu_getopt as getopt
import itertools
from lazy import lazy
import logging
import msgpack
import multiprocessing
import os
import pkg_resources
import re
import sys
import tempfile
import time
import traceback
from uuid import uuid1 as uuidgen

from urlparse import parse_qs

from .httpd import HTTPMethods, HTTPError, httpd, Graceful, json_encode, json_decode

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

CACHE = {}
BLOCKSIZE=1048576
UUID = re.compile(r'^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$')
INT = re.compile(r'^[1-9][0-9]*$')
STRING = re.compile(r'^.+$')

def date_to_timestamp(s):
    dt = dateutil.parser.parse(s)
    return time.mktime(dt.utctimetuple()) + dt.microsecond / 1e6

class LogHandler(logging.StreamHandler):
    def __init__(self, stream=sys.stdout):
        logging.StreamHandler.__init__(self, stream)
        fmt = '%(asctime)-8s %(name)s(%(process)d).%(levelname)s: %(message)s'
        fmt_date = '%Y-%m-%d %T %Z'
        formatter = logging.Formatter(fmt, fmt_date)
        self.setFormatter(formatter)


def js_dumps(obj, pretty=False):
    txt = json_encode(obj)
    if pretty:
        txt += b'\n'
    return txt

def mp_dumps(obj, pretty=False):
    return msgpack.packb(obj)

Streamer = namedtuple('Streamer', 'mime encode')
streamJS = Streamer('application/json', js_dumps)
streamMP = Streamer('application/x-msgpack', mp_dumps)

Streamers = dict((x.mime, x) for x in (streamJS, streamMP))

Edge_Reserved = Edge.reserved
Node_Reserved = Node.reserved | frozenset(['depth'])

class SeedTracker(object):
    msgpack = Serializer.msgpack()
    uint = Serializer.uint()

    def __init__(self, txn):
        self.txn = txn

    @lazy
    def _seed(self):
        return self.txn.kv('lg.seeds', serialize_key=self.uint, serialize_value=self.msgpack)

    def add(self, data):
        idx = self._seed.get(0, 1)
        self._seed[idx] = data
        self._seed[0] = idx + 1

    @property
    def seeds(self):
        try:
            gen = self._seed.itervalues()
            gen.next()
        except (KeyError, StopIteration):
            return ()
        return gen

class Handler(HTTPMethods):
    content_types = {
        'application/json': json_decode,
        'application/x-msgpack': lambda x: msgpack.unpackb(x, use_list=False),
    }
    multi = ()
    single = ()
    single_parameters = {}

    def __init__(self, collection_path=None, graph_opts=None):
        self.collection_path = collection_path
        self.graph_opts = {} if graph_opts is None else graph_opts

    @lazy
    def collection(self):
        # hackily init the collection object once per httpd worker
        # using nosync here seems to be a really good idea, since it
        # improves performance a ton, and the index is just a rebuildable
        # cache anyway. Disabling metasync doesn't appear to help significantly.
        global collection
        if collection is None:
            log.debug('worker collection init')
            collection = Collection(self.collection_path, graph_opts=self.graph_opts, nosync=True, nometasync=False)
            atexit.register(collection.close)
        return collection

    @lazy
    def lock(self):
        global lock
        return lock

    @property
    def content_type(self):
        try:
            return str(self.req.headers['Content-Type'])
        except KeyError:
            pass

    @property
    def content_length(self):
        try:
            return int(str(self.req.headers['Content-Length']))
        except:
            #if self.req.headers.contains('Transfer-Encoding', 'chunked'):
            pass

    @property
    def body(self):
        return self.req.body

    @property
    def params(self):
        return parse_qs(self.req.query, keep_blank_values=1)

    # grab latest param only
    def param(self, field, default=None):
        return self.params.get(field,[default])[-1]

    def input(self):
        try:
            encoded = ''.join(self.body)
            if len(encoded) == 0:
                return None
            unpacker = self.content_types[self.content_type]
            data = unpacker(encoded)
        except KeyError:
            raise HTTPError(409, 'Bad/missing mime type (%s) - use one of: %s' % (self.content_type, ', '.join(self.content_types.keys())))
        except ValueError:
            raise HTTPError(400, 'Decode failed for mime type: %s' % self.content_type)
        except HTTPError:
            raise
        except Exception as e:
            info = sys.exc_info()
            log.error('Unhandled exception: %s', traceback.print_exception(*info))
            raise HTTPError(409, 'Bad data - %s decode failed' % self.content_type)
        return data

    # called for each request
    def init(self, req, res):
        self.req = req
        self.res = res
        if req.headers.contains('Accept', 'application/x-msgpack', icase=True):
            self.streamer = Streamers['application/x-msgpack']
        else:
            self.streamer = Streamers['application/json']
        self.dumps = self.streamer.encode
        self.res.headers.set('Content-Type', self.streamer.mime)

    def format_edge(self, e):
        d = e.as_dict()
        d['src'] = { "ID": e.src.ID, "type": e.src.type, "value": e.src.value }
        d['tgt'] = { "ID": e.tgt.ID, "type": e.tgt.type, "value": e.tgt.value }
        del d['srcID']
        del d['tgtID']
        return d

    @property
    def creds(self):
        return {
            'user':  self.param('user', None),
            'roles': self.params.get('role', None),
        }

    @property
    def graphs_filter(self):
        filter = self.creds
        filter['enabled'] = True if 'enabled' in self.params else None
        filter['filters'] = self.params.get('filter', None)
        for created in ('created_after', 'created_before'):
            s = self.param(created, None)
            if s:
                try:
                    filter[created] = date_to_timestamp(s)
                except ValueError:
                    raise HTTPError(400, 'Bad datetime string for %s: %s' % (created, s))
        return filter

    def graph(self, uuid, locked=None, **kwargs):
        if locked is None:
            with self.lock.shared(uuid) as locked:
                g = self.graph(uuid, locked=locked, **kwargs)
            return g

        kwargs.update(self.creds)
        try:
            return self.collection.graph(uuid, **kwargs)
        except (IOError, OSError) as e:
            if e.errno is errno.EPERM:
                raise HTTPError(403, str(e))
            elif e.errno is errno.ENOSPC:
                raise HTTPError(507, str(e))
            raise HTTPError(404, "Backend graph for %s is inaccessible: %s" % (uuid, str(e)))

    def tmp_graph(self, uuid):
        fd, path = tempfile.mkstemp(dir=self.collection.dir, suffix=".db", prefix="tmp_%s_" % uuid)
        name = os.path.basename(path)
        dbname = name[:-3]
        return fd, name, dbname, path

    msgpack = Serializer.msgpack()
    def kv(self, txn):
        return txn.kv('lg.restobjs', serialize_value=self.msgpack)

def graphtxn(write=False, create=False, excl=False, on_success=None, on_failure=None):
    def decorator(func):
        def wrapper(self, _, uuid, *args, **kwargs):
            g = self.graph(uuid, readonly=not write, create=create, excl=excl)
            success = None
            try:
                with g.transaction(write=write) as txn:
                    lastID = txn.lastID
                    success = False
                    ret = func(self, g, txn, _, uuid, *args, **kwargs)
                    try:
                        next = getattr(ret, 'next')
                    except AttributeError:
                        ret = [ret] if ret is not None else []
                    finally:
                        if write:
                            txn.flush()
                            self.res.headers.set('X-lg-updates', txn.lastID - lastID)
                        self.res.headers.set('X-lg-maxID', txn.lastID)
                        for x in ret:
                            yield x
                    success = True
                # delay final chunking trailer until txn has been committed
                if write:
                    if 'x-lg-sync' in self.req.headers:
                        g.sync(force=True)
                    yield ''
            except HTTPError:
                raise
            except (IOError, OSError) as e:
                raise HTTPError(507 if e.errno is errno.ENOSPC else 500, str(e))
            except Exception as e:
                info = sys.exc_info()
                log.error('Unhandled exception: %s', traceback.print_exception(*info))
                raise
            finally:
                if success is True:
                    if on_success:
                        on_success(g)
                elif success is False:
                    if on_failure:
                        on_failure(g)
                g.close()
        return wrapper
    return decorator


class _Input(Handler):
    def do_input(self, txn, uuid, create=False, data=None):
        if data is None:
            data = self.input()
        if data is None:
            return

        lastID = txn.lastID

        seed = data.get('seed', False)
        if seed:
            SeedTracker(txn).add(data)
        if 'meta' in data:
            self.do_meta(txn, data['meta'], seed, create)
        elif create:
            self.do_meta(txn, {}, seed, create)
        if 'chains' in data:
            self.do_chains(txn, data['chains'], seed, create)
        if 'nodes' in data:
            self.do_nodes(txn, data['nodes'], seed, create)
        if 'edges' in data:
            self.do_edges(txn, data['edges'], seed, create)

    def do_meta(self, txn, meta, seed=False, create=False):
        a = txn.nextID

        for key, val in meta.iteritems():
            txn.set(key, val, merge=True)

        b = txn.nextID
        if b > a:
            # fixme - when updating graph meta, advance any adaptor bookmarks currently at end
            pass

    def do_chains(self, txn, chains, seed, create=False):
        # cleanse & merge
        for chain in chains:
            if len(chain) % 2 == 0:
                raise HTTPError(409, 'Bad data - chain length must be odd')
            nodes = self.map_nodes(txn, chain[0::2], seed=seed, create=create)
            for i, n in enumerate(nodes):
                chain[i*2] = n
            for i, x in enumerate(chain):
                if i % 2:
                    chain[i] = self.do_edge(txn, x, src=chain[i-1], tgt=chain[i+1], seed=seed, create=create)

    def do_nodes(self, txn, nodes, seed=False, create=False):
        for n in nodes:
            self.do_node(txn, n, seed, create)

    def map_nodes(self, txn, nodes, seed=False, create=False):
        return map(lambda n: self.do_node(txn, n, seed, create), nodes)

    def do_edges(self, txn, edges, seed=False, create=False):
        for e in edges:
            self.do_edge(txn, e, seed=seed, create=create)

    def do_node(self, txn, node, seed=False, create=False):
        try:
            if create:
                del node['ID']
            param = { 'ID': node['ID'] }
        except KeyError:
            param = { 'type': node['type'], 'value': node['value'] }

        props = dict( (k, v) for k, v in node.iteritems() if k not in Node_Reserved)
        if seed:
            props['seed'] = True
        elif not create:
            props.pop('seed', None)
        param['merge'] = True
        param['properties'] = props
        try:
            return txn.node(**param)
        except TypeError:
            raise HTTPError(409, "Bad node: %s" % node)

    def _edge_node(self, txn, edge, node, seed=False, create=False, is_src=False):
        if node is None:
            node = edge['src' if is_src else 'tgt']
        if not isinstance(node, Node):
            node = self.do_node(txn, node, seed, create)
        return node

    def do_edge(self, txn, edge, src=None, tgt=None, seed=False, create=False):
        try:
            if create:
                del edge['ID']
            param = { 'ID': edge['ID'] }
        except KeyError:
            src = self._edge_node(txn, edge, src, seed=seed, create=create, is_src=True)
            tgt = self._edge_node(txn, edge, tgt, seed=seed, create=create, is_src=False)
            param = dict(type=edge['type'], value=edge.get('value', ''), src=src, tgt=tgt)
        props = dict( (k, v) for k, v in edge.iteritems() if k not in Edge_Reserved)
        if seed:
            props['seed'] = seed
        elif not create:
            props.pop('seed', None)

        try:
            e = txn.edge(**param)
        except:
            raise HTTPError(409, "Bad edge: %s" % edge)

        # handle edge costs, allowed range: [0.0, 1.0]
        # silently drop bad values:
        #  cost defaults to 1 when edge is created
        #  once cost is assigned, value may not be increased, as
        #   depth recalculation could (would?) cover the entire graph
        try:
            cost = props['cost']
            if cost < 0 or cost > 1 or cost >= e['cost']:
                del props['cost']
        except KeyError:
            pass

        e.update(props, merge=True)
        return e

    @graphtxn(write=True, create=True, excl=True, on_failure=lambda g: g.delete())
    def _create(self, g, txn, _, uuid):
        self.do_input(txn, uuid, create=True)
        self.res.code = 201
        self.res.headers.set('Location', '/graph/' + uuid)
        return self.dumps({ 'uuid': uuid, 'id': uuid }, pretty=True)

class _Streamy(object):
    def _stream_js(self, gen):
        yield '['
        try:
            x = gen.next()
            yield x
            for x in gen:
                yield ','
                yield x
        except StopIteration:
            pass
        yield ']\n'

    def stream(self, *gens):
        gen = (self.dumps(x) for x in itertools.chain.from_iterable(gens))
        if 'application/json' == self.streamer.mime:
            gen = self._stream_js(gen)
        return gen

    def _dump_json(self, txn, uuid, nodes, edges):
        yield '{"graph":"%s","id":"%s","maxID":%d,"size":%d,"created":"%s","meta":' % (uuid, uuid, txn.lastID, txn.graph.size, uuid_to_utc(uuid))
        yield self.dumps(dict(txn.iteritems()))
        yield ',"nodes":['
        try:
            n = nodes.next()
            yield self.dumps(n.as_dict())
            for n in nodes:
                yield ','
                yield self.dumps(n.as_dict())
        except StopIteration:
            pass
        yield '],"edges":['
        try:
            e = edges.next()
            yield self.dumps(self.format_edge(e))
            for e in edges:
                yield ','
                yield self.dumps(self.format_edge(e))
        except StopIteration:
            pass
        yield ']}\n'

class Graph_Root(_Input, _Streamy):
    path = ('graph',)

    def get(self, _):
        queries = self.params.get('q')
        with self.collection.context(write=False) as ctx:
            graphs = ctx.graphs(**self.graphs_filter)
            gen = self._query_graphs(graphs, queries) if queries else self.stream(graphs)
            for info in gen:
                yield info

    def _query_graphs(self, graphs, queries):
        uniq = sorted(set(queries))
        qtoc = dict((q, i) for i, q in enumerate(uniq))
        uuids = (graph['id'] for graph in graphs)
        return self.stream([uniq], self.__query_graphs(uuids, uniq, qtoc))

    def __query_graphs(self, uuids, queries, qtoc):
        for uuid in uuids:
            try:
#                with self.graph(uuid, readonly=True, create=False, hook=False) as g:
                with self.graph(uuid, create=False, hook=False) as g:
                    with g.transaction(write=False) as txn:
                        try:
                            gen = txn.mquery(queries)
                        except QuerySyntaxError as e:
                            raise HTTPError(400, str(e))

                        # fixme - should we try to make edges use the format_edge foo here?
                        for query, chain in gen:
                            yield (uuid, qtoc[query], tuple(x.as_dict() for x in chain))
            except IOError:
                pass

    def post(self, _):
        uuid = str(uuidgen())
        return self._create(None, uuid)

class Graph_UUID(_Input, _Streamy):
    path = ('graph', UUID,)
    inf = float('Inf')

    def delete(self, _, uuid):
        with self.lock.exclusive(uuid) as locked:
            # opening the graph checks user/role perms
#            with self.graph(uuid, readonly=True, create=False, locked=locked) as g:
            with self.graph(uuid, create=False, locked=locked) as g:
                self.collection.drop(uuid)

    def put(self, _, uuid):
        target = self.collection.graph_path(uuid)
        with self.lock.exclusive(uuid):
            if os.access(target, os.F_OK):
                raise HTTPError(409, "Upload for %s failed: %s" % (uuid, 'already exists'))
            try:
                (fd, name, dbname, path) = self.tmp_graph(uuid)
            except Exception as e:
                raise HTTPError(409, "Upload for %s failed: %s" % (uuid, str(e)))

            cleanup = deque([
                lambda: os.unlink('%s-lock' % path),
                lambda: os.unlink(path),
            ])
            try:
                fh = os.fdopen(fd, 'wb')
                cleanup.appendleft(fh.close)
                while True:
                    data = self.body.read(BLOCKSIZE)
                    if len(data) == 0:
                        break
                    fh.write(data)
                cleanup.popleft()() # fh.close()
#                with self.collection.graph(dbname, readonly=True, hook=False, create=False) as g:
                with self.collection.graph(dbname, hook=False, create=False) as g:
                    pass
                os.rename(path, target)
                cleanup.pop() # remove os.unlink(path)
                cleanup.append(lambda: os.unlink('%s-lock' % target))
            except Exception as e:
                os.unlink(name)
                raise HTTPError(409, "Upload for %s failed: %s" % (uuid, repr(e)))
            finally:
                for x in cleanup:
                    try:
                        x()
                    except:
                        pass

    def _snapshot(self, g, uuid):
        self.res.headers.set('Content-Type', 'application/octet-stream')
        self.res.headers.set('Content-Disposition','attachment; filename="%s.db"' % uuid)
        for block in g.snapshot(bs=BLOCKSIZE):
            yield block

    single = ('stop', 'start', 'limit')
    multi = ('q',)

    @property
    def stop(self):
        try:
            s = int(self.params['stop'][-1])
        except (KeyError, IndexError):
            return 0
        if s < 1:
            raise HTTPError(400, 'Bad stop parameter - if present, must be > 0')
        return s

    @property
    def start(self):
        try:
            s = int(self.params['start'][-1])
        except (KeyError, IndexError):
            return 0
        if s < 1:
            raise HTTPError(400, 'Bad start parameter - if present, must be > 0')
        return s

    @property
    def limit(self):
        return int(self.param('limit', 0))

    @graphtxn(write=False)
    def get(self, g, txn, _, uuid):
        try:
            accept = self.req.headers['Accept'].values
        except KeyError:
            accept = ('application/json',)

        if 'application/octet-stream' in accept:
            return self._snapshot(g, uuid)

        if self.stop > 0:
            txn.beforeID = self.stop + 1

        try:
            queries = filter(bool, self.params['q'])
            if not queries:
                raise KeyError
        except KeyError:
            if self.streamer is not streamJS:
                raise HTTPError(406, 'Format for full graph dump has not been determined for non-json output')
            return self._dump_json(txn, uuid, txn.nodes(), txn.edges())

        uniq = sorted(set(queries))

        if self.param('crawl', '0') != '0':
            if self.streamer is not streamJS:
                raise HTTPError(406, 'Format for graph dump has not been determined for non-json output')
            try:
                gen = txn.mquery(uniq)
            except QuerySyntaxError as e:
                raise HTTPError(400, str(e))
            return self.spider(txn, uuid, (chain for _, chain in gen))

        qtoc = dict((q, i) for i, q in enumerate(uniq))
        try:
            gen = txn.mquery(uniq, limit=self.limit, start=self.start, stop=self.stop)
        except QuerySyntaxError as e:
            raise HTTPError(400, str(e))
        # fixme - should we try to make edges use the format_edge foo here?
        gen = ((qtoc[query], tuple(x.as_dict() for x in chain)) for query, chain in gen)
        return self.stream([uniq], gen)

    # spider out from nodes/edges found in chain data returned from queries
    def spider(self, txn, uuid, chains):
        seenIDs = set()
        edgeIDs = deque()

        def emit_nodes_harvest_edges():
            todo = deque()
            for chain in chains:
                todo.extend(e for e in chain if e.ID not in seenIDs)
                while todo:
                    e = todo.popleft()
                    seenIDs.add(e.ID)
                    if e.is_edge:
                        # stash edges for later
                        edgeIDs.append(e.ID)
                    else:
                        # emit nodes
                        yield e
                    todo.extend(e2 for e2 in e.iterlinks(filterIDs=seenIDs))

        def emit_edges():
            for ID in edgeIDs:
                yield txn.edge(ID=ID)

        return self._dump_json(txn, uuid, emit_nodes_harvest_edges(), emit_edges())

    @graphtxn(write=False)
    def head(self, g, txn, _, uuid):
        self.res.code = 200

    @graphtxn(write=True)
    def _update(self, g, txn, _, uuid):
        self.do_input(txn, uuid)

    def post(self, _, uuid):
        if 'create' in self.params:
            # attempt create
            try:
                for x in self._create(None, uuid):
                    yield x
                return
            except HTTPError as err:
                if 404 != err.code:
                    raise

        # fall back to updating an existing
        for x in self._update(None, uuid):
            yield x

class Graph_UUID_Status(Handler):
    path = ('graph', UUID, 'status')

    def get(self, _, uuid, __):
        yield self.dumps(self.status(uuid))

    def head(self, _, uuid, __):
        self.status(uuid)
        self.res.code = 200

    def status(self, uuid):
        status = self.collection.status(uuid)
        if status is None:
            raise HTTPError(404, '%s status is not cached' % uuid)
        return status

class Reset_UUID(_Input, Handler):
    path = ('reset', UUID,)

    default_keep = {
        'seeds': 1,
        'kv': True,
    }

    def put(self, _, uuid):
        with self.lock.exclusive(uuid) as locked:
            (fd, name, dbname, path) = self.tmp_graph(uuid)
            os.close(fd)
            cleanup = [lambda: os.unlink(path)]
            try:
#                with self.graph(uuid, readonly=True, locked=locked) as g1, self.collection.graph(dbname, create=True, hook=False) as g2:
                with self.graph(uuid, locked=locked) as g1, self.collection.graph(dbname, create=True, hook=False) as g2:
                    with g1.transaction(write=False) as t1, g2.transaction(write=True) as t2:
                        # fixme
                        cleanup.append(lambda: os.unlink('%s-lock' % path))
                        keep = self.input()
                        if keep is None:
                            keep = self.default_keep

                        if keep.get('kv',False):
                            self.clone_kv(t1, t2)

                        seeds = keep.get('seeds', None)
                        if seeds:
                            self.clone_seeds(uuid, t1, t2, seeds)
                    target = g1.path
                cleanup.pop()() # unlink(path-lock)
                try:
                    # fixme
                    os.unlink('%s-lock' % target)
                except OSError:
                    pass
                os.rename(path, target)
                cleanup.pop() # unlink(path)
                # bypass creds check, allow hooks to run
                self.collection.remove(uuid)
#                with self.collection.graph(uuid, readonly=True):
                with self.collection.graph(uuid):
                    pass
            except (IOError, OSError) as e:
                if e.errno is errno.EPERM:
                    raise HTTPError(403, str(e))
                elif e.errno is errno.ENOSPC:
                    raise HTTPError(507, str(e))
                raise HTTPError(404, "Reset failed for graph %s: %s" % (uuid, repr(e)))
            finally:
                for x in cleanup:
                    try:
                        x()
                    except:
                        pass

    def clone_kv(self, src, dst):
        try:
            s = self.kv(src)
        except KeyError:
            return
        d = self.kv(dst)
        for k, v in s.iteritems():
            d[k] = v

    def clone_seeds(self, uuid, src, dst, limit):
        i = 0
        for seed in SeedTracker(src).seeds:
            self.do_input(dst, uuid, data=seed)
            i += 1
            if i is limit:
                break

class D3_UUID(_Streamy, Handler):
    path = ('d3', UUID)

    @graphtxn(write=False)
    def get(self, g, txn, _, uuid):
        self.map = {}
        stop = int(self.param('stop', 0))
        if stop:
            txn.beforeID = stop + 1
        return self._dump_json(txn)

    def _dump_json(self, txn):
        yield '{ "nodes":['
        nmap = {}
        nidx = 0
        nodes = txn.nodes()
        try:
            n = nodes.next()
            nmap[n.ID] = nidx
            nidx += 1
            yield self.dumps({ 'data': n.as_dict() })
            for n in nodes:
                yield ','
                nmap[n.ID] = nidx
                nidx += 1
                yield self.dumps({ 'data': n.as_dict() })
        except StopIteration:
            pass
        yield '],"edges":['
        edges = txn.edges()
        try:
            e = edges.next()
            yield self.dumps({
                'data': e.as_dict(),
                'source': nmap[e.srcID],
                'target': nmap[e.tgtID] })
            for e in edges:
                yield ','
                yield self.dumps({
                    'data': e.as_dict(),
                    'source': nmap[e.srcID],
                    'target': nmap[e.tgtID] })
        except StopIteration:
            pass
        yield ']}\n'

class Graph_Exec(_Input, _Streamy):
    path = ('graph', 'exec')
    eat_params = ('enabled', 'user', 'role', 'created_after', 'created_before', 'filter')

    def post(self, _, __):
        if self.content_type != 'application/python':
            raise HTTPError(409, 'Bad/missing Content-Type - must be: application/python')
        code = ''.join(self.body)
        locals = {}
        globals = {
            'dumps': self.dumps,
            'stream': self.stream,
            'HTTPError': HTTPError,
        }
        with self.collection.context(write=False) as ctx:
            uuids = tuple(x['id'] for x in ctx.graphs(**self.graphs_filter))

        txns_uuids = self._txns_uuids(uuids)
        try:
            exec code in globals, locals
            params = dict((k, v) if len(v) > 1 else (k, v[0]) for k, v in self.params.iteritems() if k not in self.eat_params)
            try:
                handler = locals['handler']
            except KeyError:
                raise HTTPError(404, 'no handler function defined - should look like: def handler(txns_uuids, headers, **kwargs)')
            return handler(txns_uuids, self.res.headers, **params)
        except HTTPError:
            raise
        except Exception as e:
            raise HTTPError(400, 'handler raised exception: %s\n%s' % (e, code))

    def _txns_uuids(self, uuids):
        for uuid in uuids:
            try:
#                with self.graph(uuid, readonly=True, create=False, hook=False) as g:
                with self.graph(uuid, create=False, hook=False) as g:
                    with g.transaction(write=False) as txn:
                        yield txn, uuid
            except:
                pass

class Graph_UUID_Exec(_Input, _Streamy):
    path = ('graph', UUID,'exec')
    @graphtxn(write=False)
    def post(self, g, txn, _, uuid, __):
        if self.content_type != 'application/python':
            raise HTTPError(409, 'Bad/missing Content-Type - must be: application/python')
        code = ''.join(self.body)
        locals = {}
        globals = {
            'dumps': self.dumps,
            'stream': self.stream,
            'HTTPError': HTTPError,
        }
        try:
            exec code in globals, locals
            params = dict((k, v) if len(v) > 1 else (k, v[0]) for k, v in self.params.iteritems())
            try:
                handler = locals['handler']
            except KeyError:
                raise HTTPError(404, 'no handler function defined - should look like: def handler(txn, uuid, headers, **kwargs)')
            return handler(txn, uuid, self.res.headers, **params)
        except HTTPError:
            raise
        except Exception as e:
            raise HTTPError(400, 'handler raised exception: %s' % e)

class Graph_UUID_Meta(_Input):
    path = ('graph', UUID, 'meta')

    @graphtxn(write=False)
    def get(self, g, txn, _, uuid, __):
        yield self.dumps(dict(txn.iteritems()), pretty=True)

    @graphtxn(write=True)
    def put(self, g, txn, _, uuid, __):
        self.do_meta(txn, self.input())

class Graph_UUID_Seeds(Handler, _Streamy):
    path = ('graph', UUID, 'seeds')
    @graphtxn(write=False)
    def get(self, g, txn, _, uuid, __):
        return self.stream(SeedTracker(txn).seeds)

class Graph_UUID_Node_ID(_Input):
    path = ('graph', UUID, 'node', INT)

    @graphtxn(write=False)
    def get(self, g, txn, _, uuid, __, ID):
        try:
            return self.dumps(txn.node(ID=int(ID)).as_dict())
        except TypeError:
            raise HTTPError(404,"not a node")

    @graphtxn(write=True)
    def put(self, g, txn, _, uuid, __, ID):
        ID = int(ID)
        data = self.input()
        data['ID'] = ID
        self.do_node(txn, data)

class Graph_UUID_Edge_ID(_Input):
    path = ('graph', UUID, 'edge', INT)

    @graphtxn(write=False)
    def get(self, g, txn, _, uuid, __, ID):
        try:
            return self.dumps(self.format_edge(txn.edge(ID=int(ID))))
        except TypeError:
            raise HTTPError(404, "not an edge")

    @graphtxn(write=True)
    def put(self, g, txn, _, uuid, __, ID):
        ID = int(ID)
        data = self.input()
        data['ID'] = ID
        self.do_edge(txn, data)

class KV_UUID(Handler):
    path = ('kv', UUID)

    @graphtxn(write=False)
    def get(self, g, txn, _, uuid):
        try:
            kv = self.kv(txn)
            output = dict(kv.iteritems())
        except KeyError:
            output = {}

        yield self.dumps(output)

    @graphtxn(write=True)
    def post(self, g, txn, _, uuid):
        data = self.input()
        kv = self.kv(txn)
        try:
            gen = data.iteritems()
        except Exception as e:
            raise HTTPError(400, "bad input (%s)" % str(e))
        for k, v in gen:
            kv[k] = merge_values(kv.get(k, None), v)

    @graphtxn(write=True)
    def put(self, g, txn, _, uuid):
        data = self.input()
        kv = self.kv(txn)
        try:
            gen = data.iteritems()
        except Exception as e:
            raise HTTPError(400, "bad input (%s)" % str(e))
        for k, v in kv.iteritems():
            if k not in data:
                del kv[k]
        for k, v in gen:
            kv[k] = v

    @graphtxn(write=True)
    def delete(self, g, txn, _, uuid):
        try:
            kv = self.kv(txn)
        except KeyError:
            return
        for k in kv.iterkeys():
            del kv[k]

class KV_UUID_Key(Handler):
    path = ('kv', UUID, STRING)

    @graphtxn(write=False)
    def get(self, g, txn, _, uuid, key):
        try:
            kv = self.kv(txn)
            yield self.dumps(kv[key])
        except KeyError:
            raise HTTPError(404, 'key not found')

    @graphtxn(write=True)
    def post(self, g, txn, _, uuid, key):
        data = self.input()
        kv = self.kv(txn)
        kv[key] = merge_values(kv.get(key, None), data)

    @graphtxn(write=True)
    def put(self, g, txn, _, uuid, key):
        data = self.input()
        kv = self.kv(txn)
        kv[key] = data

    @graphtxn(write=True)
    def delete(self, g, txn, _, uuid, key):
        try:
            kv = self.kv(txn)
            del kv[key]
        except KeyError:
            raise HTTPError(404, 'key not found')

class Static(Handler):
    res = re.compile('^[^/]+\.(js|css|html)')
    mime = {
        '.html': 'text/html',
        '.css': 'text/css',
        '.js': 'text/javascript',
        '.png': 'image/png',
    }
    path = ('static', res)
    cache = {}

    def get(self, _, resource):
        try:
            body = self.cache[resource]
        except KeyError:
            body = pkg_resources.resource_string(__name__, 'data/%s' % resource)
            #self.cache[resource] = body

        extension = os.path.splitext(resource)[1]
        try:
            self.res.headers.set('Content-Type', self.mime[extension])
        except KeyError:
            pass
        self.res.headers.set('Content-Length', len(body))
        return body

class View_UUID(Static):
    path = ('view', UUID)

    def get(self, _, uuid):
        return super(View_UUID, self).get(Static.path[0], self.param('style', 'd3v4') + '.html')

class Favicon(Static):
    path = ('favicon.ico',)

    def get(self, _):
        return super(Favicon, self).get(Static.path[0], 'lemon.png')

class Server(object):
    def __init__(self, collection_path=None, graph_opts=None, **kwargs):
        classes = (
            Graph_Root,
            Graph_UUID,
            Graph_UUID_Meta,
            Graph_UUID_Node_ID,
            Graph_UUID_Edge_ID,
            Graph_Exec,
            Graph_UUID_Exec,
            Graph_UUID_Seeds,
            Graph_UUID_Status,
            Reset_UUID,
            KV_UUID,
            KV_UUID_Key,
            View_UUID,
            D3_UUID,
            Favicon,
            Static,
        )
        handlers = tuple( H(collection_path=collection_path, graph_opts=graph_opts) for H in classes)
        kwargs['handlers'] = handlers
        httpd(**kwargs)

def seed_depth0():
    while True:
        txn, entry = yield
        # first checks stay w/in the entry - last has to instantiate the parent object
        if entry.is_property and entry.key == 'seed' and entry.value and entry.is_node_property:
            entry.parent['depth'] = 0

def process_edge(txn, e, cost=1, inf=float('Inf')):
    # grab latest versions of endpoints
    src = txn.node(ID=e.srcID)
    tgt = txn.node(ID=e.tgtID)
    ds = src.get('depth', inf)
    dt = tgt.get('depth', inf)
    if ds + cost < dt:
        tgt['depth'] = ds + cost
    elif dt + cost < ds:
        src['depth'] = dt + cost

def apply_cost(txn, prop):
    # cost value validity has already been enforced above
    process_edge(txn, prop.parent, cost=prop.value)

def cascade_depth(txn, prop):
    # grab latest version of parent node
    node = txn.node(ID=prop.parentID)
    mindepth = prop.value + 1
    for n2 in node.neighbors:
        try:
            if n2['depth'] <= mindepth:
                continue
        except KeyError:
            pass
        n2['depth'] = mindepth

def update_depth_cost():
    while True:
        txn, entry = yield
        if entry.is_edge:
            # grab newly added edges, and propagate depth
            process_edge(txn, entry)
        elif entry.is_property:
            if entry.key == 'depth':
                if entry.is_node_property:
                    cascade_depth(txn, entry)
            elif entry.key == 'cost':
                if entry.is_edge_property:
                    apply_cost(txn, entry)

def usage(msg=None, fh=sys.stderr):
    print >>fh, 'Usage: python -mLemonGraph.server <options> [graphs-dir]'
    print >>fh, ''
    print >>fh, 'Options:'
    print >>fh, '  -i <bind-ip>        (127.0.0.1)'
    print >>fh, '  -p <bind-port>      (8000)'
    print >>fh, '  -d <poll-delay-ms>  (250)'
    print >>fh, '  -w <workers>        (#cpu cores)'
    print >>fh, '  -l <log-level>      (info)'
    print >>fh, '  -t <timeout>        (3 seconds)'
    print >>fh, '  -b <buflen>         (1048576)'
    print >>fh, '  -s                  enable nosync for graph dbs'
    print >>fh, '  -m                  enable nometasync for graph dbs'
    print >>fh, '  -r                  rebuild index'
    print >>fh, '  -h                  print this help and exit'
    if msg is not None:
        print >>fh, ''
        print >>fh, msg
    sys.exit(1)

def main():
    levels = ('NOTSET', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
    try:
        opts, args = getopt(sys.argv[1:], 'i:p:d:l:w:t:b:smrh')
    except GetoptError as err:
        usage(msg=str(err))
    if len(args) > 1:
        usage()

    default_level = 'info'

    ip = '127.0.0.1'
    port = 8000
    logspec = default_level
    poll = 250
    nosync = False
    nometasync = False
    rebuild = False
    path = 'graphs'
    workers = -1
    timeout = 3
    buflen = 1048576
    try:
        path = args[0]
    except IndexError:
        pass

    try:
        for o, a in opts:
            if o == '-i':
                ip = a
            elif o == '-p':
                port = sorted((0, int(a), 65536))[1]
            elif o == '-d':
                poll = sorted((20, int(a), 10000))[1]
            elif o == '-s':
                nosync = True
            elif o == '-m':
                nometasync = True
            elif o == '-r':
                rebuild = True
            elif o == '-l':
                logspec = a
            elif o == '-t':
                timeout = sorted((0.010, int(a)))[1]
            elif o == '-w':
                workers = int(a)
            elif o == '-b':
                buflen = int(a)
            elif o == '-h':
                usage(fh=sys.stdout)
            else:
                raise RuntimeError(o)
    except (KeyError, ValueError):
        usage()

    all_logs = tuple("LemonGraph.%s" % x for x in ('httpd', 'collection', 'server'))
    log_levels = dict( (k, default_level) for k in all_logs)
    for token in logspec.split(','):
        try:
            target, level = token.split('=', 1)
            alt = "LemonGraph.%s" % target
            target = alt if alt in all_logs else target
            targets = (target,)
        except ValueError:
            targets = all_logs
            level = token
        if level.upper() not in levels:
            usage()
        for target in targets:
            log_levels[target] = level

    loghandler = LogHandler()
    for target, level in log_levels.iteritems():
        logger = logging.getLogger(target)
        logger.addHandler(loghandler)
        logger.setLevel(getattr(logging, level.upper()))

    graph_opts=dict(
        serialize_property_value=Serializer.msgpack(),
        adapters=Adapters(seed_depth0, update_depth_cost),
        nosync=nosync, nometasync=nometasync,
        )

    # initialize the collection up front (which will re-create if missing)
    # before we turn on the web server
    col = Collection(path, create=True, rebuild=rebuild, graph_opts=graph_opts)
    col.close()

    def _syncd():
        collection = Collection(path, graph_opts=graph_opts, nosync=True, nometasync=False)
        try:
            collection.daemon(poll=poll)
        except Graceful:
            pass
        finally:
            collection.close()

    global collection
    collection = None

    global lock
    lock = Lock('%s.lock' % path)

    Server(collection_path=path, graph_opts=graph_opts, extra_procs={'syncd': _syncd}, host=ip, port=port, spawn=workers, timeout=timeout, buflen=buflen)

if '__main__' == __name__:
    main()
