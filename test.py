import os
import random
import shutil
import signal
import sys
import tempfile
import time
import unittest
import uuid

from LemonGraph import Graph, Serializer, dirlist, Query, cast
from LemonGraph.httpc import RESTClient
from LemonGraph.lg_lite import TPS

node = lambda i: dict((k, Nodes[i][k]) for k in ('type', 'value'))
edge = lambda i: dict((k, Edges[i][k]) for k in ('type', 'value', 'src', 'tgt'))

Nodes = [
    {'type': 'foo', 'value': 'bar', 'properties': {'np1k': 'np1v'}},
    {'type': 'foo', 'value': 'baz', 'properties': {'np2k': 'np2v'}},
    {'type': 'goo', 'value': 'gaz', 'properties': {'np3k': 'np3v'}},
]

Edges = [
    {'src': node(0), 'tgt': node(1), 'type': 'edge', 'value': 'e1'},
    {'src': node(1), 'tgt': node(2), 'type': 'edge2', 'value': 'e2'},
]


def load_data(txn):
    for obj in Nodes:
        txn.node(**obj)
    for obj in Edges:
        cpy = {}
        cpy.update(obj)
        cpy['src'] = txn.node(**cpy['src'])
        cpy['tgt'] = txn.node(**cpy['tgt'])
        txn.edge(**cpy)


class TestGraph(unittest.TestCase):
    serializer = Serializer.msgpack()

    # each test_foo() method is wrapped w/ setup/teardown around it, so each test has a fresh graph
    def setUp(self):
        fd, path = tempfile.mkstemp()
        os.close(fd)
        self.g = Graph(path, nosync=True, nometasync=True, serialize_property_value=self.serializer)

    def tearDown(self):
        self.g.delete()

    def test_commit(self):
        with self.g.transaction(write=True) as txn:
            self.assertEqual(txn.nextID, 1)
            txn.node(type='foo', value='bar')
            self.assertEqual(txn.nextID, 2)
            txn.commit()
            # should never reach this
            self.assertTrue(False)

        with self.g.transaction(write=False) as txn:
            self.assertEqual(txn.nextID, 2)

    def test_abort(self):
        with self.g.transaction(write=True) as txn:
            self.assertEqual(txn.nextID, 1)
            txn.node(type='foo', value='bar')
            self.assertEqual(txn.nextID, 2)
            txn.abort()
            # should never reach this
            self.assertTrue(False)

        with self.g.transaction(write=False) as txn:
            self.assertEqual(txn.nextID, 1)

    def test_load_data(self):
        with self.g.transaction(write=True) as txn:
            load_data(txn)
            self.assertEqual(txn.nodes_count(), len(Nodes))
            self.assertEqual(txn.edges_count(), len(Edges))

    def test_counts(self):
        with self.g.transaction(write=True) as txn:
            self.assertEqual(txn.nodes_count(), 0)
            self.assertEqual(txn.edges_count(), 0)

            n1 = txn.node(type="foo", value="bar")
            self.assertEqual(txn.nodes_count(), 1)

            n2 = txn.node(type="foo", value="baz")
            self.assertEqual(txn.nodes_count(), 2)

            self.assertEqual(txn.nodes_count(beforeID=n2.ID), 1)

            txn.edge(src=n1, tgt=n2, type="foo")
            self.assertEqual(txn.edges_count(), 1)

        with self.g.transaction(write=True) as txn:
            self.assertEqual(txn.nodes_count(beforeID=n2.ID), 1)
            txn.node(type="foo", value="blah")
            self.assertEqual(txn.nodes_count(), 3)
            n1 = txn.node(type="foo", value="bar")
            n1.delete()
            self.assertEqual(txn.nodes_count(), 2)
            self.assertEqual(txn.edges_count(), 0)

        with self.g.transaction(write=False) as txn:
            self.assertEqual(txn.nodes_count(), 2)
            self.assertEqual(txn.edges_count(), 0)
            self.assertEqual(txn.nodes_count(beforeID=txn.lastID), 3)

    def test_edges_by_type(self):
        with self.g.transaction(write=True) as txn:
            load_data(txn)
            n1 = node(1)
            n1 = txn.node(**n1)
            all_edges = sum(1 for x in n1.iterlinks())
            fewer_edges = sum(1 for x in n1.iterlinks(types=(edge(1)['type'],)))
            self.assertTrue(all_edges > fewer_edges)

    def test_edge_dirs(self):
        with self.g.transaction(write=True) as txn:
            load_data(txn)

        with self.g.transaction(write=False) as txn:
            n1 = node(1)
            n1 = txn.node(**n1)
            all_edges = set(e.ID for e in n1.edges)
            out_edges = set(e.ID for e in n1.edges(dir="out"))
            in_edges = set(e.ID for e in n1.edges(dir="in"))
            self.assertTrue(all_edges)
            self.assertTrue(out_edges)
            self.assertTrue(in_edges)
            self.assertTrue(all_edges - in_edges)
            self.assertTrue(all_edges - out_edges)
            self.assertTrue(in_edges.isdisjoint(out_edges))

    def test_query(self):
        with self.g.transaction(write=True) as txn:
            load_data(txn)

        with self.g.transaction(write=False) as txn:
            chains = 0
            for _ in txn.query("n(type='foo')->e()-n()"):
                chains += 1
            self.assertTrue(chains)

    def test_query_streaming(self):
        with self.g.transaction(write=True) as txn:
            load_data(txn)

        node1 = node(1)
        with self.g.transaction(write=True) as txn:
            n1 = txn.node(**node1)
            n1['foo'] = 1

        with self.g.transaction(write=True) as txn:
            n1 = txn.node(**node1)
            n1['foo'] = 2
            n1['bar'] = 3
            n1['foo'] = 4

        with self.g.transaction(write=False) as txn:
            # 'foo' is set 3 times
            count = sum(1 for chain in txn.query('n(foo=*)', start=1))
            self.assertEqual(count, 3)

            # 'bar' is set once (and 'foo' was already set), and 'foo' is set once after that
            count = sum(1 for chain in txn.query('n(foo=*,bar=*)', start=1))
            self.assertEqual(count, 2)

            # 'bar' is set once (and 'foo' was already set), and subsequent 'foo' update was within same txn
            count = sum(1 for chain in txn.query('n(foo=*,bar=*)', start=1, snap=True))
            self.assertEqual(count, 1)

        with self.g.transaction(write=True) as txn:
            n1 = txn.node(**node1)
            n1['baz'] = { "a": { "aa": 1 } }
            n1['baz'] = { "a": { "aa": 1, "bb": 2 } }
            n1['baz'] = { "a": { "aa": 3, "bb": 2 } }

        with self.g.transaction(write=False) as txn:
            count = sum(1 for chain in txn.query('n(baz=*)', start=1))
            self.assertEqual(count, 3)

            count = sum(1 for chain in txn.query('n(baz.a=*)', start=1))
            self.assertEqual(count, 3)

            count = sum(1 for chain in txn.query('n(baz.a.aa=*)', start=1))
            self.assertEqual(count, 2)

            count = sum(1 for chain in txn.query('n(baz.a.bb=*)', start=1))
            self.assertEqual(count, 1)

    def test_graph_props(self):
        with self.g.transaction(write=True) as txn:
            self.assertFalse('foo' in txn)
            txn['foo'] = 'bar'
            self.assertTrue('foo' in txn)

    def test_kv(self):
        with self.g.transaction(write=True) as txn:
            b = txn.kv('foo')
            keys = ('fon', 'foo', 'foobar', 'foobaz', 'fom')
            for i, key in enumerate(('fo', 'fon', 'foo', 'foobar', 'foobaz', 'fom')):
                b[key] = i
            res = tuple(b.iterkeys(pfx='foo'))
            self.assertEqual(keys[1:-1], res)
            txn.abort()

        with self.g.transaction(write=True) as txn:
            b = txn.kv('foo', map_data=True)
            self.assertFalse('bar' in b)
            b['bar'] = 'blah'
            self.assertTrue('bar' in b)
            txn.abort()

        with self.g.transaction(write=True) as txn:
            b = txn.kv('foo', map_keys=True)
            self.assertFalse('bar' in b)
            b['bar'] = 'blah'
            self.assertTrue('bar' in b)

            count = sum(1 for x in b)
            self.assertTrue(1 == count)
            txn.abort()

        with self.g.transaction(write=True) as txn:
            b = txn.kv('foo')
            for k in ('a', 'b1', 'b2', 'c'):
                b[k] = 1
            b.clear(pfx='b')
            self.assertEqual(tuple(b.iterkeys()), ('a','c'))
            b.clear()
            self.assertEqual(tuple(b.iterkeys()), ())
            txn.abort()

    def test_kv_next(self):
        with self.g.transaction(write=True) as txn:
            b = txn.kv('foo')
            for i, k in enumerate(('a', 'b', 'c')):
                b[k] = str(i)

        with self.g.transaction(write=True) as txn:
            b = txn.kv('foo')
            ret = b.next()
            self.assertEqual(ret, ('a', '0'))

        with self.g.transaction(write=True) as txn:
            b = txn.kv('foo')
            ret = b.next()
            self.assertEqual(ret, ('b', '1'))

        with self.g.transaction(write=True) as txn:
            b = txn.kv('foo')
            ret = b.next()
            self.assertEqual(ret, ('c', '2'))

        with self.g.transaction(write=True) as txn:
            b = txn.kv('foo')
            ret = b.next()
            self.assertEqual(ret, ('a', '0'))

    def test_fifo(self):
        with self.g.transaction(write=True) as txn:
            f = txn.fifo('foo', serialize_value=Serializer.uint())
            f.push(1, 2, 3)
            out = f.pop(4)
            self.assertEqual(out, [1, 2, 3])
            self.assertTrue(f.empty)
            f.push(4, 5, 6)
            self.assertFalse(f.empty)
            self.assertEqual(len(f), 3)
            out = f.pop(4)
            self.assertEqual(out, [4, 5, 6])
            self.assertEqual(len(f), 0)

    def test_pqueue(self):
        with self.g.transaction(write=True) as txn:
            pq = txn.pqueue('foo')
            pq.add('c', priority=2)
            pq.add('d', priority=1)
            pq.add('a', priority=4)
            pq.add('e', priority=1)
            pq.add('b', priority=3)

            ret = tuple(iter(pq))
            self.assertEqual(ret, tuple('abcde'))

            pq.add('d', priority=1)

            ret = tuple(iter(pq))
            self.assertEqual(ret, tuple('abced'))

            pq.add('d', priority=4)

            ret = tuple(iter(pq))
            self.assertEqual(ret, tuple('adbce'))

            pq.remove('b')

            ret = tuple(iter(pq))
            self.assertEqual(ret, tuple('adce'))

            pq.clear()

            ret = tuple(iter(pq))
            self.assertEqual(ret, tuple())

    def test_nested(self):
        with self.g.transaction(write=True) as t0:
            t0['foo'] = "t0"
            with t0.transaction(write=True) as t1:
                t1['foo'] = "t1"
                with t1.transaction(write=True) as t2:
                    self.assertTrue(t2['foo'] == "t1")
                    t2['foo'] = "t2"
                    self.assertTrue(t2['foo'] == "t2")
                    t1.abort()
                # should never get here
                self.assertTrue(False)
            self.assertTrue(t0['foo'] == "t0")
            t0['foo'] = "t00"

        with self.g.transaction(write=True) as t0:
            self.assertTrue(t0['foo'] == "t00")
            t0['foo'] = "t000"
            with t0.transaction(write=True) as t1:
                t1['foo'] = "t1"
                t0.commit()
                # should never get here
                self.assertTrue(False)
            # or here
            self.assertTrue(False)

        with self.g.transaction(write=True) as t0:
            self.assertTrue(t0['foo'] == "t000")

    def test_reset(self):
        with self.g.transaction(write=True) as txn:
            txn.node(type="foo", value="bar")
            nextID = txn.nextID
        with self.g.transaction(write=True) as txn:
            self.assertEqual(nextID, txn.nextID)
            txn.reset()
            self.assertEqual(1, txn.nextID)
        with self.g.transaction(write=False) as txn:
            self.assertEqual(1, txn.nextID)


class TestAlgorithms(unittest.TestCase):
    serializer = Serializer.msgpack()
    # each test_foo() method is wrapped w/ setup/teardown around it, so each test has a fresh graph
    def setUp(self):
        fd, path = tempfile.mkstemp()
        os.close(fd)
        self.g = Graph(path, serialize_property_value=self.serializer)

    def tearDown(self):
        self.g.delete()

    def test_sp1(self):
        with self.g.transaction(write=True) as txn:
            load_data(txn)

        with self.g.transaction(write=False) as txn:
            n0 = txn.node(**node(0))
            n1 = txn.node(**node(1))
            n2 = txn.node(**node(2))

            e0 = edge(0)
            e1 = edge(1)

            e0['src'] = txn.node(**e0['src'])
            e0['tgt'] = txn.node(**e0['tgt'])
            e1['src'] = txn.node(**e1['src'])
            e1['tgt'] = txn.node(**e1['tgt'])

            e0 = txn.edge(**e0)
            e1 = txn.edge(**e1)

            expect_path = (n0, e0, n1, e1, n2)

            res_path = n0.shortest_path(n2, cost_default=1, directed=False)
            self.assertEqual(
                tuple(x.ID for x in expect_path),
                tuple(x.ID for x in res_path))

            res_path = n0.shortest_path(n2, cost_default=1, directed=True)
            self.assertEqual(
                tuple(x.ID for x in expect_path),
                tuple(x.ID for x in res_path))

            fail = n2.shortest_path(n0, cost_default=1, directed=True)
            self.assertEqual(fail, None)

    def test_sp2(self):
        with self.g.transaction(write=True) as txn:
            '''
                 n1a
                /   \
              e0a   e1a
              /       \
            n0         n2
              \       /
              e0b   e1b
                \   /
                 n1b
            '''
            n0 = txn.node(type='foo', value='0')
            n1a = txn.node(type='foo', value='1a')
            n1b = txn.node(type='foo', value='1b')
            n2 = txn.node(type='foo', value='2')
            e0a = txn.edge(type='foo', src=n0, tgt=n1a)
            e0b = txn.edge(type='foo', src=n0, tgt=n1b)
            e1a = txn.edge(type='foo', src=n1a, tgt=n2)
            e1b = txn.edge(type='foo', src=n1b, tgt=n2)

            # should transit upper path
            self.assertPathEqual(n0.shortest_path(n2, cost_default=1), (n0, e0a, n1a, e1a, n2))

            # use cost to force it through lower path
            e1b['cost'] = 0.5
            self.assertPathEqual(n0.shortest_path(n2, cost_default=1, cost_field='cost'), (n0, e0b, n1b, e1b, n2))

            # use default cost to make it find the upper path again
            self.assertPathEqual(n0.shortest_path(n2, cost_default=0.5, cost_field='cost'), (n0, e0a, n1a, e1a, n2))


    def assertPathEqual(self, a, b):
        self.assertEqual(type(a), type(b))
        if a is not None:
            self.assertEqual(
                tuple(x.ID for x in a),
                tuple(x.ID for x in b))


class TestSerializers(unittest.TestCase):
    uic = {
        0:                  b"\x00",
        1:                  b"\x01",
        0x7f:               b"\x7f",
        0x80:               b"\x80\x80",
        0x3fff:             b"\xbf\xff",
        0x4000:             b"\xc0\x40\x00",
        0x1fffff:           b"\xdf\xff\xff",
        0x200000:           b"\xe0\x20\x00\x00",
        0xfffffff:          b"\xef\xff\xff\xff",
        0x10000000:         b"\xf0\x10\x00\x00\x00",
        0x7ffffffff:        b"\xf7\xff\xff\xff\xff",
        0x800000000:        b"\xf8\x08\x00\x00\x00\x00",
        0x3ffffffffff:      b"\xfb\xff\xff\xff\xff\xff",
        0x40000000000:      b"\xfc\x04\x00\x00\x00\x00\x00",
        0x1ffffffffffff:    b"\xfd\xff\xff\xff\xff\xff\xff",
        0x2000000000000:    b"\xfe\x02\x00\x00\x00\x00\x00\x00",
        0xffffffffffffff:   b"\xfe\xff\xff\xff\xff\xff\xff\xff",
        0x100000000000000:  b"\xff\x01\x00\x00\x00\x00\x00\x00\x00",
        0x7fffffffffffffff: b"\xff\x7f\xff\xff\xff\xff\xff\xff\xff",
        0x8000000000000000: b"\xff\x80\x00\x00\x00\x00\x00\x00\x00",
        0xffffffffffffffff: b"\xff\xff\xff\xff\xff\xff\xff\xff\xff",
    }

    def test_default(self):
        s = Serializer()
        a = (None, 'foo', 1)
        b = (b'', b'foo', b'1')
        c = ('', 'foo', '1')
        for x, y, z in zip(a, b, c):
            self.assertEqual(s.encode(x), y)
            self.assertEqual(s.decode(y), z)

    def test_uint(self):
        s = Serializer.uint()
        a = tuple(self.uic.keys())
        b = tuple(self.uic.values())
        for x, y in self.uic.items():
            self.assertEqual(s.encode(x), y)
            self.assertEqual(s.decode(y), x);

    def test_uints(self):
        s = Serializer.uints(len(self.uic))
        a = tuple(self.uic.keys())
        b = b''.join(self.uic.values())
        self.assertEqual(s.encode(a), b)
        self.assertEqual(s.decode(b), a)

    def test_uints_string(self):
        s = Serializer.uints(3, string=True)
        a = ((0, 0x7f, "foo"), (0x80, 0x3fff, ""))
        b = (b"\x00\x7f\x03foo", b"\x80\x80\xbf\xff\x00")
        for x, y in zip(a, b):
            self.assertEqual(s.encode(x), y)
            self.assertEqual(s.decode(y), x)

    def test_vuints(self):
        s = Serializer.vuints()
        a = ((0, 0x7f), (0x80, 0x3fff, 0x1fffff), (0xffffffffffffffff,))
        b = (b"\x00\x7f",
             b"\x80\x80\xbf\xff\xdf\xff\xff",
             b"\xff\xff\xff\xff\xff\xff\xff\xff\xff",
             )
        for x, y in zip(a, b):
            self.assertEqual(s.encode(x), y)
            self.assertEqual(s.decode(y), x)

    def test_uints2d(self):
        s = Serializer.uints2d()
        records = (
            (1, 2, 3),
            (4, 5, 6),
        )
        b = s.encode(records)
        out = s.decode(b)
        self.assertEqual(records, out)

    def test_msgpack(self):
        s = Serializer.msgpack()
        a = { 'foo': 'bar', u'foo\u2020': u'bar\u2020' }
        b = s.encode(a)
        c = s.decode(b)
        self.assertEqual(a, c)

    def test_uuid(self):
        s = Serializer.uuid()
        u = uuid.uuid1()
        u_str = str(u)
        u_bin = s.encode(u_str)
        self.assertEqual(u_str, s.decode(u_bin))

class TestDL(unittest.TestCase):
    def test_dirlist(self):
        dots = 0
        for x in dirlist('/'):
            if x == '.':
                dots += 1
            elif x == '..':
                dots += 2
        self.assertEqual(dots, 3)


class TestQL(unittest.TestCase):
    chains = (
        ({'foo': 'bar'},),
        ({'foo': 'bar'}, {'type': 'foo'}),
    )

    matches = {
        'n()': [0],
        'e()': [0],
        'n(foo~/^bar$/)': [0],
        'n()-n(type="foo")': [1],
    }

    def test_a(self):
        results = {}
        q = Query(self.matches.keys())
        for i, chain in enumerate(self.chains):
            for p, _ in q.validate(chain):
                try:
                    results[p].append(i)
                except KeyError:
                    results[p] = [i]
        self.assertEqual(self.matches, results)

class TestCast(unittest.TestCase):
    ints = (
        (1, 1),
        ("1", 1),
        (u"1", 1),
    )
    flts = (
        (1.0, 1.0),
        (u"1.0", 1.0),
    )
    def test_cast(self):
        for a, b in self.ints:
            self.assertEqual(cast.uint(a), b)
        for a, b in self.flts:
            self.assertEqual(cast.unum(a), b)

# for standing up a private LG server on a temporary unix domain socket
class LocalServer(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.server = pid = os.fork()
        self.sockpath = os.path.join(self.dir, 'socket')
        if pid == 0:
            try:
                os.chdir(self.dir)
                sys.argv[1:] = '-s -i ./socket -l warning -w 1 data'.split()
                import LemonGraph.server.__main__
            finally:
                os._exit(0)

        for i in range(0, 300, 5):
            if os.path.exists(self.sockpath):
                try:
                    return self.connect()
                except LemonGraph.httpc.RESTClient.error:
                    pass
            time.sleep(0.05)
        raise FileNotFoundError(self.sockpath)

    def connect(self):
        self.client = RESTClient(sockpath=self.sockpath)

    def tearDown(self):
        os.kill(self.server, signal.SIGTERM)
        self.client.close()
        pid, status = os.waitpid(self.server, 0)
        self.assertEqual(status, 0)
        shutil.rmtree(self.dir)

class Test_Depth(LocalServer):
    def test_depth(self):
        client = self.client

        # create a graph
        code, headers, data = client.post('/graph', json={'meta': {'seed': 1, 'depth': 2, 'cost': 3}})
        self.assertEqual(code, 201)
        graph = str(headers['location'])

        N0 = { 'type': 'foo', 'value': 'foo' }
        N1 = { 'type': 'foo', 'value': 'bar' }
        N2 = { 'type': 'foo', 'value': 'baz' }

        E0 = { 'type': 'foo', 'src': N0, 'tgt': N1, 'cost': 1 }
        E1 = { 'type': 'foo', 'src': N1, 'tgt': N2, 'cost': 2 }

        # add some data
        code, headers, data = client.post(graph, json={
            'nodes': [ N0, N1, N2 ],
            'edges': [ E0, E1 ],
        })

        # check no depth is set
        code, headers, data = client.get(graph)
        for i in 0, 1, 2:
            self.assertTrue('depth' not in data['nodes'][i])

        # mark first node as seed to give it depth = 0
        # depth should cascade to connected nodes
        code, headers, data = client.post(graph, json={
            'seed': True,
            'nodes': [ N0 ],
        })

        # check depth cascaded properly
        code, headers, data = client.get(graph)
        self.assertEqual(data['nodes'][0]['depth'], 0)
        self.assertEqual(data['nodes'][1]['depth'], 1)
        self.assertEqual(data['nodes'][2]['depth'], 3)

class Test_Endpoints(LocalServer):
    def test_lg_lite(self):
        # fancy adapter decorator
        def adapter(**params):
            def decorator(func):
                def wrapper(data):
                    # message looks like:
                    #   [ <task>, <record0>, ... <recordN> ]
                    # go ahead and split that up
                    it = iter(data)
                    task = next(it)
                    if task['retries']:
                        # log retry count for retried tasks
                        sys.stderr.write('[%d]' % task['retries'])
                        sys.stderr.flush()
                    return func(task, it)
                # bolt on adapter name and parameters
                setattr(wrapper, 'adapter', func.__name__.upper())
                setattr(wrapper, 'params', params)
                return wrapper
            return decorator

        # add 'foo' property to each node
        @adapter(query='n()')
        def foo(task, records):
            nodes = []
            for n, in records:
                nodes.append({ 'ID': n['ID'], 'foo': True })
            return { 'nodes': nodes }

        # create/attach 'bar' node to nodes with 'foo' property
        # only for depths [0,2]
        @adapter(query='n(foo)', filter='1(depth<3)')
        def bar(task, records):
            chains = []
            for n, in records:
                self.assertTrue('foo' in n)
                for i in 1,2,3:
                    chain = [
                        { 'ID': n['ID'] },
                        { 'type': '%s_%s' % (n['type'], 'bar') },
                        { 'type': 'bar', 'value': 'bar%d' % random.randint(0,99999) },
                    ]
                    chains.append(chain)
            return { 'chains': chains }

        # add some extra edges to existing nodes
        @adapter(query='n()->e()->n()')
        def baz(task, records):
            chains = []
            for n1, e, n2 in records:
                if random.random() < 0.2:
                    chains.append([
                        { 'ID': n2['ID'] },
                        { 'type': 'baz' },
                        { 'ID': n1['ID'] },
                    ])
                if random.random() < .1:
                    chains.append([
                        { 'ID': n1['ID'] },
                        { 'type': 'baz', 'value': '%.2f' % random.random() },
                        { 'ID': n1['ID'] },
                    ])
            return { 'chains': chains }

        adapters = { f.adapter:f for f in locals().values() if hasattr(f, 'adapter') }
        configs = { f.adapter:f.params for f in adapters.values() }

        job = {
            # adapter configuration
            'adapters': configs,

            # mark data as 'seed' data so that nodes get assigned depth = 0
            'seed': True,

            'nodes':[
                { 'type': 'foo', 'value': '0' },
                { 'type': 'foo', 'value': '1' },
                { 'type': 'foo', 'value': '2' },
                { 'type': 'foo', 'value': '3' },
                { 'type': 'foo', 'value': '4' },
                { 'type': 'foo', 'value': '5' },
                { 'type': 'foo', 'value': '6' },
                { 'type': 'foo', 'value': '7' },
                { 'type': 'foo', 'value': '8' },
                { 'type': 'foo', 'value': '9' },
            ],
        }

        client = self.client

        code, headers, data = client.post('/graph', json=job)
        self.assertEqual(code, 201)
        jobID = data['id']

        code, headers, work = client.get('/lg')
        self.assertEqual(code, 200)
        progress = True
        while work:
            if progress is False:
                # wait until next tick before polling again
                now = time.time() * TPS
                time.sleep((1 - now + int(now)) / TPS)
            progress = False
            for adapter in work.keys():
                # try to pull task for adapter - set short timeout so lost tasks get replayed sooner
                code, headers, data = client.get('/lg/adapter/%s' % adapter, params=dict(timeout=1.0/TPS))
                if code == 204:
                    continue
                self.assertEqual(code, 200)
                task = str(headers['location'])

                # log receipt of a new task
                sys.stderr.write('+')
                sys.stderr.flush()

                try:
                    r = random.random()
                    if r < .20:
                        # simulate 20% random failure
                        raise Exception("fake error")
                    elif r < .40:
                        # simulate and log 20% random loss
                        sys.stderr.write('?')
                        sys.stderr.flush()
                        continue
                    # run adapter handler function
                    result = adapters[adapter](data)
                    if result is None:
                        result = {}
                    elif not isinstance(result, dict):
                        raise ValueError(result)
                    progress = True
                except AssertionError:
                    raise
                except Exception as e:
                    # log task exception
                    sys.stderr.write('!')
                    sys.stderr.flush()
                    result = {
                        'state': 'error',
                        'details': str(e),
                    }

                # post result
                code, headers, _ = client.post(task, json=result)
                self.assertIn(code, (200, 204))

                # mark any errored tasks for immediate retry
                code, headers, retry = client.post('/lg/task/%s' % jobID, json={
                    'state': 'error',
                    'update': {
                        'state': 'retry',
                    },
                })
                self.assertEqual(code, 200)
            code, headers, work = client.get('/lg')
            self.assertEqual(code, 200)

        # ensure all tasks are marked as done
        code, headers, tasks = client.get('/lg/task/%s' % jobID)
        self.assertEqual(code, 200)
        self.assertEqual(sum(1 for t in tasks), sum(1 for t in tasks if t['state'] == 'done'))

        # delete a single task
        code, headers, _ = client.delete('/lg/task/%s/%s' % (jobID, tasks[0]['task']))

        # bulk delete tasks
        code, headers, tasks = client.post('/lg/task/%s' % jobID, json={
            'state': 'done',
            'update': {
                'state': 'delete',
             }
        })
        self.assertEqual(code, 200)

        # check job graph status
        code, headers, status = client.get('/graph/%s/status' % jobID)
        self.assertEqual(code, 200)

        # delete job graph
        code, headers, status = client.delete('/graph/%s' % jobID)
        self.assertEqual(code, 204)

if __name__ == '__main__':
    unittest.main()
