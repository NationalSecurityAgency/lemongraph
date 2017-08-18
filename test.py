import os
import tempfile
import unittest

from LemonGraph import Graph, Serializer, dirlist, Query

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
    # each test_foo() method is wrapped w/ setup/teardown around it, so each test has a fresh graph
    def setUp(self):
        fd, path = tempfile.mkstemp()
        os.close(fd)
        self.g = Graph(path)

    def tearDown(self):
        self.g.delete()

    def test_commit(self):
        with self.g.transaction(write=True) as txn:
            self.assertEqual(txn.nextID, 1)
            n = txn.node(type='foo', value='bar')
            self.assertEqual(txn.nextID, 2)
            txn.commit()
            # should never reach this
            self.assertTrue(False)

        with self.g.transaction(write=False) as txn:
            self.assertEqual(txn.nextID, 2)

    def test_abort(self):
        with self.g.transaction(write=True) as txn:
            self.assertEqual(txn.nextID, 1)
            n = txn.node(type='foo', value='bar')
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

            e1 = txn.edge(src=n1, tgt=n2, type="foo")
            self.assertEqual(txn.edges_count(), 1)

        with self.g.transaction(write=True) as txn:
            self.assertEqual(txn.nodes_count(beforeID=n2.ID), 1)
            n3 = txn.node(type="foo", value="blah")
            self.assertEqual(txn.nodes_count(), 3)
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

    def test_fifo(self):
        with self.g.transaction(write=True) as txn:
            f = txn.fifo('foo', serialize_value=Serializer.uint())
            f.push(1, 2, 3)
            out = f.pop(4)
            self.assertEqual(out, (1, 2, 3))
            self.assertTrue(f.empty)
            f.push(4, 5, 6)
            self.assertFalse(f.empty)
            self.assertEqual(len(f), 3)
            out = f.pop(4)
            self.assertEqual(out, (4, 5, 6))
            self.assertEqual(len(f), 0)

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


class TestSerializers(unittest.TestCase):
    def test_default(self):
        s = Serializer()
        a = (None, 'foo', 1)
        b = ('', 'foo', '1')
        c = ('', 'foo', '1')
        for x, y, z in zip(a, b, c):
            self.assertEqual(s.encode(x), y)
            self.assertEqual(s.decode(y), z)

    def test_uint(self):
        s = Serializer.uint()
        a = (0, 255, 256, (1 << 64) - 1)
        b = ("\x00", "\x01\xff",
             "\x02\x01\x00", "\x08\xff\xff\xff\xff\xff\xff\xff\xff")
        c = (0, 255, 256, (1 << 64) - 1)
        for x, y, z in zip(a, b, c):
            self.assertEqual(s.encode(x), y)
            self.assertEqual(s.decode(y), z)

    def test_uints(self):
        s = Serializer.uints(2)
        a = ((0, 255), (256, (1 << 64) - 1))
        b = ("\x00\x01\xff",
             "\x02\x01\x00\x08\xff\xff\xff\xff\xff\xff\xff\xff")
        c = ((0, 255), (256, (1 << 64) - 1))
        for x, y, z in zip(a, b, c):
            self.assertEqual(s.encode(x), y)
            self.assertEqual(s.decode(y), z)

    def test_uints_string(self):
        s = Serializer.uints(3, string=True)
        a = ((0, 255, "foo"), (256, (1 << 64) - 1, ""))
        b = ("\x00\x01\xff\x01\x03foo",
             "\x02\x01\x00\x08\xff\xff\xff\xff\xff\xff\xff\xff\x00")
        c = ((0, 255, "foo"), (256, (1 << 64) - 1, ""))
        for x, y, z in zip(a, b, c):
            self.assertEqual(s.encode(x), y)
            self.assertEqual(s.decode(y), z)


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
        self.assertDictEqual(self.matches, results)


if __name__ == '__main__':
    unittest.main()
