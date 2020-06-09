from __future__ import print_function

import os
from random import randint
import sys
import tempfile
from time import time

import LemonGraph

def log(msg, *params):
    times.append(time())
    fmt = "%.3lf\t+%.3lf\t" + msg
    args = (times[-1] - times[0], times[-1] - times[-2]) + tuple(params)
    print(fmt % args)
    sys.stdout.flush()

def munge(t, n):
    return t + str(n % 5)

mil = tuple(range(0, 1000000))
pairs = set()
while len(pairs) < 1000000:
    pairs.add((randint(0, 999999), randint(0, 999999)))
pairs = sorted(pairs)

for run in (1, 2, 3):
    fd, path = tempfile.mkstemp()
    os.close(fd)
    nuke = [path]
    try:
        g = LemonGraph.Graph(path, serialize_property_value=LemonGraph.Serializer.msgpack(), noreadahead=True, nosync=True)
        nuke.append(path + '-lock')
        ret = LemonGraph.lib.graph_set_mapsize(g._graph, (1<<30) * 10)
        assert(0 == ret)

        times = [time()]
        with g.transaction(write=True) as txn:
            start = times[-1]
            nodes = [txn.node(type=munge('node', x), value=x) for x in mil]
            log("+1m nodes")
            if run == 1:
                elapsed = times[-1] - start
                print("total node insert time: %.3lf" % elapsed)
                print("total node insert rate: %.3lf" % (1000000 / elapsed))
                txn.commit()

            start = times[-1]
            for x, n in enumerate(nodes):
                n[munge('prop', x)] = munge('value', x)
            log("+1m props")
            if run == 2:
                elapsed = times[-1] - start
                print("total prop insert time: %.3lf" % elapsed)
                print("total prop insert rate: %.3lf" % (1000000 / elapsed))
                txn.commit()

            start = times[-1]
            for i, x_y in enumerate(pairs):
                x, y = x_y
                e = txn.edge(type=munge('edge', x+y), value=i, src=nodes[x], tgt=nodes[y])
            log("+1m edges")
            elapsed = times[-1] - start
            print("total edge insert time: %.3lf" % elapsed)
            print("total edge insert rate: %.3lf" % (1000000 / elapsed))
        print('size: %d' % g.size)
        g.close()
        print('')

    finally:
        for p in nuke:
            try:
                os.unlink(p)
            except:
                pass
