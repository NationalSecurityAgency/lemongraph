from . import Graph, Serializer, QuerySyntaxError

import msgpack
import os
import re
import readline
import sys
import time

s = Serializer.msgpack()
cache = {}

HELP = '''Help:

To set ad-hoc query mode (default), simply enter: 0
To set streaming query mode, enter a numeric start,stop tuple or a non-zero start.

To query, enter one or more query patterns, joined by semi-colons.
To force streaming or ad-hoc mode, the line may be preceded by:
	start,stop:
or just:
	start:

Examples:
	400,500:n()
	n(type="foo")->n()
	1:e(type="bar")
'''

RANGE1 = re.compile(r'^(\d+)(?:\s*,\s*(\d+))?$')
RANGE2 = re.compile(r'^(\d+)(?:\s*,\s*(\d+))?\s*:\s*')

def parse_range(m, default=None):
    a = int(m.group(1))
    try:
        b = int(m.group(2)) if a else 0
    except TypeError:
        b = 0
    return a, b

def do_query(txn, query, start=0, stop=0, interactive=False):
    m = RANGE2.match(query)
    if m:
        start, stop = parse_range(m)
        query = query[m.end():]

    total = 0
    queries = query.split(';')
    tstart = time.time()
    mode = 'streaming' if start else 'ad-hoc'
    try:
        if 'dump' == query:
            txn.dump(start=start, stop=stop)
            mode = 'dump'
            total = None
        elif 'g' == query:
            print dict( (k, v) for k,v in txn.iteritems() )
            mode = 'graph properties'
            total = None
        elif len(queries) > 1:
            print queries
            for q, chain in txn.mquery(queries, cache=cache, start=start, stop=stop):
                print q, chain
                total += 1
        else:
            for chain in txn.query(query, cache=cache, start=start, stop=stop):
                print chain
                total += 1
    except KeyboardInterrupt:
        if interactive:
            print >>sys.stderr, "<cancelled>"
        else:
            raise
    tstop = time.time()
    return total, tstop-tstart, mode


def main(g):
    input = sys.argv[2:]
    prompt = None
    start = stop = 0
    with g.transaction(write=False) as txn:
        lastID = txn.lastID

    if not input or (len(input) == 1 and input[0] == '-'):
        if os.isatty(sys.stdin.fileno()):
            while True:
                if prompt is None:
                    prompt = '%s> ' % (repr((start, stop) if stop else (start,lastID)) if start else lastID)

                try:
                    line = raw_input(prompt)
                    while True:
                        hlen = readline.get_current_history_length()
                        if hlen < 2 or readline.get_history_item(hlen-1) != readline.get_history_item(hlen-2):
                            break
                        readline.remove_history_item(hlen-1)
                    line = line.strip()
                except KeyboardInterrupt:
                    continue
                except EOFError:
                    sys.exit(0)
                if not line:
                    continue

                m = RANGE1.match(line)
                if m:
                    start, stop = parse_range(m)
                    print "streaming %s" % ('enabled' if start else 'disabled')
                    prompt = None
                    line = None

                elif line in ('help','?'):
                    print HELP
                    line = None

                elif 'r' == line:
                    line = prompt = None
                elif 'history' == line:
                    for i in xrange(1, readline.get_current_history_length()):
                        print readline.get_history_item(i)
                    line = None

                if line or prompt is None:
                    with g.transaction(write=False) as txn:
                        if line:
                            try:
                                total, delta, mode = do_query(txn, line, start=start, stop=stop if stop else lastID, interactive=True)
                            except QuerySyntaxError as e:
                                print >>sys.stderr, str(e)
                                total = None
                                mode = 'error'
                                delta = 0
                            if total is None:
                                print "\n%s (%f seconds)\n" % (mode, delta)
                            else:
                                print "\n%d chains (%f seconds, %s)\n" % (total, delta, mode)
                        if lastID != txn.lastID:
                            lastID = txn.lastID
                            prompt = None

        else:
            input = sys.stdin

    for line in input:
        line = line.strip()
        with g.transaction(write=False) as txn:
            do_query(txn, line)

if '__main__' == __name__:
    with Graph(sys.argv[1], serialize_property_value=s, create=False) as g:
        main(g)
