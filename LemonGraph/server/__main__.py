from __future__ import print_function

import logging
import sys
from getopt import GetoptError, gnu_getopt as getopt

from . import Server
from .. import Adapters, Serializer
from ..collection import Collection
from ..httpd import Graceful


class LogHandler(logging.StreamHandler):
    def __init__(self, stream=sys.stdout):
        logging.StreamHandler.__init__(self, stream)
        fmt = '%(asctime)-8s %(name)s(%(process)d).%(levelname)s: %(message)s'
        fmt_date = '%Y-%m-%d %T %Z'
        formatter = logging.Formatter(fmt, fmt_date)
        self.setFormatter(formatter)


def usage(msg=None, fh=sys.stderr):
    print('Usage: python -mLemonGraph.server <options> [graphs-dir]', file=fh)
    print('', file=fh)
    print('Options:', file=fh)
    print('  -i <bind-ip>        (127.0.0.1)', file=fh)
    print('  -p <bind-port>      (8000)', file=fh)
    print('  -d <poll-delay-ms>  (250)', file=fh)
    print('  -w <workers>        (#cpu cores)', file=fh)
    print('  -l <log-level>      (info)', file=fh)
    print('  -t <timeout>        (3 seconds)', file=fh)
    print('  -b <buflen>         (1048576)', file=fh)
    print('  -s                  enable nosync for graph dbs', file=fh)
    print('  -m                  enable nometasync for graph dbs', file=fh)
    print('  -r                  rebuild index', file=fh)
    print('  -h                  print this help and exit', file=fh)
    if msg is not None:
        print('', file=fh)
        print(msg, file=fh)
    sys.exit(1)


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
    log_levels = dict((k, default_level) for k in all_logs)
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
    for target, level in log_levels.items():
        logger = logging.getLogger(target)
        logger.addHandler(loghandler)
        logger.setLevel(getattr(logging, level.upper()))

    graph_opts = dict(
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

    Server(collection_path=path, graph_opts=graph_opts, extra_procs={'syncd': _syncd}, host=ip, port=port, spawn=workers, timeout=timeout, buflen=buflen)


main()
