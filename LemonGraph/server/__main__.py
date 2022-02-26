from __future__ import print_function
from .. import Serializer, Adapters
from ..collection import Collection
from . import Server
from .. import syncd
from ..uuidgen import setnode

import signal
import sys
import logging
import os
from getopt import GetoptError, gnu_getopt as getopt

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
    print('  -i <bind-ip|unix-socket> (127.0.0.1)', file=fh)
    print('  -p <bind-port>           (8000)', file=fh)
    print('  -w <workers>             (#cpu cores)', file=fh)
    print('  -l <log-level>           (info)', file=fh)
    print('  -t <timeout>             (3 seconds)', file=fh)
    print('  -b <buflen>              (1048576)', file=fh)
    print('  -u macaddr               MAC address to use in v1 uuids', file=fh)
    print('  -s                       enable nosync for graph dbs', file=fh)
    print('  -m                       enable nometasync for graph dbs', file=fh)
    print('  -n                       enable notls', file=fh)
    print('  -r                       rebuild index', file=fh)
    print('  -C                       disable static resource cache', file=fh)
    print('  -h                       print this help and exit', file=fh)
    if msg is not None:
        print('', file=fh)
        print(msg, file=fh)
    sys.exit(1)

def _neighbor_cost(n):
    for e in n.outbound:
        try:
            yield float(e['cost']), e.tgt
        except (KeyError, TypeError):
            continue
    for e in n.inbound:
        try:
            yield float(e['cost']), e.src
        except (KeyError, TypeError):
            continue

def _seed(entry, parent):
    if parent.is_node:
        parent['depth'] = 0

def _depth(entry, parent, inf=float('inf')):
    if parent.is_node:
        for cost, n in _neighbor_cost(parent):
            if cost < 0:
                continue
            depth = entry.value + cost
            if depth < n.get('depth', inf):
                n['depth'] = depth

def _cost(entry, parent, inf=float('inf')):
    if parent.is_edge:
        try:
            cost = float(entry.value)
        except TypeError:
            return
        src = parent.src
        tgt = parent.tgt
        ds = src.get('depth', inf)
        dt = tgt.get('depth', inf)
        if dt > ds + cost:
            tgt['depth'] = ds + cost
        elif ds > dt + cost:
            src['depth'] = dt + cost

def apply_seed_depth_cost():
    jump={'seed':_seed, 'depth': _depth, 'cost':_cost}
    while True:
        txn, entry = yield
        if entry.is_property:
            try:
                handler = jump[entry.key]
            except KeyError:
                continue
            if entry.parent is None:
                continue
            handler(entry, entry.parent)

def update_last_modified():
    while True:
        txn, entry = yield
        if entry.is_edge_property or entry.is_node_property:
            entry.parent['last_modified'] = txn._timestamp
        elif entry.is_edge or entry.is_node:
            entry['last_modified'] = txn._timestamp

def main():
    levels = ('NOTSET', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL')
    try:
        opts, args = getopt(sys.argv[1:], 'i:p:d:l:w:t:b:u:smnrCh')
    except GetoptError as err:
        usage(msg=str(err))
    if len(args) > 1:
        usage()

    default_level = 'info'

    ip = '127.0.0.1'
    port = 8000
    logspec = default_level
    nosync = False
    nometasync = False
    notls = False
    rebuild = False
    cache = True
    path = 'graphs'
    workers = -1
    timeout = 3
    buflen = 1048576
    try:
        path = os.path.abspath(args[0])
    except IndexError:
        pass

    warnings = set()
    try:
        for o, a in opts:
            if o == '-i':
                ip = a
            elif o == '-p':
                port = sorted((0, int(a), 65536))[1]
            elif o == '-d':
                warnings.add('-d option is deprecated and has no effect!')
            elif o == '-s':
                nosync = True
            elif o == '-m':
                nometasync = True
            elif o == '-n':
                notls = True
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
            elif o == '-u':
                setnode(a)
            elif o == '-C':
                cache = False
            elif o == '-h':
                usage(fh=sys.stdout)
            else:
                raise RuntimeError(o)
    except (KeyError, ValueError):
        usage()

    all_logs = tuple("LemonGraph.%s" % x for x in ('proc', 'httpd', 'collection', 'server', 'syncd', 'sockd'))
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
    for target, level in log_levels.items():
        logger = logging.getLogger(target)
        logger.addHandler(loghandler)
        logger.setLevel(getattr(logging, level.upper()))

    log = logging.getLogger('LemonGraph.server')
    for msg in warnings:
        log.warning(msg)

    graph_opts = dict(
        serialize_property_value=Serializer.msgpack(),
        adapters=Adapters(apply_seed_depth_cost, update_last_modified),
        nosync=nosync, nometasync=nometasync,
        )

    # initialize the collection up front (which will re-create if missing)
    # before we turn on the web server
    col = Collection(path, create=True, rebuild=rebuild, graph_opts=graph_opts, notls=notls)
    col.close()

    sd = syncd.Syncd(path)

    kwargs = dict(
        collection_path=path,
        collection_syncd=sd,
        graph_opts=graph_opts,
        notls=notls,
        extra_procs={'syncd': sd.receiver },
        host=ip,
        port=port,
        spawn=workers,
        timeout=timeout,
        buflen=buflen,
        cache=cache,
        )
    Server(**kwargs)

    # close syncd pipe, allowing syncd child to exit
    sd.shutdown()

    # and wait for any extra_procs (syncd child) to terminate
    try:
        while True:
            os.wait()
    except:
        pass

main()
