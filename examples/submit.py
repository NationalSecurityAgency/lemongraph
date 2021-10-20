from __future__ import print_function

import argparse
import os
import time
from six.moves.urllib.parse import urlparse
import sys

basedir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(basedir)
sys.path.append(basedir)

from LemonGraph.httpc import RESTClient

parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-u', '--url', help='LemonGraph REST service base url', default='http://localhost:8000')
parser.add_argument('-s', '--socket', help='LemonGraph REST service UNIX domain socket', default=None)
parser.add_argument('-p', '--priority', help='job priority', type=int, default=100)
parser.add_argument('count', help='job count', type=int, nargs='?', default=1)

args = parser.parse_args()

server = urlparse(args.url)
if args.socket is not None:
    sockpath = args.socket
    if sockpath[0] == '@':
        sockpath[0] = '\0'
else:
    sockpath = None

rc = RESTClient(sockpath=sockpath, host=server.hostname, port=server.port or 80)

job = {
    "meta": { "priority": args.priority },
    "seed": True,
    "adapters":{
        "FOO": { "query": "n()" },
        "BAR": { "query": "n(foo)", "filter": "1(depth<3)" },
        "BAZ": { "query": "n()->e()->n()" },
    },
    "nodes":[
        { "type": "foo", "value": "bar0" },
        { "type": "foo", "value": "bar1" },
        { "type": "foo", "value": "bar2" },
        { "type": "foo", "value": "bar3" },
        { "type": "foo", "value": "bar4" },
        { "type": "foo", "value": "bar5" },
        { "type": "foo", "value": "bar6" },
        { "type": "foo", "value": "bar7" },
        { "type": "foo", "value": "bar8" },
        { "type": "foo", "value": "bar9" },
    ],
}

i = 0
while i < args.count:
    # submit new job
    try:
        status, headers, ret = rc.post('/graph', json=job)
    except rc.error:
        time.sleep(1)
        continue

    if status != 201:
        time.sleep(1)
        continue

    print(ret['id'])

    i += 1

rc.close()
