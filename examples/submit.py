from __future__ import print_function

import argparse
import requests
import time
import sys

parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-u', '--url', help='LemonGraph REST service base url', default='http://localhost:8000')
parser.add_argument('-p', '--priority', help='job priority', type=int, default=100)
parser.add_argument('count', help='job count', type=int, nargs='?', default=1)

args = parser.parse_args()

job = {
    "meta": { "priority": args.priority },
    "adapters":{
        "FOO": { "query": "n()" },
        "BAR": { "query": "n(foo)" }
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
        r = requests.post(args.url + '/graph', json=job, headers={ 'x-please-pipeline': 'true' })
    except requests.exceptions.ConnectionError:
        time.sleep(1)
        continue

    if r.status_code >= 300:
        time.sleep(1)
        continue

    ret = r.json()
    print(ret['id'])

    i += 1
