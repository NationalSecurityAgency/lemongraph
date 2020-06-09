from __future__ import print_function

import argparse
import requests
import time

parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-u', '--url', help='LemonGraph REST service base url', default='http://localhost:8000')
parser.add_argument('-l', '--limit', help='desired number of records per task', type=int, default=200)

args = parser.parse_args()

while True:
    # fetch task json - we are just scanning for nodes
    try:
        r = requests.get(args.url + '/lg/adapter/FOO', params={ 'query': 'n()', 'limit': args.limit })
    except requests.exceptions.ConnectionError:
        time.sleep(1)
        continue

    if r.status_code != 200:
        time.sleep(1)
        continue
    task = r.json()
    records = iter(task)

    # first item is task metadata
    meta = next(records)
    print(meta['location'], r.headers['x-lg-priority'])
    nodes = []
    # remaining items are node/edge chains from query
    for chain in records:
        # our chains for the above query are single nodes
        n, = chain

        # safest to make new node or edge objects when updating
        # just use ID or type/value to identify target node/edge
        n_upd = { 'ID': n['ID'], 'foo': 1}
        print("  ",n)
        # append to updates
        nodes.append(n_upd)

    ret = None
    while ret is None:
        try:
            ret = requests.post(args.url + meta['location'], json={'nodes': nodes})
        except requests.exceptions.ConnectionError:
            time.sleep(1)
