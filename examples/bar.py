from __future__ import print_function
import requests
import time

lg = 'http://localhost:8000'
while True:
    # fetch task json - we are just scanning for nodes
    try:
        r = requests.post(lg + '/lg/adapter/BAR', json={ 'query': 'n(foo)' })
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
        n_upd = { 'ID': n['ID'], 'bar': 1}

        # append to updates
        nodes.append(n_upd)

    ret = None
    while ret is None:
        try:
            ret = requests.post(lg + meta['location'], json={'nodes': nodes})
        except requests.exceptions.ConnectionError:
            time.sleep(1)
