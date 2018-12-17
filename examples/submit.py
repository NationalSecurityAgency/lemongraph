from __future__ import print_function
import requests
import time
import sys

lg = 'http://localhost:8000'

try:
    count = int(sys.argv[-1])
except:
    count = 1

job = {
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
while i < count:
    # submit new job
    try:
        r = requests.post(lg + '/graph', json=job, headers={ 'x-please-pipeline': "true" })
    except requests.exceptions.ConnectionError:
        time.sleep(1)
        continue

    if r.status_code >= 300:
        time.sleep(1)
        continue

    ret = r.json()
    print(ret['id'])

    i += 1
