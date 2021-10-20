#!/usr/bin/env python3

import aiohttp
import argparse
import asyncio
import sys
import time

from aiohttp.client_exceptions import ClientConnectorError

parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-u', '--url', help='LemonGraph REST service base url', default='http://localhost:8000')
parser.add_argument('-s', '--socket', help='LemonGraph REST service UNIX domain socket', default=None)
parser.add_argument('-p', '--priority', help='job priority', type=int, default=100)
parser.add_argument('count', help='job count', type=int, nargs='?', default=1)

args = parser.parse_args()

if args.socket is not None:
    sockpath = args.socket
    if sockpath[0] == '@':
        sockpath[0] = '\0'
    connector = aiohttp.UnixConnector(path=sockpath)
else:
    connector = None

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

async def main():
    async with aiohttp.ClientSession(connector=connector) as session:
        i = 0
        while i < args.count:
            # submit new job
            try:
                r = await session.post(args.url + '/graph', json=job)
            except ClientConnectorError:
                await asyncio.sleep(1)
                continue

            if r.status >= 300:
                await asyncio.sleep(1)
                continue

            ret = await r.json()
            print(ret['id'])

            i += 1

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
