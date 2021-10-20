#!/usr/bin/env python3

import aiohttp
import argparse
import asyncio
import random
import sys
import time

from aiohttp.client_exceptions import ClientConnectorError, ServerDisconnectedError, ClientOSError

class Adapter(object):
    max_active = 1
    def __init__(self, loop, base, sockpath, delay):
        self.loop = loop
        self.base = base
        self.poll = base + '/lg/adapter/' + self.name
        self.key = self.name, self.query
        self.delay = delay
        self.sockpath = sockpath
        self.connector = None
        if sockpath is not None:
            self.connector = aiohttp.UnixConnector(path=sockpath)

    async def run(self, active):
        print("spawn: %s" % self.name)
        try:
            async with aiohttp.ClientSession(connector=self.connector) as session:
                sleep = 0.05
                while sleep < 0.5:
                    if await asyncio.shield(self._do_task(session)):
                        sleep = 0.05
                    else:
                        await asyncio.sleep(sleep)
                        sleep += 0.05
        except Exception as e:
            print(e)
            raise
        finally:
            active.append(self)
            print("done: %s" % self.name)

    async def _do_task(self, session):
        try:
            async with session.post(self.poll, json={'query': self.query}) as res:
                if res.status != 200:
                    return
                loc = res.headers['Location']
                task = await res.json()
                chains = iter(task)
                meta = next(chains)
                ret = await self.handler(meta, chains)
                if ret is None:
                    ret = {}
                while True:
                    try:
                        await session.post(self.base + loc, json=ret)
                        if self.delay:
                            await asyncio.sleep(self.delay)
                        return True
                    except (ClientConnectorError, ServerDisconnectedError, ClientOSError):
                        await asyncio.sleep(1)
        except (ClientConnectorError, ServerDisconnectedError, ClientOSError):
            pass

class Foo(Adapter):
    name = 'FOO'
    query = 'n()'

    async def handler(self, meta, chains):
        nodes = []
        for n, in chains:
            n_upd = { 'ID': n['ID'], 'foo': 1 }
            nodes.append(n_upd)
        return { 'nodes': nodes }

class Bar(Adapter):
    name = 'BAR'
    query = 'n(foo)'

    async def handler(self, meta, chains_in):
        chains_out = []
        for n, in chains_in:
            for i in 1,2,3:
                if random.random() < 0.25:
                    continue
                chain = [
                    { 'ID': n['ID'] },
                    { 'type': '%s_%s' % (n['type'], 'bar') },
                    { 'type': 'bar', 'value': 'bar%d' % random.randint(0,99999) },
                ]
                # append to updates
                chains_out.append(chain)
        return { 'chains': chains_out }

class Baz(Adapter):
    name = 'BAZ'
    query = 'n()->e()->n()'

    async def handler(self, meta, chains_in):
        chains = []
        for n1, e, n2 in chains_in:
            if random.random() < 0.2:
                chains.append([
                    { 'ID': n2['ID'] },
                    { 'type': 'baz' },
                    { 'ID': n1['ID'] },
                ])
            if random.random() < .1:
                chains.append([
                    { 'ID': n1['ID'] },
                    { 'type': 'baz', 'value': '%.2f' % random.random() },
                    { 'ID': n1['ID'] },
                ])
        return { 'chains': chains }

class Monitor(object):
    def __init__(self, loop, base, sockpath, delay):
        self.loop = loop
        self.base = base
        self.adapters = {}
        self.poll = base + '/lg'
        self.delay = delay
        self.sockpath = sockpath
        self.connector = None
        if sockpath is not None:
            self.connector = aiohttp.UnixConnector(path=sockpath)

    async def run(self, *adapters):
        for cls in adapters:
            inst = cls(self.loop, self.base, self.sockpath, self.delay)
            try:
                self.adapters[inst.key].append(inst)
            except KeyError:
                self.adapters[inst.key] = [inst]

        async with aiohttp.ClientSession(connector=self.connector) as session:
            while True:
                try:
                    res = await session.get(self.poll)
                except (ClientConnectorError, ServerDisconnectedError, ClientOSError):
                    await asyncio.sleep(1)
                    continue
                status = await res.json()
                for name, data in status.items():
                    for query, count in data.items():
                        print((name, count, query))
                        try:
                            insts = self.adapters[name, query]
                        except KeyError:
                            continue
                        while count and insts:
                            count -= 1
                            asyncio.ensure_future(insts.pop().run(insts), loop=self.loop)
                await asyncio.sleep(1)

async def main(loop, base, sockpath, delay):
    if sockpath is not None and sockpath[0] == '@':
        sockpath[0] = '\0'
    m = Monitor(loop, base, sockpath, delay)
    await m.run(Foo, Bar, Baz, Foo, Bar, Baz)

parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-u', '--url', help='LemonGraph REST service base url', default='http://localhost:8000')
parser.add_argument('-s', '--socket', help='LemonGraph REST service UNIX domain socket', default=None)
parser.add_argument('-d', '--delay', help='delay in seconds per task', type=float, default=0)

args = parser.parse_args()

loop = asyncio.get_event_loop()
loop.run_until_complete(main(loop, args.url, args.socket, args.delay))
