#!/usr/bin/env python3

import aiohttp
import asyncio
import random
import sys

from aiohttp.client_exceptions import ClientConnectorError, ServerDisconnectedError

class Adapter(object):
    max_active = 1
    def __init__(self, loop, base):
        self.loop = loop
        self.base = base
        self.poll = base + '/lg/adapter/' + self.name
        self.key = self.name, self.query

    async def run(self, active):
        print("spawn: %s" % self.name)
        try:
            async with aiohttp.ClientSession(headers={'x-please-pipeline':'1'}) as session:
                sleep = 0.05
                while sleep < 0.5:
                    if await asyncio.shield(self._do_task(session), loop=self.loop):
                        sleep = 0.05
                    else:
                        await asyncio.sleep(sleep, self.loop)
                        sleep += 0.05
        except Exception as e:
            print(e)
            raise
        finally:
            active.append(self)
            print("done: %s" % self.name)

    async def _do_task(self, session):
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
                    return True
                except ClientConnectorError:
                    await asyncio.sleep(1, loop=self.loop)

class Foo(Adapter):
    name = 'FOO'
    query = 'n()'

    async def handler(self, meta, chains):
        nodes = []
        for chain in chains:
            if random.random() > .3:
                continue
            n, = chain
            n_upd = { 'ID': n['ID'], 'foo': 1 }
            nodes.append(n_upd)
        return { 'nodes': nodes }

class Bar(Adapter):
    name = 'BAR'
    query = 'n(foo)'

    async def handler(self, meta, chains_in):
        chains_out = []
        for n, in chains_in:
            chain_out = [{ 'ID': n['ID'], 'bar': 1 }]
            if random.random() < .3:
                chain_out.append({ "type": "foo", "value": "bar" })
                chain_out.append({ "type": "bar", "value": "bar" + str(n['ID']) })
            chains_out.append(chain_out)
        return { 'chains': chains_out }

class Monitor(object):
    def __init__(self, loop, base):
        self.loop = loop
        self.base = base
        self.adapters = {}
        self.poll = base + '/lg'

    async def run(self, *adapters):
        for cls in adapters:
            inst = cls(self.loop, self.base)
            try:
                self.adapters[inst.key].append(inst)
            except KeyError:
                self.adapters[inst.key] = [inst]

        async with aiohttp.ClientSession(headers={'x-please-pipeline':'1'}) as session:
            while True:
                try:
                    res = await session.get(self.poll)
                except (ClientConnectorError, ServerDisconnectedError):
                    await asyncio.sleep(1, loop=self.loop)
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
                await asyncio.sleep(1, loop=self.loop)

async def main(loop, base):
    m = Monitor(loop, base)
    await m.run(Foo, Bar, Foo, Bar)

loop = asyncio.get_event_loop()
loop.run_until_complete(main(loop, 'http://localhost:8000'))
