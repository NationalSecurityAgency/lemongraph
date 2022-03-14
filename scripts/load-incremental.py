#!/usr/bin/env python3

# for testing streaming graph updates on /view/<graph>

from __future__ import print_function

import argparse
import os
import time
from six.moves.urllib.parse import urlparse
import sys
import random
import webbrowser

basedir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(basedir)
sys.path.append(basedir)

from LemonGraph.httpc import RESTClient

parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-u', '--url', help='LemonGraph REST service base url', default='http://localhost:8000')
parser.add_argument('-d', '--delay', help='delay in seconds per task', type=float, default=1)

args = parser.parse_args()
server = urlparse(args.url)
rc = RESTClient(host=server.hostname, port=server.port or 80)

status, headers, js = rc.post('/graph', json={})
job = js['id']
webbrowser.open('%s/view/%s' % (args.url, job))
print(job)
links = []
for i in range(10):
	links.append(('foo', 'bar%d' % i))
	for j in range(10):
		links.append(('bar%d' % i, 'baz%d%d' % (i,j)))
	for j in range(5):
		r = random.randint(0,9)
		links.append(('foo', 'baz%d%d' % (j,r)))

e = 1
for src,dst in links:
	time.sleep(args.delay)
	rc.post('/graph/%s' % job, json={'chains':[[
		{'type':'x','value':src},
		{'type':'link', 'value':str(e)},
		{'type':'x','value':dst},
	]]})
	e += 1

rc.close()
