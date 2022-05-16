'''This exists solely for testing/example purposes, so I didn't have to add `requests` as a dependency - not recommended for use'''
from __future__ import print_function

import json as JSON
import socket

from six.moves.urllib.parse import quote
from six.moves.urllib.request import Request as request
from six.moves import http_client
from six import iteritems
from .httpd import Headers
from . import unspecified

# basic httplib/http.client wrapper for python 2/3
class RESTClient(object):
    '''This exists solely for testing/example purposes, so I didn't have to add `requests` as a dependency - not recommended for use'''
    class error(Exception):
        pass

    def __init__(self, sockpath=None, **kwargs):
        self.conn = None
        self.conn_args = kwargs
        if sockpath is not None:
            kwargs['sockpath'] = sockpath
            self.HTTPConnection = HTTPUnixConnection
        else:
            self.HTTPConnection = http_client.HTTPConnection

    def connect(self):
        if self.conn is None:
            try:
                self.conn = self.HTTPConnection(**self.conn_args)
            except Exception as e:
                raise self.error(e)

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def reconnect(self):
        self.close()
        self.connect()

    def __del__(self):
        self.close()

    def head(self, url, **kwargs):
        return self.request('HEAD', url, **kwargs)

    def get(self, url, **kwargs):
        return self.request('GET', url, **kwargs)

    def post(self, url, **kwargs):
        return self.request('POST', url, **kwargs)

    def put(self, url, **kwargs):
        return self.request('PUT', url, **kwargs)

    def delete(self, url, **kwargs):
        return self.request('DELETE', url, **kwargs)

    def request(self, method, url, body=None, headers={}, params={}, json=unspecified):
        self.connect()
        h = Headers()
        for label, value in iteritems(headers):
            h.add(label, value)
        if json is not unspecified:
            body = JSON.dumps(json, separators=(',',':'))
            h.set('Content-Type', 'application/json')

        if params:
            p = iteritems(params) if isinstance(params, dict) else params
            url += '?' + '&'.join('%s=%s' % (quote(k), quote(str(v))) for k, v in p)

        hh = dict(h.items())
        while True:
            try:
                self.conn.request(method, url=url, body=body, headers=hh)
                res = self.conn.getresponse()
                break
            except (BrokenPipeError, http_client.error):
                self.reconnect()
            except Exception as e:
                self.conn.close()
                raise self.error(e)

        headers = Headers()
        for header, value in res.getheaders():
            headers.add(header, value)
        data = res.read()
        if 'application/json' in headers['Content-Type'] and len(data):
            data = JSON.loads(data)

        try:
            if 'close' in headers['Connection']:
                self.close()
        except KeyError:
            pass

        return res.status, headers, data

# httplib/http.client unix domain socket connection support
class HTTPUnixConnection(http_client.HTTPConnection):
    def __init__(self, sockpath, host='localhost', **kwargs):
        self.sockpath = sockpath
        http_client.HTTPConnection.__init__(self, host, **kwargs)

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(self.sockpath)
