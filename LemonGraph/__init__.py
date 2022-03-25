from __future__ import print_function
try:
    from ._lemongraph_cffi import ffi, lib
except ImportError:
    from setup import fetch_external
    fetch_external()

    # fall back to inline verify
    from .cffi_stubs import ffi, C_HEADER_SRC, C_KEYWORDS
    lib = ffi.verify(C_HEADER_SRC, **C_KEYWORDS)

try:
    import __builtin__ as builtin
except ImportError:
    import builtins as builtin

from collections import deque
import datetime
import itertools
from lazy import lazy
import os
from six import iteritems, itervalues
import sys

from .version import VERSION as __version__
from .callableiterable import CallableIterableMethod
from .hooks import Hooks
from .dirlist import dirlist
from .indexer import Indexer, BaseIndexer
from .MatchLGQL import QueryCannotMatch, QuerySyntaxError
from .lg_lite import LG_Lite
from . import algorithms
from . import wire
from . import unspecified

# these imports happen at the bottom
'''
from .kv import KV
from .query import Query
from .fifo import Fifo
from .serializer import Serializer
from .sset import SSet
from .pqueue import PQueue
'''

# so I can easily create keys/items/values class
# methods that behave as the caller expects
def listify_py2(func):
    # for Python 3+, return the supplied iterator/generator
    if sys.version_info[0] > 2:
        return func

    # for Python 2, slurp the result into a list
    def listify(*args, **kwargs):
        return list(func(*args, **kwargs))
    return listify

# todo:
#   think about splitting deletes off into its own btree so the log is truly append-only

# filled in after classes are defined
ConstructorsByRecType = {}

CastsByRecType = {
    lib.GRAPH_NODE: lib.asNode,
    lib.GRAPH_EDGE: lib.asEdge,
    lib.GRAPH_PROP: lib.asProp,
    lib.GRAPH_DELETION: lib.asDel,
}


def merge_values(a, b):
    """merges nested dictionaries, in case of type mismatch or non-dictionary, overwrites fields in a from b"""
    if isinstance(b, dict):
        if isinstance(a, dict):
            for k, v in iteritems(b):
                a[k] = merge_values(a.get(k,None), v)
            return a
    elif isinstance(b, (tuple, list)):
        # also merge and sort lists, but do not descend into them
        # we make no effort to deal w/ complex child objects
        try:
            c = set(a if isinstance(a, (tuple, list)) else ())
            c.update(b)
            return sorted(c)
        except TypeError:
            pass
    return b

class Graph(object):
    serializers = tuple('serialize_' + x for x in ('node_type', 'node_value', 'edge_type', 'edge_value', 'property_key', 'property_value'))

    def __init__(self, path, flags=0, mode=0o760, nosubdir=True, noreadahead=False, notls=False, nosync=False, nometasync=False, readonly=False, create=True, excl=False, hooks=Hooks(), adapters=None, **kwargs):
        self._graph = None
        for func in self.serializers:
            setattr(self, func, kwargs.pop(func, None) or self.default_serializer)
        if kwargs:
            raise KeyError(str(kwargs.keys()))

        if readonly:
            os_flags = lib.O_RDONLY
        else:
            os_flags = lib.O_RDWR
            if create:
                os_flags |= lib.O_CREAT
            if excl:
                os_flags |= lib.O_EXCL

        if noreadahead:
            flags |= lib.DB_NORDAHEAD
        if nosync:
            flags |= lib.DB_NOSYNC
        if nometasync:
            flags |= lib.DB_NOMETASYNC
        if notls:
            flags |= lib.DB_NOTLS

        self._graph = lib.graph_open(wire.encode(path), os_flags, mode, flags)

        if self._graph == ffi.NULL:
            raise IOError(ffi.errno, ffi.string(lib.graph_strerror(ffi.errno)))
        self._path = os.path.abspath(path)
        self.nosubdir = nosubdir
        self._inline_adapters = adapters
        self._hooks = hooks
        self._hooks.opened(self)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def sync(self, force=False):
        lib.graph_sync(self._graph, int(bool(force)))

    @builtin.property
    def size(self):
        return int(lib.graph_size(self._graph))

    def remap(self):
        lib.graph_remap(self._graph)

    @builtin.property
    def updated(self):
        return bool(lib.graph_updated(self._graph))

    def close(self):
        """close graph handle"""
        if self._graph not in (None, ffi.NULL):
            lib.graph_close(self._graph)
        self._graph = None

    @lazy
    def default_serializer(self):
        return Serializer()

    def transaction(self, write=True, beforeID=None):
        """returns transaction handle, to be used in a 'with' block"""
        return Transaction(self, write=write, beforeID=beforeID)

    def snapshot(self, bs=1048576, compact=True):
        """returns iterator that will produce blocks of binary snapshot data"""
        return SnapshotIterator(self, bs=bs, compact=compact)

    def delete(self):
        """close graph handle if it wasn't already, and delete backing files from filesystem"""
        if self._graph is not None:
            self.close()
        path = self.path
        if self.nosubdir:
            os.unlink(path)
            os.unlink(path+"-lock")
        else:
            os.unlink(os.path.join(path, 'data.mdb'))
            os.unlink(os.path.join(path, 'lock.mdb'))
            os.rmdir(path)
        self._hooks.deleted()

    @builtin.property
    def path(self):
        return self._path

    @builtin.property
    def fd(self):
        return lib.graph_fd(self._graph)


class SnapshotIterator(object):
    def __init__(self, graph, bs=10485760, compact=True):
        self._data = lib.graph_snapshot_new(graph._graph, int(compact))
        if self._data == ffi.NULL:
            raise Exception
        self._buffer = None
        self.bs = bs

    def __del__(self):
        if self._data and self._data != ffi.NULL:
            lib.db_snapshot_close(self._data)

    def __iter__(self):
        return self

    def __next__(self):
        if self._buffer is None:
            self._buffer = ffi.new('unsigned char[]', self.bs)
        buflen = lib.db_snapshot_read(self._data, self._buffer, self.bs)
        if 0 == buflen:
            raise StopIteration
        return ffi.buffer(self._buffer, buflen)[:]

    next = __next__


class GraphItem(object):
    def __init__(self):
        # subclasses must provide these
        self.ID = None
        self.beforeID = None
        self.txn = None
        self._txn = None
        raise NotImplementedError('write your own!')

    # return first non None value for beforeID from: supplied args or object
    def b4ID(self, *args):
        try:
            ret = next(x for x in args if x is not None)
        except StopIteration:
            ret = self.beforeID
        return ret

    # fetch Buffer for given ID - do not use resulting buffer outside of the transaction
    # it was obtained in, or after a potential modification (new nodes/edges/properties, etc)
    def string(self, ID, update=False):
        size = ffi.new('size_t *')
        buffer = lib.graph_string(self._txn, ID, size)
        return ffi.buffer(buffer, size[0])[:]

    # fetch graph/node/edge/property property object by key
    # yes, the graph itself as well as properties may have child properties
    def property(self, key):
        if key in self.reserved:
            return NativeProperty(self, key)
        _prop = self._get_property(self.txn.serialize_property_key.encode(key))
        return None if _prop == ffi.NULL else Property(self.txn, _prop)

    def get(self, key, default=None):
        prop = self.property(key)
        if prop is None:
            return default
        return prop.value

    def set(self, key, value, merge=False):
        self[key] = merge_values(self.get(key, None), value) if merge else value

    def update(self, upd, merge=False):
        if hasattr(upd, 'keys'):
            for k in upd.keys():
                self.set(k, upd[k], merge)
        else:
            for k, v in upd:
                self.set(k, v, merge)

    def __getitem__(self, key):
        prop = self.property(key)
        if prop is None:
            raise KeyError(key)
        return prop.value

    def unset(self, key):
        if key in self.reserved:
            raise KeyError
        self._unset_property(self.txn.serialize_property_key.encode(key))

    def __setitem__(self, key, value):
        if key in self.reserved:
            raise KeyError
        p = self._set_property(self.txn.serialize_property_key.encode(key), self.txn.serialize_property_value.encode(value))
        lib.free(p)

    def __delitem__(self, key):
        if key in self.reserved:
            raise KeyError
        prop = self.property(key)
        if prop is None:
            raise KeyError(key)
        prop.delete()

    def __contains__(self, key):
        prop = self.property(key)
        return prop is not None

    def __iter__(self):
        return self.iterkeys()

    def __len__(self):
        return sum(1 for p in self.properties())

    def iteritems(self):
        return self.properties(handler=lambda p: (p.key, p.value))

    def iterkeys(self):
        return self.properties(handler=lambda p: p.key)

    def itervalues(self):
        return self.properties(handler=lambda p: p.value)

    keys = listify_py2(iterkeys)
    items = listify_py2(iteritems)
    values = listify_py2(itervalues)


class EndTransaction(Exception):
    def __init__(self, txn):
        self.txn = txn

class AbortTransaction(EndTransaction):
    pass

class CommitTransaction(EndTransaction):
    pass

class Transaction(GraphItem):
    reserved = ()
    ID = 0

    # only use a transaction from within the creating thread, unless txn is readonly and DB_NOTLS was specified
    def __init__(self, graph, write=True, beforeID=None, _parent=None):
        self.graph = graph
        for func in graph.serializers:
            setattr(self, func, getattr(graph, func))
        self.write = write
        self.beforeID = 0 if beforeID is None else int(beforeID)
        self._parent = ffi.NULL if _parent is None else _parent
        self._txn = None
        self.txn_flags = 0 if write else lib.DB_RDONLY
        self._inline_adapters = graph._inline_adapters if write else None

    # create a child transaction - not possible if DB_WRITEMAP was enabled (it is disabled)
    def transaction(self, write=None, beforeID=None):
        return Transaction(self.graph,
            self.write if write is None else write,
            self.b4ID(beforeID),
            self._txn)

    def __enter__(self):
        self._txn = lib.graph_txn_begin(self.graph._graph, self._parent, self.txn_flags)
        if self._txn == ffi.NULL:
            raise IOError(ffi.errno, ffi.string(lib.graph_strerror(ffi.errno)))
        self._flushID = self._startID = self.nextID if self.write else None
        return self

    @builtin.property
    def updated(self):
        return lib.graph_txn_updated(self._txn)

    def flush(self, updated=False):
        if (updated or self.updated) and self._inline_adapters is not None and self.nextID > self._flushID:
            # exercise inline graph adapters
            self._timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            self._inline_adapters.update(self, self._flushID)
            del self._timestamp
            self._flushID = self.nextID

    def reset(self):
        err = lib.graph_txn_reset(self._txn)
        if err:
            raise IOError(int(err), ffi.string(lib.graph_strerror(err)))

    def _commit(self):
        if self.updated:
            self.flush(updated=True)
            self.lg_lite.ffwd(start=self._startID)
            updates = self.nextID - self._startID
            err = lib.graph_txn_commit(self._txn)
            if err:
                raise IOError(int(err), ffi.string(lib.graph_strerror(err)))
            self.graph._hooks.updated(self.graph, self._startID, updates)
        else:
            # no updates? just abort
            lib.graph_txn_abort(self._txn)

    def __exit__(self, type, value, traceback):
        suppress = False
        try:
            if type is None:
                self._commit()
            elif type is AbortTransaction and self is value.txn:
                suppress = True
                lib.graph_txn_abort(self._txn)
            elif type is CommitTransaction and self is value.txn:
                suppress = True
                self._commit()
            else:
                lib.graph_txn_abort(self._txn)
            return suppress
        finally:
            self.graph = self._txn = self._parent = None

    def abort(self):
        """immediately aborts a transaction and short circuits out of its 'with' block"""
        raise AbortTransaction(self)

    def commit(self):
        """immediately commits a transaction and short circuits out of its 'with' block"""
        raise CommitTransaction(self)

    # we do this because we really don't want any object ref cycles - that could cause problems with our __del__ methods
    # this lets us always use self.txn in the parent class
    def __getattr__(self, name):
        if name == 'txn':
            return self
        raise AttributeError(name)

    def _get_property(self, key):
        return lib.graph_get(self._txn, key, len(key), self.beforeID)

    def _set_property(self, key, value):
        return lib.graph_set(self._txn, key, len(key), value, len(value))

    def _unset_property(self, key):
        lib.graph_unset(self._txn, key, len(key))

    def properties(self, handler=None):
        return Iterator(self, lib.graph_props, lib.iter_next_prop, handler=handler, beforeID=self.beforeID)

    def _node_edge_property(self, Class, **kwargs):
        constructor = Class.ByID if 'ID' in kwargs else Class.ByAttr
        return constructor(self, **kwargs)

    # fetch node by ID or type/value, applying beforeID filter
    def node(self, **kwargs):
        return self._node_edge_property(Node, **kwargs)

    # fetch edge by ID or src/tgt/type/value, applying beforeID filter
    def edge(self, **kwargs):
        return self._node_edge_property(Edge, **kwargs)

    # fetch property by ID, applying beforeID filter
    # heads up! we are inspecting supplied parameters to decide whether or not to override!
    # if overridden, this will fetch a property object by ID, applying optional beforeID filter
    def property(self, *args, **kwargs):
        # if positional args were passed, we punt to the parent function
        if args:
            return super(Transaction, self).property(*args, **kwargs)
        return self._node_edge_property(Property, **kwargs)

    # return nodes iterator, optionally for a specific type
    def nodes(self, **kwargs):
        by_type = bool('type' in kwargs)
        type = kwargs.pop('type', None)
        beforeID = kwargs.pop('beforeID', None)
        if kwargs:
            raise TypeError('got an unexpected keyword argument[s]: {}'.kwargs.keys())

        if by_type:
            type = self.serialize_node_type.encode(type)
            ret = Iterator(self, lib.graph_nodes_type, lib.iter_next_node, args=(type, len(type)), beforeID=self.b4ID(beforeID))
        else:
            ret = Iterator(self, lib.graph_nodes, lib.iter_next_node, beforeID=self.b4ID(beforeID))
        return ret

    # return edges iterator, optionally for a specific type
    def edges(self, **kwargs):
        by_type = by_type_value = False
        try:
            type = kwargs.pop('type')
            by_type = True
            value = kwargs.pop('value')
            by_type_value = True
        except KeyError:
            pass

        beforeID = kwargs.pop('beforeID', None)
        if kwargs:
            raise TypeError('got an unexpected keyword argument[s]: {}'.kwargs.keys())

        if by_type:
            type = self.serialize_edge_type.encode(type)
            if by_type_value:
                value = self.serialize_edge_value.encode(value)
                ret = Iterator(self, lib.graph_edges_type_value, lib.iter_next_edge, args=(type, len(type), value, len(value)), beforeID=self.b4ID(beforeID))
            else:
                ret = Iterator(self, lib.graph_edges_type, lib.iter_next_edge, args=(type, len(type)), beforeID=self.b4ID(beforeID))
        else:
            ret = Iterator(self, lib.graph_edges, lib.iter_next_edge, beforeID=self.b4ID(beforeID))
        return ret

    def nodes_count(self, beforeID=None):
        return lib.graph_nodes_count(self._txn, self.b4ID(beforeID))

    def edges_count(self, beforeID=None):
        return lib.graph_edges_count(self._txn, self.b4ID(beforeID))

    @builtin.property
    def nextID(self):
        return int(lib.graph_log_nextID(self._txn))

    @builtin.property
    def lastID(self):
        return self.nextID - 1

    @builtin.property
    def updateID(self):
        return int(lib.graph_updateID(self._txn, self.beforeID))

    def as_dict(self):
        return dict(self.iteritems())

    def entry(self, ID, beforeID=None):
        _data = lib.graph_entry(self._txn, ID)
        if _data == ffi.NULL:
            raise IndexError
        return ConstructorsByRecType[_data.rectype](self, CastsByRecType[_data.rectype](_data), beforeID=self.b4ID(beforeID))

    def scan(self, start=1, stop=0):
        if start < 1:
            start = 1
        while True:
            if start == self.beforeID:
                return
            try:
                ret = self.entry(start, beforeID=start+1)
            except IndexError:
                return
            yield ret
            if start == stop:
                return
            start += 1

    def pretty(self, **kwargs):
        """pretty printer"""
        out = kwargs.pop('out', sys.stdout)
        for obj in self.scan(**kwargs):
            print(obj.pretty(), file=out)

    def dump(self, **kwargs):
        """not-so-pretty printer"""
        out = kwargs.pop('out', sys.stdout)
        for obj in self.scan(**kwargs):
            print(obj.dump(), file=out)

    def kv(self, domain, map_data=False, map_keys=False, serialize_key=None, serialize_value=None):
        """domain-specific key/value storage"""
        kwargs = {}
        if serialize_key:
            kwargs['serialize_key'] = serialize_key
        if serialize_value:
            kwargs['serialize_value'] = serialize_value
        return KV(self, domain, map_data=map_data, map_keys=map_keys, **kwargs)

    def sset(self, domain, map_values=False, serialize_value=None):
        """domain-specific sorted set"""
        kwargs = {}
        if serialize_value:
            kwargs['serialize_value'] = serialize_value
        return SSet(self, domain, map_values=map_values, **kwargs)

    def pqueue(self, domain, map_values=False, serialize_value=None):
        """domain-specific priority queue"""
        kwargs = {}
        if serialize_value:
            kwargs['serialize_value'] = serialize_value
        return PQueue(self, domain, map_values=map_values, **kwargs)

    def fifo(self, domain, map_values=False, serialize_value=None):
        """domain-specific fifo"""
        kwargs = {}
        if serialize_value:
            kwargs['serialize_value'] = serialize_value
        return Fifo(self, domain, map_values=map_values, **kwargs)

    def updates(self, **kwargs):
        """returns iterator for tuples of: node/edge before modification, node/edge after modification, the set of key names that changed, first and last txn ID to contribute to the delta, and the last logID processed"""
        changed = {}
        batch=kwargs.pop('batch', 500)
        for target in self.scan(**kwargs):
            ID = target.ID
            updated = []
            if isinstance(target, Property):
                # it is a property
                updated.append((target.parent, set([target.key])))
            else:
                updated.append((target, set(target)))
                # new edges cause changes in referenced nodes' properties
                if isinstance(target,Edge):
                    updated.append((target.src, Node.reserved_src_updates))
                    updated.append((target.tgt, Node.reserved_tgt_updates))

            for (obj, keys) in updated:
                if obj.ID in changed:
                    # updated record w/ latest revision of node/edge, and merge in affected key names
                    changed[obj.ID][1] = obj
                    changed[obj.ID][2].update(keys)
                    changed[obj.ID][4] = ID
                else:
                    # collect changes for up to $batch node/edges before yielding change sets
                    if len(changed) == batch:
                        for before_after_keys in itervalues(changed):
                            before_after_keys.append(ID)
                            yield tuple(before_after_keys)
                        changed.clear()
                    # snag copy of the object just before this update (might not exist yet), this object, and affected keys
                    changed[obj.ID] = [obj.clone(beforeID=ID), obj, set(keys), ID, ID]
        for before_after_keys in itervalues(changed):
            before_after_keys.append(ID)
            yield tuple(before_after_keys)

    def query(self, pattern, cache=None, **kwargs):
        q = Query((pattern,), cache=cache)
        for _, chain in q.execute(self, **kwargs):
            yield chain

    def mquery(self, patterns, cache=None, **kwargs):
        q = Query(patterns, cache=cache)
        return q.execute(self, **kwargs)

    # fetch numeric ID for binary buffer
    def stringID(self, data, update=unspecified):
        # default to txn read/write setting, allow 'update' param to override
        if update is unspecified:
            update = self.write
        ID = ffi.new('strID_t *')
        resolve = lib.graph_string_resolve if update else lib.graph_string_lookup
        if resolve(self._txn, ID, data, len(data)):
            return ID[0]
        raise KeyError(data)

    @lazy
    def lg_lite(self):
        return LG_Lite(self)

    @builtin.property
    def enabled(self):
        try:
            return bool(self['enabled'])
        except KeyError:
            return True

    @enabled.setter
    def enabled(self, val):
        self['enabled'] = bool(val)

    # priority [0..255], higher number is higher priority
    @builtin.property
    def priority(self):
        try:
            return sorted((0, int(self['priority']), 255))[1]
        except (KeyError, ValueError):
            return 100

    @priority.setter
    def priority(self, val):
        self['priority'] = sorted((0, int(val), 255))[1]

    def _snap(self, ID):
        return lib.graph_snap_id(self._txn, ID)


class NodeEdgeProperty(GraphItem):
    _lib_byID = None
    is_node = None
    is_edge = None
    is_property = None
    is_graph_property = None
    is_node_property = None
    is_edge_property = None
    is_deletion = None

    @builtin.classmethod
    def ByID(Class, txn, ID, beforeID=None, properties={}, merge=False):
        beforeID = txn.b4ID(beforeID)
        if ID < 1 or beforeID and ID >= beforeID:
            raise KeyError
        return Class(txn, Class._lib_byID(txn._txn, ID), beforeID=beforeID, properties=properties, merge=merge)

    @builtin.classmethod
    def ByAttr(Class, txn, **kwargs):
        properties = kwargs.pop('properties', None) or {}
        beforeID = txn.b4ID(kwargs.pop('beforeID', None))
        query = kwargs.pop('query', False)
        merge = kwargs.pop('merge', False)
        attr = Class._attr(txn, **kwargs)
        if query:
            attr = attr + (beforeID,)
            _data = Class._lib_lookup(txn._txn, *attr)
            if ffi.NULL == _data:
                return None
        else:
            assert(0 == beforeID)
            _data = Class._lib_resolve(txn._txn, *attr)
        return Class(txn, _data, beforeID=beforeID, properties=properties, merge=merge)

    def clone(self, beforeID=None):
        try:
            return self.ByID(self.txn, self.ID, beforeID=self.b4ID(beforeID))
        except KeyError:
            pass

    def __init__(self, txn, _data, beforeID=None, properties=None, merge=False):
        self.txn = txn
        self._txn = txn._txn
        self._data = _data
        if self._data is None or self._data == ffi.NULL:
            raise TypeError
        self.beforeID = txn.b4ID(beforeID)
        self.ID = int(_data.id)
        self.next = int(_data.next)
        if properties is None:
            return
        for key, value in iteritems(properties):
            self.set(key, value, merge=merge)

    @builtin.property
    def is_new(self):
        return bool(self._data.is_new)

    @builtin.property
    def updateID(self):
        return int(self._lib_updateID(self._txn, self._data, self.beforeID))

    def __del__(self):
        if self._data and self._data != ffi.NULL:
            lib.free(self._data)

    def _get_property(self, key):
        return self._lib_prop_get(self._txn, self._data, key, len(key), self.beforeID)

    def _set_property(self, key, value):
        return self._lib_prop_set(self._txn, self._data, key, len(key), value, len(value))

    def _unset_property(self, key):
        self._lib_prop_unset(self._txn, self._data, key, len(key))

    def _prop_iter(self, keys, handler=None):
        for key in keys:
            if key in self.reserved:
                obj = NativeProperty(self, key)
                yield obj if handler is None else handler(obj)

    # return iterator over node/edge/property properties
    def properties(self, handler=None, native=None):
        if native is None:
            native = self.discoverable
        props = Iterator(self.txn, self._lib_props_iter, lib.iter_next_prop, arg=self._data, handler=handler, beforeID=self.beforeID)
        return itertools.chain(self._prop_iter(native, handler=handler), props)

    def delete(self):
        self._lib_delete(self._txn, self._data)

    def __repr__(self):
        pairs = ["%s: %s" % (repr(key),repr(value)) for (key, value) in self.iteritems()]
        return '{' + ', '.join(pairs) + '}'

    def as_dict(self, trans=None, update=None, native=None):
        if trans is None:
            ret = dict( (prop.key, prop.value) for prop in self.properties(native=native) )
        else:
            ret = dict( (trans.get(prop.key, prop.key), prop.value) for prop in self.properties(native=native) )
        if update:
            ret.update(update)
        return ret


class NativeProperty(object):
    def __init__(self, obj, key):
        self.obj = obj
        self.key = key
        self.ID = 0

    @builtin.property
    def value(self):
        return getattr(self.obj, self.key)


class Deletion(NodeEdgeProperty):
    code = 'D'
    is_deletion = True
    discoverable = ('ID', 'targetID')

    def delete(self):
        raise AttributeError("cannot delete")

    @builtin.property
    def targetID(self):
        return self.next

    @builtin.property
    def target(self):
        return self.txn.entry(self.next, beforeID=self.ID)

    def dump(self):
        return self.ID, (self.code, self.next)

    def properties(self, handler=None):
        if handler is None:
            return iter(self.discoverable)
        return (handler(NativeProperty(self, x)) for x in self.discoverable)


class Node(NodeEdgeProperty):
    _lib_props_iter = lib.graph_node_props
    _lib_prop_get = lib.graph_node_get
    _lib_prop_set = lib.graph_node_set
    _lib_prop_unset = lib.graph_node_unset
    _lib_delete = lib.graph_node_delete
    _lib_byID = lib.graph_node
    _lib_lookup = lib.graph_node_lookup
    _lib_resolve = lib.graph_node_resolve
    _lib_updateID = lib.graph_node_updateID
    directions = {
        None:   0,
        'in':   lib.GRAPH_DIR_IN,
        'out':  lib.GRAPH_DIR_OUT,
        'both': lib.GRAPH_DIR_BOTH,
    }
    code = 'N'
    pcode = 'n'
    discoverable = ('ID', 'type', 'value')
    reserved_src_updates = frozenset(('outbound_count', 'edge_count', 'neighbor_count', 'neighbor_types'))
    reserved_tgt_updates = frozenset(('inbound_count',  'edge_count', 'neighbor_count', 'neighbor_types'))
    reserved_both = frozenset(('edges', 'edgeIDs', 'edge_count', 'neighbors', 'neighborIDs', 'neighbor_count', 'neighbor_types'))
    reserved_src_only = frozenset(('outbound', 'outboundIDs', 'outbound_count'))
    reserved_tgt_only = frozenset(('inbound',  'inboundIDs',  'inbound_count'))
    reserved_src = reserved_both | reserved_src_only
    reserved_tgt = reserved_both | reserved_tgt_only
    reserved_internal = frozenset(discoverable + ('typeID', 'valueID'))
    reserved = reserved_internal | reserved_src | reserved_tgt
    is_node = True

    @builtin.staticmethod
    def _attr(txn, type=None, value=None):
        type = txn.serialize_node_type.encode(type)
        value = txn.serialize_node_value.encode(value)
        return type, len(type), value, len(value)

    @builtin.property
    def type(self):
        return self.txn.serialize_node_type.decode(self.string(self._data.type))

    @builtin.property
    def value(self):
        return self.txn.serialize_node_value.decode(self.string(self._data.val))

    @builtin.property
    def inbound(self):
        return Iterator(self.txn, lib.graph_node_edges_in, lib.iter_next_edge, arg=self._data, beforeID=self.beforeID)

    @builtin.property
    def outbound(self):
        return Iterator(self.txn, lib.graph_node_edges_out, lib.iter_next_edge, arg=self._data, beforeID=self.beforeID)

    @CallableIterableMethod
    def edges(self, types=None, dir=None):
        direction = self.directions[dir]
        if types is None:
            for e in Iterator(self.txn, lib.graph_node_edges_dir, lib.iter_next_edge, args=(self._data, direction), beforeID=self.beforeID):
                yield e
            return

        for t in types:
            t = self.txn.serialize_edge_type.encode(t)
            for e in Iterator(self.txn, lib.graph_node_edges_dir_type, lib.iter_next_edge, args=(self._data, direction, t, len(t)), beforeID=self.beforeID):
                yield e

    @builtin.property
    def edgeIDs(self):
        return [e.ID for e in self.edges]

    @builtin.property
    def inboundIDs(self):
        return [e.ID for e in self.inbound]

    @builtin.property
    def outboundIDs(self):
        return [e.ID for e in self.outbound]

    @builtin.property
    def typeID(self):
        return int(self._data.type)

    @builtin.property
    def valueID(self):
        return int(self._data.val)

    @builtin.property
    def edge_count(self):
        return sum(1 for x in self.edges)

    @builtin.property
    def inbound_count(self):
        return sum(1 for x in self.inbound)

    @builtin.property
    def outbound_count(self):
        return sum(1 for x in self.outbound)

    @builtin.property
    def neighbor_count(self):
        return sum(1 for x in self._neighbors())

    @builtin.property
    def neighbor_types(self):
        types = {}
        for node in self.neighbors:
            types[node.type] = types.get(node.type,0) + 1
        return types

    @builtin.property
    def neighbors(self):
        return self._neighbors().values()

    @builtin.property
    def neighborIDs(self):
        return self._neighbors().keys()

    def _neighbors(self):
        neighbors = {}
        for e in self.edges:
            for n in e.iterlinks():
                if n.ID != self.ID:
                    neighbors[n.ID] = n
        return neighbors

    def iterlinks(self, filterIDs=None, types=None, dir=None):
        if filterIDs is None:
            filterIDs = ()
        for e in self.edges(types=types, dir=dir):
            if e.ID not in filterIDs:
                yield e

    def dump(self):
        return self.ID, (self.code, self.type, self.value, self.next)

    def shortest_path(self, target, **kwargs):
        if not isinstance(target, Node):
            raise ValueError(target)
        return algorithms.shortest_path(self, target, **kwargs)


class Edge(NodeEdgeProperty):
    _lib_props_iter = lib.graph_edge_props
    _lib_prop_get = lib.graph_edge_get
    _lib_prop_set = lib.graph_edge_set
    _lib_prop_unset = lib.graph_edge_unset
    _lib_delete = lib.graph_edge_delete
    _lib_byID = lib.graph_edge
    _lib_lookup = lib.graph_edge_lookup
    _lib_resolve = lib.graph_edge_resolve
    _lib_updateID = lib.graph_edge_updateID
    code = 'E'
    pcode = 'e'
    discoverable = ('ID', 'type', 'value', 'srcID', 'tgtID')
    reserved = frozenset(discoverable + ('src', 'tgt', 'typeID', 'valueID'))
    is_edge = True

    @builtin.staticmethod
    def _attr(txn, src=None, tgt=None, type=None, value=None):
        type = txn.serialize_edge_type.encode(type)
        value = txn.serialize_edge_value.encode(value)
        return src._data, tgt._data, type, len(type), value, len(value)

    @builtin.property
    def type(self):
        return self.txn.serialize_edge_type.decode(self.string(self._data.type))

    @builtin.property
    def value(self):
        return self.txn.serialize_edge_value.decode(self.string(self._data.val))

    @builtin.property
    def src(self):
        return self.txn.node(ID=self._data.src, beforeID=self.beforeID)

    @builtin.property
    def tgt(self):
        return self.txn.node(ID=self._data.tgt, beforeID=self.beforeID)

    @builtin.property
    def srcID(self):
        return self.src.ID

    @builtin.property
    def tgtID(self):
        return self.tgt.ID

    @builtin.property
    def typeID(self):
        return int(self._data.type)

    @builtin.property
    def valueID(self):
        return int(self._data.val)

    def iterlinks(self, filterIDs=None, types=None, dir=None):
        if filterIDs is None:
            filterIDs = ()
        if self.srcID != self.tgtID:
            # for non-loopback edges, in => src, out => tgt
            if dir != 'out' and self.srcID not in filterIDs:
                if types is None or self.src.type in types:
                    yield self.src
            if dir != 'in' and self.tgtID not in filterIDs:
                if types is None or self.tgt.type in types:
                    yield self.tgt
        else:
            # if edge is loopback, direction doesn't matter - just emit one node
            if self.srcID not in filterIDs:
                if types is None or self.src.type in types:
                    yield self.src

    def dump(self):
        return self.ID, (self.code, self.type, self.value, int(self._data.src), int(self._data.tgt), self.next)


class Property(NodeEdgeProperty):
    _lib_props_iter = lib.graph_prop_props
    _lib_prop_get = lib.graph_prop_get
    _lib_prop_set = lib.graph_prop_set
    _lib_prop_unset = lib.graph_prop_unset
    _lib_delete = lib.graph_prop_delete
    _lib_byID = lib.graph_prop
    _lib_updateID = lib.graph_prop_updateID
    pcode = 'p'
    discoverable = ('ID', 'key', 'value', 'parentID')
    reserved     = frozenset(discoverable + ('parent', 'keyID', 'valueID'))
    is_property = True

    # methods - note that we do not cache the key/value, as both:
    #  a) they could unpack to mutable complex data structures
    #  b) buffers returned by self.string() could be invalid after writes
    @builtin.property
    def key(self):
        return self.txn.serialize_property_key.decode(self.string(self._data.key))

    @builtin.property
    def value(self):
        return self.txn.serialize_property_value.decode(self.string(self._data.val))

    @lazy
    def parent(self):
        return self.txn.entry(self._data.pid, beforeID=self.beforeID) if self.parentID else None

    @builtin.property
    def parentID(self):
        return int(self._data.pid)

    @builtin.property
    def keyID(self):
        return int(self._data.key)

    @builtin.property
    def valueID(self):
        return int(self._data.val)

    @lazy
    def code(self):
        return self.parent.pcode if self.parentID else 'g'

    def dump(self):
        # omit parentID for graph properties
        pID = (self.parentID,) if self.parentID else ()
        return self.ID, (self.code,) + pID + (self.key, self.value, self.next)

    @lazy
    def is_graph_property(self):
        return self.parentID == 0

    @lazy
    def is_node_property(self):
        return self.parentID and self.parent.is_node

    @lazy
    def is_edge_property(self):
        return self.parentID and self.parent.is_edge


class Iterator(object):
    def __init__(self, txn, func, next_func, arg=None, args=(), handler=None, beforeID=None, filterIDs=None):
        assert(isinstance(txn, Transaction))
        self.txn = txn
        self.beforeID = txn.b4ID(beforeID)
        self.next_func = next_func
        self.handler = handler
        self.filterIDs = filterIDs

        if args:
            args = (txn._txn,) + tuple(args) + (self.beforeID,)
        elif arg is not None:
            args = (txn._txn, arg, self.beforeID)
        else:
            args = (txn._txn, self.beforeID)
        self._iter = func(*args)

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            _data = self.next_func(self._iter)
            if _data == ffi.NULL:
                lib.graph_iter_close(self._iter)
                self._iter = None
                raise StopIteration
            obj = ConstructorsByRecType[_data.rectype](self.txn, _data, beforeID=self.beforeID)
            if self.filterIDs is None or obj.ID not in self.filterIDs:
                break
        return obj if self.handler is None else self.handler(obj)

    def __del__(self):
        if self._iter is not None:
            lib.graph_iter_close(self._iter)

    next = __next__

# constructor glue
ConstructorsByRecType[lib.GRAPH_NODE] = Node
ConstructorsByRecType[lib.GRAPH_EDGE] = Edge
ConstructorsByRecType[lib.GRAPH_PROP] = Property
ConstructorsByRecType[lib.GRAPH_DELETION] = Deletion


class Adapters(object):
    def __init__(self, *generators):
        handlers = {}
        patterns = deque()
        scanners = deque()
        for gen in generators:
            if callable(gen):
                gen = gen()

            try:
                pattern = next(gen)
            except StopIteration:
                raise Exception("generator failed to return pattern (or None)")

            if pattern is None:
                scanners.append(gen)
                continue

            try:
                handlers[pattern].append(gen)
            except KeyError:
                handlers[pattern] = deque([gen])
                patterns.append(pattern)

        patterns = tuple(patterns)
        scanners = tuple(scanners)

        def scanner(entry):
            for gen in scanners:
                gen.send((entry.txn, entry))

        query = Query(patterns) if patterns else None

        if query is not None:
            def update_both(txn, start):
                for pattern, chain in query.execute(txn, start=start, scanner=scanner if scanners else None):
                    for gen in handlers[pattern]:
                        gen.send((txn,) + chain)
            self.update = update_both
        elif scanners:
            def update(txn, start):
                for entry in txn.scan(start=start):
                    scanner(entry)
            self.update = update
        else:
            raise Exception("no handlers!")

from .kv import KV
from .query import Query
from .fifo import Fifo
from .serializer import Serializer
from .sset import SSet
from .pqueue import PQueue
