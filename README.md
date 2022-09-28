# LemonGraph

LemonGraph is a log-based transactional graph (nodes/edges/properties) database engine that is backed by a single file. The primary use case is to support streaming seed set expansion - something along the lines of:

1. Create new graph
2. In client code, initialize log position bookmark: `pos = 0`
3. Inject seed data into graph
4. Starting at log position `pos`, query graph for desired patterns
5. Process returned results
6. Update client log position bookmark `pos = nextID` (either `txn.nextID` or via http header: `X-lg-maxID` + 1)
7. Inject new data into graph
8. Goto step 4

The core of the graph library is written in C, and the Python layer adds [friendly bindings](#python-example), a [query language](#query-language), and a [REST service](#rest-service). LemonGraph rides on top of (and inherits a lot of awesome from) Symas LMDB - a transactional key/value store that the OpenLDAP project developed to replace BerkeleyDB.

# Benchmarks

Using PyPy 6.0 and stock CentOS 7 Python 2.7.5, under a single transaction (3.4ghz i7-4770, plenty of RAM):

| Test  | Description                 | Disk usage |
| ----- | --------------------------- | ---------- |
| T1    | insert 1 million nodes      | 108mb      |
| T2    | add a property on each node | 157mb      |
| T3    | add 1 million random edges  | 292mb      |

| Test         | T1 (time) | T2 (time) | T3 (time) | T1 (rate) | T2 (rate) | T3 (rate) |
| ------------ | --------- | --------- | --------- | --------- | --------- | --------- |
| Python 2.6.9 | 23.915s   | 8.622s    | 16.787s   | 41.8k/s   | 116.0k/s  | 59.6k/s   |
| Python 2.7.5 | 17.238s   | 5.443s    | 18.164s   | 58.0k/s   | 183.7k/s  | 55.1k/s   |
| Python 3.5.5 | 12.159s   | 5.322s    | 15.434s   | 82.2k/s   | 187.9k/s  | 64.8k/s   |
| Python 3.7.0 | 11.740s   | 5.119s    | 15.235s   | 85.2k/s   | 195.4k/s  | 65.6k/s   |
| PyPy2 6.0    | 3.730s    | 2.145s    | 6.863s    | 268.1k/s  | 466.1k/s  | 145.7k/s  |
| PyPy3.5 6.0  | 5.117s    | 2.294s    | 8.815s    | 195.4k/s  | 435.8k/s  | 113.4k/s  |


# Features

Symas LMDB provides transactions, multi-process MVCC abilities (single writer, multiple non-blocking readers), nested write transactions, and rapid non-blocking binary snapshots. We also inherit some caveats:

* max database mapsize has to be manually maintained, but on 64-bit platforms it is cheap to overestimate
* transactions should only be used from the thread that created them
* the database file should be opened only once within a process
* [see also](http://symas.com/mdb/doc/)

The graph library on top supports:

* nodes - uniqued within graph by type and value
* directed edges - uniqued by source node, target node, type, and value
* property key/value pairs attached to the graph, nodes, edges, or properties - uniqued by key and parent object
* deletion of nodes/edges/properties
* historical views - examine graph as of log position X
* automatic mapsize growth, as long as a single txn's updates stay under 1gb

# Python

The Python wrapper provides a friendly interface:

* transactions are managed using ```with``` blocks
* graph/node/edge properties as well as domain key/value pairs can be manipulated using standard python dictionary-style operations
* custom serializer objects may be employed to facilitate complex data structures (i.e. json or msgpack)
* ad-hoc or streaming queries can be performed using a custom graph [query language](#query-language)

# Installation

## Python setup

Note that the REST service cannot run on CentOS 6's Python 2.6, as we rely on the new `memoryview` magic.

Python 3 should work now though - replace `python`/`pypy`/`easy_install` and package names with appropriate Python 3 variants.

* CPython on CentOS 6/7:
	* `yum install -y gcc gcc-c++ make libffi-devel zlib-devel python-devel python-setuptools`
	* `easy_install 'cffi>=1.0'`
* CPython on Ubuntu 15.04 - 16.04:
	* `apt-get install libffi-dev zlib1g-dev python-dev python-cffi`
* CPython (compiled) - just bootstrap setuptools and install cffi:
	* `python -mensurepip`
	* `easy_install 'cffi>=1.0'`
* Pypy - just bootstrap setuptools - cffi is bundled:
	* `pypy -mensurepip`

## LemonGraph installation

* `python setup.py install` (or you know, `pypy`)

Or to run without proper installation, you must manually install dependencies:
* CPython:
	* `easy_install lazy msgpack pysigset python-dateutil six ujson`
* Pypy:
	* `easy_install lazy msgpack pysigset python-dateutil six`

# Python Example
```python
import LemonGraph
import os
import sys
import tempfile

fd, path = tempfile.mkstemp()
os.close(fd)

# open graph object
print("graph save to ", path)
g = LemonGraph.Graph(path)

# enter a write transaction
with g.transaction(write=True) as txn:
    # create a couple of nodes
    node1 = txn.node(type='foo', value='bar')
    node2 = txn.node(type='foo', value='baz')

    # create an edge between them
    edge1 = txn.edge(src=node1, tgt=node2, type='foo', value='foobar')

    # set some properties
    node1['prop1'] = 'propval1'
    node2['prop2'] = 'propval2'
    node2['prop3'] = 'propval3'
    edge1['prop4'] = 'propval4'

    # set a graph property
    txn['thing1'] = 'thing2'

    # print out nodes and edges
    for n in txn.nodes():
        print(n)

    for e in txn.edges():
        print(e)

    b4_delete = txn.lastID

    # delete a property
    del node1['prop1']

    # delete a node - cascades to edges and properties
    node2.delete()
    print()

with g.transaction(write=False) as txn:
    # run an ad-hoc query before delete
    print("ad-hoc query: nodes before deletions")
    for chain in txn.query('n()', stop=b4_delete):
        print(chain)
    print()

    # run an ad-hoc query
    print("ad-hoc query: nodes after deletions")
    for chain in txn.query('n()'):
        print(chain)
    print()

    # run streaming queries
    print("streaming query: nodes/edges")
    for q, chain in txn.mquery(['n(prop3)','e()','n()->n()'], start=1):
        print(q, "=>", chain)
    print()

    # dump the internal graph log to stdout
    print("dump:")
    txn.dump()

# delete graph artifacts from disk
g.delete()
```

# Tools

Query a (potentially active) graph interactively:
* `python -mLemonGraph.dbtool path/to/foo.db`

Snapshot a (potentially active) database:
* `python -mLemonGraph.snapshot path/to/foo.db > copy.db`

# REST Service
Run the REST service (use `-h` to list options):
* `python -mLemonGraph.server <options>`

The rest service maintains an indexed cache of basic graph information. Every _N_ milliseconds (`-d` option), it will check for and flush updated graphs to disk.

For greater throughput, use `-s` at the expense of possible data loss in the event of OS/hardware/power-related failure.

By default, it will run:
* one master process responsible for [re]spawning and killing child threads
* one sync process responsible for syncing graphs to disk
* _N_ (`-w`) worker processes to handle http requests

# Query Language

This query language is designed to query for arbitrarily long node-edge-node chains in the graph, and supports two querying styles:
* ad-hoc patterns against the full graph
* concurrently evaluating one or more patterns in streaming mode over a range of graph log positions, returning newly matching patterns only

Note that in streaming mode, nodes and edges are captured as soon as they match the required filters. Any additional properties that are added later will not be present in the results.

A query pattern must specify at least one node or edge, but many may be chained together:
* `n()-e()-n()`

Results are returned as chains (lists) of the requested nodes and/or edges.

Arrow heads may be added to perform directed queries, and nodes or edges may be inferred. Inferred components are not included in the result set. To manually omit non-inferred components, prepend an 'at' sign to the node or edge token. These two queries are identical:
* `n()->n()`
* `n()->@e()->n()`

By default, nodes and edges must be unique in a returned chain. To suppress that for a given slot in the query pattern, upper-case the node/edge token:
* `n()->N()->n()`

To filter (and potentially accelerate) queries, a list of property filters may be provided inside the parenthesis for nodes and edges in the query:
* `n(type="foo")`

Property filters must specify at least a key, and optionally an operator with value[s].

Simple keys (consists entirely of 'word' characters, may not start w/ digit) may be specified as barewords, or otherwise be quoted. Nested keys for objects are supported - separate key components by unquoted dots.

## Operators:

### Bare keys:
* have no operator or value, and simply test for key existence

### Comparison operators:
* include the standard `<`, `>`, `<=`, `>=`, and translate directly into Python

### All remaining operators:
* may be negated by prepending a '!' to the operator
* may match against either a value or a list of values enclosed by square brackets

### Equality (`=`), values can be:
* numeric - floating point, integer, hexadecimal, octal
* string  - enclosed by quotes
* boolean - case insensitive bareword 'true' or 'false'
* null    - case insensitive bareword 'none' or 'null'

### Regular expression (`~`) (python flavored, in perl-style notation):
* Evaluated using Python's `re` module
* Formatted in Perl style, so we can add flags easily: `/pattern/flags`
* Flags can be any combo of:
	* `i` - case insensitive
	* `m` - multiline
	* `s` - dot matches newlines
	* `x` - verbose

### Value type (`:`):
* `boolean` - true/false
* `number`  - any numeric type
* `array`   - list
* `object`  - dict/hash

## Example queries:
* `n()` - select all nodes
* `e()` - select all edges
* `n()-e()-n()` - select all node/edge/node chains
* `n()-n()` - select node-to-node chains w/out the edge
* `n()->n()<-n()` - directed queries
* `n(type="foo")`
* `e(lon:[string,number], lat:[string,number])`
* `n(depth<=1, value~/unicorn/i)`

## Aliases and additional filters:

One or more additional filters may optionally be appended to a query pattern. The contents of each filter are effectively merged with the target node[s]/edge[s].

Indexing is one-based:
* `n(type="foo")-n(), 1(value!="bar"), 2(blah)`

Bareword aliases are supported as well - an alias can refer to one or more nodes or edges, and additional filters can be attached via the alias:
* `n:blah()-n:blah(), blah(edge_count<5)`

Internally, aliases are case-folded, but if either the alias or the additional filter in the query contains upper case characters, it is an error for the other not to be present.

Fine:
* `n:blah()`         (alias is ignored)
* `n(), blah()`      (filter is ignored)
* `n:blah(), blah()` (filter is applied to aliased node)
* `n:Blah(), BLAH()` (filter is applied to aliased node)

Error:
* `n:Blah()`    (missing filter)
* `n(), Blah()` (missing alias)

# Implementation Details

LMDB is an MVCC transactional key/value store (btree) and supports the concept of sub-tables. We use several tables to accomplish our goals:

Together, these provide an on-disk hash table for byte sequences - records in these tables are never deleted or updated:

* __scalar__ - maps incrementing numeric ID to byte sequence
* __scalar_idx__ - maps crc32(byte_sequence) to list of __scalar__ IDs


Actual graph data is stored in a log - records are never deleted, but a numeric field is updated in the event of deletion or property value update:

* __log__ - maps incrementing numeric logID to a graph update event: new node, edge, or property, or a deletion. Types/keys/values are transformed into numeric IDs using the above hash table.

Track stats for write transactions that grow the log:

* __txnlog__ - maps incrementing txnID and log offset/range to total node/edge counts

To provide speedy operations, graph data is indexed several ways (we reserve the right to add more) - records are never deleted or updated:

* __node_idx__ - stores typeID/valueID/logID tuples
* __edge_idx__ - stores typeID/valueID/srcID/tgtID/logID tuples
* __prop_idx__ - stores parentID/keyID/logID tuples
* __srcnode_idx__ - stores nodeID/typeID/edgeID tuples - referenced node is the source of the referenced edge of specified type
* __tgtnode_idx__ - stores nodeID/typeID/edgeID tuples - referenced node is the target of the referenced edge of specified type


We also provide a non-logged domain/key => value storage interface. Domains are mapped to IDs using the above __scalar__ storage, and keys and values can be as well. Mapping keys and/or values can result in less overall storage, but anything mapped never truly goes away. For non-mapped keys, maximum key length is limited to about 500 bytes. Unlike the others, deletes may be performed against this table:

* __kv__ - maps domain & optionally ID-mapped key tuple to optionally ID-mapped value
