import LemonGraph
import os
import sys
import tempfile

# fd, path = tempfile.mkstemp()
# os.close(fd)
path = "./graphs/example"

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
    print("\n\n==== NODES ====")
    for n in txn.nodes():
        print(n)
    print("\n\n==== EDGES ====")
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
    print("\n\nad-hoc query: 'n()' nodes before deletions")
    for chain in txn.query('n()', stop=b4_delete):
        print(chain)

    print("\n\nad-hoc query: 'n(ID=1)' nodes before deletions")
    for chain in txn.query('n(ID=1)', stop=b4_delete):
        print(chain)

    print("\n\nad-hoc query: 'n(type=\"foo\")' nodes before deletions")
    for chain in txn.query('n()', stop=b4_delete):
        print(chain)

    # run an ad-hoc query
    print("\n\nad-hoc query: 'n()' nodes after deletions")
    for chain in txn.query('n()'):
        print(chain)

    # run streaming queries
    print("\n\nstreaming query: 'n(prop3)','e()','n()->n()'  nodes/edges")
    for q, chain in txn.mquery(['n(prop3)','e()','n()->n()'], start=1):
        print(q, "=>", chain)

    # dump the internal graph log to stdout
    print("\n\ndump:")
    txn.dump()

# delete graph artifacts from disk
g.delete()
print("DONE")