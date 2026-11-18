"""Export a LemonGraph transaction's view of the graph to NetworkX.

    import LemonGraph
    from LemonGraph.nx import to_networkx

    g = LemonGraph.Graph(path)
    with g.transaction(write=False) as txn:
        G = to_networkx(txn)

NetworkX is not a LemonGraph dependency - it is imported on first use, and
ImportError is raised if it is missing.

Mapping (default graph_class=networkx.MultiDiGraph):

    graph properties     -> G.graph
    node                 -> G node keyed by node ID
    node type/value      -> node attributes 'type' and 'value'
    node properties      -> node attributes
    edge                 -> G edge (srcID, tgtID, key=edge ID)
    edge type/value      -> edge attributes 'type' and 'value'
    edge properties      -> edge attributes

Edges keep their direction (source -> target). Property values are whatever
the graph's serializers decode them to. The view is that of txn, so a
transaction opened with beforeID exports the graph as of that log position.
"""

from __future__ import absolute_import


def _attrs(obj, skip):
    return dict((k, v) for k, v in obj.as_dict().items() if k not in skip)


def to_networkx(txn, graph_class=None, node_type=None, edge_type=None):
    """Return a NetworkX graph of the nodes/edges visible to txn.

    graph_class: NetworkX graph class to instantiate (default MultiDiGraph).
                 With a non-multigraph class, parallel edges between the same
                 pair of nodes collapse to the last one seen, and the edge ID
                 is kept in the 'ID' edge attribute instead of the edge key.
                 With an undirected class, edge direction is lost.
    node_type:   only export nodes of this type; edges are then restricted to
                 those whose endpoints were both exported
    edge_type:   only export edges of this type
    """
    try:
        import networkx
    except ImportError:
        raise ImportError('LemonGraph.nx requires networkx (pip install networkx)')

    if graph_class is None:
        graph_class = networkx.MultiDiGraph
    G = graph_class()
    G.graph.update(txn.as_dict())

    nodes = txn.nodes() if node_type is None else txn.nodes(type=node_type)
    for node in nodes:
        G.add_node(node.ID)
        G.nodes[node.ID].update(_attrs(node, ('ID',)))

    edges = txn.edges() if edge_type is None else txn.edges(type=edge_type)
    multi = G.is_multigraph()
    for edge in edges:
        if edge.srcID not in G or edge.tgtID not in G:
            continue
        attrs = _attrs(edge, ('ID', 'srcID', 'tgtID'))
        if multi:
            G.add_edge(edge.srcID, edge.tgtID, key=edge.ID)
            G.edges[edge.srcID, edge.tgtID, edge.ID].update(attrs)
        else:
            G.add_edge(edge.srcID, edge.tgtID)
            attrs['ID'] = edge.ID
            data = G.edges[edge.srcID, edge.tgtID]
            data.clear()
            data.update(attrs)
    return G
