#include<stdlib.h>
#include<inttypes.h>
#include<fcntl.h>
#include<signal.h>

#include<db.h>
#include<lemongraph.h>
#include<osal.h>

db_snapshot_t graph_snapshot_new(graph_t g, int compact){
    return db_snapshot_new((db_t)g, compact);
}

int graph_set_mapsize(graph_t g, size_t mapsize){
    return db_set_mapsize((db_t)g, mapsize);
}

size_t graph_get_mapsize(graph_t g){
	size_t size;
	int r = db_get_mapsize((db_t)g, &size);
	return r ? 0 : size;
}

size_t graph_get_disksize(graph_t g){
	size_t size;
    int r = db_get_disksize((db_t)g, &size);
    return r ? 0 : size;
}

node_t asNode(entry_t e){
    return (node_t)e;
}

edge_t asEdge(entry_t e){
    return (edge_t)e;
}

prop_t asProp(entry_t e){
    return (prop_t)e;
}

deletion_t asDel(entry_t e){
    return (deletion_t)e;
}

node_t iter_next_node(graph_iter_t iter){
    return (node_t) graph_iter_next(iter);
}

edge_t iter_next_edge(graph_iter_t iter){
    return (edge_t) graph_iter_next(iter);
}

prop_t iter_next_prop(graph_iter_t iter){
    return (prop_t) graph_iter_next(iter);
}

void graph_node_delete(graph_txn_t txn, node_t e){
    graph_delete(txn, (entry_t)e);
}

void graph_edge_delete(graph_txn_t txn, edge_t e){
    graph_delete(txn, (entry_t)e);
}

void graph_prop_delete(graph_txn_t txn, prop_t e){
    graph_delete(txn, (entry_t)e);
}
