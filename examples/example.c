
#include<stdlib.h>
#include<stdio.h>
#include<errno.h>
#include <time.h>

#define	O_RDWR		0x0002
#define	O_CREAT		0x0200

#include "lemongraph.h"

const char * const  path = "./graphs/example";
const int           mode = 0760;
const int           os_flags = O_RDWR | O_CREAT;
      int           db_flags = 0;

logID_t             B4_DELETE = 0;

int writable(graph_t g);
int readonly(graph_t g);

int main(int argc, const char *argv[]) {

    graph_t g = graph_open(path,
        os_flags, mode,
        db_flags//|DB_NOSYNC|DB_NORDAHEAD
    );

    if (g == NULL        
        || writable(g)
        || readonly(g)
        || errno)
    {
        fprintf(stderr, graph_strerror(errno));
        return 1;
    }

    // graph_sync(g,1);
    // graph_close(g);// TODO: seg faults
    return 0;
}

int writable(graph_t g) {
    // enter a write transaction
    graph_txn_t txn = graph_txn_begin(g, NULL, 0);
    if (txn == NULL)
        return 1;
    logID_t _startID = graph_log_nextID(txn);

    // create a couple of nodes
    node_t n1 = graph_node_resolve(txn, "foo",3, "bar",3);
    node_t n2 = graph_node_resolve(txn, "foo",3, "baz",3);

    // create an edge between them
    edge_t e1 = graph_edge_resolve(txn, n1, n2, "foo",3, "foobar",6);

    // set some properties
    graph_node_set(txn, n1, "prop1",5, "propval1",8);
    graph_node_set(txn, n2, "prop2",5, "propval2",8);
    graph_node_set(txn, n2, "prop3",5, "propval3",8);
    graph_edge_set(txn, e1, "prop4",5, "propval4",8);

    // set a graph property
    graph_set(txn, "thing1",6, "thing2",6);

    // print out nodes and edges
    puts("\n==== NODES ====");
    graph_nodes_print(
        graph_nodes(txn, 0)
    );
    puts("\n==== EDGES ====");
    graph_edges_print(
        graph_edges(txn, 0)
    );

    // delete a property # del node1['prop1']
    B4_DELETE = graph_log_nextID(txn) - 1;
    prop_t prop = graph_node_get(txn, n1, "prop1",5, 0);
    if (prop) {
        graph_delete(txn, (entry_t)prop);
        free(prop);
    }

    // delete a node - cascades to edges and properties
    graph_delete(txn, (entry_t)n2);

    free(n1);
    free(n2);
    free(e1);

    // commit if updated
    if (graph_txn_updated(txn)) {
        // flush self._inline_adapters.update(self, self._flushID) self._flushID = self.nextID
        int updates = graph_log_nextID(txn) - _startID;
        printf("\nupdates = %i", updates);
        graph_txn_commit(txn);
    }
    else {
        graph_txn_abort(txn);
    }

    return 0;
}

int readonly(graph_t g) {
    graph_txn_t txn = graph_txn_begin(g, NULL, DB_RDONLY);
    if (txn == NULL)
        return 1;

    // run an ad-hoc query before delete
    puts("\n\nad-hoc query: 'n()' B4_DELETE");
    graph_nodes_print(
        graph_nodes(txn, B4_DELETE)
    );
    puts("\nAFTER");
    graph_nodes_print(
        graph_nodes(txn, 0)
    );

    puts("\n\nad-hoc query: 'n(ID=1)' B4_DELETE");
    node_t node = graph_node(txn, 1);
    if (node) {
        graph_node_print(txn, node, B4_DELETE);
        puts("\nAFTER");
        graph_node_print(txn, node, 0);
        free(node);
    }

    puts("\n\nad-hoc query: 'n(type=\"foo\")' B4_DELETE");
    graph_nodes_print(
        graph_nodes_type(txn, "foo",3, B4_DELETE)
    );
    puts("\nAFTER");
    graph_nodes_print(
        graph_nodes_type(txn, "foo",3, 0)
    );

    graph_txn_abort(txn);
    return 0;
}
