from collections import deque

# source/target must be LemonGraph.Node objects from the same graph
# if cost_cb is supplied:
#   it must be a callable and will be passed the edge being traversed
#   it must return a number that will be interpreted as edge weight/cost
# else cost_field may be supplied to pull edge weight/cost from a named edge property, defaulting to cost_default
# in all cases, the calculated edge weight/cost must be a non-negative number (floating point is fine)
def shortest_path(source, target, directed=False, cost_field=None, cost_default=None, cost_cb=None):
    if target.ID == source.ID:
        return 0, (source,)

    if cost_cb is None:
        if cost_field is None:
            cost_cb = lambda edge: cost_default
        else:
            cost_cb = lambda edge: edge.get(cost_field, cost_default)

    helper = _shortest_path_helper_directed if directed else _shortest_path_helper_undirected

    chain = deque([source])
    seen = deque([source.ID])
    state = deque([helper(source, seen, cost_cb, 0)])

    ret = None
    lowest = None
    while True:
        try:
            edge, node, cost = next(state[-1])
            if node.ID == target.ID:
                if lowest is None or cost < lowest:
                    ret = tuple(chain) + (edge, node)
                    lowest = cost
            elif lowest is None or cost < lowest:
                chain.extend((edge, node))
                seen.extend((edge.ID, node.ID))
                state.append(helper(node, seen, cost_cb, cost))
        except StopIteration:
            state.pop()
            if not state:
                return ret
            chain.pop()
            chain.pop()
            seen.pop()
            seen.pop()

def _shortest_path_helper_directed(node, seen, cost_cb, cost):
    for edge in node.iterlinks(filterIDs=seen, dir='out'):
        delta = cost_cb(edge)
        cost1 = cost + delta
        if cost1 < cost:
            raise ValueError(delta)
        yield edge, edge.tgt, cost1

def _shortest_path_helper_undirected(node, seen, cost_cb, cost):
    for edge in node.iterlinks(filterIDs=seen, dir='out'):
        delta = cost_cb(edge)
        cost1 = cost + delta
        if cost1 < cost:
            raise ValueError(delta)
        yield edge, edge.tgt, cost1
    for edge in node.iterlinks(filterIDs=seen, dir='in'):
        delta = cost_cb(edge)
        cost1 = cost + delta
        if cost1 < cost:
            raise ValueError(delta)
        yield edge, edge.src, cost1
