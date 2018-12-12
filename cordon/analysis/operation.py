import networkx as nx

class ParameterError(Exception):
    pass

class Operation(object):
    def __init__(self,func):
        self.func = func

    def get_input_elements(self, network, focus_node = None, focus_edge = None):
        if not isinstance(network,nx.DiGraph):
            raise ParameterError('first parameter should be a DiGraph object')
        if focus_node!=None and focus_edge!=None or focus_node==focus_edge:
            raise ParameterError('focus should be either a node or an edge')
        if focus_node and not network.has_node(focus_node):
            raise ParameterError('node {} does not exist'.format(focus_node))
        if focus_edge and not network.has_edge(*focus_edge):
            raise ParameterError('edge {} does not exist'.format(focus_edge))

    def apply(self, network, on_node=True, on_edge=False, in_place='column_name', role=None):
        node_results = []
        edge_results = []
        if on_node:
            for node in network.nodes():
                ns, es = self.get_input_elements(network, focus_node=node)
                nodes_data = [network.node[n]['records'] for n in ns]
                edges_data = [network[e[0]][e[1]]['records'] for e in es]
                result = self.func(nodes_data, edges_data)
                if in_place:
                    network.node[node]['records'].add_feature(in_place, data=result, role=role)
                node_results.append((node, result))
        if on_edge:
            for edge in network.edges():
                ns, es = self.get_input_elements(network, focus_edge=edge)
                nodes_data = [network.node[n]['records'] for n in ns]
                edges_data = [network[e[0]][e[1]]['records'] for e in es]
                result = self.func(nodes_data,edges_data)
                if in_place:
                    network[edge[0]][edge[1]]['records'].add_feature(in_place, data=result, role=role)
                edge_results.append((edge, result))
        return node_results, edge_results


class LocalOperation(Operation):
    def get_input_elements(self, network, focus_node = None, focus_edge = None):
        super().get_input_elements(network,focus_node,focus_edge)
        if focus_node:
            nodes = [focus_node,]
            edges = []
        else:
            nodes = []
            edges = [focus_edge,]
        return nodes, edges





class FocalOpeartion(Operation):
    def __init__(self, func, order = 1):
        super().__init__(func)
        self.order = order

    def get_input_elements(self, network, focus_node = None, focus_edge = None):
        super().get_input_elements(network,focus_node,focus_edge)
        if focus_node:
            path = nx.single_source_dijkstra_path(source=focus_node,cutoff=self.order)
            nodes = set([focus_node,])
            edges = set([])
            for _, p in path.items():
                nodes.union(set(p))
                edges.union(set([ x for x in zip(p,p[1:])]))
            return list(nodes), list(edges)
        else:
            node_a, node_b = focus_edge
            nodes = set([])
            edges = set([focus_edge])
            path = nx.single_source_dijkstra_path(source=node_a, cutoff=self.order - 1)
            for _, p in path.items():
                nodes.union(set(p))
                edges.union(set([ x for x in zip(p,p[1:])]))
            path = nx.single_source_dijkstra_path(source=node_b, cutoff=self.order - 1)
            for _, p in path.items():
                nodes.union(set(p))
                edges.union(set([x for x in zip(p, p[1:])]))
            return list(nodes), list(edges)






class ZonalOperation(Operation):
    def get_input_elements(self, network, focus_node = None, focus_edge = None, zone_dict={}):
        super().get_input_elements(network,focus_node,focus_edge)
        nodes = set([])
        edges = set([])
        if zone_dict:
            for n,d in network.nodes(data=True):
                shared = {k:zone_dict[k] for k in zone_dict if k in d and zone_dict[k]==d[k]}
                if shared == zone_dict:
                    nodes.add(n)
            for n,m,d in network.edges(data=True):
                shared = {k: zone_dict[k] for k in zone_dict if k in d and zone_dict[k] == d[k]}
                if shared == zone_dict:
                    edges.add((n,m))
            return list(nodes),list(edges)
        else:
            return list(network.nodes()),list(network.edges())
