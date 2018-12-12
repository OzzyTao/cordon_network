import networkx as nx
from .edge import EdgeDataWrapper
from .node import NodeDataWrapper

class DataTypeError(Exception):
    pass


class CordonNetwork(nx.DiGraph):
    def get_node_records(self, n):
        return self.node[n]['records']

    def set_node_records(self, n, records):
        if isinstance(records, NodeDataWrapper):
            self.node[n]['records']=records
        else:
            raise DataTypeError('node should be attached with NodeDataWrapper')

    def get_edge_records(self, edge):
        return self[edge[0]][edge[1]]['records']

    def set_edge_records(self, edge, records):
        if isinstance(records, EdgeDataWrapper):
            self[edge[0]][edge[1]]['records']=records
        else:
            raise DataTypeError('Edge should be attached with EdgeDataWrapper')

    def set_node_geometry(self, node, geom):
        self.node[node]['geom'] = geom

    def get_node_geometry(self, node):
        return self.node[node]['geom']

    def set_edge_geometry(self, edge, geom):
        self[edge[0]][edge[1]]['geom'] = geom

    def get_edge_geometry(self, edge, geom):
        return self[edge[0]][edge[1]]['geom']

# def create_cordon_from_network(edges_df, checkpoints_df, movement_df):
#     #checkpoints_df should include column id and geom
#     #edges_df should include from, to and geom columns
#     #movement_df includes checkpoint id and timestamp
#
