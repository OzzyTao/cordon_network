import networkx as nx
from .edge import EdgeDataWrapper
from .node import NodeDataWrapper
from .tabular.records import PresenceFieldsMapping, TransactionFieldsMapping
from .tabular.records import PresenceRecords, TransactionRecords
from .analysis.operation import LocalOperation
import pandas as pd
import geopandas as gpd
from .mapping.folium_helper import plot_dataframe

class DataTypeError(Exception):
    pass

class ParameterError(Exception):
    pass

class CordonNetwork(nx.DiGraph):
    def get_node_records(self, n):
        if 'records' in self.node[n]:
            return self.node[n]['records']
        else:
            return None

    def set_node_records(self, n, fieldmapping, records_df=None, static_data=None):
        if isinstance(fieldmapping, PresenceFieldsMapping):
            self.node[n]['records']=NodeDataWrapper(PresenceRecords(fieldmapping,records_df), static_data=static_data)
        else:
            raise DataTypeError('node should be attached with FieldsMapping object')

    def get_edge_records(self, edge):
        if 'records' in self[edge[0]][edge[1]]:
            return self[edge[0]][edge[1]]['records']
        else:
            return None

    def set_edge_records(self, edge, fieldmapping, records_df=None, static_data=None):
        if isinstance(fieldmapping, TransactionFieldsMapping):
            self[edge[0]][edge[1]]['records']=EdgeDataWrapper(TransactionRecords(fieldmapping, records_df), static_data=static_data)
        else:
            raise DataTypeError('Edge should be attached with FieldsMapping object')

    def set_node_geometry(self, node, geom):
        self.node[node]['geom'] = geom

    def get_node_geometry(self, node):
        return self.node[node]['geom']

    def set_edge_geometry(self, edge, geom):
        self[edge[0]][edge[1]]['geom'] = geom

    def get_edge_geometry(self, edge):
        return self[edge[0]][edge[1]]['geom']

    def map_presences(self, folium_map, color_field=None, weight_field=None):
        ids = []
        color_values = []
        weight_values = []
        geoms = []
        if color_field==None and weight_field==None:
            raise ParameterError('either a color_field or a weight_field has to be provieded')
        for n, d in self.nodes(data=True):
            nodedata = self.get_node_records(n)
            if nodedata!=None:
                ids.append(n)
                geoms.append(self.get_node_geometry(n))
                if isinstance(color_field, str):
                    if color_field in nodedata.static_data:
                        color_values.append(nodedata.static_data[color_field])
                    else:
                        color_values.append(None)
                elif callable(color_field):
                    v = color_field(nodedata.data)
                    if isinstance(v, int) or isinstance(v, float):
                        color_values.append(v)
                    else:
                        raise ParameterError('The provided function must return a single numetric value')
                else:
                    raise DataTypeError('color_field not recognized')

                if isinstance(weight_field, str):
                    if weight_field in nodedata.static_data:
                        weight_values.append(nodedata.static_data[weight_field])
                    else:
                        weight_values.append(None)
                elif callable(weight_field):
                    v = weight_field(nodedata.data)
                    if isinstance(v, int) or isinstance(v,float):
                        weight_values.append(v)
                    else:
                        raise ParameterError('The provided function must return a single numetric value')
                else:
                    raise DataTypeError('weight_field not recognized')
        df = pd.DataFrame({'id':ids,'color_value':color_values,'weight_value':weight_values,'geom':geoms})
        gdf = gpd.GeoDataFrame(df,geometry='geom')
        gdf.crs = {'init':'epsg:4326'}
        plot_dataframe(gdf,folium_map, color_field='color_value',size_field='weight_value')


    def map_transactions(self, folium_map, color_field=None, weight_field=None):
        from_ids = []
        to_ids =[]
        color_values = []
        weight_values = []
        geoms = []
        if color_field == None and weight_field == None:
            raise ParameterError('either a color_field or a weight_field has to be provieded')
        for e1,e2,d in self.edges(data=True):
            edgedata = self.get_edge_records((e1,e2))
            if edgedata!=None:
                from_ids.append(e1)
                to_ids.append(e2)
                geoms.append(self.get_edge_records((e1,e2)))
                if isinstance(color_field, str):
                    if color_field in edgedata.static_data:
                        color_values.append(edgedata.static_data[color_field])
                    else:
                        color_values.append(None)
                elif callable(color_field):
                    v= color_field(edgedata.data)
                    if isinstance(v, int) or isinstance(v, float):
                        color_values.append(v)
                    else:
                        raise ParameterError('The provided function must return a single numetric value')
                else:
                    raise DataTypeError('color_field not recognized')

                if isinstance(weight_field, str):
                    if weight_field in edgedata.static_data:
                        weight_values.append(edgedata.static_data[weight_field])
                    else:
                        weight_values.append(None)
                elif callable(weight_field):
                    v = weight_field(edgedata.data)
                    if isinstance(v, int) or isinstance(v, float):
                        weight_values.append(v)
                    else:
                        raise ParameterError('The provided function must return a single numetric value')
                else:
                    raise DataTypeError('weight_field not recognized')
        df = pd.DataFrame({'from_id':from_ids,'to_id':to_ids,'color_value':color_values,'weight_value':weight_values,'geom':geoms})
        gdf = gpd.GeoDataFrame(df, geometry='geom')
        gdf.crs = {'init':'epsg:4326'}
        plot_dataframe(gdf,folium_map,color_field='color_value',size_field='weight_value')


