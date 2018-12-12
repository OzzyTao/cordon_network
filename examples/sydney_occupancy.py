from cordon.network import CordonNetwork
import peartree as pt
import os
from shapely.geometry import Point, LineString

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
path = os.path.join(BASE_DIR,'gtfs.zip')

feed = pt.get_representative_feed(path)
start = 0
end = 24*60*60
G = pt.load_feed_as_graph(feed,start,end)

cordon = CordonNetwork()
for n, d in G.nodes(data=True):
    name = n.split('_')[-1]
    cordon.add_node(name)
    cordon.set_node_geometry(name,Point(d['x'],d['y']))

for a, b, d in G.edges(data=True):
    from_name = a.split('_')[-1]
    to_name = b.split('_')[-1]
    segment_name = '''{}-{}'''.format(from_name,to_name)
    from_point = cordon.get_node_geometry(from_name)
    to_point = cordon.get_node_geometry(to_name)
    line = LineString([from_point, to_point])
    cordon.add_node(segment_name)
    cordon.set_node_geometry(segment_name, line)
    cordon.add_edge(from_name,segment_name)
    cordon.set_edge_geometry((from_name, segment_name),from_point)
    cordon.add_edge(segment_name,to_name)
    cordon.set_edge_geometry((segment_name, to_name),to_point)