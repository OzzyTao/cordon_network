from cordon.network import CordonNetwork
import peartree as pt
import os
from shapely.geometry import Point, LineString
import psycopg2
from cordon.tabular.records import PresenceFieldsMapping
from cordon.analysis.operation import LocalOperation
import folium

#read transportation network from gtfs file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
path = os.path.join(BASE_DIR,'gtfs.zip')

feed = pt.get_representative_feed(path)
start = 0
end = 24*60*60
G = pt.load_feed_as_graph(feed,start,end)

#constructe cordon network based on transportation network
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

stop_name_mapping_cache = {}
def match_stop(movement_stop_name):
    if movement_stop_name in stop_name_mapping_cache:
        return stop_name_mapping_cache[movement_stop_name]
    select_df = feed.stops[feed.stops.apply(lambda x: movement_stop_name.lower() in x['stop_name'].lower(), axis=1)]
    if select_df.empty:
        stop_name_mapping_cache[movement_stop_name] = ''
    else:
        stop_name_mapping_cache[movement_stop_name] = select_df.iloc[0]['stop_id']
    return stop_name_mapping_cache[movement_stop_name]

def num_occupancy_level(occupancy_range):
    if occupancy_range:
        level = occupancy_range.split(':')[0]
        if level == 'Low':
            return 1
        elif level == 'Medium':
            return 2
        elif level == 'High':
            return 3
    return 0

def segment_trip(trip_table):
    segments = []
    destination = None
    stops = []
    for row in trip_table:
        if row['stop'] == destination:
            stops.append(row)
            segments.append(stops)
            stops = [row]
            if row['destination'] == destination:
                destination = None
            else:
                destination = row['destination']
    if len(stops)>1:
        segments.append(stops)
    return segments

# populate cordon netowrk with occupancy data as presence information
db = psycopg2.connect('''dbname=sydney''')
sql = '''select distinct day_of_month, trip_name from train_occupancy;'''
trip_id = []
with db.cursor() as cur:
    cur.execute(sql)
    trip_id = cur.fetchall()
for t in trip_id[:100]:
    sql = '''select stop, direction, service_line, origin, destination, dprt_time, occupancy_range from train_occupancy
    			where day_of_month={day} and trip_name='{trip_name}' order by dprt_time;'''
    with db.cursor() as cur:
        cur.execute(sql.format(day=t[0], trip_name=t[1]))
        trip =cur.fetchall()
        trip_table = [{'stop':r[0], 'direction':r[1],'service_line':r[2], 'origin':r[3],'destination':r[4], 'dprt_time':r[5], 'occupancy_range':r[6]} for r in trip]
        for seg_trip in segment_trip(trip_table):
            segs = []
            route = []
            for seg_a, seg_b in zip(seg_trip,seg_trip[1:]):
                node_a = match_stop(seg_a['stop'])
                node_b = match_stop(seg_b['stop'])
                if node_a and node_b:
                    occupancy_dict = {'id':t[1]+'-'+str(t[0]),
                                        'start_time':seg_a['dprt_time'],
                                      'end_time':seg_b['dprt_time'],
                                      'occupancy_level': num_occupancy_level(seg_b['occupancy_range'])}
                    cordon_node = node_a+'_'+node_b
                    if cordon.has_node(cordon_node):
                        datawrapper = cordon.get_node_records(cordon_node)
                        datawrapper.add_record(occupancy_dict)

# plot train segment usage, i.e. the original presence data
field_mapping = PresenceFieldsMapping(id='id',start_time='start_time',end_time='end_time',occupancy_level='occupancy_level')
def average_occupancy(records):
    return records.df['occupancy_level'].mean()
operation = LocalOperation(average_occupancy)
map = folium.Map(tiles='Stamen Toner')
segment_group = folium.FeatureGroup(name='train_segment')
cordon.map_presences(segment_group,weight_field=operation)
map.add_child(segment_group)
map.save('./train_segment.html')




