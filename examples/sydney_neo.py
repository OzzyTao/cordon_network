from cordon.network_neo import GraphDBModel
import pandas as pd

CSVFILE=''
URI =''
USER = ''
PASSWORD = ''

df = pd.read_csv(CSVFILE)
trips = df['Trip.Name'].unique()
nodes = {}
segments = {}
connectivities = []




graph = GraphDBModel(URI,USER,PASSWORD)

for trip in trips:
    t = df[df['Trip.Name']==trip]
    pre_node = None
    for index, row in t.iterrows():
        station = row['Actual.Stop.Station']
        if station not in nodes:
            nodes[station] = {}
        if pre_node is not None and pre_node+'-'+station not in segments:
            segments[pre_node+'-'+station]  = {}
            connectivities.append((pre_node, pre_node+'-'+station, station))
        pre_node = row
graph.create_cordon_network(nodes, segments, connectivities)

movers = {}
for trip in trips:
    movers[trip] = {}
graph.add_movers(movers)

presences = []
for trip in trips:
    t = df[df['Trip.Name']==trip]
    pre_row = None
    for index, row in t.iterrows():
        presences.append({
            'mover_id':trip,
            'CNode_id':row['c'],
            'enter_time': row['Actual.Station.Arrv.Time'],
            'exit_time': row['Actual.Station.Dprt.Time']
        })
        if pre_row is not None:
            presences.append({
                'mover_id':trip,
                'CNode_id':pre_row['Actual.Stop.Station']+'-'+row['Actual.Stop.Station'],
                'enter_time' : pre_row['Actual.Station.Dprt.Time'],
                'exit_time' : row['Actual.Station.Arrv.Time'],
                'occupancy': row['Occupancy Status']
            })
        pre_row = row
graph.add_presence_records(presences)


