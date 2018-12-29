import folium
import branca
import numpy as np


def plot_dataframe(dataframe, foliumMap, color_field=None, size_field=None, color='red', size=3, cm_colors=['red','yellow','green'],sm=[1,6],svr=None):
	if color_field:
		cm = branca.colormap.LinearColormap(colors=cm_colors)
		vmin = dataframe[color_field].min()
		vmax = dataframe[color_field].max()
		colorscale = cm.scale(vmin,vmax)
	if size_field:
		if svr:
			sizescale = svr
		else:
			smin = dataframe[size_field].min()
			smax = dataframe[size_field].max()
			sizescale = [smin,smax]
	if not dataframe.geometry.empty:
		geom_name = dataframe.geometry.name
		for index, row in dataframe.iterrows():
			geom  = row[geom_name]
			c = colorscale(row[color_field]) if color_field else color
			s = np.interp(row[size_field],sizescale,sm) if size_field else size
			if geom.geom_type =='Point':
				marker = folium.CircleMarker([geom.y,geom.x],radius=s, fill=True, fill_color=c,fill_opacity=0.7,weight=0)
				marker.add_to(foliumMap)
			elif geom.geom_type == 'LineString':
				xs = list(geom.coords.xy[0])
				ys = list(geom.coords.xy[1])
				polyline = folium.PolyLine([list(zip(ys,xs))],color=c,weight=s)
				polyline.add_to(foliumMap)

#
# def plot(dataframe, foliumMap, color_field=None,tool_tip_fields=[]):
# 	if color_field:
# 		vmin = dataframe[color_field].min()
# 		vmax = dataframe[color_field].max()
# 		colorscale = branca.colormap.linear.YlGnBu.scale(vmin,vmax)
# 	if not dataframe.geometry.empty:
# 		geom_name = dataframe.geometry.name
# 		if dataframe.geometry.geom_type == 'Point' or 'Point' in dataframe.geometry.geom_type:
# 			for index,row in dataframe.iterrows():
# 				geom = row[geom_name]
# 				x = geom.x
# 				y = geom.y
# 				tips_str = '\n'.join([str(row[col]) for col in tool_tip_fields])
# 				if color_field:
# 					marker = folium.CircleMarker([y,x],radius=6, popup=tips_str, color=colorscale(row[color_field]))
# 				else:
# 					marker = folium.CircleMarker([y,x],radius=6, popup=tips_str)
# 				marker.add_to(foliumMap)
# 		elif dataframe.geometry.geom_type == 'LineString':
# 			for index, row in dataframe.iterrows():
# 				geom = row[geom_name]
# 				xs = list(geom.coords.xy[0])
# 				ys = list(geom.coords.xy[1])
# 				if color_field:
# 					polyline = folium.PolyLine([list(zip(ys,xs))],color=colorscale(row[color_field]))
# 				else:
# 					polyline = folium.PolyLine([list(zip(ys,xs))])
# 				polyline.add_to(foliumMap)
#
# def plot_categories(dataframe, foliumMap, color_field, tool_tip_fields=[]):
# 	categories = dataframe[color_field].unique()
# 	cate_dict = {}
# 	for i, item in enumerate(categories):
# 		cate_dict[item] = i
# 	colors = plt.cm.get_cmap('tab20',len(categories))
# 	success = 0
# 	if not dataframe.geometry.empty:
# 		geom_name = dataframe.geometry.name
# 		if dataframe.geometry.geom_type[0] == 'Point':
# 			for index,row in dataframe.iterrows():
# 				geom = row[geom_name]
# 				x = geom.x
# 				y = geom.y
# 				tips_str = '\n'.join([str(row[col]) for col in tool_tip_fields])
# 				folium.CircleMarker([y,x],radius=6, popup=tips_str, color=colors(cate_dict[row[color_field]])).add_to(foliumMap)
# 				success += 1
# 		if dataframe.geometry.geom_type[0] == 'LineString':
# 			for index, row in dataframe.iterrows():
# 				geom = row[geom_name]
# 				xs = list(geom.coords.xy[0])
# 				ys = list(geom.coords.xy[1])
# 				lines=[list(zip(ys,xs))]
# 				folium.PolyLine(lines,color=colorscale(row[color_field])).add_to(foliumMap)
# 	return success