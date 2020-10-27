# -*- coding: utf-8 -*-
"""
Created on Wed Feb  5 10:31:11 2020

@author: bdobson
"""

import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString

"""Options
"""
CRS = "EPSG:3857"
driver = 'GeoJSON'
extension = '.geojson'

"""Addresses
"""

data_root = os.path.join("C:\\","Users","Barney","Documents","GitHub","cwsd_demand","data")

arc_path = os.path.join(data_root,"raw","arclist.csv")
node_path = os.path.join(data_root,"raw","nodelist.csv")
location_path = os.path.join(data_root,"raw","node_locations.csv")

nodes_output = os.path.join(data_root,"processed","nodes" + extension)
arcs_output = os.path.join(data_root,"processed","arcs" + extension)

"""Load data and convert to shapely objects
"""

arc_raw = pd.read_csv(arc_path, sep=',')
node_raw = pd.read_csv(node_path, sep=',')
loc_raw = pd.read_csv(location_path, sep=',')

#Use only locations in node_raw
nodes_of_interest = node_raw.name.unique()
loc_raw = loc_raw.set_index('node').loc[nodes_of_interest].reset_index()
arc_raw = arc_raw.loc[arc_raw.inPort.isin(nodes_of_interest) & arc_raw.outPort.isin(nodes_of_interest)]

node_df = pd.concat([loc_raw.set_index('node'), node_raw.pivot(index='name',columns='key',values='value')], axis=1, sort = False).reset_index()
nodes_geometry = [Point(xy) for xy in zip(loc_raw.lat, loc_raw.lon)]

arcs_geometry = []
starttype = []
endtype = []
for idx, arc in arc_raw.iterrows():
    startind = node_df.loc[node_df['index'] == arc.inPort].index[0]
    endind = node_df.loc[node_df['index'] == arc.outPort].index[0]
    starttype.append(node_df.loc[node_df['index'] == arc.inPort].type.values[0])
    endtype.append(node_df.loc[node_df['index'] == arc.outPort].type.values[0])
    arcs_geometry.append(LineString([nodes_geometry[startind],nodes_geometry[endind]]))

"""Print data
"""
nodes_gdf = gpd.GeoDataFrame(node_df, geometry = nodes_geometry)
nodes_gdf.crs = CRS
nodes_gdf.to_file(filename = nodes_output, driver = driver)

arc_gdf = gpd.GeoDataFrame(arc_raw, geometry = arcs_geometry)
arc_gdf.crs = CRS
arc_gdf['start_type'] = starttype
arc_gdf['end_type'] = endtype
arc_gdf.to_file(filename = arcs_output, driver = driver)