# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import os
import ukcensusapi.Nomisweb as census_api
import pandas as pd
import geopandas as gpd

"""
household data: QS406UK
commuter data: WU03EW
"""

os.environ["NOMIS_API_KEY"] = "<your_api_key>" #You can get API key by following instructions here https://pypi.org/project/ukcensusapi/
api = census_api.Nomisweb("/tmp/UKCensusAPI/")

#Addresses and misc
data_root = os.path.join("C:\\", "Users", "bdobson", "Documents", "GitHub", "cwsd_demand", "data")

msoa_fid = os.path.join("C:\\", "Users", "bdobson", "OneDrive - Imperial College London", "maps", "Middle_Layer_Super_Output_Areas__December_2011__Boundaries-shp", "Middle_Layer_Super_Output_Areas__December_2011__Boundaries.shp") # Available at https://geoportal.statistics.gov.uk/datasets/826dc85fb600440889480f4d9dbb1a24_0

area_fid = os.path.join(data_root, "raw", "wastewater_zones_traced.geojson")
output_fid = os.path.join(data_root, "processed", "zone_population_london.csv")

msoa_name = "msoa11cd"

area_name = "zone_name"

#Data
msoa_gdf = gpd.read_file(msoa_fid)
area_gdf = gpd.read_file(area_fid)

#Validation versions
# pc_lookup_fid = os.path.join("D:\\", "Barney", "data", "NSPL_NOV_2019_UK.csv")
# lad_fid = os.path.join(map_address, "Census_Merged_Local_Authority_Districts__December_2011__Boundaries-shp", "Census_Merged_Local_Authority_Districts__December_2011__Boundaries.shp")
# area_fid = os.path.join(map_address, "aws_dma", "Join_of_Sheet1_and_GMPRODI_DMA.shp")
# lad_name = "cmlad11cd"
# pc_lookup = pd.read_csv(pc_lookup_fid, usecols = ['pcd','admin_area11','lat', 'long'])
# lad_gdf = gpd.read_file(lad_fid)


#Define data retrieval
def query_api(table, GEOGRAPHY = None, USUAL_RESIDENCE = None, PLACE_OF_WORK = None):
    query_params = {}
    query_params["date"] = "latest"
    query_params["MEASURES"] = "20100"
    
    if table == "WU03EW":
        query_params["TRANSPORT_POWPEW11"] = "0"
        if USUAL_RESIDENCE:
            query_params["USUAL_RESIDENCE"] = USUAL_RESIDENCE 
        if PLACE_OF_WORK:
            query_params["PLACE_OF_WORK"] = PLACE_OF_WORK
        query_params["select"] = "USUAL_RESIDENCE,PLACE_OF_WORK,OBS_VALUE"
    elif table == "QS406UK":
        query_params["select"] = "GEOGRAPHY,CELL,OBS_VALUE"
        query_params["GEOGRAPHY"] = GEOGRAPHY
        query_params["CELL"] = "1...8"
    elif table == "QS112EW":
        query_params["select"] = "GEOGRAPHY,OBS_VALUE"
        query_params["GEOGRAPHY"] = GEOGRAPHY
        query_params['RURAL_URBAN'] = '0'
        query_params['C_HHCHUK11'] = '0'
        
    data = api.get_data(table, query_params)
    return data

#Process to find locations - classify msoa's to wwz's. Split if they cross boundaries
msoa_classified = gpd.overlay(msoa_gdf, area_gdf, how='intersection').explode().reset_index(drop=True) #May result in duplicated admin_area's that cross boundaries
# lad_joined = gpd.sjoin(lad_gdf,area_gdf, op='intersects') #May result in duplicated admin_area's that cross boundaries
msoa_classified['area'] = msoa_classified.geometry.area

locations_msoa = ','.join(msoa_classified[msoa_name].unique().tolist())
# locations_lad = ','.join(lad_joined[lad_name].unique().tolist())

#Get household data
household_data = query_api("QS406UK", locations_msoa)
md = api.get_metadata("QS406UK")['fields']['CELL']
household_data.CELL = [md[x] for x in household_data.CELL]
household_data = household_data.pivot(columns='CELL', index = 'GEOGRAPHY', values = 'OBS_VALUE')

#Get commute data
england_and_wales_id = 2092957703 # Census ID for'England and Wales'
# internal_commuters = query_api("WU03EW", USUAL_RESIDENCE = locations_lad, PLACE_OF_WORK = locations_lad)
total_workers_from = query_api("WU03EW", USUAL_RESIDENCE = locations_msoa, PLACE_OF_WORK = england_and_wales_id) # i.e. nighttime working population
total_workers_to = query_api("WU03EW", USUAL_RESIDENCE = england_and_wales_id, PLACE_OF_WORK = locations_msoa) # i.e. daytime working population

#Get population data
population_data = query_api("QS112EW", locations_msoa)

combined_population_data = pd.merge(population_data.rename(columns={'OBS_VALUE' : 'household_pop'}), total_workers_from.rename(columns={'OBS_VALUE' : 'workers_from'}), left_on='GEOGRAPHY', right_on='USUAL_RESIDENCE')
combined_population_data = pd.merge(combined_population_data, total_workers_to.rename(columns={'OBS_VALUE' : 'workers_to'}), left_on='GEOGRAPHY', right_on='PLACE_OF_WORK')
combined_population_data = combined_population_data[['GEOGRAPHY','household_pop','workers_from','workers_to']]
combined_population_data['workday_pop'] = combined_population_data['household_pop'] - combined_population_data['workers_from'] + combined_population_data['workers_to']

#Merge relevant data
df = pd.merge(msoa_classified[[msoa_name,'area','st_areasha',area_name]], combined_population_data, left_on = msoa_name, right_on = 'GEOGRAPHY')
df = pd.merge(df, household_data.reset_index(), left_on = msoa_name, right_on = 'GEOGRAPHY')
df = df[[area_name,'area','st_areasha','household_pop','workday_pop','workers_from','workers_to'] + household_data.columns.tolist()]

#Scale cross-boundary OA's based on area
df[df.columns.drop(area_name)] = df[df.columns.drop(area_name)].mul( df.area / df.st_areasha , axis='index')

#Aggregate to zone scale
df = df.groupby(area_name).sum().drop(['area', 'st_areasha'], axis = 1)

df.to_csv(output_fid)

#Create commute map
zones = msoa_classified.zone_name.unique()
msoa_lookup = msoa_classified[[msoa_name,'zone_name']]
workflows = []
for zone in zones:
    msoas_zone = msoa_classified.set_index('zone_name').loc[zone,msoa_name]
    msoas_zone = ','.join(msoas_zone.unique().tolist())
    
    msoas_not_zone = msoa_classified.set_index('zone_name').loc[zones[zones != zone],msoa_name]
    msoas_not_zone = ','.join(msoas_not_zone.unique().tolist())
    
    workflow_in = query_api("WU03EW", USUAL_RESIDENCE = msoas_not_zone, PLACE_OF_WORK = msoas_zone)
    workflow_in = pd.merge(workflow_in, msoa_lookup, left_on = 'USUAL_RESIDENCE', right_on = msoa_name).rename(columns={'zone_name' : 'home'})
    workflow_in = pd.merge(workflow_in, msoa_lookup, left_on = 'PLACE_OF_WORK', right_on = msoa_name).rename(columns={'zone_name' : 'work'})
    workflow_in = workflow_in.loc[(workflow_in.work == zone) & (workflow_in.home != zone)] #Sort out zones that cross boundaries (assume that's just internal movement)
    workflow_in = workflow_in.groupby(['home','work']).sum()
    
    workflows.append(workflow_in)
    
    # workflow_out = query_api("WU03EW", USUAL_RESIDENCE = msoas_zone, PLACE_OF_WORK = msoas_not_zone)
    # workflow_internal = query_api("WU03EW", USUAL_RESIDENCE = msoas_zone, PLACE_OF_WORK = msoas_zone)
    # workflow_ext_in = query_api("WU03EW", USUAL_RESIDENCE = england_and_wales_id, PLACE_OF_WORK = msoas_zone)
    # workflow_ext_out = query_api("WU03EW", USUAL_RESIDENCE = msoas_zone, PLACE_OF_WORK = england_and_wales_id)
    
workflows = pd.concat(workflows)    
    
total_workers_to = query_api("WU03EW", USUAL_RESIDENCE = england_and_wales_id, PLACE_OF_WORK = locations_msoa) # i.e. nighttime working population
