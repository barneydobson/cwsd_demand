# -*- coding: utf-8 -*-
"""
Created on Wed Apr  8 10:59:33 2020

@author: bdobson
"""
import os
import pandas as pd
from tqdm import tqdm

"""Addresses
"""
wims_root = os.path.join("C:\\","Users","bdobson","Documents","data","wims","")
ceh_address = os.path.join("C:\\","Users","bdobson","Documents","data","quality","CEHThamesInitiative_WaterQualityData_2009-2013.csv")
project_data_root = os.path.join("C:\\","Users","bdobson","Documents","GitHub","cwsd_demand","data")
london_parameters_root = os.path.join(project_data_root, "raw")
ceh_converter_address = os.path.join(project_data_root, "raw", "misc", "wims_ceh_converter.csv")
processed_root = os.path.join(project_data_root, "processed")

"""Load and format data
"""
#Load WIMS data - warning uses a lot of ram
date_list = [str(x) for x in range(2000,2020)]
data = []
for date in tqdm(date_list):
    data.append(pd.read_csv(wims_root + date + ".csv",sep=','))

#Format data
data = pd.concat(data)
rename = {'sample.samplingPoint.notation' : 'id',
             'sample.samplingPoint.label' : 'location',
             'sample.sampleDateTime' : 'date',
             'determinand.label' : 'variable',
             'determinand.definition' : 'description',
             'resultQualifier.notation' : 'qualifier',
             'result' : 'result',
             'determinand.unit.label' : 'unit',
             'sample.sampledMaterialType.label' : 'sample_material',
             'sample.purpose.label' : 'purpose',
             'sample.samplingPoint.easting' : 'easting',
             'sample.samplingPoint.northing' : 'northing'}
data = data[rename.keys()]
data = data.rename(columns = rename)
data.date = pd.to_datetime(data.date)

#Add CEH initiative
df = pd.read_csv(ceh_address,sep=',',encoding='unicode_escape')
convert = pd.read_csv(ceh_converter_address ,sep=',')
convert = convert.dropna(subset=['name-in-wims'],how='any',axis=0)
df = df[convert['name-in-ceh-initiative']]
df = df.rename(columns = convert.set_index('name-in-ceh-initiative')['name-in-wims'].to_dict())
df = df.melt(id_vars=['id','date'])
convert = convert.drop('name-in-ceh-initiative',axis=1).drop_duplicates()
df['unit'] = convert.set_index('name-in-wims')['unit-in-ceh'].loc[df.variable].values
df = df.rename(columns={'value':'result'})
df.result = pd.to_numeric(df.dropna(subset=['result'],axis=0).result,errors='coerce')
data = data[df.columns]
df.date = pd.to_datetime(df.date,format='%d/%m/%Y')
data = pd.concat([data,df],axis=0,sort=False)

#Load nodes
node_path = os.path.join(london_parameters_root,"nodelist.csv")
node_raw = pd.read_csv(node_path,sep=',')
node_raw['value'] = [[float(y) for y in x.strip('[]').split(',')] if len(x.strip('[]').split(',')) > 1 else x for x in node_raw['value']]
numeric_ind = pd.to_numeric(node_raw['value'], errors='coerce').notna()
node_raw.loc[numeric_ind,'value'] = node_raw.loc[numeric_ind,'value'].astype(float)
node_raw = node_raw.drop(['unit','source'],axis = 1)

def buildNodedict(node_df):
    nodedict = {}
    
    for idx, group in node_df.groupby('name',sort = False):
        group = dict(group.drop('name',axis=1).values)
        group['name'] = idx
        nodedict[idx] = group['type']
    return nodedict

nodedict = buildNodedict(node_raw)
del node_raw, numeric_ind, node_path

#Assign data to nodes
wims_to_node = {'ravensbourne' : ['TH-PRVR0026'],
                'beckton-wwtw' : ['TH-PTNE0007'],
                'riverside-wwtw' : ['TH-PRGE0080'],
                'crossness-wwtw' : ['TH-PTSE0028'],
                'thames-estuary-mixer' : ['TH-PTTR0058','TH-PTTR0111','TH-PTTR0020','TH-PTTR0021','TH-PTTR0023'],
                'thames-central-wwtw-mixer' : ['TH-PTTG0035','TH-PTTR0057','TH-PTTR0019','TH-PTTR0018'],
                'thames-lee-ravensbourne-mixer' : ['TH-PTTR0015','TH-PTTR0016','TH-PTTR0017'],
                'deephams-wwtw' : ['TH-PLEE0040', 'TH-PLER0130'],
                'lee' : ['TH-PLER0060','TH-PLER0053'],
                'thames-wandle-mixer' : ['TH-PTTR0011'],
                'beddington-wwtw' : ['TH-PWAE0010'],
                'wandle-beddington-mixer' : ['TH-PWAR0062'],
                'hogsmill-wwtw' : ['TH-PHME0008'],
                'thames-at-teddington' : ['TH-PTHR0107'],
                'longreach-wwtw' : ['TH-PTSE0088','TH-PTSE0087'],
                'mogden-wwtw' : ['TH-PTNE0065'],
                'beverley-brent-incremental' : ['TH-PBRR0018','TH-PBVR0006','TH-PCRR0025'], #Stations have quite different catchment areas and 06/18 behave quite differently!
                'thames-at-dartford-incremental' : ['SO-E0000142','SO-E0000123','TH-PRGR0018','TH-PRGR0003','TH-PRGR0038'],
                'thames-upstream' : ['River Thames at Runnymede'],
                'lee-deephams-mixer' : ['TH-PLER0057']
                }

wq_df = pd.DataFrame(columns = data.columns.drop('id').tolist() + ['node'])
val_wq_df = pd.DataFrame(columns = data.columns.tolist() + ['node'])

gb = data.groupby(['id','variable'])
variables = []#need to add variables we're interested in with correct labels!
for var in variables:
    for name, stations in wims_to_node.items():
        data_stations = []
        for station in stations:
            try:
                sg = gb.get_group((station,var))
                scaled_results = sg.loc[sg.unit.str.contains('µ'),'result']/1000
                sg.loc[sg.unit.str.contains('µ'),'result'] = scaled_results
                sg = sg.set_index('date')
                data_stations.append(station)
            except:
                pass

        if len(data_stations) > 0:
            sg = [gb.get_group((x,var)) for x in data_stations]
            sg = pd.concat(sg,axis=0)
            mu_ind = sg.unit.str.contains('µ')
            scaled_results = sg.loc[mu_ind,'result']/1000
            sg.loc[mu_ind,'result'] = scaled_results
            sg.loc[mu_ind,'unit'] = 'mg/l'
            sg['node'] = name
            val_wq_df = pd.concat([val_wq_df, sg],axis=0)
            if nodedict[name] == 'Inflow':
                sg.date = pd.to_datetime(pd.DatetimeIndex(sg.date).date)
                sg['result']=sg['result'].astype(float)
                avg = sg.set_index('date').groupby('id').resample('D').mean().interpolate().reset_index().groupby('date').mean()[['result']]
                avg['variable'] = var
                avg['unit'] = sg.unit.unique()[0]
                avg['node'] = name
                wq_df = pd.concat([wq_df,avg.reset_index()],axis=0)


wq_df.to_csv(os.path.join(processed_root,"wq_forcing.csv"),sep = ',',index=False)
val_wq_df.to_csv(os.path.join(processed_root,"wq_val.csv"),sep = ',',index=False)
