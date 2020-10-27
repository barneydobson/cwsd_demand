# -*- coding: utf-8 -*-
"""
Created on Wed Feb  5 09:29:01 2020

@author: bdobson
"""
import os
import pandas as pd
from matplotlib import pyplot as plt
import numpy as np


"""Addresses
"""

data_root = os.path.join("C:\\","Users","Barney","Documents","GitHub","cwsd_demand","data")
nrfa_data_root = os.path.join(data_root,"raw","nrfa") 
processed_data_root = os.path.join(data_root, "processed")

"""Gauge to node data_input matching (note - not all of these are actually used)
"""
names = {'38001_gdf' : 'lee-valley-flow',
         '38001_nat' : 'lee_nat',
         '39104_gdf' : 'mole',
         '39012_gdf' : 'hogsmill-river',
         '39079_gdf' : 'wey',
         '39128_gdf' : 'bourne',
         '39094_gdf' : 'crane',
         '39010_gdf' : 'colne',
         '39001_gdf' : 'teddington',
         '39001_nat' : 'teddington_nat',
         '39005_gdf' : 'beverley-brent',
         '39131_gdf' : 'brent',
         '39003_gdf' : 'wandle',
         '39056_gdf' : 'ravensbourne',
         '39095_gdf' : 'quaggy',
         '37001_gdf' : 'dartford-incremental',
         '40016_gdf' : 'cray',
         '40012_gdf' : 'darent',
         '37019_gdf' : 'beam',
         '37018_gdf' : 'ingrebourne',
         '38032_gdf' : 'lea-bridge',
         '37034_gdf' : 'mar_dyke',
         '39072_gdf' : 'thames-before-datchet',
         '38005_gdf' : 'ash',
         '38031_gdf' : 'lee-tributary',
         '38027_gdf' : 'stort'
         }

"""Load gauge data
"""
df = []
for gno in names.keys():
    df_ = pd.read_csv(os.path.join(nrfa_data_root, gno + '.csv'), sep=',',skiprows=20,error_bad_lines=False,warn_bad_lines=False, header=None)
    df_ = df_.rename(columns={0:'date',1:names[gno]}).set_index('date')
    df_.index = pd.to_datetime(df_.index)
    df.append(df_)
df = pd.concat(df,axis=1)

"""Drop missing naturalised flows
"""

df = df.loc[df['teddington_nat'].isna() == False]
df = df.loc[df['lee_nat'].isna() == False]

"""Fill in missing flows based on a linear scale on the mean from teddington nat or lee nat
"""
def scale(df_, scale_from):
    df = df_.copy()
    cols = df.columns.drop(scale_from)
    
    #Create dataframe where flows are entirely scaled
    scales = df[cols].div(df[scale_from],axis=0).mean()
    scaled_df = df.copy()
    scaled_df[cols] = np.array([df[scale_from].values] * len(cols)).T 
    scaled_df[cols] = scaled_df[cols].mul(scales)
    
    #Fill in dataframe with scaled values if missing data
    na_mask = df.isna()
    df[na_mask] = scaled_df[na_mask]
    return df
df1 = scale(df, 'teddington_nat')
df2 = scale(df, 'lee_nat')

df = df1
df[['lee-tributary','stort','ash']] = df2[['lee-tributary','stort','ash']]

"""Misc
"""
df['wandle'] = np.maximum(df['wandle'] - 0.5, 0) # to account for wwtw flow component (gauges are downstream of wwtw)
df['hogsmill-river'] = np.maximum(df['hogsmill-river'] - 0.5, 0) # to account for wwtw flow component (gauges are downstream of wwtw)
df['thames-upstream'] = np.maximum(df['teddington_nat'] - df['hogsmill-river'], 0)

"""Print to file
"""
df.to_csv(os.path.join(processed_data_root, "scaled_nrfa_flows.csv"),sep=',')