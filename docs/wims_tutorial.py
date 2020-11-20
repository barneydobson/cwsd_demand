# -*- coding: utf-8 -*-
"""
Created on Wed Mar 18 11:09:04 2020

@author: bdobson
"""
import os
import pandas as pd
from tqdm import tqdm
from matplotlib import pyplot as plt

"""Load data
"""
data_root = os.path.join("C:\\","Users","bdobson","Documents","data","wims","")
date_list = [str(x) for x in range(2000,2020)]
data = []
for date in tqdm(date_list):
    data.append(pd.read_csv(data_root + date + ".csv",sep=','))

"""Format data
"""
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

"""Find sample locations
"""
samples = data[['id','easting','northing','sample_material']].drop_duplicates()
samples = samples.set_index('id')
samples['n_measurements'] = data[['id','easting']].groupby('id').count()

variables = data[['id','variable']].drop_duplicates()
variables = variables.groupby('id')['variable'].unique()
samples['variables'] = variables
samples.to_csv(data_root + 'wims_points.csv',sep=',')

"""Plot individual stations
"""
ids = ['AN-WEN250','AN-YAR180','AN-YAR200']

subset = data.loc[data.id.isin(ids)]

id_var_counts = subset[['id','variable']].drop_duplicates().variable.value_counts()
common_vars = id_var_counts.loc[id_var_counts == len(ids)].index

variables = ['Nitrate-N','Phosphorus-P','Ammonia(N)']

f, axs = plt.subplots(len(variables))
for var, ax in zip(variables,axs):
    df_plot = subset.loc[subset.variable == var].pivot(index = 'date',columns = 'id',values = 'result')
    df_plot.plot(linestyle = '', marker = '.', ax=ax)
    ax.set_ylabel(var)
    

