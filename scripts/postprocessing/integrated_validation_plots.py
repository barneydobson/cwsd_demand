# -*- coding: utf-8 -*-
"""
Created on Thu Nov 12 11:44:38 2020

@author: Barney
"""
import os
import pandas as pd
from matplotlib import pyplot as plt

"""Addresses
"""
data_root = os.path.join("C:\\","Users","Barney","Documents","GitHub","cwsd_demand","data")

raw_root = os.path.join(data_root, "raw")
processed_root = os.path.join(data_root, "processed")
results_root = os.path.join(data_root, "results")

wq_fid = os.path.join(processed_root, "wq_val.csv")
pol_fid = os.path.join(results_root, "pollutants.csv")
arc_fid = os.path.join(raw_root, "arclist.csv")

"""Read
"""
wq_df = pd.read_csv(wq_fid)
pol_df = pd.read_csv(pol_fid)
arc_df = pd.read_csv(arc_fid)

wq_df = pd.merge(wq_df, arc_df[['inPort','name']], left_on='node', right_on = 'inPort')
wq_df.date = pd.to_datetime(wq_df.date).dt.date
wq_df = wq_df.loc[wq_df.variable.isin(pol_df.pollutant.unique())]

pol_df.date = pd.to_datetime(pol_df.date).dt.date

for arc in wq_df.name.unique():
    val_data = wq_df.loc[wq_df.name == arc]
    sim_data = pol_df.loc[pol_df.arc == arc]
    
    pollutants = val_data.variable.unique()
    n_pol = len(pollutants)
    
    f, axs = plt.subplots(2,4)
    
    for ax, pol in zip(axs.reshape(-1), pollutants):
        y = val_data.loc[val_data.variable == pol]
        dates = y.date
        ind = set(dates).intersection(sim_data.date)
        y = y.loc[y.date.isin(ind)]
        y_ = sim_data.loc[(sim_data.pollutant == pol) & (sim_data.date.isin(ind))]
        
        y = y.set_index('date').loc[ind, 'result']
        y_= y_.set_index('date').loc[y.index,'val']
        maxo = max(y_.max(), y.max())
        mino = min(y_.min(), y.min())
        ax.scatter(y, y_,c='b')
        ax.plot([mino,maxo],[mino,maxo], color='r', linestyle = '--')
        ax.set_aspect('equal', 'box')
        ax.set_ylabel(pol)
    f.suptitle(arc)
    f.savefig(os.path.join(results_root, arc + '.png'))
    plt.close(f)