# -*- coding: utf-8 -*-
"""
Created on Thu Nov 12 11:44:38 2020

@author: Barney
"""
import os
import pandas as pd
from matplotlib import pyplot as plt

"""Misc
"""
TIMESTEP = 'H'

"""Addresses
"""
data_root = os.path.join("C:\\","Users","bdobson","Documents","GitHub","cwsd_demand","data")

raw_root = os.path.join(data_root, "raw")
processed_root = os.path.join(data_root, "processed")
results_root = os.path.join(data_root, "results")

wq_fid = os.path.join(processed_root, "wq_val.csv")
pol_fid = os.path.join(results_root, "pollutants.csv")
arc_fid = os.path.join(raw_root, "arclist.csv")
flow_fid = os.path.join(results_root, 'flows.csv')

"""Read
"""
flow_df = pd.read_csv(flow_fid)
wq_df = pd.read_csv(wq_fid)
pol_df = pd.read_csv(pol_fid)
arc_df = pd.read_csv(arc_fid)

wq_df = pd.merge(wq_df, arc_df[['inPort','name']], left_on='node', right_on = 'inPort')
wq_df.date = pd.to_datetime(wq_df.date).dt.round(TIMESTEP)

wq_df = wq_df.loc[wq_df.variable.isin(pol_df.pollutant.unique())]

pol_df.date = pd.to_datetime(pol_df.date)
def r2 (y, y_):
    return 1 - sum((y - y_)**2)/sum((y - y.mean())**2)

results = []
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
        if y.size > 1:
            results.append({'arc' : arc, 'pol' : pol, 'r2' : r2(y,y_)})
        maxo = max(y_.max(), y.max())
        mino = min(y_.min(), y.min())
        ax.scatter(y, y_,c='b')
        ax.plot([mino,maxo],[mino,maxo], color='r', linestyle = '--')
        ax.set_aspect('equal', 'box')
        ax.set_ylabel(pol)
    f.suptitle(arc)
    # f.savefig(os.path.join(results_root, arc + '.png'))
    plt.close(f)

combined_df = pd.merge(wq_df, pol_df, how='left', left_on=['date','variable','name'], right_on=['date','pollutant','arc'])
combined_df = combined_df.dropna()

combined_df['lin_cor'] = combined_df.result/combined_df.val

gb = pol_df.groupby(['arc','pollutant'])
def plot_arc(arc):
    
    f, axs = plt.subplots(1 + len(pol_df.pollutant.unique()),1)
    axs[0].plot(flow_df.loc[(flow_df.arc == arc),'val'])
    axs[0].set_ylabel('Flow (Ml/d)')
    
    for polut, ax in zip(pol_df.pollutant.unique(), axs[1:]):
        ax.plot(gb.get_group((arc, polut)).set_index('date').val, color='b')
        y = wq_df.loc[(wq_df.variable == polut) & (wq_df.name == arc)]
        if y.size > 0:
            y.set_index('date').result.plot(color='r',marker='x',linestyle='',ax=ax)
        ax.set_ylabel(polut + ' (mg/l)')

    ax.set_xlabel('Time (days)')
    f.suptitle(arc)
    return f

def plot_arc_p(arc,pol):
    f, ax = plt.subplots()
    ax.plot(gb.get_group((arc, pol)).set_index('date').val, color='b')
    y = wq_df.loc[(wq_df.variable == pol) & (wq_df.name == arc)]
    if y.size > 0:
        y.set_index('date').result.plot(color='r',marker='.',linestyle='',ax=ax,markersize=10)
    ax.set_ylabel(pol + ' (mg/l)')
    ax.set_xlabel('Time (days)')
    f.suptitle(arc)
    return f

plot_arc_p('wandle-to-thames','phosphate')