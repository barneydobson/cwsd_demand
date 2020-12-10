# -*- coding: utf-8 -*-
"""
Created on Thu Nov 12 11:44:38 2020

@author: Barney
"""
import os
import pandas as pd
from matplotlib import pyplot as plt
import numpy as np
from scipy.stats import spearmanr
import misc

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
results_d = []
for arc in wq_df.name.unique():
    val_data = wq_df.loc[wq_df.name == arc]
    sim_data = pol_df.loc[pol_df.arc == arc]
    
    val_data_d = val_data.set_index('date').groupby(['name','variable']).resample('D').mean().reset_index().dropna()
    sim_data_d = sim_data.set_index('date').groupby(['arc','pollutant']).resample('D').mean().reset_index()
    
    pollutants = val_data.variable.unique()
    n_pol = len(pollutants)
    
    f, axs = plt.subplots(2,4)
    
    for ax, pol in zip(axs.reshape(-1), pollutants):
        def form(vd, sd):
            y = vd.loc[vd.variable == pol]
            dates = y.date
            ind = set(dates).intersection(sd.date)
            y = y.loc[y.date.isin(ind)]
            y_ = sd.loc[(sd.pollutant == pol) & (sd.date.isin(ind))]
            
            y = y.set_index('date').loc[ind, 'result']
            y_= y_.set_index('date').loc[y.index,'val']
            return y, y_
        y, y_ = form(val_data, sim_data)
        
        if y.size > 1:
            results.append({'arc' : arc, 'pol' : pol, 'r2' : r2(y,y_), 'n' : len(y), 'pb' : (y_ - y).sum()/y.sum(), 'sr' : spearmanr(y_, y)[0]})
        
        y, y_ = form(val_data_d, sim_data_d)
        if y.size > 1:
            results_d.append({'arc' : arc, 'pol' : pol, 'r2' : r2(y,y_), 'n' : len(y), 'pb' : (y_ - y).sum()/y.sum(), 'sr' : spearmanr(y_, y)[0]})
        
            
        maxo = max(y_.max(), y.max())
        mino = min(y_.min(), y.min())
        ax.scatter(y, y_,c='b')
        ax.plot([mino,maxo],[mino,maxo], color='r', linestyle = '--')
        ax.set_aspect('equal', 'box')
        ax.set_ylabel(pol)
    f.suptitle(arc)
    # f.savefig(os.path.join(results_root, arc + '.png'))
    plt.close(f)

results= pd.DataFrame(results)
# results['r2n'] = results.r2.round(2).astype(str) + ' (' + results.n.astype(str) + ')'
# results['f'] = results.pb.round(2).astype(str) + '|' + results.sr.round(2).astype(str) + '|' + results.r2.round(2).astype(str) + ' (' + results.n.astype(str) + ')'
results.pb *= 100
results = results.round(2)

"""Percentage bias heatmaps
"""
arc_labels = {'lee-to-thames' : 'LEE',
                'wandle-to-thames' : 'WAN',
                'thames-to-crane' : 'WQ site 2',
                'thames-flow-5' : 'WQ site 4',                
                'thames-flow-6' : 'WQ site 5',
                'thames-flow-7' : 'WQ site 6',
                'thames-flow-8' : 'WQ site 7/8/9',
                'thames-outflow' : 'WQ site 10/11/12',
                }
# results.arc = results.arc.str.replace('-treated-effluent', '')
results.arc = results.arc.replace(arc_labels)
rr_map = results.pivot(index='arc',columns='pol',values='pb')
#REmoved treated...
rr_map = rr_map.reindex(list(rr_map.index[rr_map.index.str.contains('treated')]) + list(arc_labels.values()))
rr_map.index = [x[0].upper() + x[1:] for x in rr_map.index]
rr_map.index = rr_map.index.str.replace('-treated-effluent', '')
rr_map = rr_map.rename(columns={'solids' : 'TSS',
                                'phosphorus' : 'P',
                                'phosphate' : 'PO4',
                                'nitrite' : 'NO2',
                                'nitrate' : 'NO3',
                                'cod' : 'COD',
                                'ammonia': 'NH3',
                                })
rr_map[rr_map > 100] = 100
rr_map[rr_map < -100] = 100
f = misc.colorgrid_plot(rr_map.T, isVal=True)
f.savefig(os.path.join(results_root, "percent_bias.svg"),bbox_inches='tight')
sum('asd')
#Print table
ss = results.pivot(index = 'arc',columns = 'pol', values = ['pb','sr','r2','n'])
ss.columns = ss.columns.swaplevel(0,1)
ss = ss.reindex(list(ss.index[ss.index.str.contains('(treated)')]) + list(arc_labels.values()))
ss.index = [x[0].upper() + x[1:] for x in ss.index]
ss = ss.sort_index(axis=1, level=0)
ss.to_csv(os.path.join(results_root, 'summary_r2.csv'))



combined_df = pd.merge(wq_df, pol_df, how='left', left_on=['date','variable','name'], right_on=['date','pollutant','arc'])
combined_df = combined_df.dropna()

combined_df['lin_cor'] = combined_df.result/combined_df.val

pol_df_d = pol_df.set_index('date').groupby(['arc','pollutant']).resample('D').mean().reset_index()
gb = pol_df.groupby(['arc','pollutant'])
gb_d = pol_df_d.groupby(['arc','pollutant'])
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

def plot_arc_p(arc,pol,dr, f, ax ,yl = None):
    p1 = ax.plot(gb.get_group((arc, pol)).set_index('date').val, color='b', label = 'Simulated (hr)')
    y = wq_df.loc[(wq_df.variable == pol) & (wq_df.name == arc)]
    p2 = ax.plot(gb_d.get_group((arc, pol)).set_index('date').val, color='c',linestyle='--',linewidth=2, label = 'Simulated (d)')
    if y.size > 0:
        p3 = y.set_index('date').result.plot(color='r',marker='.',linestyle='',ax=ax,markersize=10, label='Sampled')
    ax.set_ylabel(pol + ' (mg/l)')
    ax.set_xlabel('')
    ax.set_xlim(dr[0], dr[1])
    if yl is not None:
        ax.set_ylim( yl[0], yl[1] )
    ax.legend(loc = "upper left")
    return f
f, ax = plt.subplots(2,2, figsize = (10,10))
plot_arc_p('wandle-to-thames','phosphate',[pd.Timestamp('2008-02-25'), pd.Timestamp('2008-06-07')], f, ax[0][0], [0,8])
plot_arc_p('beckton-treated-effluent','solids',[pd.Timestamp('2009-01-01'), pd.Timestamp('2009-07-01')], f, ax[0][1],[0,50])
plot_arc_p('lee-to-thames','cod',[pd.Timestamp('2015-08-01'), pd.Timestamp('2016-07-01')], f, ax[1][0], [0,140])
plot_arc_p('longreach-treated-effluent','phosphate',[pd.Timestamp('2006-03-01'), pd.Timestamp('2006-06-15')], f, ax[1][1],[0,13])


ax[1][0].set_xlabel('Date')
ax[1][1].set_xlabel('Date')
f.tight_layout()
f.savefig(os.path.join(results_root, "example_evaluation_plots.svg"))