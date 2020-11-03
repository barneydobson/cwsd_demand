# -*- coding: utf-8 -*-
"""
Created on Fri Oct 16 11:02:18 2020

@author: Barney
"""

import os
import pandas as pd
from matplotlib import pyplot as plt
import misc
#Address
data_root = os.path.join("C:\\","Users","Barney","Documents","GitHub","cwsd_demand","data","results")

#Read
def read(title):
    df = pd.read_csv(os.path.join(data_root, title))
    df.date = pd.to_datetime(df.date)
    return df.set_index('date')
flow_df = []
pol_df = []
spill_df = []
for fn in ['','_covid_workapp', '_covid_workfix', '_covid_lockdown']:
    td = read("flows" + fn + ".csv")
    td['scenario'] = fn
    flow_df.append(td)
    td = read("pollutants" + fn + ".csv")
    td['scenario'] = fn
    pol_df.append(td)
    td = read("spills" + fn + ".csv")
    td['scenario'] = fn
    spill_df.append(td)
flow_df = pd.concat(flow_df)
pol_df = pd.concat(pol_df)
spill_df = pd.concat(spill_df)

gb = pol_df.groupby(['arc','pollutant','scenario'])
def plot_arc(arc, scenarios):
    
    f, axs = plt.subplots(1 + len(pol_df.pollutant.unique()),1)
    for scenario in scenarios:
        axs[0].plot(flow_df.loc[(flow_df.arc == arc) & (flow_df.scenario == scenario),'val'])
        
    axs[0].set_ylabel('Flow (Ml/d)')
    plt.title(arc)
    for scenario in scenarios:
        for pol, ax in zip(pol_df.pollutant.unique(), axs[1:]):
            ax.plot(gb.get_group((arc, pol, scenario)).val)
    
            ax.set_ylabel(pol + ' (mg/l)')
    
    ax.set_xlabel('Time (days)')
    
    return f

# plot_arc('beckton-wwtw-input')
plot_arc('thames-outflow',['', '_covid_workapp', '_covid_workfix', '_covid_lockdown'])
# plot_arc('beddington-treated-effluent')
# plot_arc('mogden-untreated-effluent')
# plot_arc('lee-to-thames')

household_effluents = ['beckton-household-waste',
                        'beddington-household-waste',
                        'crossness-household-waste',
                        'deephams-household-waste',
                        'hogsmill-household-waste',
                        'longreach-household-waste',
                        'mogden-household-waste',
                        'riverside-household-waste']

wwtw_treated = ['beckton-treated-effluent',
                'beddington-treated-effluent',
                'crossness-treated-effluent',
                'deephams-treated-effluent',
                'hogsmill-treated-effluent',
                'longreach-treated-effluent',
                'mogden-treated-effluent',
                'riverside-treated-effluent']

untreated_effluent = ['beckton-untreated-effluent',
                        'beddington-untreated-effluent',
                        'crossness-untreated-effluent',
                        'deephams-untreated-effluent',
                        'hogsmill-untreated-effluent',
                        'longreach-untreated-effluent',
                        'mogden-untreated-effluent',
                        'riverside-untreated-effluent']

river_flows = ['hogsmill-outflow',
                'wandle-to-thames',
                'thames-flow-5',
                'lee-to-thames',
                'thames-flow-8',
                'thames-outflow']

deephams_flows = ['deephams-household-waste',
                  'deephams-storm-effluent',
                  'deephams-wwtw-house-input',
                  'deephams-treated-effluent',
                  'lee-to-thames',
                  ]

flow_df['pollutant'] = 'flow'
df = pd.concat([pol_df,flow_df])

def make_boxplot(df, labels,stitle, drop0 = None):
    f, axs = plt.subplots(4,2, figsize = (10,8))
    pols = df.pollutant.unique()
    for pollutant, ax in zip(pols, axs.reshape(-1)[0:len(pols)]):
        ss = df.loc[df.arc.isin(labels) & (df.pollutant == pollutant),['val','scenario','arc']]
        if drop0:
            ss = ss.loc[ss.val > 0]
        cax = ss.boxplot(by='scenario', column='val', ax = ax, showmeans=True,showfliers=False)
        ax.set_title('')
        ax.set_ylabel(pollutant)
        ax.set_xlabel('')
        xl = ax.get_xticklabels()
        ax.set_xticklabels([])
    axs[3,0].set_xticklabels(xl)
    axs[3,1].set_xticklabels(xl)
    f.suptitle(stitle)
    return f

make_boxplot(df,river_flows, 'wwtw_treated', drop0=True)

def print_table(df, labels,fid = None):
    ss = df.loc[df.arc.isin(labels)].groupby(['arc','pollutant','scenario']).mean()
    ss = ss.reset_index()
    base = ss.loc[ss.scenario.isin(['']),['arc','pollutant','val']]
    ss = pd.merge(ss,base,on=['arc','pollutant'])
    ss['val'] = ss.val_x.div(ss.val_y)
    ss = ss.loc[~ss.scenario.isin([''])]
    ss = ss.pivot_table(columns=['arc','scenario'],index='pollutant',values='val')
    
    if fid is not None: 
        ss.to_csv(fid)
    return ss
house_ss = print_table(df, household_effluents,os.path.join(data_root, "scenario_change_house.csv"))
ww_ss = print_table(df, wwtw_treated,os.path.join(data_root, "scenario_change_wwtw.csv"))
un_ss = print_table(df, untreated_effluent,os.path.join(data_root, "scenario_change_untreated.csv"))
riv_ss = print_table(df, river_flows,os.path.join(data_root, "scenario_change_river.csv"))
deep_ss = print_table(df, deephams_flows)
misc.colorgrid_plot(house_ss)
misc.colorgrid_plot(ww_ss)
misc.colorgrid_plot(un_ss)
misc.colorgrid_plot(riv_ss)
misc.colorgrid_plot(deep_ss)

def make_boxplot(df, labels,stitle, drop0 = None):
    cm = plt.get_cmap('Pastel1')
    # cm = plt.cm.Pastel1
    # cols = [cm(1.*i/len(labels)) for i in range(len(labels))]
    cols = [cm(i) for i in range(len(labels))]
    cols = {x : y for x,y in zip(labels, cols)}
    f, axs = plt.subplots(4,2, figsize = (12,10))
    pols = df.pollutant.unique()
    cc = []
    for pollutant, ax in zip(pols, axs.reshape(-1)[0:len(pols)]):
        ss = df.loc[df.arc.isin(labels) & (df.pollutant == pollutant),['val','scenario','arc']]
        if drop0:
            ss = ss.loc[ss.val > 0]
        ss['lab'] = list(zip(ss.arc, ss.scenario))
        ss = ss.reset_index().sort_values(by=['arc','scenario','date']).set_index('date')
        for pos, var in enumerate(ss.lab.unique()): 
            cax = ax.boxplot(ss.loc[ss.lab == var,'val'], positions = [pos], showfliers=False,widths=0.5,patch_artist=True)
            cax['boxes'][0].set_facecolor(cols[var[0]])
            cc.append(cax['boxes'][0])
        # cax = sns.boxplot(x = 'lab', y = 'val', data = ss, ax = ax)
        ax.set_title('')
        ax.set_ylabel(pollutant)
        ax.set_xlabel('')
        xl = ax.get_xticklabels()
        ax.set_xticklabels([])
        ax.set_xticks([])
    # axs[3,0].set_xticklabels(xl,rotation=90)
    # axs[3,1].set_xticklabels(xl,rotation=90)
    plt.legend(cc[0:7], ss.arc.unique(),bbox_to_anchor=(-0.15, -0.5), loc='center')
    f.suptitle(stitle)
    return f
# f = make_boxplot(df,household_effluents, 'household_effluents', drop0=True)
# f.savefig('household_effluents.png')
# plt.close(f)