# -*- coding: utf-8 -*-
"""
Created on Fri Oct 16 11:02:18 2020

@author: Barney
"""

import os
import pandas as pd
from matplotlib import pyplot as plt
import geopandas as gpd
import misc

#misc
CRS = "EPSG:27700"
driver = 'GeoJSON'
extension = '.geojson'

#Address

repo_root = os.path.join("C:\\","Users","Barney","Documents","GitHub","cwsd_demand","data")
data_root = os.path.join(repo_root,"results")
proc_root = os.path.join(repo_root, "processed")
demand_root =  os.path.join(proc_root,"wc_morning")

map_fid = os.path.join(repo_root, "raw", "wastewater_zones_traced.geojson")
act_fid = os.path.join(proc_root, "worker_activity.csv")
pop_fid = os.path.join(proc_root, "zone_population.csv")

map_out_fid = os.path.join(data_root, "change_map" + extension)

#Read map data
workforce_factor = 0.1
gdf = gpd.read_file(map_fid).drop(['id','area'],axis=1)
act_df = pd.read_csv(act_fid)
pop_df = pd.read_csv(pop_fid)[['zone_name','household_pop','workday_pop','workers_to','workers_from']]

#Read results

scenarios = ['', 'workfix', 'lockdown']
#Plot demand profiles
d_df = []
for scenario in ['','_covid_workfix', '_covid_lockdown']:
    temp_df = pd.read_csv(os.path.join(demand_root, "household_demand" + scenario + ".csv"))
    temp_df['scenario'] = scenario.replace("_covid_","")
    d_df.append(temp_df)
d_df = pd.concat(d_df)
d_gb = d_df.groupby(['zone','period'])

f, axs = plt.subplots(2,2)
for idx, ax in zip(zip(['beckton','beckton','hogsmill','hogsmill'],['week','weekend','week','weekend']),axs.reshape(-1)):
    
    group = d_gb.get_group(idx)
    d_gb_plot = group.groupby('scenario')
    for idx_plot, group_plot in d_gb_plot:
        ax.plot(group_plot.time,group_plot.tot)
    
    ax.set_title(idx)
    ax.set_xlabel('time')
    ax.set_ylabel('consumption (l/hr)')
    ax.xaxis.set_major_locator(plt.MaxNLocator(5))
    plt.legend(d_gb_plot.groups.keys())
#Read

def read(title):
    df = pd.read_csv(os.path.join(data_root, title))
    df.date = pd.to_datetime(df.date)
    return df.set_index('date')


def read_results(option = ""):
    flow_df = []
    pol_df = []
    spill_df = []
    for fn in ['','_covid_workfix', '_covid_lockdown']:
        td = read("flows" + fn + option + ".csv")
        td['scenario'] = fn.replace("_covid_","")
        flow_df.append(td)
        td = read("pollutants" + fn + option + ".csv")
        td['scenario'] = fn.replace("_covid_","")
        pol_df.append(td)
        td = read("spills" + fn + option + ".csv")
        td['scenario'] = fn.replace("_covid_","")
        spill_df.append(td)
    flow_df = pd.concat(flow_df)
    pol_df = pd.concat(pol_df)
    spill_df = pd.concat(spill_df)
    return flow_df, pol_df
flow_df, pol_df = read_results()
flow_df_drought, pol_df_drought = read_results("_drought")

gb = pol_df.groupby(['arc','pollutant','scenario'])

#read map data
workforce_factor = 0.1
gdf = gpd.read_file(map_fid).drop(['id','area'],axis=1)
act_df = pd.read_csv(act_fid)
pop_df = pd.read_csv(pop_fid)[['zone_name','household_pop','workday_pop','workers_to','workers_from']]


#Format map
weekday_factor = act_df.loc[act_df.dow < 5,'percentage_working'].mean()
weekend_factor = act_df.loc[act_df.dow >= 5,'percentage_working'].mean()


pop_df['day_pop_week'] = pop_df.household_pop + (pop_df.workers_to - pop_df.workers_from) * weekday_factor
pop_df['day_pop_week_c'] = pop_df.household_pop + (pop_df.workers_to - pop_df.workers_from) * weekday_factor * workforce_factor
pop_df['day_pop_weekend'] = pop_df.household_pop + (pop_df.workers_to - pop_df.workers_from) * weekend_factor
pop_df['day_pop_weekend_c'] = pop_df.household_pop + (pop_df.workers_to - pop_df.workers_from) * weekend_factor * workforce_factor

pop_df['population_week'] = pop_df['day_pop_week_c']/pop_df['day_pop_week']
pop_df['population_weekend'] = pop_df['day_pop_weekend_c']/pop_df['day_pop_weekend']
pop_df = pop_df[['zone_name','population_week','population_weekend']]



def plot_arc(flow, pol, arc):
    
    f, axs = plt.subplots(1 + len(pol.pollutant.unique()),1)
    for scenario in scenarios:
        axs[0].plot(flow.loc[(flow.arc == arc) & (flow.scenario == scenario),'val'])
    
    gb = pol.groupby(['arc','pollutant','scenario'])

    axs[0].set_ylabel('Flow (Ml/d)')
    plt.title(arc)
    for scenario in scenarios:
        for polut, ax in zip(pol.pollutant.unique(), axs[1:]):
            ax.plot(gb.get_group((arc, polut, scenario)).val)
    
            ax.set_ylabel(polut + ' (mg/l)')
    
    ax.set_xlabel('Time (days)')
    
    return f

# plot_arc('beckton-wwtw-input')

# plot_arc('thames-outflow',)

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


flow_df_drought['pollutant'] = 'flow'
df_drought = pd.concat([pol_df_drought,flow_df_drought])

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

# make_boxplot(df,river_flows, 'wwtw_treated', drop0=True)

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

#Drought
riv_ss_drought = print_table(df_drought, river_flows)
misc.colorgrid_plot(riv_ss)
misc.colorgrid_plot(riv_ss_drought)
riv_ss = riv_ss.rename(columns = {'lockdown':'LD-W','workfix' :'WH-W'}, level=1)
riv_ss_drought = riv_ss_drought.rename(columns = {'lockdown':'LD-D','workfix' :'WH-D'}, level=1)
riv_ss_c = pd.concat([riv_ss,riv_ss_drought],axis=1)
riv_ss_c = riv_ss_c.reindex(columns=riv_ss_c.columns.sort_values())
misc.colorgrid_plot(riv_ss_c )

#make make
gdf = pd.merge(gdf, pop_df, on='zone_name')

def format_flows(df, period):
    if period == 'week':
        ss = print_table(df.loc[df.index.weekday < 5], household_effluents)
    elif period == 'weekend':
        ss = print_table(df.loc[df.index.weekday >= 5], household_effluents)
    # ss = ss.loc['flow'].reset_index()
    ss = ss.reset_index().melt(id_vars='pollutant')
    ss = ss.loc[ss.scenario == 'lockdown'].rename(columns={'arc':'zone_name'})
    ss['zone_name'] = ss.zone_name.str.replace('-household-waste','')
    ss = ss.drop('scenario',axis=1).pivot(index = 'zone_name', columns = 'pollutant',values='value')
    ss.columns = ss.columns + '-' + period
    return ss
gdf = pd.merge(gdf, format_flows(df, 'week'), on = 'zone_name')
gdf = pd.merge(gdf, format_flows(df, 'weekend'), on = 'zone_name')
gpd.GeoDataFrame(gdf, crs = CRS).to_file(map_out_fid , driver=driver)

sum('asd')


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



#Format map
weekday_factor = act_df.loc[act_df.dow < 5,'percentage_working'].mean()
weekend_factor = act_df.loc[act_df.dow >= 5,'percentage_working'].mean()


pop_df['day_pop_week'] = pop_df.household_pop + (pop_df.workers_to - pop_df.workers_from) * weekday_factor
pop_df['day_pop_week_c'] = pop_df.household_pop + (pop_df.workers_to - pop_df.workers_from) * weekday_factor * workforce_factor
pop_df['day_pop_weekend'] = pop_df.household_pop + (pop_df.workers_to - pop_df.workers_from) * weekend_factor
pop_df['day_pop_weekend_c'] = pop_df.household_pop + (pop_df.workers_to - pop_df.workers_from) * weekend_factor * workforce_factor

pop_df['population_week'] = pop_df['day_pop_week_c']/pop_df['day_pop_week']
pop_df['population_weekend'] = pop_df['day_pop_weekend_c']/pop_df['day_pop_weekend']
pop_df = pop_df[['zone_name','population_week','population_weekend']]

gdf = pd.merge(gdf, pop_df, on='zone_name')

def format_flows(df, period):
    if period == 'week':
        ss = print_table(df.loc[df.index.weekday < 5], household_effluents)
    elif period == 'weekend':
        ss = print_table(df.loc[df.index.weekday >= 5], household_effluents)
    ss = ss.loc['flow'].reset_index()
    ss = ss.loc[ss.scenario.str.contains('lockdown')]
    ss['arc'] = ss.arc.str.replace('-household-waste','')
    return ss[['arc','flow']].rename(columns={'arc':'zone_name','flow' : '_'.join(['flow',period])})
gdf = pd.merge(gdf, format_flows(df, 'week'), on = 'zone_name')
gdf = pd.merge(gdf, format_flows(df, 'weekend'), on = 'zone_name')
# gpd.GeoDataFrame(gdf, crs = CRS).to_file(map_out_fid , driver=driver)
f, axs = plt.subplots(4,1)
for (ax, name) in zip(axs, gdf.columns[1:].drop('geometry')):
    ax.bar(list(range(8)),gdf.set_index('zone_name')[name] - 1, color='k',width=0.5)
    ax.plot([0,8],[0,0],color='r',linestyle='--')
    ax.set_ylabel(name)
    if ax == axs[-1]:
        ax.set_xticks(list(range(8)))
        ax.set_xticklabels(gdf.zone_name, rotation = 45)
    else:
        ax.set_xticks([])
    