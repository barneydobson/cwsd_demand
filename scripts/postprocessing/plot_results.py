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
import matplotlib.gridspec as gridspec

#misc
CRS = "EPSG:27700"
driver = 'GeoJSON'
extension = '.geojson'

#Address
repo_root = os.path.join("C:\\","Users","bdobson","Documents","GitHub","cwsd_demand","data")
data_root = os.path.join(repo_root,"results")
proc_root = os.path.join(repo_root, "processed")
demand_root =  os.path.join(proc_root, "full_sample_disagg")

map_fid = os.path.join(repo_root, "raw", "wastewater_zones_traced.geojson")
act_fid = os.path.join(proc_root, "worker_activity_london.csv")
pop_fid = os.path.join(proc_root, "zone_population_london.csv")

map_out_fid = os.path.join(data_root, "change_map" + extension)


isdrought = False
#Read map data
workforce_factor = 0.1
gdf = gpd.read_file(map_fid).drop(['id','area'],axis=1)
act_df = pd.read_csv(act_fid)
pop_df = pd.read_csv(pop_fid)[['zone_name','household_pop','workday_pop','workers_to','workers_from']]

#Read results

scenarios = ['', 'workfix', 'lockdown','popdec']
#Plot demand profiles
d_df = []
for scenario in ['','_covid_workfix', '_covid_lockdown','_covid_popdec']:
    temp_df = pd.read_csv(os.path.join(demand_root, "household_demand" + scenario + ".csv"), header=[0,1], index_col=[0,1])
    temp_df = temp_df.iloc[:,[0,-1]].dropna().reset_index()
    temp_df.columns = ['time','zone','period','tot']
    temp_df['scenario'] = scenario.replace("_covid_","")
    d_df.append(temp_df)
d_df = pd.concat(d_df)
d_gb = d_df.groupby(['zone','period'])

f, axs = plt.subplots(2,2, figsize=(8,8))
for idx, ax in zip(zip(['beckton','beckton','hogsmill','hogsmill'],['week','weekend','week','weekend']),axs.reshape(-1)):
    
    group = d_gb.get_group(idx)
    d_gb_plot = group.groupby('scenario')
    for idx_plot, group_plot in d_gb_plot:
        if idx_plot == '':
            lw = 4
        else:
            lw = 2
        ax.plot(group_plot.time,group_plot.tot,label = {'' : 'Baseline',
                                                        'lockdown': 'LD',
                                                        'popdec': 'PD',
                                                        'workfix' : 'WH'}[idx_plot],linewidth=lw)
    
    idx = list(idx)
    idx[0] = idx[0][0].upper() + idx[0][1:]
    
    ax.set_title(idx[0] + '-' + idx[1])
    ax.set_xlabel('Time')
    ax.set_ylabel('Consumption (l/hr)')
    
    ax.xaxis.set_major_locator(plt.MaxNLocator(5))
    # ax.set_xticklabels(pd.date_range('2000-01-01',periods = 5, freq = '6H').time)
    if idx[0] == 'Beckton':
        ax.set_ylim([0,3e+7])
    else:
        ax.set_ylim([0,3.7e+6])
    ax.legend()
f.tight_layout()
f.savefig(os.path.join(data_root, 'fig6.svg'))

#Read

def read(title):
    df = pd.read_csv(os.path.join(data_root, title))
    df.date = pd.to_datetime(df.date)
    return df.set_index('date')


def read_results(option = ""):
    flow_df = []
    pol_df = []
    spill_df = []
    for fn in ['','_covid_workfix', '_covid_lockdown','_covid_popdec']:
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
if isdrought:
    flow_df_drought, pol_df_drought = read_results("_drought")

gb = pol_df.groupby(['arc','pollutant','scenario'])

#read map data
workforce_factor = 0.1
gdf = gpd.read_file(map_fid).drop(['id','area'],axis=1)
act_df = pd.read_csv(act_fid)
pop_df = pd.read_csv(pop_fid)[['zone_name','household_pop','workday_pop','workers_to','workers_from']]


def pf(arc):
    ss = flow_df.loc[flow_df.arc == arc].reset_index().pivot(columns='scenario',index = 'date',values='val')
    ss.plot()

def plot_arc(arc, pol = None, flow = None):
    if pol is None:
        pol = pol_df
    if flow is None:
        flow = flow_df
    f, axs = plt.subplots(1 + len(pol.pollutant.unique()),1)
    for scenario in scenarios:
        axs[0].plot(flow.loc[(flow.arc == arc) & (flow.scenario == scenario),'val'])
    
    # gb = pol.groupby(['arc','pollutant','scenario'])

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

storm_flows = ['mogden-storm-effluent',
                  'deephams-storm-effluent',
                  'riverside-storm-effluent',
                  'hogsmill-storm-effluent',
                  'beddington-storm-effluent',
                  'longreach-storm-effluent',
                  ]

flow_df['pollutant'] = 'flow'
df = pd.concat([pol_df,flow_df])

if isdrought:
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
    ss = df.loc[df.arc.isin(labels)]
    ss = ss.loc[ss.val > 0]
    ss = ss.groupby(['arc','pollutant','scenario']).mean()
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
storm_ss = print_table(df,storm_flows)
misc.colorgrid_plot((house_ss-1).mul(100)).savefig(os.path.join(data_root,'household_change.svg'),bbox_inches='tight')
misc.colorgrid_plot((ww_ss-1).mul(100)).savefig(os.path.join(data_root,'treated_change.svg'),bbox_inches='tight')
misc.colorgrid_plot((un_ss-1).mul(100)).savefig(os.path.join(data_root,'untreated_change.svg'),bbox_inches='tight')
misc.colorgrid_plot((storm_ss-1).mul(100)).savefig(os.path.join(data_root,'storm_change.svg'),bbox_inches='tight')

misc.colorgrid_plot((riv_ss-1).mul(100)).savefig(os.path.join(data_root,'river_change.svg'),bbox_inches='tight')
misc.colorgrid_plot(deep_ss.mul(100))

pol_names = {'solids' : 'TSS',
            'phosphorus' : 'P',
            'phosphate' : 'PO4',
            'nitrite' : 'NO2',
            'nitrate' : 'NO3',
            'cod' : 'COD',
            'ammonia': 'NH3',
            'flow' : 'Q'
            }

def bars(df, type_):
    ss = (df - 1).T
    l = 0
    for zone in ss.index.get_level_values(0).unique():
        tss = ss.loc[zone].mul(100)
        tss.index = [{'lockdown' : 'LD', 'popdec' : 'PD', 'workfix' : 'WH'}[x] for x in tss.index]
        tss = tss.rename(columns=pol_names).T
        f, axs = plt.subplots(2,1,gridspec_kw={'height_ratios': [1, 4]},figsize=(2,4))
        f.patch.set_alpha(0.7)
        shade = [0.8] * 3
        tss.loc[['Q']].plot.barh(ax=axs[0],legend=False)
        axs[0].axvline(x= 0,ymin=-100,ymax=100,color=shade,linewidth=1,linestyle='--')
        if l == 0:
            tss.drop('Q',axis=0).plot.barh(ax=axs[1],legend=True)
        else:
            tss.drop('Q',axis=0).plot.barh(ax=axs[1],legend=False)
        axs[1].axvline(x= 0,ymin=0,ymax=10,color=shade,linewidth=1,linestyle='--')
        l+=1
        if type_ == 'house':
            xl1 = [-16,5]
            xl2 = [-10,12]
            lab = zone.replace('-household-waste','')
            lab = lab[0].upper() + lab[1:]
        elif type_ == 'river':
            xl1 = [-3,3]
            xl2 = [-10,5]
            lab = zone.replace('-' , ' ').title()
            if lab == 'Thames Flow 5':
                lab = 'WQ Site 4'
            elif lab == 'Thames Flow 8':
                lab = 'WQ Site 7/8/9'
            elif lab == 'Thames Outflow':
                lab = 'WQ Site 10/11/12'
            elif lab == 'Lee To Thames':
                lab = 'LEE'
            elif lab == 'Wandle To Thames':
                lab = 'WAN'
        elif type_ == 'un':
            
            xl1 = [-2,2]
            xl2 = [-20,10]
            lab = zone.replace('-untreated-effluent','')
            lab = lab[0].upper() + lab[1:]
        if type_ == 'ww':
            xl1 = [-10,5]
            xl2 = [-10,12]
            lab = zone.replace('-treated-effluent','')
            lab = lab[0].upper() + lab[1:]
        
        axs[0].set_ylabel('')
        axs[0].set_xlim(xl1)
        axs[1].set_ylabel('')
        axs[1].set_xlim(xl2)
        axs[1].set_xlabel(lab)
        f.tight_layout()
        f.savefig(os.path.join(data_root, zone + '.svg'))
        plt.close(f)
bars(riv_ss, 'river')
bars(house_ss, 'house')
bars(ww_ss, 'ww')
bars(un_ss, 'un')
#Drought
if isdrought:
    riv_ss_drought = print_table(df_drought, river_flows)
    misc.colorgrid_plot(riv_ss)
    misc.colorgrid_plot(riv_ss_drought)
    riv_ss = riv_ss.rename(columns = {'lockdown':'LD-W','workfix' :'WH-W' ,'popdec': 'PD-W'}, level=1)
    riv_ss_drought = riv_ss_drought.rename(columns = {'lockdown':'LD-D','workfix' :'WH-D','popdec' : 'PD-W'}, level=1)
    riv_ss_c = pd.concat([riv_ss,riv_ss_drought],axis=1)
    riv_ss_c = riv_ss_c[riv_ss_c.columns.sort_values()]
    # misc.colorgrid_plot(riv_ss_c) #Nothign interesting and horrid plot

#make make
gdf = pd.merge(gdf, pop_df, on='zone_name')

def format_flows(df, period):
    if period == 'week':
        ss = print_table(df.loc[df.index.weekday < 5], household_effluents)
    elif period == 'weekend':
        ss = print_table(df.loc[df.index.weekday >= 5], household_effluents)
    # ss = ss.loc['flow'].reset_index()
    ss = ss.reset_index().melt(id_vars='pollutant')
    ss = ss.loc[ss.scenario == 'workfix'].rename(columns={'arc':'zone_name'})
    ss['zone_name'] = ss.zone_name.str.replace('-household-waste','')
    ss = ss.drop('scenario',axis=1).pivot(index = 'zone_name', columns = 'pollutant',values='value')
    ss.columns = ss.columns + '-' + period
    return ss
gdf = pd.merge(gdf, format_flows(df, 'week'), on = 'zone_name')
gdf = pd.merge(gdf, format_flows(df, 'weekend'), on = 'zone_name')
gpd.GeoDataFrame(gdf, crs = CRS).to_file(map_out_fid , driver=driver)



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
    ss = ss.loc[ss.scenario.str.contains('workfix')]
    ss['arc'] = ss.arc.str.replace('-household-waste','')
    return ss[['arc','flow']].rename(columns={'arc':'zone_name','flow' : '_'.join(['flow',period])})
gdf = pd.merge(gdf, format_flows(df, 'week'), on = 'zone_name')
gdf = pd.merge(gdf, format_flows(df, 'weekend'), on = 'zone_name')
# gpd.GeoDataFrame(gdf, crs = CRS).to_file(map_out_fid , driver=driver)

gdf = gdf[['zone_name','population_week','population_weekend','flow_week','flow_weekend']]
gdf.zone_name = [x[0].upper() + x[1:] for x in gdf.zone_name]

f, axs = plt.subplots(2,1,figsize=(7,5))
for (ax, name) in zip(axs, [gdf.columns[1:3], gdf.columns[3:5]]):
    # ax.bar(list(range(8)),gdf.set_index('zone_name')[name] - 1, color='k',width=0.5)
    ylab = {'flow' : 'Household wastewater\n change (%)',
            'population' : 'Population change (%)'}[name[0].split('_')[0]]
    xlab = {x : x.split('_')[1] for x in name}
    
    (gdf.set_index('zone_name')[name].rename(columns=xlab) - 1).mul(100).plot.bar(ax=ax)
    ax.plot([0,8],[0,0],color='r',linestyle='--')
    ax.set_ylabel(ylab)
    ax.set_xlabel('')
    if ax == axs[-1]:
        ax.set_xticks(list(range(8)))
        ax.set_xticklabels(gdf.zone_name, rotation = 45)
    else:
        ax.set_xticks([])
    ax.legend()
f.tight_layout()
f.savefig(os.path.join(data_root, 'fig5.svg'))