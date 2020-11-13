#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 22 13:01:56 2020

@author: Barney
"""
import os
import pandas as pd
from model import London
import constants
print('started')
for covid in ["", "_covid_popdec", "_covid_workfix","_covid_lockdown"]:
# for covid in [""]:
    
    isdrought = True
    """Addresses
    """
    data_root = os.path.join("C:\\","Users","bdobson","Documents","GitHub","cwsd_demand","data")
    
    raw_root = os.path.join(data_root, "raw")
    processed_root = os.path.join(data_root, "processed")
    parameter_root = os.path.join(data_root, "raw")
    output_root = os.path.join(data_root, "results")
    
    addresses = {}
    addresses['flow_fid'] = os.path.join(processed_root, "scaled_hourly_flows.csv")
    addresses['rain_fid'] = os.path.join(raw_root, "rainfall_data.csv")
    
    addresses['wqf_fid'] = os.path.join(processed_root, "wq_forcing.csv")
    addresses['wqv_fid'] = os.path.join(processed_root, "wq_val.csv")
    
    addresses['nodes_fid'] = os.path.join(parameter_root, "nodelist.csv")
    addresses['arcs_fid'] = os.path.join(parameter_root, "arclist.csv")

    addresses['demand_fid'] = os.path.join(processed_root,"full_sample_disagg", "household_demand" + covid + ".csv")
    addresses['wqh_fid'] = os.path.join(processed_root,"full_sample_disagg", "household_demand_wq" + covid + ".csv")
    addresses['appliance_fid'] = os.path.join(raw_root, "appliance_loads.csv")
    
    """Load data
    """
    #Node data
    nodes_df = pd.read_csv(addresses['nodes_fid'], sep=',')
    nodes_df = nodes_df.apply(pd.to_numeric, errors='coerce').fillna(nodes_df) # Ensure numbers are numeric datatypes
    
    #Arc data
    arcs_df = pd.read_csv(addresses['arcs_fid'], sep=',')
    arcs_df = arcs_df.fillna(constants.UNBOUNDED_CAPACITY).drop('source',axis=1)
    
    #Pivoted data (i.e. used by read_input functions)
    flow_df = pd.read_csv(addresses['flow_fid'], sep = ',', index_col = 'date')
    flow_df = flow_df.mul(constants.M3_S_TO_ML_H)
    
    
        
    
    rain_df = pd.read_csv(addresses['rain_fid'], sep = ',', index_col = 'time')
    rain_df.index = pd.to_datetime(rain_df.index, format = '%d-%b-%Y %H:%M:%S').astype(str)
    rain_df.columns = rain_df.columns + '-rainfall'
    rain_df = rain_df.replace(-1,0)#set missing data to 0
    
    if isdrought:
        flow_df.index = pd.to_datetime(flow_df.index)
        flow_df = flow_df.loc[pd.date_range(start='1995-05-01 00:00:00',periods = 15312, freq='H')]
        flow_df.index = rain_df.index[0:15312]
    
    inputs = pd.concat([flow_df,rain_df],axis=1).dropna(how='any')
    
    #Melted data (i.e. wq only)
    wq_df = pd.read_csv(addresses['wqf_fid'], sep = ',')
    wq_df = wq_df.loc[wq_df.date.isin(inputs.index)]
    
    #Demand data
    demand_df = pd.read_csv(addresses['demand_fid'], header=[0,1], index_col=[0,1])
    demand_df = demand_df.iloc[:,[0,-1]].dropna().reset_index()
    demand_df.columns = ['time','zone','period','tot']
    
    appliance_df = pd.read_csv(addresses['appliance_fid'])
    wqh_df = pd.read_csv(addresses['wqh_fid'])
    
    #Manually remove nitrogen - don't have enough info on this
    wqh_df = wqh_df.loc[wqh_df.variable != 'nitrogen']
    
    wqh_df = wqh_df.replace({'variable' : 'codt'}, {'variable' : 'cod'}) # wrong assumption for now
    
    del addresses, raw_root, parameter_root, processed_root, data_root
    
    constants.POLLUTANTS = list(wqh_df.variable.unique())
    
    """Format data
    """
    nodes_dict = nodes_df.groupby('name').apply(lambda x: dict(zip(x.key, x.value))).to_dict()
    arcs_dict = arcs_df.set_index('name').T.to_dict()
    
    inputs_dict = {col : inputs[col].dropna().to_dict() for col in inputs.columns}
    
    #Apparently nested dicts are bad practice.. but the stackoverflow responses about it seemed complicated
    wq_dict = {}
    for node, group in wq_df.groupby('node'):   
        wq_dict[node] = group.groupby('variable').apply(lambda x: dict(zip(x['date'], x['result'])))
    
    
    demand_df['hour'] = pd.to_datetime(demand_df.time).dt.hour
    wqh_df['hour'] = pd.to_datetime(wqh_df.time).dt.hour
    
    demand_dict = {}
    for node, group in demand_df.drop('time',axis=1).groupby('zone'):   
        demand_dict[node] = group.drop('zone',axis=1).set_index(['period','hour']).tot.to_dict()
        
    wqh_dict = {}
    for node, group in wqh_df.groupby('zone'):
        wqh_dict[node] = group.drop('zone',axis=1).set_index(['period','hour','variable']).conc.to_dict()
    
    
    """Build model
    """
    london_model = London()
    
    #Add nodes
    london_model.add_nodes(nodes_dict)
    
    #Add arcs
    london_model.add_arcs(arcs_dict)
    
    #Assign inputs
    london_model.add_inputs(inputs_dict)
    
    #Assign wq
    london_model.add_wq(wq_dict)
    
    #Assign demands
    london_model.add_demands(demand_dict)
    
    #Assign water quality household
    london_model.add_wqh(wqh_dict)
    
    #Perform misc specific processing
    london_model.process()
    
    """Run model
    """
    # london_model.dates = london_model.dates[36144:41987] #Sample
    # london_model.dates = london_model.dates[25552:29203] #Drought
    
    
    
    # london_model.dates = london_model.dates[41473:] #Recent rainfall and flow overlap
    # if isdrought:
    #     london_model.dates = drought_dates

    results = london_model.run()

    # flows = pd.DataFrame(results['flows'])
    # gb = flows.groupby('arc')
    
    """Print results
    """
    if isdrought:
        covid += '_drought'
        
    for key, item in results.items():
        df = pd.DataFrame(item)
        df.date = pd.to_datetime(df.date)
        df = df.set_index('date')
        df.to_csv(os.path.join(output_root, key + covid + ".csv"), sep=',')