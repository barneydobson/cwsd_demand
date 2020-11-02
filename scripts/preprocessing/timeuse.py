# -*- coding: utf-8 -*-
"""
Created on Fri Oct  2 15:46:53 2020

@author: Barney
"""

import os
import pandas as pd
import scipy.stats as stats
import numpy as np

#Misc
MINUTES_IN_HOUR = 60 #minutes
DAYS_IN_WEEK = 7 #days
HOURS_IN_DAY = 24 #hours
DIARY_INTERVAL = 10 #minutes
DIARY_OFFSET = 4 #hours (i.e. diary starts at 4am)

#Addresses
data_root = os.path.join("D:\\", "Barney" , "data", "UKDA-8128-tab", "tab") # DOI : 10.5255/UKDA-SN-8128-1

output_root = os.path.join("C:\\", "Users", "Barney", "Documents", "GitHub", "cwsd_demand", "data", "processed")

timeuse_fid = os.path.join(data_root, "uktus15_diary_wide.tab")
indiv_fid = os.path.join(data_root, "uktus15_individual.tab")
week_fid = os.path.join(data_root, "uktus15_wksched.tab")

#Load data
indiv_df = pd.read_csv(indiv_fid, sep = '\t', low_memory = False)
timeuse_df = pd.read_csv(timeuse_fid, sep= '\t', low_memory = False)
week_df = pd.read_csv(week_fid, sep ='\t', low_memory = False)

#Extract London (should test with and without)
london_number = 7
indiv_df = indiv_df.loc[indiv_df.dgorpaf == london_number]
timeuse_df = timeuse_df.loc[timeuse_df.serial.isin(indiv_df.serial.unique())]

#Format timeuse_df
timeuse_df = timeuse_df.rename(columns = {'DiaryDate_Act' : 'date'})
timeuse_df.date = pd.to_datetime(timeuse_df.date, format = '%m/%d/%Y', errors = "coerce")
timeuse_df = timeuse_df.dropna()
timeuse_df = timeuse_df.set_index(['serial', 'pnum', 'date'])

def format_record(df, columns):
    time_num = [int(x[1]) for x in columns.str.split('_')]
    df_ = df[columns].copy()
    df_.columns = time_num
    return df_

location_df = format_record(timeuse_df, timeuse_df.columns[timeuse_df.columns.str.contains('wher')])
activity_df = format_record(timeuse_df, timeuse_df.columns[28:28+144])

#Generate action dataframe
df = pd.DataFrame(columns = activity_df.columns, index = activity_df.index)

int_map = {'sleep' : 0, 'work' : 1, 'home' : 2, 'away' : 3}

athome_ind = location_df == 11
df[activity_df.isin([110,111])] = int_map['sleep']
df[(activity_df >= 1000) & (activity_df < 2000) & ~athome_ind] = int_map['work']
df[df.isna() & athome_ind] = int_map['home']
df[df.isna() & ~athome_ind] = int_map['away']

#Format columns to timeoffsets
hour = (np.array([int((x-1) * DIARY_INTERVAL / MINUTES_IN_HOUR) for x in df.columns]) + DIARY_OFFSET) % HOURS_IN_DAY
mins = ((df.columns - 1) * DIARY_INTERVAL) % MINUTES_IN_HOUR
df.columns = [pd.DateOffset(minutes = x, hours = y) for x, y in zip(mins.tolist(),hour.tolist())]

#Extract state changes
time_results_df = []
for idx, row in df.iterrows():
    changes = row[row.diff() != 0]
    changes = changes.map({x:y for y ,x in int_map.items()})
    
    changes = changes.reset_index()
    changes.columns = ['time','measurement']
    changes['serial'] = idx[0]
    changes['pnum'] = idx[1]
    changes['datetime'] = idx[2] + changes.time
    changes['period'] = 'week'
    changes.loc[changes.datetime.dt.weekday >= 5, 'period'] = 'weekend'
    changes = changes.drop('time', axis=1)
    changes['workstatus'] = 'nonwork'
    if (changes.measurement == 'work').any():
        changes['workstatus'] = 'work'
    time_results_df.append(changes)

#Print
time_results_df = pd.concat(time_results_df)
time_results_df.to_csv(os.path.join(output_root,'sample_activity.csv'), index=False)

#Add employment - defined either by an indiviudal in employment, or by any working days
time_results_df['date'] = time_results_df.datetime.dt.date
time_results_df['dow'] = time_results_df.datetime.dt.weekday
workers = time_results_df.groupby(['serial','pnum']).apply(lambda x : (x.workstatus == 'work').any())
workers = workers.map({False : 'nonworker', True : 'worker'}).rename('employment').reset_index()
time_results_df = pd.merge(time_results_df, workers, on=['serial','pnum']) 

time_results_df = pd.merge(time_results_df, indiv_df[['serial','pnum','deconact']], on=['serial','pnum'])
time_results_df.deconact = pd.to_numeric(time_results_df.deconact,errors='coerce').fillna(-1)
time_results_df.loc[(time_results_df.deconact <= 5) & (time_results_df.deconact > 0), 'employment'] = 'worker'

time_results_df = time_results_df.loc[time_results_df.employment == 'worker']

# Create employment summary (Assume workers behave the same on nonworking days as nonworkers)
days_summary = time_results_df.groupby('dow').workstatus.value_counts().rename('count').reset_index()
days_summary = days_summary.pivot(columns = 'workstatus', index = 'dow', values = 'count')
days_summary = days_summary.work.div(days_summary.sum(axis=1)).rename('percentage_working')
days_summary.to_csv(os.path.join(output_root, 'worker_activity.csv'))
