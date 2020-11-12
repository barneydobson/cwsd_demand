# -*- coding: utf-8 -*-
"""
Created on Tue Sep 15 10:18:02 2020

@author: Barney
"""
import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from matplotlib import pyplot as plt
from datetime import datetime, timedelta

#Constants
SEC_PER_DAY = 60 * 60 * 24
LITRE_TO_M3 = 1/1000
TARGET_RESOLUTION = '60min'
DIARY_OFFSET = 4 #hours (i.e. diary starts at 4am)
HOURS_PER_DAY = 24
#Options
sample_demand = 1000 #Assume 10000 samples will be representative
kitchen_tap_adjustment = 12.6 #scale kitchen tap activities from this
daily_external = 10 # l/h/d (check pp and ph)
external_hours = pd.date_range(start = '09:00', end = '19:00', freq = 'H').time
daily_external = (daily_external / len(external_hours) )
start_day = '2020-03-12T04:00'
end_day = '2020-03-26T04:00'

area_name = 'DMA'

#Addresses
data_root = os.path.join("C:\\", "Users", "bdobson", "Documents", "GitHub", "cwsd_demand", "data")

loads_fid = os.path.join(data_root, "raw", "appliance_loads.csv")
fdi_fid = os.path.join(data_root, "raw", "fdi_appliance_activity.csv")

output_folder = os.path.join(data_root, "processed","hist")

activity_fid = os.path.join(data_root, "processed", "worker_activity.csv")
timing_fid = os.path.join(data_root, "processed","sample_activity.csv")
population_fid = os.path.join(data_root, "processed","zone_population.csv")    

#Load data
loads_df = pd.read_csv(loads_fid, sep=',')
fdi_df = pd.read_csv(fdi_fid, sep=',')

activity_df = pd.read_csv(activity_fid, sep=',').set_index('dow')
population_df = pd.read_csv(population_fid, sep=',').set_index(area_name )

timing_df = pd.read_csv(timing_fid, sep=',')
timing_df.datetime = pd.to_datetime(timing_df.datetime)

historic = True
if historic:
    historic_fid = os.path.join(data_root, "raw", "dma_meter_data.csv")
    historic_df = pd.read_csv(historic_fid)
    historic_df.DateTime = pd.to_datetime(historic_df.DateTime)

for iscovid in [False,'poponly','workfix','lockdown','workapp']:
    print(iscovid)
    #Add factors that change behaviour
    if iscovid:
        if iscovid == 'workapp':
            workforce_factor = 0.1
            handwash_factor = 3
            shower_factor = 1.5
        elif (iscovid == 'workfix') | (iscovid == 'lockdown'):
            workforce_factor = 0.1
            handwash_factor = 1
            shower_factor = 1
        elif (iscovid == 'poponly'):
            workforce_factor = 1
            handwash_factor = 1
            shower_factor = 1
    else:
        workforce_factor = 1
        handwash_factor = 1
        shower_factor = 1

    fdi_df_ = fdi_df.copy()
    ind = (fdi_df_.activity == 'handwash') & (fdi_df_.key == 'events_per_day')
    fdi_df_.loc[ind,'value'] = fdi_df_.loc[ind,'value'].astype(float) * handwash_factor
    
    ind = (fdi_df_.activity == 'wash') & (fdi_df_.key == 'events_per_day')
    fdi_df_.loc[ind,'value'] = fdi_df_.loc[ind,'value'].astype(float) * handwash_factor
    
    ind = (fdi_df_.activity == 'shower') & (fdi_df_.key == 'events_per_day')
    fdi_df_.loc[ind,'value'] = fdi_df_.loc[ind,'value'].astype(float) * shower_factor
    
    #Add factors to account for higher weekend than weekday consumption
    if historic:
        #Adjust for weekend/weekday
        historic_df['period'] = 'week'
        historic_df.loc[historic_df.DateTime.dt.weekday >= 5, 'period'] = 'weekend'
        gb1 = historic_df.groupby('DMA').mean().Consumption.rename('mean_')
        gb2 = historic_df.groupby(['DMA','period']).mean().Consumption
        gb2 = gb2.reset_index().pivot(index = 'DMA',columns = 'period', values= 'Consumption')
        gb1 = pd.merge(gb1, gb2, on ='DMA').drop('BURYRDMA', axis=0)
        week_factor = gb1.week.div(gb1.mean_).mean()
        weekend_factor = gb1.weekend.div(gb1.mean_).mean()
    else:
        week_factor = 0.99
        weekend_factor = 1.03
        
        
    #Create simulation dates and dataframes to store
    unique_activities = set(list(zip(fdi_df_.appliance,fdi_df_.activity)))
    times = pd.date_range(start = start_day, end = end_day, freq = TARGET_RESOLUTION)
    times_seconds = pd.date_range(start = start_day, end = end_day, freq = '1s')
    days = np.unique(times.date)
    n_days = len(days)
    flows = pd.DataFrame(index = times, 
                         columns = unique_activities,
                         data = np.zeros((len(times),len(unique_activities)))
                         )
    
    activity_gb = fdi_df_.groupby(['appliance','activity']) #Activities
    #e.g. to access one group:
        #idx = ('bath','bath')
        #group = gb.get_group(idx)
        
    timing_gb = timing_df.groupby(['period','workstatus'])
    
    """For each zone, for each activity, for each person or household, 
    we generate a timeseries of both household population demand, and workplace
    population demand. Then apply timings.
    """
    printnow = lambda text : print(text + " at " + datetime.now().strftime("%H:%M:%S"))
    #Iterate over zones (computer RAM will start to struggle with more than N_people*N_days = 50 million in a zone)
    flows_df = []
    results = []
    for zone in population_df.index:
        demo = population_df.loc[zone]
        
        #Generate timings for each day
        timings = {}
        printnow("starting " + zone)
        work_activity = {}
        
     
        for day in days:
            #Extract timeuse samples
            period = 'week'
            factor = week_factor
            if day.weekday() >= 5:
                period = 'weekend'
                factor = weekend_factor
                
            nonworker_samples = timing_gb.get_group((period, 'nonwork')).copy()
            worker_samples = timing_gb.get_group((period, 'work')).copy()
            
            #% of working population at work
            pct = activity_df.loc[day.weekday()]
            
            #Number of people consuming water at work but not home today
            dayworkpop = int(demo.loc['workers_to'] * pct * workforce_factor)
            
            #Number of people consuming water at home but not work today
            dayleavepop = int(demo.loc['workers_from'] * pct * workforce_factor)
            
            #Number of people consuming water in this zone and not working
            nighthousepop = int(demo.loc['household_pop']) - dayleavepop
            
            work_activity[day] = {'dayworkpop' : dayworkpop,
                                  'dayleavepop' : dayleavepop,
                                  'nighthousepop' : nighthousepop}
            
            #Generate timings
    
            def sample_times(df, N):
                df['date'] = df.datetime.dt.date
                
                sample_ = df[['serial','pnum','date']].drop_duplicates()
                sample = sample_.sample(N, replace = True)
                sample['personday'] = range(sample.shape[0])
                sample = pd.merge(sample, df, on = ['serial','pnum','date'])
                
                sample.datetime = sample.datetime.map(lambda t : t.replace(day=day.day, month = day.month, year = day.year))
                ind = sample.datetime.dt.time < datetime.strptime('4', '%H').time()
                sample.loc[ind,'datetime'] += timedelta(days=1)
                
                sample['seconds_index'] = (sample.datetime - pd.to_datetime(start_day)).dt.total_seconds().astype(int).values
                
                sample = sample[['personday','seconds_index','measurement']].sort_values(by = ['personday','seconds_index'])
                
                return sample.groupby('personday').apply(lambda x : {y : x[y].tolist() for y in x.columns})
    
            # if historic:
            #     home_samples = sample_times(nonworker_samples,  min(nighthousepop, sample_demand))
            #     leaver_samples = sample_times(worker_samples, min(dayleavepop, sample_demand))
            #     worker_samples = sample_times(worker_samples, min(dayworkpop, sample_demand))
            # else:
            home_samples = sample_times(nonworker_samples,  sample_demand)
            leaver_samples = sample_times(worker_samples, sample_demand)
            worker_samples = sample_times(worker_samples, sample_demand)
    
            if day == pd.to_datetime(start_day).date():
                day_start_times = 0
            else:
                day_start_times = int((pd.to_datetime(day) - pd.to_datetime(start_day)).total_seconds())
            day_end_times = int((pd.to_datetime(day) + pd.Timedelta(days = 1) - pd.to_datetime(start_day)).total_seconds())
            diary_end_times = int((pd.to_datetime(day) + pd.Timedelta(days = 1) + timedelta(hours=DIARY_OFFSET) - pd.to_datetime(start_day)).total_seconds())
            
            timings[day] = {'home' : home_samples,
                            'leaver' : leaver_samples,
                            'worker' : worker_samples,
                            'day_start' : day_start_times,
                            'day_end' : day_end_times,
                            'diary_end' : diary_end_times,
                            'factor' : factor}
    
        # activity_locations = [(x[0], x[1], y) for y in ['household', 'workplace'] for x in activity_gb.groups.keys()]
        flows_zone = pd.DataFrame(index = times, columns = activity_gb.groups.keys())
        flows_zone.iloc[:,:] = np.zeros(flows_zone.shape)
        printnow("starting " + " activities")
        
        #Iterate over activities
        for idx, group in activity_gb:
            
            if (idx[0] == 'shower') | (idx[0] == 'bath_tap') : 
                occurrence_cdf = {'sleep' : 9/100,
                                  'away' : 30/100,
                                  'morninghome' : 70/100, # shower stats from https://www.watefnetwork.co.uk/files/default/resources/Conference_2015/Presentations/06-HendrickxFinal.pdf
                                  'home' : 1}
                # occurrence_cdf = {'sleep' : 1.5/100,
                #                   'away' : 1.5/100,
                #                   'morninghome' : 51.5/100, # shower stats from https://www.watefnetwork.co.uk/files/default/resources/Conference_2015/Presentations/06-HendrickxFinal.pdf
                #                   'home' : 1}
            else:
                occurrence_cdf = {'sleep' : 9/100,
                                  'away' : 30/100,
                                  'morninghome' : 50/100,
                                  'home' : 1}
                # occurrence_cdf = {'sleep' : 1.5/100,
                #                   'away' : 1.5/100,
                #                   'morninghome' : 11.5/100,
                #                   'home' : 1}
            
            group = group.set_index('key').value.T # Get values only
            group = group.apply(pd.to_numeric, errors='coerce').fillna(group) # Ensure numbers are numeric datatypes
            
            for location in ['household']:
                flow_activity = pd.DataFrame(np.zeros(len(times)),index=times,columns=[idx])
                
                #Determine number of timeseries (N)
    
                if (location == 'household') & (group.presence == 'workplace'):
                    break
                
                if (location == 'workplace') & (group.presence == 'household'):
                    break
                
                N = sample_demand
                #Generate occurrences
                """ occurrences as inter-arrival-events
                buffer = 1.5
                n_occurrences = int(per_day_adjustment * n_days * group.events_per_day * buffer)
                if group.frequency_distribution == "poisson":
                    param = per_day_adjustment * group.events_per_day / SEC_PER_DAY # Average number events per second
                    occurrences = -np.log(1 - np.random.random((N_,n_occurrences)))/param
                    # Start time to occur at a random point between the 0th and 1st occurrence
                    occurrences = np.array([np.cumsum(x[1:]) + np.random.random() * x[0] for x in occurrences]).astype(int)"""
                
                if group.frequency_distribution == "poisson":
                    occurrences = np.random.poisson(group.events_per_day, (N, n_days))
                elif group.frequency_distribution == "binomial":
                    # binomial is a two parameter distribution, but blokker only gives one, so we use poisson
                    occurrences = np.random.poisson(group.events_per_day, (N, n_days))
                elif group.frequency_distribution == "n_binomial":    
                    occurrences = np.random.negative_binomial(group.frequency_r, group.frequency_p, (N, n_days))
                
                if idx[0] == "kitchen_tap":
                    occurrences = np.round(occurrences * group.events_per_day / kitchen_tap_adjustment).astype(int)
                
                n_occurrences = int(occurrences.sum(axis = 1).max())
                
                #Generate durations
                if group.duration_distribution == "constant":
                    durations = np.ones((N , n_occurrences)) * group.duration_mu
                elif group.duration_distribution == "lognormal":
                    durations = np.round(np.log(np.random.lognormal(group.duration_mu, group.duration_sig, (N, n_occurrences))))
                elif group.duration_distribution == "chi2":
                    #I'm not 100% whether it should be random.chi2(mu) or random.chi2(1)*mu
                    durations = np.round(np.random.chisquare(group.duration_mu, (N,n_occurrences)))
                    
                durations = np.maximum(0,durations).astype(int) # Ensure duration is non-negative int
                
                #Generate intensities
                if group.intensity_distribution == "constant":
                    intensities = np.ones((N , n_occurrences)) * group.intensity
                elif group.intensity_distribution == "uniform":
                    intensities = np.random.random((N, n_occurrences))*(group.intensity_upper - group.intensity_lower) + group.intensity_lower
                # plot = True
                
                def sample_times(times, N):
                    return times[(np.random.random(N) * len(times)).astype(int)]
                
                def get_individual(person_type, person_number):
                    """
                    person_type can be "worker", "leaver", "home"
                    """
                    flow_individual = np.zeros(len(times_seconds))
                    
                    n_tot = 0
    
                    for l in range(n_days):
                        day = days[l]

                        #Allocate dict (5 micros)
                        day_times = {'sleep' : np.array([]),
                                     'morninghome' : np.array([]),
                                     'home' : np.array([]),
                                     'away' : np.array([]),
                                     'work' : np.array([])}
                        
                        
                        
                        timeranges = timings[day][person_type][person_number]
                        
                        
                            
                        #Create ranges (60 micros)
                        ranges = [np.arange(x,y-1).astype(int) for x,y in zip(timeranges['seconds_index'],
                                                                timeranges['seconds_index'][1:] + [timings[day]['diary_end']])]
                        
                        #Allocate ranges (130 micros)
                        for r, m in zip(ranges, timeranges['measurement']): 
                            if (m == 'home') & (day_times['morninghome'].size == 0):
                                if (r.size > 0):
                                    if (r[-1] < (times_seconds.size-1)):
                                        if (times_seconds[r[-1]].hour >= 5) & (times_seconds[r[-1]].hour < 11):
                                            m = 'morninghome'
                            # if (m == 'home') & (times_seconds[r[-1]].hour < 11):
                            #                 m = 'morninghome'
                            day_times[m] = np.concatenate([day_times[m], r]).astype(int)
                            
                        if iscovid == 'lockdown':
                            day_times['home'] = np.concatenate([day_times['home'],day_times['away']]).astype(int)
                            day_times['away'] = np.array([])
                        
                        n_today = occurrences[person_number][l]
                        rands = np.random.random((n_today, 2))
                        ind = np.array(['home'] * n_today).astype(object)
                        start_times = np.array([]).astype(int)
                        
                        if n_today > 0:
                            ind[rands[:,0] < occurrence_cdf['morninghome']] = "morninghome"
                            ind[rands[:,0] < occurrence_cdf['away']] = 'away'
                            ind[rands[:,0] < occurrence_cdf['sleep']] = 'sleep'
                            
                            if location == "household":
                                tot_sleep = sum(ind == 'sleep')
                                if (tot_sleep > 0) & (day_times['sleep'].size > 0):
                                        start_times = np.concatenate([start_times,sample_times(day_times['sleep'],tot_sleep)])
                                
                                tot_away = sum(ind == 'away')
                                if (tot_away > 0) & (day_times['away'].size > 0):
                                    # print('warning - away consumption')
                                    start_times = np.concatenate([start_times,sample_times(day_times['away'],tot_away)])
                                        
                                tot_morn = sum(ind == 'morninghome')
                                if (tot_morn > 0) & (day_times['morninghome'].size > 0):
                                    start_times = np.concatenate([start_times,sample_times(day_times['morninghome'],tot_morn)])
                            
                            tot_home = sum(ind == 'home')
                            
                            
                            tot_work = 0
                            if (person_type == "leaver") | (person_type == "worker"):
                                #Calculate occurrences at work
                                if (group.presence == "any") | (group.presence == "workplace"):
                                    tot_work = np.round(tot_home * len(day_times['work']) / (len(day_times['work']) + len(day_times['home']))).astype(int)
                                    tot_home = tot_home - tot_work # Remove occurrences at work (for leavers)
                            
                            if location == "household":
                                if (tot_home > 0) & (day_times['home'].size > 0):
                                    start_times = np.concatenate([start_times,sample_times(day_times['home'],tot_home)])
                            
                            if location == "workplace":
                                if (tot_work > 0):
                                    start_times = sample_times(day_times['work'],tot_work)

                            n_today = len(start_times)
    
                        if start_times.size > 0:
                            for occ, dur, ins in zip(start_times, durations[i][n_tot : (n_tot + n_today)], intensities[i][n_tot : (n_tot + n_today)]):
                                start_time = occ
                                end_time = occ + dur    
                                flow_individual[start_time:end_time] = ins * timings[day]['factor']
                            
                            n_tot += n_today
                       
                    return flow_individual
                
                def scale_sample(sample, person_type):
                    sample = pd.DataFrame(sample, index = times_seconds, columns = [idx]).copy()
                    
                    for day in days:
                        if group.scaling == 'person':
                            N = work_activity[day][person_type]
                            factor = (N/sample_demand)
                        elif group.scaling == 'household':
                            factor = demo.loc[demo.index.str.contains('in household')].sum() / sample_demand
                        sample.loc[sample.index.date == day] *= factor
                    
                    sample = sample.resample(TARGET_RESOLUTION).sum()
                    return sample
                
                #Sum amount (iterating over people in sample)
                if location == 'workplace':
                    flow_sample = np.zeros(len(times_seconds))                    
                    for i in tqdm(range(N)):
                        flow_individual = get_individual('worker', i) 
                        flow_sample += flow_individual
                    flow_activity += scale_sample(flow_sample, 'dayworkpop')
                    
                if location == 'household':
                    flow_sample_h = np.zeros(len(times_seconds))
                    flow_sample_l = np.zeros(len(times_seconds))
                    for i in tqdm(range(N)):
                        flow_individual = get_individual('home', i)
                        flow_sample_h += flow_individual
                        if group.scaling != 'household':
                            flow_individual = get_individual('leaver', i)
                            flow_sample_l += flow_individual
                    
                    
                    flow_activity += scale_sample(flow_sample_h, 'nighthousepop')
                    if group.scaling != 'household':
                        flow_activity += scale_sample(flow_sample_l, 'dayleavepop')
                

                printnow('completed ' + '-'.join(idx) +  ' for ' + location)

                flows_zone[idx] += flow_activity[idx]
        
        flows_zone['tot'] = flows_zone.sum(axis=1)
        flows_zone['time'] = flows_zone.index.time
        
        
        f, ax = plt.subplots()
        plt_df = pd.DataFrame(index = pd.date_range(flows_zone.index[0], flows_zone.index[-1], freq='H'),
                              columns = ['time','tot'],
                              data = flows_zone[['time','tot']].copy().values) #Why can't I just use the copy directly? No f-in clue
        plt_df = plt_df.astype({'tot' : float})
        plt_df.tot /= demo.household_pop
        plt_df.loc[plt_df.time.isin(external_hours),'tot'] += daily_external

        weekend_ind = plt_df.index.weekday >= 5
        y_week = plt_df.loc[~weekend_ind].groupby('time').mean()
        y_weekend = plt_df.loc[weekend_ind].groupby('time').mean()
        
        # plt_df.tot = plt_df.tot/demo.household_pop
        # plt_df.loc[weekend_ind].plot(x='time', y='tot', marker ='.', linestyle='none', color= 'r', ax = ax)
        # plt_df.loc[~weekend_ind].plot(x='time', y='tot', marker ='.', linestyle='none', color= 'b', ax = ax)
        y_weekend.tot.plot(color='m', ax = ax, linewidth=2)
        y_week.tot.plot(color='c', ax = ax, linewidth = 2)
                
        if historic:
            group = historic_df.loc[historic_df.DMA == zone]
            group = group.set_index('DateTime')
            group = group.sort_index()
            
            #COVID pop adjustment
            if iscovid:
                post = group.loc[group.index > pd.to_datetime('2020-03-01'),'Consumption'].mean()
                pre = group.loc[group.index < pd.to_datetime('2020-03-01'),'Consumption'].mean()
                pop_increase = post/pre
            else:
                pop_increase = 1
            
            group.Consumption = group.Consumption.div(group.Occupancy * pop_increase)
            group = group.loc[group.index > pd.to_datetime('2017-02-01')] # stuff looks dodgy before here
            group = group.resample(TARGET_RESOLUTION).sum()
            group['time'] = (group.index - timedelta(hours=1)).time
            group = group[['time','Consumption']]
            if iscovid:
                cov_name = iscovid
                covid_ind = group.index > pd.to_datetime('2020-03-01')
            else:
                cov_name = ''
                covid_ind = group.index < pd.to_datetime('2020-03-01')
            weekend_ind = group.index.weekday >= 5
            
            
            
            
            
            # group.loc[covid_ind & weekend_ind].plot(x='time',y = 'Consumption', marker = '.', linestyle ='none', color = 'm', ax = ax)
            # group.loc[covid_ind & ~weekend_ind].plot(x='time',y = 'Consumption', marker = '.', linestyle ='none', color = 'c', ax = ax)
            
    
            group.loc[covid_ind & weekend_ind].groupby('time').mean().plot(color = 'm', linestyle = '--', ax=ax)
            group.loc[covid_ind & ~weekend_ind].groupby('time').mean().plot(color = 'c', linestyle = '--', ax = ax)
            
            def r2 (y, y_):
                return 1 - sum((y - y_)**2)/sum((y - y.mean())**2)
                
            r2weekend = r2(group.loc[covid_ind & weekend_ind].groupby('time').mean().Consumption, y_weekend.tot)
            r2week = r2(group.loc[covid_ind & ~weekend_ind].groupby('time').mean().Consumption, y_week.tot)
            
            results.append({'zone' : zone,
                            'r2weekend' : r2weekend,
                            'r2week' : r2week})
            # ax.set_title('Sample size = ' + str( work_activity[days[0]]['nighthousepop']) + '\n' + '_'.join([zone, 'r2week', str(np.round(r2week,2)), 'r2we', str(np.round(r2weekend,2))]))
        else:
            pass
            # ax.set_title('Sample size = ' + str( work_activity[days[0]]['nighthousepop']) + '\n' + zone)
        ax.set_xlabel('Time (hours)')
        ax.set_ylabel('Consumption (l/hour)')
        plt.legend(['Weekend Gen','Weekday Gen','Weekend Hist','Weekday Hist'], loc='lower right')
        # sum('asd')
        f.savefig(os.path.join(output_folder, "_".join(['sample', str(sample_demand), 'zone', zone,cov_name]) + '.png'))
        plt.close(f)
        flows_zone['zone'] = zone
        flows_df.append(flows_zone)
    
    
    flows_df = pd.concat(flows_df)
    flows_df['period'] = 'week'
    flows_df.loc[flows_df.index.weekday >= 5,'period'] = 'weekend'
    flows_gb = flows_df.groupby(['time','zone','period']).mean()

    if iscovid:
        output_fid = os.path.join(output_folder, "household_demand_covid_" + iscovid + ".csv")
    else:
        output_fid = os.path.join(output_folder, "household_demand.csv")
    flows_gb.tot.to_csv(output_fid)
    
    #format
    flows_gb = flows_gb.drop('tot', axis=1)
    flows_gb = flows_gb.reset_index().melt(id_vars = ['time','zone','period'])
    flows_gb = pd.merge(flows_gb, loads_df, left_on = 'variable_0', right_on ='appliance')
    
    def f(x):
        tot_vol = x.value.sum()
        tot_pol = x.value.mul(x.load).sum()
        return pd.Series({'flow' : tot_vol, 'conc' : tot_pol / tot_vol})
        
    flows_gb = flows_gb.groupby(['time','zone','period','variable']).apply(f)
    if iscovid:
        output_fid = os.path.join(output_folder, "household_demand_wq_covid_" + iscovid + ".csv")
    else:
        output_fid = os.path.join(output_folder, "household_demand_wq.csv")
    flows_gb.drop('flow',axis=1).to_csv(output_fid)
    
    results = pd.DataFrame(results)
    print(results)
    results.to_csv(os.path.join(output_folder,"r2_results_" +cov_name + ".csv"))