#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 22 16:29:17 2019

@author: barney
"""
import constants
#import numpy as np
compulsory_variables = []

class Node:
    def __init__(self,**kwargs):
        self.inArcs = {}
        self.outArcs = {}
        self.name = None
        self.date = None
        self.flag = 0
        self.queried = False
        self.query_value = 0
        self.month = 0
        self.year = 0
        self.losses = 0
        self.__dict__.update(kwargs)
        self.totqueried = 0
    def read_input(self):
        if not self.queried:
#            self.query_value = self.inputs.loc[self.date,self.data_input]
            self.query_value = self.data_input_dict[self.date]
            self.totqueried += self.query_value
            self.queried = True            
        return self.query_value
    
    
    def get_connected(self,of_type = None):
        priorities = {}
        total_avail = 0
        total_priority = 0
            
        for name, arc in self.inArcs.items():
            if (arc.inPort.type == of_type) | (of_type is None):
                avail = arc.send_pull_check()
                if arc.preference < constants.FLOAT_ACCURACY:
                    avail = 0
                priorities[name] = avail*arc.preference
                total_avail += avail
                total_priority += avail*arc.preference
        
        return {'total_avail' : total_avail, 'total_priority' : total_priority,'priorities' : priorities}
    
    def get_push_connected(self,of_type = None):
        priorities = {}
        total_avail = 0
        total_priority = 0
        for name, arc in self.outArcs.items():
            if (arc.outPort.type == of_type) | (of_type is None):
                avail = arc.send_push_check()
                priorities[name] = avail*arc.preference
                total_avail += avail
                total_priority += avail*arc.preference
        
        return {'total_avail' : total_avail, 'total_priority' : total_priority,'priorities' : priorities}
    
    def end_timestep(self):
        self.queried = False
        self.totqueried = 0
        
    def raw_concentration(self):
        return {'volume' : 0,
                'phosphorus' : 1,
                'phosphate' : 1,
                'bod' : 2,
                'cod' : 20,
                'ammonia' : 0.05,
                'solids' : 15,
                'do' : 8,
                'ph' : 7.8,
                'nitrate' : 4,
                'nitrite' : 0.02}
        
    def copy_concentration(self,c):
        return dict([('volume',c['volume'])] + [(key,c[key]) for key in constants.POLLUTANTS])
    
    def empty_concentration(self):
        return dict([('volume',0)] + [(key,0) for key in constants.POLLUTANTS])
    
    def blend_concentrations(self, c1, c2):
        c = self.empty_concentration()
        
        c['volume'] = c1['volume'] + c2['volume']
        if c['volume'] > constants.FLOAT_ACCURACY:
            for pollutant in constants.POLLUTANTS:
                c[pollutant] = (c1[pollutant]*c1['volume'] + c2[pollutant] * c2['volume'])/c['volume']
            
        return c
        
    
    def generate_outpreference_order(self):
        #Currently assume to iterate over possible outputs, sending as much as possible in preference order
        self.outPreferenceOrder = [(name,arc.preference) for name, arc in self.outArcs.items()]
        def takeSecond(elem):
            return elem[1]        
        self.outPreferenceOrder.sort(key=takeSecond, reverse=True)

        self.outPreferenceOrder = list(list(zip(*self.outPreferenceOrder))[0])

    
    
class Junction(Node):
    def __init__(self, **kwargs):
        self.type = 'Junction'
        super().__init__(**kwargs)
                
        # if hasattr(self,'mrf'):
        #     self.get_mrf = lambda : self.mrf
        # else:
        #     self.get_mrf = lambda : 0
            
    def get_pull_available(self):
        
        avail = self.get_connected()['total_avail']
        
        for name, arc in self.inArcs.items():
            # avail += arc.send_pull_check()
            avail += arc.flow
        
        for name, arc in self.outArcs.items():
            avail -= arc.flow
        
        # avail -= self.get_mrf()
        avail = max(avail,0)
        
        return avail
    
    def get_push_available(self):
        return constants.UNBOUNDED_CAPACITY
    
    def set_pull_request(self,request):
        #Pull requests take volumes as an argument
        # connected = self.get_connected('Inflow')
        abstracted = self.empty_concentration()
        # if (connected['total_avail'] > constants.FLOAT_ACCURACY):
        #     for name in connected['priorities'].keys():
        #         conc = self.inArcs[name].send_pull_request(request * connected['priorities'][name]/connected['total_priority'])
        #         abstracted = self.blend_concentrations(abstracted,conc)
        
        # deficit = request - abstracted['volume']
        deficit = request
        
        connected = self.get_connected()
        while (deficit > constants.FLOAT_ACCURACY) & (connected['total_avail'] > constants.FLOAT_ACCURACY):
            for name in connected['priorities'].keys():
                conc = self.inArcs[name].send_pull_request(deficit * connected['priorities'][name]/connected['total_priority'])
                abstracted  = self.blend_concentrations(abstracted,conc)
            
            deficit = (request - abstracted['volume'])
            connected = self.get_connected()
            
        return abstracted
    
    def set_push_request(self,request):
        #Push requests take concentrations as an argument
        #We're assuming junctions can always receive water (strictly not true in cases where river is higher that discharge point?) - we can add a level-flow relationship in the future if so
        #We're also assuming that junctions that make push requests only have one out-location which is to another junction
        if 'waste' in self.name:
            spill = self.empty_concentration()
        else:
            for name, arc in self.outArcs.items():
                if arc.outPort.type == 'Junction':
                    request = arc.send_push_request(request)
            if (request['volume'] > 0):
                print('spill at junction : ' + self.name + ' time: ' + self.date)
                
            spill = self.copy_concentration(request)
        return spill


class Demand(Node):
    def __init__(self, **kwargs):
        #Set defaults
        self.type = 'Demand'
        self.restriction_pct_reduction = [0,2.2,9.1,13.3,31.3]
        self.property_leakage = 0
        self.indoor_returned = 0
        self.consumed = 0
        
        #Pollutants params
        self.bod_param = 10
        self.cod_param = 800
        self.solids_param = 285
        self.ammonia_param = 30
        self.do_param = 4
        self.phosphorus_param = 10
        self.phosphate_param = 20
        self.ph_param = 7.5
        self.nitrate_param = 1
        self.nitrite_param = 0.1
        
        super().__init__(**kwargs)
        
        
        #Initialise        
        self.demand = 0
        self.supplied = self.empty_concentration()
        self.spill = self.empty_concentration()                                   
        self.rainfallLink = None
        self.satisfied_by_rainfall = self.empty_concentration()
        
        
        
        if hasattr(self,'data_input'):
            self.get_demand = self.read_input
        else:
            self.get_demand = self.calculate_demand
        
        if hasattr(self,'area'):
            self.garden_area = self.area * constants.PCT_GARDENS
            self.external *= constants.GARDEN_MULTIPLIER
        else:
            self.garden_area = 0
            self.area = 0
            
    def update_concentration(self,request):
        return {'volume' : request, 
                'bod' : self.bod_param,
                'cod' : self.cod_param,
                'solids' : self.solids_param,
                'ammonia' : self.ammonia_param,
                'do' : self.do_param, 
                'phosphorus' : self.phosphorus_param,
                'phosphate' : self.phosphate_param,
                'ph' : self.ph_param,
                'nitrate' : self.nitrate_param,
                'nitrite' : self.nitrite_param}
    
    def calculate_demand(self):
        cons = indem = outdem = 0 
        multiplier = 1
        if hasattr(self,'restriction_pct_reduction'):
            multiplier = (1 - self.restriction_pct_reduction[self.restrictions] * constants.PCT_TO_PROP)
        if hasattr(self,'demand_profile'):
            multiplier *= self.demand_profile[self.month - 1]
        
        if hasattr(self,'constant'):
            cons = self.constant * multiplier
            
        if hasattr(self,'population'):
            if hasattr(self,'indoor'):
                indem = self.population * self.indoor * constants.L_TO_ML * multiplier
            if hasattr(self,'external'):
                outdem = self.population * self.external * constants.L_TO_ML * multiplier
        
        if self.garden_area > 0:
            expected_rainfall = self.rainfallLink.inPort.external_input() * constants.MM_M2_TO_SIM_VOLUME * self.garden_area
            satisfied_by_rainfall = min(outdem, expected_rainfall)
            satisfied_by_rainfall = self.rainfallLink.send_pull_request(satisfied_by_rainfall)
            self.satisfied_by_rainfall = self.blend_concentrations(self.satisfied_by_rainfall, satisfied_by_rainfall)
            outdem -= satisfied_by_rainfall['volume']
        
        #apply property leakage
        if hasattr(self,'property_leakage'):
            demand = (indem + cons + outdem)/(1 - self.property_leakage)
        else:
            demand = (indem + cons + outdem)
        return demand
    
    def produce_waste(self):
        #apply losses on property
        losses = self.supplied['volume'] * self.property_leakage
        self.losses += losses
        supplied = self.supplied['volume'] - losses
        
        consumed = supplied * (1 - self.indoor_returned)
        self.consumed += consumed
        waste = supplied - consumed
        waste = self.update_concentration(waste)
        if any(self.outArcs):
            arc = list(zip(*self.outArcs.items()))[1][0] #Assuming only one out arc    
            self.spill = self.blend_concentrations(self.spill, arc.send_push_request(waste))
        else:
            self.spill = self.blend_concentrations(self.spill, waste) # might need a copy here but I think not
            
    def set_push_request(self, request):
        self.supplied = self.copy_concentration(request)
        self.supplied['volume'] = min(request['volume'], self.demand)
        unrequired_water = self.copy_concentration(request)
        unrequired_water['volume'] = max(self.supplied['volume'] - self.demand, 0)
        return unrequired_water
    
    def end_timestep(self):
        self.spill = self.empty_concentration()
        self.supplied = self.empty_concentration()
        self.satisfied_by_rainfall = self.empty_concentration()
        self.demand = 0
        self.consumed = 0
        self.queried = False
        self.losses = 0
        
class Land(Node):
    def __init__(self, **kwargs):
        #Pollutants params
        self.bod_param = 5
        self.cod_param = 130
        self.solids_param = 15
        self.ammonia_param = 0.5
        self.do_param = 7
        self.phosphorus_param = 0.2
        self.phosphate_param = 0.5
        self.ph_param = 7.7
        self.nitrate_param = 2
        self.nitrite_param = 0.2

        self.type = 'Land'
        
        super().__init__(**kwargs)
        
        self.impervious_storage = self.empty_concentration()
        self.impervious_storage_ = self.empty_concentration()
        
        self.greenspace_storage = self.empty_concentration()
        self.greenspace_storage_ = self.empty_concentration()
        self.greenspace_spill = self.empty_concentration()
        self.greenspace_attenuation_capacity = 10 # mm - est
        self.greenspace_attenuation_capacity *= (self.area * constants.MM_M2_TO_SIM_VOLUME)
        self.greenspace_dissipation_rate = 2 # mm - avg pet
        self.greenspace_dissipation_rate *= (self.area * constants.MM_M2_TO_SIM_VOLUME)
        self.greenspace_dissipation = 0
        self.sewer_spill = self.empty_concentration()

        
    def update_concentration(self,request):
        return {'volume' : request, 
                'bod' : self.bod_param,
                'cod' : self.cod_param,
                'solids' : self.solids_param,
                'ammonia' : self.ammonia_param,
                'do' : self.do_param, 
                'phosphorus' : self.phosphorus_param,
                'phosphate' : self.phosphate_param,
                'nitrate' : self.nitrate_param,
                'nitrite' : self.nitrite_param,
                'ph' : self.ph_param,
                }
        
    def create_runoff(self):
        
        #Load rain - currently assuming land has only 1 input (rain)
        connected = self.get_connected()
        rain_arc = [x for x in self.inArcs.values()][0]
        self.rain = rain_arc.send_pull_request(connected['total_avail'])
        
        # if hasattr(self,'impervious_dissipation_rate'):
        #     self.rain = max(self.rain - self.impervious_dissipation_rate, 0)
#        self.rain = self.rain * self.area * constants.MM_M2_TO_SIM_VOLUME
        
        #Send rain to sewer
        runoff_to_sewer = self.rain['volume'] * self.runoff_coef
        runoff_to_sewer = self.update_concentration(runoff_to_sewer)
        runoff_to_sewer = self.blend_concentrations(runoff_to_sewer, self.impervious_storage)
        self.impervious_storage = self.empty_concentration()
        
        for arc in self.outArcs.values():
            runoff_to_sewer_ = self.copy_concentration(runoff_to_sewer)
            runoff_to_sewer_['volume'] *= arc.preference # Send along arcs according to preference
            reply = arc.send_push_request(runoff_to_sewer_)
            self.impervious_storage = self.blend_concentrations(reply, self.impervious_storage)

        #Rain on greenspace
        # --- NO blending currently since we're assume no pollutants in rain... maybe this is wrong
        self.greenspace_dissipation = min(self.greenspace_storage['volume'], self.greenspace_dissipation_rate)
        self.greenspace_storage['volume'] -= self.greenspace_dissipation # this should probably increase concentration (v1)?
        rain_on_greenspace = self.rain['volume'] * (1 - self.runoff_coef)
        self.greenspace_storage['volume'] += rain_on_greenspace
        gspill = max(self.greenspace_storage['volume'] - self.greenspace_attenuation_capacity, 0 )        
        self.greenspace_spill['volume'] += gspill
        self.greenspace_storage['volume'] -= gspill
        
    def end_timestep(self):
        self.sewer_spill = self.empty_concentration()
        self.greenspace_spill = self.empty_concentration()
        self.greenspace_storage_ = self.copy_concentration(self.greenspace_storage)
        self.impervious_storage_ = self.copy_concentration(self.impervious_storage)
        self.queried = False
        # self.impervious_storage = self.empty_concentration()
        
class Sewerage(Node):
    def __init__(self, **kwargs):
        self.leakage = 0
        
        self.storage = 0
        
        self.discharge_preference_type = 'absolute' #Can be 'absolute' or 'proportional'
        
        self.type = 'Sewerage'
        
        super().__init__(**kwargs)
        
        temp = self.raw_concentration()
        temp['volume'] = self.storage
        self.storage = temp
        self.storage_ = temp
        self.losses = 0
        self.sewer_spill = self.empty_concentration()
        
    def get_push_available(self):
        avail = self.capacity - self.storage['volume']
        return avail
        
    def set_push_request(self,request):
        
        request_ = self.copy_concentration(request)
        request_vol = request['volume']
        
        #Check sewer capacity
        spill_vol = max(request_vol + self.storage['volume'] - self.capacity, 0 )        
        
        request_['volume'] -= spill_vol
        
        self.storage = self.blend_concentrations(self.storage, request_)
        
        request_['volume'] = spill_vol
        return request_
    
    def make_discharge(self):
        
        discharge_ = self.copy_concentration(self.storage)
        discharge = self.copy_concentration(self.storage)
        
        #Apply leakage
        discharge['volume'] *= (1 - self.leakage * constants.PCT_TO_PROP)
        
        #This could be calculated at end of timestep but for mass balance checking
        self.losses += discharge_['volume'] - discharge['volume']
        
        #Send push request to attached nodes
        if self.discharge_preference_type == 'absolute':
            for name in self.outPreferenceOrder:
                discharge = self.outArcs[name].send_push_request(discharge)

        else:
            print('warning: no discharge preferences for node ' + self.name)

        self.storage['volume'] = discharge['volume']
        
        # junction_spill = discharge['volume']/(1 - self.leakage * constants.PCT_TO_PROP)
        # if junction_spill >= constants.FLOAT_ACCURACY:
        #     print('sewer overloaded at : ' + self.name + ' date : ' + self.date)
        
        
        
    
    """Pulling from sewer discharge
    This is assumes that no pulls can be made - maybe is OK?
    """
    
    def set_pull_request(self,request):
        return self.empty_concentration()
    
    def get_pull_available(self):
        return 0
    
    def end_timestep(self):
        self.storage_ = self.copy_concentration(self.storage)
        # self.storage = self.empty_concentration()
        self.losses = 0
    
class Wwtw(Node):
    def __init__(self, **kwargs):
        
        self.percentage_solid = 0.0002 # as a percentage of influent + recycle

        self.liquor_multiplier = {'volume' : 0.03,
                                     'bod' : 1.7,
                                     'cod' : 1.7,
                                     'solids' : 2.6,
                                     'ammonia' : 5.6,
                                     'phosphorus' : 2,
                                     'phosphate' : 2,
                                     'nitrate' : 8, # punt
                                     'nitrite' : 8, #punt
                                     'do' : 0, # punt
                                     'ph' : 1.2, #punt
                                     } # as a percentage of influent + recycle
        
        #Pollutants params (based on WIMS reduction from crude to final sewage)
        self.bod_mul = 0.03
        self.cod_mul = 0.08
        self.solids_mul = 0.05
        self.ammonia_mul = 0.03
        self.do_mul = 1.2
        self.phosphorus_mul = 0.1
        self.phosphate_mul = 1.3
        self.ph_mul = 1
        self.nitrate_mul = 20 #nitrification of ammonia to nitrate I think!
        self.nitrite_mul = 5
        self.wq = {}
        
        self.type = 'Wwtw'
        
        super().__init__(**kwargs)
        
        
        
        #Could this just not be set before the super() call?
#        if not hasattr(self,'previous_input'):
#            p_inp = self.treatment_capacity * 0.5
#        
#        
#        self.previous_input = self.empty_concentration()
#        self.previous_input['volume'] = p_inp
#        self.previous_input = self.update_concentration(self.previous_input)
        
        self.liquor = {'volume' : self.treatment_capacity * 0.5 * self.liquor_multiplier['volume'],
                       'bod' : 350,
                       'cod' : 350,
                       'solids' : 800,
                       'ammonia' : 200,
                       'phosphorus' : 80,
                       'phosphate' : 80,
                       'nitrate' : 50,
                       'nitrite' : 5,
                       'do' : 0,
                       'ph' : 7}
        self.liquor_ = self.copy_concentration(self.liquor)
        self.discharge = self.empty_concentration()
        self.current_input = self.empty_concentration()
        self.stormwater_storage = self.empty_concentration()
        self.stormwater_storage_ = self.empty_concentration()
        self.throughput = self.empty_concentration()
        self.losses = self.empty_concentration()
#        self.max_rate_change = self.treatment_capacity        
    
    def get_push_available(self):
        return self.treatment_capacity - self.current_input['volume']
    
    def set_push_request(self,request):
        
        request_vol = request['volume']
        request_input = self.copy_concentration(request)
        request_stored = self.copy_concentration(request)
        temporary_input = self.copy_concentration(self.stormwater_storage)
        
        #Check capacities
#        max_throughput = min(self.previous_input['volume'] + self.max_rate_change, self.treatment_capacity) - self.current_input['volume']
        max_throughput = self.treatment_capacity - self.current_input['volume']
        if max_throughput < -constants.FLOAT_ACCURACY:
            print('warning max throughput < 0')
            max_throughput = 0
        available_storage = self.stormwater_capacity - self.stormwater_storage['volume']
        
        #Send request for treatment
        request_input['volume'] = min(request_vol,max_throughput)
        self.current_input = self.blend_concentrations(request_input, self.current_input)
        
        #Store the rest
        request_stored['volume'] = min(request_vol - request_input['volume'], available_storage)
        self.stormwater_storage = self.blend_concentrations(request_stored,self.stormwater_storage)
        
        #Clear storage if possible
        temporary_input['volume'] = max(min(max_throughput - self.current_input['volume'], self.stormwater_storage['volume']),0)
        
        self.current_input = self.blend_concentrations(self.current_input, temporary_input)
        self.stormwater_storage['volume'] -= temporary_input['volume']
                

        #Difference is sent back
        spill = self.copy_concentration(request)
        spill['volume'] -= (request_input['volume'] + request_stored['volume'])
        # self.discharge = self.blend_concentrations(self.discharge, spill)
        
        return spill
    
    def set_pull_request(self,request):
        avail = self.copy_concentration(self.discharge)
        avail['volume'] = min(request, avail['volume'])
        self.discharge['volume'] -= avail['volume']
        return avail
    
    def get_pull_available(self):
        avail = self.discharge['volume']
        for name, arc in self.outArcs.items():
            avail -= arc.flow
        return avail     
    
    def calculate_discharge(self):
        influent = self.blend_concentrations(self.current_input, self.liquor)
        self.losses = self.empty_concentration()
        
        effluent_multiplier =        {'volume' : 1 - self.percentage_solid - self.liquor_multiplier['volume'],
                                     'bod' : self.bod_mul,
                                     'cod' : self.cod_mul,
                                     'solids' : self.solids_mul,
                                     'ammonia' : self.ammonia_mul,
                                     'phosphorus' : self.phosphorus_mul,
                                     'phosphate' : self.phosphate_mul,
                                     'nitrate' : self.nitrate_mul,
                                     'nitrite' : self.nitrite_mul,
                                     'do' : self.do_mul,
                                     'ph' : self.ph_mul} # as a percentage of influent + recycle
        
        #Calculate effluent and liquor
        discharge_holder = self.empty_concentration()
        for key in constants.POLLUTANTS + ['volume']:
            discharge_holder[key] = influent[key] * effluent_multiplier[key]
            self.liquor[key] = influent[key] * self.liquor_multiplier[key]
        
        self.losses['volume'] = influent['volume'] * self.percentage_solid # should sum with discharge and liquor volume
        for key in constants.POLLUTANTS:
            self.losses[key] = influent[key] * influent['volume'] - \
                                discharge_holder[key] * discharge_holder['volume'] - \
                                self.liquor[key] * self.liquor['volume']
        
        #Blend with any existing discharge
        self.discharge = self.blend_concentrations(self.discharge, discharge_holder)
        
    def make_discharge(self):

        
        #Currently assuming can always make discharge
        for name in self.outPreferenceOrder:
            #Send push request to attached nodes
            self.discharge = self.outArcs[name].send_push_request(self.discharge)
        
        if self.discharge['volume'] > 0:
            print('warning, unable to discharge at ' + self.name + ' during ' + self.date)
    
    def end_timestep(self):
#        self.previous_input = self.current_input #Don't think this needs to be copied because of the way python handles pointers
        self.liquor_ = self.copy_concentration(self.liquor)
        self.current_input = self.empty_concentration()
        self.discharge = self.empty_concentration()
        self.stormwater_storage_ = self.copy_concentration(self.stormwater_storage)
        
class Inflow(Node):
    #Inflows can only connect forward
    
    def __init__(self, **kwargs):
        self.wq = {}
        
        self.bod_param = self.raw_concentration()['bod']
        self.cod_param = self.raw_concentration()['cod']
        self.solids_param = self.raw_concentration()['solids']
        self.ammonia_param = self.raw_concentration()['ammonia']
        self.do_param = self.raw_concentration()['do']
        self.phosphorus_param = self.raw_concentration()['phosphorus']
        self.phosphate_param = self.raw_concentration()['phosphate']
        self.ph_param = self.raw_concentration()['ph']
        self.nitrate_param = self.raw_concentration()['nitrate']
        self.nitrite_param = self.raw_concentration()['nitrite']
        
        self.type = 'Inflow'
        
        super().__init__(**kwargs)
        
        
        #This is super hacky
        if 'rainfall' in self.name:
            self.multiplier = self.area * constants.MM_M2_TO_SIM_VOLUME
        else:
            self.multiplier = 1
        
        if hasattr(self,'data_input'):
            self.external_input = lambda : self.multiplier*self.read_input()
        elif hasattr(self,'constant'):
            self.external_input = lambda : self.constant
        else:
            self.external_input = lambda : 0
        
        
    
            
    def get_pull_available(self):
        
        avail = self.external_input()
        
        """This should be included... I can't remember why it isn't :/
        """
        # for name, arc in self.outArcs.items():
        #     avail -= arc.flow
        
        return avail
    
    def update_concentration(self,request):
        conc = self.raw_concentration()
        
        conc['bod'] = self.bod_param
        conc['cod'] = self.cod_param
        conc['solids'] = self.solids_param
        conc['ammonia'] = self.ammonia_param
        conc['do'] = self.do_param
        conc['phosphorus'] = self.phosphorus_param
        conc['phosphate'] = self.phosphate_param
        conc['ph'] = self.ph_param
        conc['nitrate'] = self.nitrate_param
        conc['nitrite'] = self.nitrite_param
        
        if hasattr(self,'wq'):
            for var in self.wq.keys():
                if self.date in self.wq[var].keys():
                    conc[var] = self.wq[var][self.date]
#                if self.date in self.wq[var].index:
#                    conc[var] = self.wq[var][self.date]

        conc['volume'] = request
        return conc
    
    def set_pull_request(self, request):
        avail_ = self.get_pull_available()        
        return self.update_concentration(min(avail_,request))
