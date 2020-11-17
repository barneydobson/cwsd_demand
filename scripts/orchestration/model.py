# -*- coding: utf-8 -*-
"""
Created on Wed Jul 22 13:08:57 2020

@author: Barney
"""
from tqdm import tqdm
import nodes
import constants
from arcs import Arc
from datetime import datetime

class dm_Demand(nodes.Demand):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        del(self.restriction_pct_reduction)
        if hasattr(self, 'external'):
            self.external *= constants.PER_DAY_TO_PER_HOUR
        if hasattr(self, 'constant'):
            if self.name not in ['upstream-abstractions', 'lee-take']:
                self.constant *= constants.PER_DAY_TO_PER_HOUR
    
    def calculate_indoor(self):
        #Retrieve demand
        demand = self.dem[(self.period, self.hour)]
        
        #Convert to l/p/dt
        unit_demand = demand / self.population # might get dodgy considering the population growth since census
        
        return unit_demand
        
    def update_concentration(self, request):
        conc = {pol : self.wqh[(self.period, self.hour, pol)] for pol in constants.POLLUTANTS}
        
        conc['volume'] = request
        
        #Manual factors to account for changes in WQ between household and WWTW
        #Based on conc from household and conc in WIMS Crude sewage
        conc['ammonia'] *= 1.2
        conc['nitrate'] *= 0.25
        conc['nitrite'] *= 0.35
        conc['phosphate'] *= 0.2
        conc['phosphorus'] *= 0.25
        conc['cod'] *= 0.6
        conc['solids'] *= 0.6
        return conc


class Model:
    """Class that contains cwsd nodes and arcs with functions to add them from generic databases
    """
    def __init__(self):
        self.model_arcs = {}
        self.model_nodes = {}
        self.model_nodes_type = {}
        for node in nodes.Node.__subclasses__():
            self.model_nodes_type[node.__name__] = {}
            
    def add_nodes(self, nodes_dict):
        for name, data in nodes_dict.items():
            data['name'] = name
            if data['type'] == 'Demand':
                self.model_nodes_type[data['type']][name] = dm_Demand(**dict(data))
            else:
                self.model_nodes_type[data['type']][name] = getattr(nodes,data['type'])(**dict(data))
            self.model_nodes[name] = self.model_nodes_type[data['type']][name]

            
    def add_arcs(self, arcs_dict):
        for name, arc in arcs_dict.items():
            self.model_arcs[name] = Arc(name=name,
                                        inPort=self.model_nodes[arc['inPort']],
                                        outPort=self.model_nodes[arc['outPort']],
                                        capacity=arc['capacity'],
                                        preference=arc['preference'])
            
    def add_inputs(self, inputs_dict):
        dates = []
        for node in self.model_nodes.values():
            if hasattr(node,'data_input'):
                node.data_input_dict = inputs_dict[node.data_input]
                dates.append(node.data_input_dict.keys())
                        
        #Inputs must be defined for all dates
        self.dates = list(set.intersection(*[set(x) for x in dates]))
        
        #Relies on dates being in ISO 8601 to sort correctly
        self.dates.sort()
        
    def add_wq(self, wq_dict):
        for name, wq in wq_dict.items():
            self.model_nodes[name].wq = wq
        
    def add_demands(self, demand_dict):
        for name, dem in demand_dict.items():
            self.model_nodes[name].dem = dem
    
    def add_wqh(self, wqh_dict):
        for name, wqh in wqh_dict.items():
            self.model_nodes[name].wqh = wqh
    
class London(Model):
    #Contains functions that perform misc functions specific to the London model setup
    
    def process(self):
        
        #Convert demand profiles to numeric
        for node in self.model_nodes_type['Demand'].values():
            if hasattr(node, 'demand_profile'):
                d_p = node.demand_profile
                d_p = [float(y) for y in d_p.strip('[]').split(',')]
                node.demand_profile = d_p
                
        #Assign garden rainfall
        for node in self.model_nodes_type['Demand'].values():
            if node.garden_area > 0:
                for arc in node.inArcs.values():
                    if arc.inPort.type == 'Inflow':
                        node.rainfallLink = arc
                        
        #Add arc related node options
        for node in list(self.model_nodes_type['Sewerage'].values()) + list(self.model_nodes_type['Wwtw'].values()):
            node.generate_outpreference_order()
        
        #Add upstream effluent
        upstream_effluent = 10.5 # Ml/hr
        in_data = self.model_nodes['thames-upstream'].data_input_dict
        for key in in_data.keys():
            in_data[key] += upstream_effluent
        
        #Remove non-drained demands
        del(self.model_nodes_type['Demand']['lee-take'])
        del(self.model_nodes_type['Demand']['upstream-abstractions'])
        
    def add_ltoa(self, df):
        self.ltoa = df
        
    def run(self):
        
        """Initiliase results
        """
        flows = []
        pollutants = []
        flows_h = []
        pollutants_h = []
        sewer_spill = []
        
        """Misc
        """
        teddington_mrf = 33 # Ml/hr
        
        """Iterate over dates
        """
        for date in tqdm(self.dates):
            month = int(date.split('-')[1])
            year =  int(date.split('-')[0])
            hour = int(date.split(' ')[1].split(':')[0])
            period = 'week'
            if datetime.strptime(date, '%Y-%m-%d %H:%M:%S').weekday() >= 5:
                period = 'weekend'
                
            for node in self.model_nodes.values():
                node.date = date
                node.month = month
                node.year = year
                node.period = period
                

            """Calculate demand & Produce waste
            """
            for node in self.model_nodes_type['Demand'].values():
                node.hour = hour
                node.indoor = node.calculate_indoor()
                node.supplied = {'volume' : node.get_demand()}
                node.produce_waste()
            
            """Make it rain & store spill
            """
            for node in self.model_nodes_type['Land'].values():
                node.create_runoff()
                sewer_spill.append({'date' : date, 'hour' : hour, 'node': node.name, 'val' : node.impervious_storage['volume']})
                
            """Discharge sewers
            """
            for node in self.model_nodes_type['Sewerage'].values():
                node.make_discharge()
                
        
            """Calculate WWTW output
            """
            for node in self.model_nodes_type['Wwtw'].values():
                node.calculate_discharge()
            
            """Set MRFs and route downstream
            """
            teddington_mrf_reply = self.model_arcs['thames-to-crane'].send_pull_request(teddington_mrf)
            should_be_0 = self.model_arcs['thames-flow-5'].send_push_request(teddington_mrf_reply)
            
            lee_mrf = self.model_arcs['lee-flow-1'].send_pull_request(self.model_nodes['lee-abstraction'].mrf)
            should_be_0 = self.model_arcs['lee-to-thames'].send_push_request(lee_mrf)
            
            """Abstract water
            """
            def force_abstraction(node):
                reply = list(node.inArcs.values())[0].send_pull_request(node.demand)
                node.consumed = reply['volume']
            force_abstraction(self.model_nodes['upstream-abstractions'])
            force_abstraction(self.model_nodes['lee-take'])
            
            """Discharge remaining WWTW water
            """
            for node in self.model_nodes_type['Wwtw'].values():
                node.make_discharge()
                
            """Route remaining flows
            """
            for name, node in self.model_nodes_type['Inflow'].items():
                if ('gw' not in name) & ('non-simulated' not in name) & ('rainfall' not in name):
                    outArc = list(node.outArcs.values())[0]
                    spill = outArc.send_push_request(node.set_pull_request(constants.MAX_INFLOW))
                    if spill['volume'] > 0:
                        print('spill at ' + date + ' for ' + name)
                        
            """Store daily simulation data
            """
            for name, arc in self.model_arcs.items():
                flows.append({'date' : date, 'arc' : name, 'val' : arc.flow})
                for pollutant in constants.POLLUTANTS:
                    pollutants.append({'date' : date, 'arc': name, 'pollutant' : pollutant, 'val' : arc.concentration[pollutant]})
            
            """Check mass balance
            """
            system_in = 0
            system_out = 0
            system_dt = 0
            for name, node in self.model_nodes.items():
                if 'waste' not in node.name:
                    totin = 0
                    totout = 0
                    dt = 0
                    for arc in node.inArcs.values():
                        totin += arc.flow
                    for arc in node.outArcs.values():
                        totout += arc.flow

                    if node.type == 'Demand':
                        totout += (node.consumed + node.losses + node.spill['volume'] + node.satisfied_by_rainfall['volume'])
                    elif node.type == 'Land':
                        dt = (node.greenspace_storage['volume'] - node.greenspace_storage_['volume'])
                        dt += (node.impervious_storage['volume'] - node.impervious_storage_['volume'])
                        totout += (node.greenspace_dissipation + node.greenspace_spill['volume'])
                    elif node.type == 'Sewerage':
                        totout += node.losses
                        dt = (node.storage['volume'] - node.storage_['volume'])
                    elif node.type == 'Wwtw':
                        dt = (node.stormwater_storage['volume'] - node.stormwater_storage_['volume'])
                        dt += (node.liquor['volume'] - node.liquor_['volume'])
                        totout += node.losses['volume']
                    elif node.type == 'Inflow':
                        node.queried = True
                        totin += (node.totqueried - node.get_pull_available())
                    
                    if (totin - totout - dt) > constants.FLOAT_ACCURACY:
                        print('mass balance error at : ' + name + ' at date : ' + date)
                    
                    system_in += totin
                    system_out += totout
                    system_dt += dt
                    
            if (system_in - system_out - system_dt) > constants.FLOAT_ACCURACY:
                print('system mass balance error at date : ' + date)
                
            """End timestep
            """
            for node in self.model_nodes.values():
                node.end_timestep()        
                    
            for arc in self.model_arcs.values():
                arc.end_timestep()
        
        return {'flows' : flows, 'pollutants' : pollutants, 'spills' : sewer_spill}