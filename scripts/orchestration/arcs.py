#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 03 09:48:00

@author: barney
"""
import constants

class Arc:
    def __init__(self,**kwargs):
        self.name = None
        self.inPort = None
        self.outPort = None
        self.flow = 0
        self.capacity = None  
        self.__dict__.update(kwargs)
        
        self.inPort.outArcs[self.name] = self
        self.outPort.inArcs[self.name] = self
        
        self.concentration = self.empty_concentration()
    
    def copy_concentration(self, c):
        return dict([(key,c[key]) for key in constants.POLLUTANTS])
    
    def empty_concentration(self):
        return dict([(key,0) for key in constants.POLLUTANTS])
    
    def checkArc(self):
        """Checks if any values in Arc are None
        """
        
        flag = 0
        for key, item in self.__dict__.items():
            if item is None:
                print('%s is None\n' % (key))
                flag = 1
        return flag        
    
    def getExcess(self):
        capacity = self.capacity
        return capacity - self.flow
    
    def blend_arc_concentrations(self, c1):
        
        t_flow = c1['volume'] + self.flow
        if t_flow > 0:
            for pollutant in constants.POLLUTANTS:
                self.concentration[pollutant] = (self.concentration[pollutant]*self.flow + c1[pollutant] * c1['volume'])/t_flow
            
        self.flow = t_flow

    """There is a possible problem here:
        If a request is sent along the same arc multiple times in the same 
        timestep and with different concentrations then - although the arc 
        concentration is updated accurately - the requests take the
        concentration of their source and not the arc (could this have feedforward effects?)
    """
    
    def send_push_request(self,request):
        request = self.inPort.copy_concentration(request)
        excess = self.getExcess() 
        not_pushed = max(0,request['volume'] - excess)
        request['volume'] -= not_pushed
        reply = self.outPort.set_push_request(request) 
        request['volume'] -= reply['volume']
        self.blend_arc_concentrations(request) ##arc flow is determined based on the volume pushed
        reply['volume'] += not_pushed
        return reply
    
    def send_pull_request(self,request):
        request = max(min(request, self.getExcess()),0)
        supplied = self.inPort.set_pull_request(request)
        self.blend_arc_concentrations(supplied)
        return supplied
    
    def send_pull_check(self): 
        avail = self.inPort.get_pull_available()
        avail = max(min(avail, self.getExcess()),0)
        return avail
    
    def send_push_check(self):
        avail = self.outPort.get_push_available()
        avail = min(avail, self.getExcess())
        return avail
    
    def end_timestep(self):
        self.flow = 0
        self.concentration = self.empty_concentration()
        