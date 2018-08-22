# -*- coding: utf-8 -*-
"""
Created on Tue Aug 14 03:07:58 2018

@author: Zifan
"""

import numpy as np
from Log import Log

LOG_FILE = r'test.log'

class Coflow(object):
    precision = 1e-8
    FINISHED = 1
    UNFINISHED = 2
    
    def __init__(self, index, numOfMappers, mappers, numOfReducers, reducers):
        self.log = Log(LOG_FILE).getLogger()
        self.index = index
        self.numOfMappers = numOfMappers
        self.mappers = mappers
        self.numOfReducers = numOfReducers
        self.reducers = reducers
        self.bytes = sum(i[1] for i in self.reducers)
    
    def __repr__(self):
        return "id:%d nom:%d nor:%d size:%f" % (self.index, self.numOfMappers, 
                                                self.numOfReducers, self.bytes)
    
    def size(self):
        return self.bytes


class GenCoflow(Coflow):
    def __init__(self, index, numOfMappers, mappers, numOfReducers, reducers, jobIndex, flows):
        Coflow.__init__(self, index, numOfMappers, mappers, numOfReducers, reducers)
        self.originFlows = np.copy(flows)
        self.jobIndex = jobIndex
        self.numOfMachines = len(flows)
        '''narray'''
        self.flows = flows 
        self.activeFlows = np.where(self.flows > Coflow.precision)
        self.nFlows = len(np.nonzero(self.flows)[0])
        self.portFlows = np.concatenate((np.count_nonzero(self.flows > Coflow.precision,axis=1),
                                         np.count_nonzero(self.flows > Coflow.precision,axis=0)))
        self.updataPorts()
        self.originBottleneck = self.bottleneck
        self.originBottleneckSize = self.bottleneckSize
        self.remainSize = self.bytes
#        self.ports = np.concatenate((np.sum(self.flows,axis=1),np.sum(self.flows,axis=0)))
#        self.demand = self.ports / np.max(self.ports)
#        self.bottleneck = np.argmax(self.ports)
#        self.bottleneckSize = self.ports[self.bottleneck]
#        self.released = False
    
    def init(self):
        self.__init__(self.index, self.numOfMappers, self.mappers, self.numOfReducers, self.reducers,
                      self.jobIndex, self.originFlows)
    
    def __repr__(self):
        nActiveFlows = len(self.activeFlows[0])
        if not nActiveFlows:
            return "id: " + str(self.index) + "\nfinished: yes"
        else:
            return "id: " + str(self.index) + "\nfinished: no\nactive flows number: " + str(
                    nActiveFlows) + "\n" + "\n".join(["upport: %d downport: %d size: %f" % (
                            i,j,self.flows[i,j]) for i,j in np.transpose(self.activeFlows)])
        
    def transTime(self, net):
        if net[self.activeFlows].all():
            return np.max(self.flows[self.activeFlows] / net[self.activeFlows])
        else:
            return np.finfo(np.float64).max
    
    def updataPorts(self):
        self.ports = np.concatenate((np.sum(self.flows,axis=1),np.sum(self.flows,axis=0)))
        self.bottleneck = np.argmax(self.ports)
        self.bottleneckSize = self.ports[self.bottleneck]
    
    def transmit(self, net, transtime):
#        print(type(transtime))
        self.flows[self.activeFlows] -= net[self.activeFlows] * transtime
        self.activeFlows = np.where(self.flows > Coflow.precision)
        self.portFlows = np.concatenate((np.count_nonzero(self.flows > Coflow.precision,axis=1),
                                         np.count_nonzero(self.flows > Coflow.precision,axis=0)))
        self.remainSize = np.sum(self.flows[self.activeFlows])
        if len(self.activeFlows[0]) == 0:
            return Coflow.FINISHED
        else:
            self.updataPorts()
            return Coflow.UNFINISHED
#    def release(self):
#        self.released = True
    
#    def transmit(self, net, time):
#        self.flows -= net * time
    def getTransedSize(self):
        return self.bytes - self.remainSize