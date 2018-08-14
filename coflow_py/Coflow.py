# -*- coding: utf-8 -*-
"""
Created on Tue Aug 14 03:07:58 2018

@author: Zifan
"""

import numpy as np

class Coflow(object):
    def __init__(self, index, numOfMappers, mappers, numOfReducers, reducers):
        self.index = index
        self.numOfMappers = numOfMappers
        self.mappers = mappers
        self.numOfReducers = numOfReducers
        self.reducers = reducers
        self.bytes = sum(i[1] for i in self.reducers)
    
    def __repr__(self):
        return "id:%d nom:%d nor:%d size:%f" % (self.index, self.numOfMappers, 
                                                self.numOfReducers, self.bytes)


class GenCoflow(Coflow):
    def __init__(self, index, numOfMappers, mappers, numOfReducers, reducers, jobIndex, flows):
        Coflow.__init__(self, index, numOfMappers, mappers, numOfReducers, reducers)
        self.jobIndex = jobIndex
        self.numOfMachines = len(flows)
        self.flows = flows
        self.ports = np.concatenate((np.sum(self.flows,axis=1),np.sum(self.flows,axis=0)))
        self.bottleneck = max(enumerate(self.ports),key=lambda x:x[1])[0]