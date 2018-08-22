# -*- coding: utf-8 -*-
"""
Created on Wed Aug 15 17:38:14 2018

@author: Zifan
"""


import numpy as np
#from Instance import Instance
#from Scheduler import *



class Event(object):
    dc = None
    sdc = None
    remainUp = None
    remainDown = None
    precision = 1e-8
    FINISHED = 1
    
    def __init__(self, inst, startTime):
        self.inst = inst
        self.startTime = startTime
    
    def updateRemain(self, upport, downport, size, coflowIndex):
        size = np.min((size, Event.remainUp[upport], Event.remainDown[downport]))
        if size > Event.precision:
            Event.dc[upport][downport][coflowIndex] = size
            Event.sdc[upport][downport] += size
            Event.remainUp[upport] -= size
            Event.remainDown[downport] -= size
    
    def fillPorts(self):
        Event.dc = np.zeros((self.inst.numOfMachines, self.inst.numOfMachines, 
                       self.inst.numOfCoflows))
        Event.sdc = np.zeros((self.inst.numOfMachines, self.inst.numOfMachines))
        Event.remainUp = np.ones(self.inst.numOfMachines)
        Event.remainDown = np.ones(self.inst.numOfMachines)
        superCoflow = np.zeros(self.inst.numOfMachines * 2)
        '''super coflows'''
        for i in self.inst.orderedCoflows:
            if self.inst.released[i]:
                currentCoflow = self.inst.coflows[i]
                superCoflow += currentCoflow.ports
                newOrder = np.argsort(superCoflow)
                bottleneck = np.argmax(superCoflow)
                if bottleneck < self.inst.numOfMachines:
                    newOrder = newOrder[newOrder < self.inst.numOfMachines]
                    for m,n in np.transpose(np.nonzero(currentCoflow.flows[newOrder])):
                        self.updateRemain(m,n,currentCoflow.flows[m,n]/superCoflow[bottleneck],i)
                else:
                    newOrder = newOrder[newOrder >= self.inst.numOfMachines] - self.inst.numOfMachines
                    for m,n in np.transpose(np.nonzero(currentCoflow.flows.T[newOrder])):
                        self.updateRemain(n,m,currentCoflow.flows[n,m]/superCoflow[bottleneck],i)
    
    def fillBack(self):
        '''fillback'''
        for i in np.where(Event.remainUp > Event.precision)[0]:
            corresponding = np.nonzero(Event.sdc[i])[0]
            corresponding = corresponding[Event.remainDown[corresponding] > Event.precision]
            sumDown = np.sum(Event.remainDown[corresponding])
            if Event.remainUp[i] >= sumDown:
                Event.dc[i,corresponding,:] *= (Event.remainDown[corresponding] / 
                        Event.sdc[i,corresponding] + 1).reshape(len(corresponding),1)
                Event.sdc[i,corresponding] += Event.remainDown[corresponding]
                Event.remainUp[i] -= sumDown
                Event.remainDown[corresponding] = 0.
            else:
                while Event.remainUp[i] > Event.precision:
                    add = Event.sdc[i,corresponding] * Event.remainUp[i] / np.sum(Event.sdc[i,corresponding])
                    add = np.where(add > Event.remainDown[corresponding], Event.remainDown[corresponding], add)
                    Event.dc[i,corresponding,:] *= (add / Event.sdc[i,corresponding] + 1).reshape(len(corresponding),1)
                    Event.sdc[i,corresponding] += add
                    Event.remainUp[i] -= np.sum(add)
                    Event.remainDown[corresponding] -= add
                    corresponding = corresponding[Event.remainDown[corresponding] > Event.precision]
#        if self.finishTime == 0:
#            return Event.FINISHED
    
    def transmit(self):
        firstCoflow = 0
        if not self.inst.released.any():
            self.inst.numOfActiveJobs = 0
            return 0.
        while not self.inst.released[self.inst.orderedCoflows[firstCoflow]]:
            firstCoflow += 1
        finishCoflowNo = self.inst.orderedCoflows[firstCoflow]
        transTime = self.inst.coflows[finishCoflowNo].bottleneckSize
        for i in self.inst.orderedCoflows[firstCoflow+1:]:
            currentCoflow = self.inst.coflows[i]
            if self.inst.released[i] and currentCoflow.bottleneckSize < transTime:
                tmpTime = currentCoflow.transTime(self.dc[:,:,i])
                if tmpTime < transTime:
                    transTime = tmpTime
                    finishCoflowNo = i
        for i in self.inst.orderedCoflows[firstCoflow:]:
            if self.inst.released[i]:
                if i == finishCoflowNo:
                    self.inst.coflowFinish(i, self.startTime + transTime)
                else:
                    self.inst.transmitCoflow(i, self.dc[:,:,i], self.startTime, transTime)
        return transTime
    
    def checkInstState(self):
        if (self.inst.state != self.inst.MYALGO):
            self.inst.getMyOrder()
    
    def run(self):
        self.checkInstState()
        self.fillPorts()
        self.fillBack()
        transTime = self.transmit()
        return self.__class__()(self.inst, self.startTime + transTime)
    
    def __class__(self):
        return Event


