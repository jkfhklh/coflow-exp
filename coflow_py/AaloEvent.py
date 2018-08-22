# -*- coding: utf-8 -*-
"""
Created on Wed Aug 22 05:08:11 2018

@author: Zifan
"""
import numpy as np
from Event import Event

class AaloEvent(Event):
#    delta = 1.
    
    def __init__(self, inst, startTime):
        Event.__init__(self, inst, startTime)

    def checkInstState(self):
        if (self.inst.state != self.inst.AALO):
            self.inst.getAaloOrder()
    
    def updateRemain(self, upport, downport, size, coflowIndex, queueIndex):
#        size = np.min((size, Event.remainUp[upport], Event.remainDown[downport]))
        if size > Event.precision:
            Event.dc[upport][downport][coflowIndex] = size
            Event.sdc[upport][downport] += size
            Event.remainUp[queueIndex][upport] -= size
            Event.remainDown[queueIndex][downport] -= size
    
    def fillPorts(self):
        Event.dc = np.zeros((self.inst.numOfMachines, self.inst.numOfMachines, 
                       self.inst.numOfCoflows))
        Event.sdc = np.zeros((self.inst.numOfMachines, self.inst.numOfMachines))
        weights = np.array([[self.inst.K - i] for i in range(self.inst.K)],dtype=np.float64)
        noEmptyQueue = np.array([(True if len(i) > 0 else False) for i in self.inst.q])
        Event.remainUp = np.ones((self.inst.K, self.inst.numOfMachines)) * weights / np.sum(weights[noEmptyQueue])
        Event.remainDown = np.ones((self.inst.K, self.inst.numOfMachines)) * weights / np.sum(weights[noEmptyQueue])
#        superCoflow = np.zeros(self.inst.numOfMachines * 2)
        for s in range(3):
#            print("K: %d, length of queue: %d" % (self.inst.K, len(self.inst.q)))
            for i,j in enumerate(self.inst.q):
                if len(j) == 0:
                    continue
                for k in j:
                    if self.inst.released[k]:
                        currentCoflow = self.inst.coflows[k]
                        activeFlows = np.transpose(currentCoflow.activeFlows)
                        portFlows = currentCoflow.portFlows.copy()
                        while len(activeFlows) > 0:
                            activePorts = np.where(portFlows > 0)[0]
#                            try:
                            portPieces = np.true_divide(np.concatenate((Event.remainUp[i],Event.remainDown[i])), portFlows)
#                            except ZeroDivisionError:
#                                pass
                            aggr = activePorts[np.argmax(portPieces[activePorts])]
                            if portPieces[aggr] <= Event.precision:
                                break
                            if aggr < self.inst.numOfMachines:
                                for m,n in activeFlows:
                                    if m == aggr:
                                        self.updateRemain(m, n, portPieces[aggr], k, i)
                                activeFlows = activeFlows[activeFlows[:,0] == aggr]
                                portFlows[m] = 0
                            else:
                                newaggr = aggr - self.inst.numOfMachines
                                for m,n in activeFlows:
                                    if n == newaggr:
                                        self.updateRemain(m, n, portPieces[aggr], k, i)
                                activeFlows = activeFlows[activeFlows[:,1] == newaggr]
                                portFlows[aggr] = 0
                slices = list(range(self.inst.K))
                slices.remove(i)
                Event.remainUp[slices] += weights[slices] * Event.remainUp[i] / np.sum(weights[slices])
                Event.remainDown[slices] += weights[slices] * Event.remainDown[i] / np.sum(weights[slices])
    
    def delta(self):
        for i,j in enumerate(self.inst.q):
            if len(j) > 0:
                break
        return self.inst.threshold[i]/10
    
    def transmit(self):
        for i,j in enumerate(self.inst.coflows):
            if self.inst.released[i]:
#                print(type(self.startTime))
                self.inst.aaloTransmitCoflow(i, self.dc[:,:,i], self.startTime, self.delta())
        return self.delta()
#        '''fillback'''
#        for i in np.where(Event.remainUp > Event.precision)[0]:
#            corresponding = np.nonzero(Event.sdc[i])[0]
#            corresponding = corresponding[Event.remainDown[corresponding] > Event.precision]
#            sumDown = np.sum(Event.remainDown[corresponding])
#            if Event.remainUp[i] >= sumDown:
#                Event.dc[i,corresponding,:] *= (Event.remainDown[corresponding] / 
#                        Event.sdc[i,corresponding] + 1).reshape(len(corresponding),1)
#                Event.sdc[i,corresponding] += Event.remainDown[corresponding]
#                Event.remainUp[i] -= sumDown
#                Event.remainDown[corresponding] = 0.
#            else:
#                while Event.remainUp[i] > Event.precision:
#                    add = Event.sdc[i,corresponding] * Event.remainUp[i] / np.sum(Event.sdc[i,corresponding])
#                    add = np.where(add > Event.remainDown[corresponding], Event.remainDown[corresponding], add)
#                    Event.dc[i,corresponding,:] *= (add / Event.sdc[i,corresponding] + 1).reshape(len(corresponding),1)
#                    Event.sdc[i,corresponding] += add
#                    Event.remainUp[i] -= np.sum(add)
#                    Event.remainDown[corresponding] -= add
#                    corresponding = corresponding[Event.remainDown[corresponding] > Event.precision]
#    
    def run(self):
        self.checkInstState()
        self.fillPorts()
        Event.remainUp = np.sum(Event.remainUp,axis = 0)
        Event.remainDown = np.sum(Event.remainDown,axis = 0)
        self.fillBack()
        transTime = self.transmit()
        return self.__class__()(self.inst, self.startTime + transTime)
    
    def __class__(self):
        return AaloEvent
