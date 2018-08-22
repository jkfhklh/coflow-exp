# -*- coding: utf-8 -*-
"""
Created on Tue Aug 14 04:42:26 2018

@author: Zifan
"""
import numpy as np
#from Coflow import GenCoflow
from Log import Log


LOG_FILE = r'test.log'

class Job(object):
    FINISHED = 1
    def __init__(self, numOfCoflows, coflows = list(), DAG = list()):
        self.log = Log(LOG_FILE).getLogger()
        self.numOfCoflows = numOfCoflows
        self.numOfActiveCoflows = numOfCoflows
        self.coflows = coflows
        self.coflowMap = {j:i for i,j in enumerate(self.coflows)}
        self.originDAG = np.copy(DAG)
        self.DAG = DAG
        self.noDependencyCoflows = [j for i,j in enumerate(self.coflows) if i not in np.nonzero(self.DAG)[1]]
#        self.finished = False
        self.finishTime = -1.
        self.coflowFinishTimes = np.zeros(self.numOfCoflows)
    
    def init(self):
        self.__init__(self.numOfCoflows, self.coflows, self.originDAG)
        
    def reorderCoflows(self, newOrder):
        self.coflows = self.coflows[newOrder]
        self.originDAG = self.originDAG[:,newOrder][newOrder]
        self.DAG = self.DAG[:,newOrder][newOrder]
    
    def __repr__(self):
        return "nCoflows: " + str(self.numOfCoflows) + "\ncoflows: " + str(
                self.coflows.tolist()) + "\nfinished: " + (("yes\nfinish time: "+ str(self.
                                   finishTime)) if self.finished() else "no")
        
    def coflowFinish(self, coflowNo, finishTime):
        releaseCoflows = list()
        self.numOfActiveCoflows -= 1
        index = self.coflowMap[coflowNo]
        self.coflowFinishTimes[index] = finishTime
        for i in np.nonzero(self.DAG[index])[0]:
            self.DAG[index][i] = False
            if not self.DAG[:,i].any():
                releaseCoflows.append(self.coflows[i])
        if self.numOfActiveCoflows <= 0 and finishTime > 0:
#            self.finished = True
            self.finishTime = finishTime
        return releaseCoflows
    
    def getCoflowFinishTime(self, coflowNo):
        return self.coflowFinishTimes[self.coflowMap[coflowNo]]
    
    def finished(self):
        return self.numOfActiveCoflows <= 0
