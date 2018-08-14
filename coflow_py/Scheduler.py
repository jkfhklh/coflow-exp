# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

from Log import Log
from Coflow import Coflow,GenCoflow
from Job import Job
import numpy as np
import random

DEBUG = True

LOG_FILE = r'test.log'
TRACE_FILE = r'FB2010-1Hr-150-0.txt'

class Instance(object):
        def __init__(self, numOfMachines, numOfCoflowsInJob, coflows = list(), jobs = list(), priority = list()):
            self.numOfMachines = numOfMachines
            self.numOfCoflowsInJob = numOfCoflowsInJob
            self.coflows = coflows
            self.jobs = jobs
            self.priority = priority
        
        def jobReview(self):
            return [i.size for i in self.jobs]

class Scheduler(object):
    id = 0
    
    def __init__(self):
        self.log = Log(LOG_FILE).getLogger()
        self.originCoflows = list()
        self.originNumberOfMachines = 0
        self.originNumberOfCoflows = 0
    
    def loadTraceFile(self,file):
        with open(file,'r') as f:
            self.originNumberOfMachines, self.originNumberOfCoflows = [int(i) for i in 
                                                          f.readline()[:-1].split(' ')]
            self.log.info('%d %d' % (self.originNumberOfMachines,self.originNumberOfCoflows))
            for cinfo in f.readlines():
                cinfo = cinfo[:-1].split(' ')
                cid = int(cinfo[0])
                cnumOfMappers = int(cinfo[2])
                cMappers = [int(i) for i in cinfo[3:3+cnumOfMappers]]
                cnumOfReducers = int(cinfo[3+cnumOfMappers])
                cReducers = [[int(i),float(j)] for i,j in 
                             [k.split(':') for k in cinfo[4+cnumOfMappers:]]]
                self.originCoflows.append(Coflow(cid, cnumOfMappers, cMappers, 
                                                 cnumOfReducers, cReducers))
    
    def genInstance(self, numOfMachines, numOfCoflowsInJob):
        machineClusterSize = int(float(self.originNumberOfMachines)/numOfMachines) + 1
        machineClusters = np.random.permutation(machineClusterSize * numOfMachines)
        machineClusters = machineClusters.reshape(numOfMachines, machineClusterSize)
#        self.log.info(str(machineClusters))
#        machineMap = dict()
#        for d in [{k:i for k in j} for i,j in enumerate(machineClusters)]:
#            machineMap.update(d)
        machineMap = {i:j for j in range(numOfMachines) for i in machineClusters[j]}
#        self.log.info(str(machineMap))
        
        numOfJobs = int(float(self.originNumberOfCoflows)/numOfCoflowsInJob) + 1
        numOfCoflows = numOfJobs * numOfCoflowsInJob
        coflowSequence = np.random.permutation(numOfCoflows)
#        jobClusters = jobClusters.reshape(numOfJobs, numOfCoflowsInJob)
        jobClusters = [0] + sorted(random.sample(range(1,numOfCoflows), numOfJobs-1)) + [numOfCoflows]
#        jobMap = dict()
#        for d in [{k:i for k in j} for i,j in enumerate(jobClusters)]:
#            jobMap.update(d)
        jobMap = {coflowSequence[i]:j for j in range(numOfJobs) for i in range(jobClusters[j],jobClusters[j+1])}
        
        coflows = list()
        for i in range(numOfCoflows):
            if i >= self.originNumberOfCoflows:
                j = np.random.randint(0,self.originNumberOfCoflows)
            else:
                j = i
            flows = np.zeros((numOfMachines,numOfMachines))
#            self.log.info(str([machineMap[k] for k in self.originCoflows[j].mappers]))
            mappers = sorted(list(set([machineMap[k] for k in self.originCoflows[j].mappers])))
            numOfMappers = len(mappers)
            reducers = dict(zip(range(numOfMachines),[0]*numOfMachines))
            for k in self.originCoflows[j].reducers:
                reducers[machineMap[k[0]]] += k[1]
            reducers = sorted([[k,l] for k,l in reducers.items() if l > 0])
            numOfReducers = len(reducers)
            for k in reducers:
                rand = np.random.uniform(size=numOfMappers)
                if i == 520:
                    self.log.info(str(rand))
                rand = rand * k[1] / np.sum(rand)
                if i == 520:
                    self.log.info(str(rand))
                flows[mappers,k[0]] += rand
            coflows.append(GenCoflow(i, numOfMappers, mappers, numOfReducers, reducers, jobMap[j], flows))
        
        jobs = list()
        for i in range(numOfJobs):
            size = jobClusters[i+1] - jobClusters[i]
            dependency = np.zeros((size,size),dtype=bool)
            numOfDependencies = np.random.randint(size)
            for j in range(numOfDependencies):
                tempd = np.random.randint(size,size=2)
                while (tempd[0] >= tempd[1]):
                    tempd = np.random.randint(size,size=2)
                dependency[tempd[0],tempd[1]] = True
            jobs.append(Job(size, coflowSequence[jobClusters[i]:jobClusters[i+1]], dependency))
            
        return Instance(numOfMachines, numOfCoflowsInJob, coflows, jobs)
        
    def DRFsort(self,inst):
        totalbytes = np.zeros(inst.numOfMachines * 2, dtype=float)
        for i in inst.jobs:
            for j in i.noDependencyCoflows:
                totalbytes += inst.coflows[j].ports
        
        

if __name__ == '__main__':
    S = Scheduler()
    S.loadTraceFile(TRACE_FILE)
    inst = S.genInstance(30, 10)