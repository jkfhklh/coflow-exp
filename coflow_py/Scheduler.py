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
from Instance import Instance
from Event import Event
from UtopiaEvent import UtopiaEvent
from AaloEvent import AaloEvent

DEBUG = True
EXPERIMENT_WARNING = True

LOG_FILE = r'test.log'
TRACE_FILE = r'FB2010-1Hr-150-0.txt'

#class Instance(object):
#        def __init__(self, numOfMachines, numOfCoflowsInJob, coflows = list(), jobs = list(), DRFtime = list()):
#            self.numOfMachines = numOfMachines
#            self.numOfCoflowsInJob = numOfCoflowsInJob
#            self.coflows = coflows
#            self.jobs = jobs
#            self.DRFtime = DRFtime
#            self.jobPriority = [i for i,j in sorted(enumerate(DRFtime),key=lambda x:x[1])]
#            self.orderedCoflows = np.concatenate([self.jobs[i].coflows for i in self.jobPriority])
#        
#        def jobReview(self):
#            return [i.size for i in self.jobs]

class Scheduler(object):
    dc = None
    
    def __init__(self):
        self.log = Log(LOG_FILE).getLogger()
        self.log.info("===============================new test===============================")
        self.originCoflows = list()
        self.originNumberOfMachines = 0
        self.originNumberOfCoflows = 0
        self.totalSize = 0.
        self.inst = None
    
    def loadTraceFile(self,file):
        with open(file,'r') as f:
            self.originNumberOfMachines, self.originNumberOfCoflows = [int(i) for i in 
                                                          f.readline()[:-1].split(' ')]
#            self.log.info('%d %d' % (self.originNumberOfMachines,self.originNumberOfCoflows))
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
                self.totalSize += self.originCoflows[-1].size()
            self.log.info("Load tracefile " + file + "success!")
    
    '''
    function:
    generate a valid coflow and job instance for one experiment
    parameter: 
    1:num of machines
    2:num of expected coflows in a job
    '''
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
            reducers = [[k,l] for k,l in zip(range(numOfMachines),[0]*numOfMachines)]
            for k in self.originCoflows[j].reducers:
                reducers[machineMap[k[0]]][1] += k[1]
            reducers = [k for k in reducers if k[1] > 0]
            numOfReducers = len(reducers)
            for k in reducers:
                nrand = np.random.randint(1,numOfMappers + 1)
                randmapper = random.sample(mappers,nrand)
                rand = np.random.uniform(size=nrand)
#                if i == 520:
#                    self.log.info(str(rand))
                rand = rand * k[1] / np.sum(rand)
#                if i == 520:
#                    self.log.info(str(rand))
                flows[randmapper,k[0]] += rand
            coflows.append(GenCoflow(i, numOfMappers, mappers, numOfReducers, reducers, jobMap[i], flows))
        
        jobs = list()
#        DRFtime = list()
        for i in range(numOfJobs):
            size = jobClusters[i+1] - jobClusters[i]
            dependency = np.zeros((size,size),dtype=bool)
            numOfDependencies = np.random.randint(size)
            for j in range(numOfDependencies):
                tempd = np.random.randint(size,size=2)
                while (tempd[0] >= tempd[1] or dependency[tempd[0],tempd[1]]):
                    tempd = np.random.randint(size,size=2)
                dependency[tempd[0],tempd[1]] = True
#            if i == 14:
#                print(dependency)
#            coflowsInJob = coflowSequence[jobClusters[i]:jobClusters[i+1]]
#            coflowTime = np.array([coflows[k].bottleneckSize for k in coflowsInJob])
#            sumTime = np.zeros(len(coflowTime))
#            for j,k in enumerate(coflowTime):
#                dep = np.nonzero(dependency[:,j])
#                if len(dep[0]) == 0:
#                    sumTime[j] = k
#                else:
#                    sumTime[j] = max(sumTime[k] for k in dep[0]) + k
##            if i == 14:
##                print(sumTime)
#            newOrder = np.argsort(sumTime)
##            jobs[i].coflows = jobs[i].coflows[newOrder]
#            sumTime = sumTime[newOrder]
#            dependency = dependency[:,newOrder][newOrder]
#            if i == 14:
#                print(dependency)
#                print(sumTime)
#            DRFtime.append(sumTime[-1])
            jobs.append(Job(size, coflowSequence[jobClusters[i]:jobClusters[i+1]], dependency))
            
        return Instance(numOfMachines, numOfCoflowsInJob, coflows, jobs)

if __name__ == '__main__':
    S = Scheduler()
    S.loadTraceFile(TRACE_FILE)
    for i in range(100):
        for machines in [20,30,40,50]:
            for coflowinjobs in [10, 15, 20, 25, 30]:
                inst = S.genInstance(machines, coflowinjobs)
#                inst.exportInst()
                event = None
                event = inst.run(event,Event)
                inst.exportData('results\\%d-%d-%d-my' % (machines,coflowinjobs,i))
                print("machine:%d cinjob:%d, id:%d myalgo finish!" % (machines, coflowinjobs, i))
                inst.init()
                event = None
                event = inst.run(event, AaloEvent)
                inst.exportData('results\\%d-%d-%d-aalo' % (machines,coflowinjobs,i))
                print("machine:%d cinjob:%d, id:%d aalo finish!" % (machines, coflowinjobs, i))
                inst.init()
                event = None
                event = inst.run(event, UtopiaEvent)
                inst.exportData('results\\%d-%d-%d-utopia' % (machines,coflowinjobs,i))
                print("machine:%d cinjob:%d, id:%d utopia finish!" % (machines, coflowinjobs, i))
                if EXPERIMENT_WARNING:
                    print('Experiment %d completes' % (i))