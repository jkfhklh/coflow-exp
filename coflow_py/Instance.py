# -*- coding: utf-8 -*-
"""
Created on Wed Aug 15 17:36:08 2018

@author: Zifan
"""

import numpy as np
from Coflow import Coflow
from Event import Event
from Log import Log

LOG_FILE = r'test.log'
JOB_DEBUG = True
COFLOW_DEBUG = False
ROUND_INFO = False
ROUND_WARNING = False
PROGRESS_WARNING = True
PROGRESS_STEP = 10

class Instance(object):
    INIT = 1
    MYALGO = 2
    UTOPIA = 3
    AALO = 4
    
    def __init__(self, numOfMachines, numOfCoflowsInJob, coflows = list(), jobs = list(), DRFtime = list()):
        self.log = Log(LOG_FILE).getLogger()
        self.numOfMachines = numOfMachines
        self.numOfCoflowsInJob = numOfCoflowsInJob
        self.coflows = coflows
        self.numOfCoflows = len(coflows)
        self.jobs = jobs
        self.numOfJobs = len(jobs)
        self.jobFinishTimes = np.zeros(self.numOfJobs)
        self.numOfActiveJobs = self.numOfJobs
#        self.DRFtime = DRFtime
        self.jobPriority = None
#        [i for i,j in sorted(enumerate(DRFtime),key=lambda x:x[1])]
        self.orderedCoflows = None
        self.state = Instance.INIT
#        np.concatenate([self.jobs[i].coflows for i in self.jobPriority])
#            for i in self.jobs:
#                for j in i.noDependencyCoflows:
#                    coflows[j].release()
        self.released = np.zeros(self.numOfCoflows, dtype=bool)
        self.nReleased = 0
        for i in self.jobs:
            self.nReleased += len(i.noDependencyCoflows)
            for j in i.noDependencyCoflows:
                self.released[j] = True
        self.round = 0
        self.numOfFinishedCoflows = 0
        self.coflowProgress = PROGRESS_STEP
        self.haveRun = False
    
    def init(self):
        if self.haveRun:
            for i in self.coflows:
                i.init()
            for i in self.jobs:
                i.init()
            self.__init__(self.numOfMachines, self.numOfCoflowsInJob, self.coflows, self.jobs)
    
    def getMyOrder(self):
        self.state = Instance.MYALGO
        DRFtime = list()
        for i in self.jobs:
            coflowTime = np.array([self.coflows[k].bottleneckSize for k in i.coflows])
            sumTime = np.zeros(i.numOfCoflows)
            for j,k in enumerate(coflowTime):
                dep = np.nonzero(i.DAG[:,j])
                if len(dep[0]) == 0:
                    sumTime[j] = k
                else:
                    sumTime[j] = max(sumTime[m] for m in dep[0]) + k
            newOrder = np.argsort(sumTime)
            sumTime = sumTime[newOrder]
            i.reorderCoflows(newOrder)
            DRFtime.append(sumTime[-1])
        self.jobPriority = np.argsort(np.array(DRFtime))
        self.orderedCoflows = np.concatenate([self.jobs[i].coflows for i in self.jobPriority])
    
    def getUtopiaOrder(self):
        self.state = Instance.UTOPIA
        self.jobPriority = None
        self.orderedCoflows = np.argsort(np.array([i.bottleneckSize for i in self.coflows]))
        
    def getAaloOrder(self):
        self.state = Instance.AALO
        self.jobPriority = None
        self.orderedCoflows = np.random.permutation(self.numOfCoflows)
        self.E = 10
        self.threshold = [10.]
        self.q = [[i for i in self.orderedCoflows if self.released[i]]]
#        self.activeCoflows = self.q[0].copy()
        self.coflowMap = np.zeros(self.numOfCoflows, dtype=np.int64) - 1
        self.coflowMap[self.released] = 0
        self.K = 1
        self.W = [1]
    
    def aaloCoflowFinish(self, coflowNo, finishTime):
        self.numOfFinishedCoflows += 1
        if PROGRESS_WARNING:
            if self.numOfFinishedCoflows / self.numOfCoflows >= self.coflowProgress * 0.01:
                print("coflow progress: " + str(self.coflowProgress) + r"%")
                self.coflowProgress += PROGRESS_STEP
        if COFLOW_DEBUG:
            self.log.info("Coflow:" + str(coflowNo) + " finishes at time:" + str(finishTime))
        self.released[coflowNo] = False
        self.nReleased -= 1
        self.q[self.coflowMap[coflowNo]].remove(coflowNo)
        self.coflowMap[coflowNo] = -1
        jobIndex = self.coflows[coflowNo].jobIndex
        job = self.jobs[jobIndex]
        releasedCoflows = job.coflowFinish(coflowNo, finishTime)
        if job.finished():
            if JOB_DEBUG:
                self.log.info("Job:" + str(jobIndex) + " finishes at time:" + str(finishTime))
            self.numOfActiveJobs -= 1
        else:
            self.released[releasedCoflows] = True
            self.nReleased += len(releasedCoflows)
            self.q[0].extend(releasedCoflows)
            self.coflowMap[releasedCoflows] = 0
    
    def aaloTransmitCoflow(self, coflowNo, net, startTime, transTime):
        
        if self.coflows[coflowNo].transmit(net, transTime) == Coflow.FINISHED:
            self.aaloCoflowFinish(coflowNo, startTime + transTime)
        if self.coflows[coflowNo].getTransedSize() >= self.threshold[self.coflowMap[coflowNo]]:
            self.q[self.coflowMap[coflowNo]].remove(coflowNo)
            self.coflowMap[coflowNo] += 1
            if self.K == self.coflowMap[coflowNo]:
                self.K += 1
                self.q.append([])
                self.threshold.append(self.threshold[-1] * self.E)
            self.q[self.coflowMap[coflowNo]].append(coflowNo)
    
    def jobReview(self):
        return [i.numOfCoflows for i in self.jobs]
    
    def coflowFinish(self, coflowNo, finishTime):
        self.numOfFinishedCoflows += 1
        if PROGRESS_WARNING:
            if self.numOfFinishedCoflows / self.numOfCoflows >= self.coflowProgress * 0.01:
                print("coflow progress: " + str(self.coflowProgress) + r"%")
                self.coflowProgress += PROGRESS_STEP
        if COFLOW_DEBUG:
            self.log.info("Coflow:" + str(coflowNo) + " finishes at time:" + str(finishTime))
        self.released[coflowNo] = False
        self.nReleased -= 1
        jobIndex = self.coflows[coflowNo].jobIndex
        job = self.jobs[jobIndex]
        releasedCoflows = job.coflowFinish(coflowNo, finishTime)
        if job.finished():
            if JOB_DEBUG:
                self.log.info("Job:" + str(jobIndex) + " finishes at time:" + str(finishTime))
            self.numOfActiveJobs -= 1
        else:
            self.released[releasedCoflows] = True
            self.nReleased += len(releasedCoflows)
    
    def transmitCoflow(self, coflowNo, net, startTime, transTime):
#        print(type(transTime))
        if self.coflows[coflowNo].transmit(net, transTime) == Coflow.FINISHED:
            self.coflowFinish(coflowNo, startTime + transTime)
                
    def runOneEvent(self, event = None, eventClass = Event):
        self.haveRun = True
        if event == None:
            event = eventClass(self, 0.)
        self.nextRound()
        return event.run()
    
    def finished(self):
        return self.numOfActiveJobs <= 0
    
    '''
    run experiment
    '''
    def run(self, event = None, eventClass = Event):
        while not self.finished():
            event = self.runOneEvent(event, eventClass)
#        if PROGRESS_WARNING:
#            print("coflow progress: " + str(self.coflowProgress) + r"%")
        return event
    
    def nextRound(self):
        self.round += 1
        if ROUND_INFO:
            self.log.info("Round " + str(self.round) + ":")
        if ROUND_WARNING:
            print("Round " + str(self.round) + ":")
    
    def coflowData(self, coflowNo):
        '''coflow data:index totalsize bottlenecksize finishtime'''
        
        return "%d %f %f %f" % (coflowNo, 
                                self.coflows[coflowNo].bytes,
                                self.coflows[coflowNo].originBottleneckSize,
                                self.jobs[self.coflows[coflowNo].jobIndex].getCoflowFinishTime(coflowNo))
    
    def getJobSize(self, jobNo):
        return np.sum([self.coflows[i].bytes for i in self.jobs[jobNo].coflows])
    
    def jobData(self, jobNo):
        '''job data:index coflownumber totalsize finishtime'''
        return "%d %d %f %f" % (jobNo,
                                self.jobs[jobNo].numOfCoflows,
                                self.getJobSize(jobNo),
                                self.jobs[jobNo].finishTime)
    
    def exportData(self,file):
        with open(file + "-coflow.txt","w") as f:
            f.writelines([self.coflowData(i) + "\n" for i in range(self.numOfCoflows)])
        with open(file + "-job.txt","w") as f:
            f.writelines([self.jobData(i) + "\n" for i in range(self.numOfJobs)])
    
    def exportInst(self, file):
        with open(file + "-inst.txt","w") as f:
            f.write(str(self.numOfMachines))