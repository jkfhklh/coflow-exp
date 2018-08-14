# -*- coding: utf-8 -*-
"""
Created on Tue Aug 14 04:42:26 2018

@author: Zifan
"""
import numpy as np
#from Coflow import GenCoflow

class Job(object):
    def __init__(self, size, coflows = list(), DAG = list()):
        self.size = size
        self.coflows = coflows
        self.DAG = DAG
        self.noDependencyCoflows = [j for i,j in enumerate(self.coflows) if i not in np.nonzero(self.DAG)[1]]