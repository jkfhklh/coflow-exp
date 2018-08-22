# -*- coding: utf-8 -*-
"""
Created on Wed Aug 22 05:07:16 2018

@author: Zifan
"""

from Event import Event

class UtopiaEvent(Event):
    def __init__(self, inst, startTime):
        Event.__init__(self, inst, startTime)
    
    def checkInstState(self):
        if (self.inst.state != self.inst.UTOPIA):
            self.inst.getUtopiaOrder()
    def __class__(self):
        return UtopiaEvent
    def run(self):
        self.checkInstState()
        self.fillPorts()
        self.fillBack()
        transTime = self.transmit()
        return self.__class__()(self.inst, self.startTime + transTime)
