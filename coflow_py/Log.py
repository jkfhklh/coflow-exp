# -*- coding: utf-8 -*-
"""
Created on Mon Aug 13 16:17:56 2018

@author: Zifan
"""
import logging
import logging.handlers


fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(levelname)s - %(message)s'
formatter = logging.Formatter(fmt)


class Log(object):
    def __init__(self,file):
        self.logger = logging.getLogger(file)
        if not self.logger.hasHandlers():
            handler =  logging.handlers.RotatingFileHandler(file, maxBytes = 1024*1024, backupCount = 5)
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        self.logger.setLevel(logging.DEBUG)
        
    def debug(self, msg = ''):
        self.logger.debug(msg)
    
    def info(self,msg = ''):
        self.logger.info(msg)
    
    def getLogger(self):
        return self.logger
