#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun  3 13:24:03 2017

@author: huanyeliu
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
#import numpy as np
#import matplotlib.pyplot as plt



class MRIncomeAnnual(MRJob):
    
    def mapper_first(self,_,line):
        rlist = line.split(',')
        try:
            total = float(rlist[14])
        except:
            total = 0
        taxi_id = rlist[1]
        year = rlist[2].split('-')[0]
        total = float(rlist[14])
        if (taxi_id!=''):
            yield (taxi_id,year),total

    def combiner_first(self,key,fares):
        yield key,sum(fares)
        
    def reducer_first(self,key,fares):
        yield key[0],sum(fares)
    
    def reducer_final(self,key,fares):
        f_list = list(fares)
        n_year = len(f_list)
        f_total = sum(f_list)
        average = f_total/n_year
        #income_dict[key]=average
        yield key, average
    
    def steps(self):
        return [MRStep(mapper=self.mapper_first,
                       combiner=self.combiner_first,
                       reducer=self.reducer_first),
                MRStep(reducer=self.reducer_final)
                ]
                
if __name__ == '__main__':
    MRIncomeAnnual.run()