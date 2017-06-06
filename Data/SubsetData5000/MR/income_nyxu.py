#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from mrjob.job import MRJob
from mrjob.step import MRStep
import numpy as np

class MRIncomeAnnual(MRJob):
    
    def mapper_first(self,_,line):
        rlist = line.split(',')
        taxi_id = rlist[1]
        year = rlist[2].split('/')[2][0:4]
        total = float(rlist[18])
        if (taxi_id!=''):
            yield (taxi_id,year),total

    def combiner_first(self,key,fares):
        yield key,sum(fares)
        
    def reducer_first(self,key,fares):
        yield key[0],sum(fares)
    
    def reducer_further(self,key,fares):
        f_list = list(fares)
        n_year = len(f_list)
        f_total = sum(f_list)
        average = f_total/n_year
        # syield the average income of a driver 
        yield key, average
        
    def reducer_final_init(self):
        self.income_dict={}
        self.income_class={}
        
    def reducer_final(self,key,average):
        # store the average income into a dictionary
        self.income_dict[key] = list(average)[0]
        
    def reducer_final_final(self):
        # log transform drivers' average income and
        # classify drivers into normal drivers and rich drivers
        #  based on the distribution of the log incomes.
        income_list = np.array(list(self.income_dict.values()))
        ddev = np.std(np.log(income_list+0.02))
        dmean = np.mean(np.log(income_list+0.02))
        for dID in self.income_dict:
            # if a driver's income is higher than 2 standard deviation above the mean
            # then the driver is a rich driver
            if np.log(self.income_dict[dID]+0.02)>ddev*2+dmean:
                self.income_class[dID] = 1
            # if a driver's income is between 2 standard deviation from the mean
            # then the driver is a normal driver
            if dmean-2*ddev<=np.log(self.income_dict[dID]+0.02)<=dmean+2*ddev:
                self.income_class[dID] = 0
        # output the dictionary to a file for later use
        yield None,self.income_class
        
    def steps(self):
        return [MRStep(mapper=self.mapper_first,
                       combiner=self.combiner_first,
                       reducer=self.reducer_first),
                MRStep(reducer=self.reducer_further),
                MRStep(reducer_init = self.reducer_final_init,
                       reducer = self.reducer_final,
                       reducer_final = self.reducer_final_final)
                ]
                
if __name__ == '__main__':
    MRIncomeAnnual.run()
    
