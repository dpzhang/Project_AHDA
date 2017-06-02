#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun  1 12:12:06 2017

@author: huanyeliu
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
import numpy as np
import matplotlib.pyplot as plt

income_dict={}
income_class={}

class MRIncomeAnnual(MRJob):
    
    def mapper_first(self,_,line):
        rlist = line.split(',')
        taxi_id = rlist[1]
        year = rlist[2].split('-')[0]
        total = float(rlist[14])
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
        income_dict[key]=average
        #yield key, average
    
    def steps(self):
        return [MRStep(mapper=self.mapper_first,
                       combiner=self.combiner_first,
                       reducer=self.reducer_first),
                MRStep(reducer=self.reducer_final)
                ]

class MRIncomeDiff(MRJob):
    
    def mapper_first(self,_,line):
        rlist = line.split(',')
        taxi_id = rlist[1]
        year = rlist[2].split('-')[0]
        pick_up = rlist[25]
        drop_off = rlist[26]
        actual_dist = float(rlist[5])
        ab_dist = float(rlist[27])
        rrsl = rlist[28]
        rrst = rlist[31]
        if actual_dist!=0 and ab_dist!=0:
            yield (taxi_id,year,pick_up,drop_off), (actual_dist/ab_dist,float(rrsl),float(rrst))
        
    
    def reducer_first(self,key,tuples):
        tuple_list = list(tuples)
        ratio_list = [tuple_list[i][0] for i in range(len(tuple_list))]
        rrsl_list = [tuple_list[i][1] for i in range(len(tuple_list))]
        rrst_list = [tuple_list[i][2] for i in range(len(tuple_list))]
        ave_ratio = sum(ratio_list)/len(ratio_list)
        ave_rrsl  = sum(rrsl_list)/len(rrsl_list)
        ave_rrst  = sum(rrst_list)/len(rrst_list)
        if key[0] in income_class:
            yield (key[1],key[2],key[3],income_class[key[0]]),(ave_ratio,ave_rrsl,ave_rrst)
                   
    def reducer_further(self,key,tuples):
        tuple_list = list(tuples)
        ratio_list = [tuple_list[i][0] for i in range(len(tuple_list))]
        rrsl_list = [tuple_list[i][1] for i in range(len(tuple_list))]
        rrst_list = [tuple_list[i][2] for i in range(len(tuple_list))]
        ave_ratio = sum(ratio_list)/len(ratio_list)
        ave_rrsl  = sum(rrsl_list)/len(rrsl_list)
        ave_rrst  = sum(rrst_list)/len(rrst_list)
        #key [0], key[1], key[2] are year, pick_up and drop_off respectively
        yield (key[0],key[1],key[2]),(key[3],ave_ratio,ave_rrsl,ave_rrst)
        
    def reducer_final(self,key,ratio_tuple):
        ratio_tuple_list = list(ratio_tuple)
        if len(ratio_tuple_list)==2:
            if ratio_tuple_list[0][0]==1:
                yield (key[0],key[1],key[2]),(ratio_tuple_list[0][1]-ratio_tuple_list[1][1],\
                       ratio_tuple_list[0][2]-ratio_tuple_list[1][2],\
                       ratio_tuple_list[0][3]-ratio_tuple_list[1][3])
            else:
                yield (key[0],key[1],key[2]),(ratio_tuple_list[1][1]-ratio_tuple_list[0][1],\
                       ratio_tuple_list[1][2]-ratio_tuple_list[0][2],\
                       ratio_tuple_list[1][3]-ratio_tuple_list[0][3])
        
    def steps(self):
        return [MRStep(mapper=self.mapper_first,
                       reducer=self.reducer_first),
                MRStep(reducer=self.reducer_further),
                MRStep(reducer=self.reducer_final)
                ]    
                

                
if __name__ == '__main__':
    MRIncomeAnnual.run()
    income_list = np.array(list(income_dict.values()))
    plt.hist(income_list,1000)
    #print(max(income_list))
    plt.xlim([0,1000])
    #plt.savefig('temp.png')

    ddev = np.std(np.log(income_list+0.02))
    dmean = np.mean(np.log(income_list+0.02))
    for dID in income_dict:
        if np.log(income_dict[dID]+0.02)>ddev*2+dmean:
            income_class[dID] = 1
        if dmean-2*ddev<=np.log(income_dict[dID]+0.02)<=dmean+2*ddev:
            income_class[dID] = 0
    #print(sum(income_class.values()))
    MRIncomeDiff.run()
    
         