#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun  3 17:16:51 2017

@author: huanyeliu
"""

import numpy as np
from mrjob.job import MRJob
from mrjob.step import MRStep

income_dict={}
income_class={}

class MRIncomeDiff(MRJob):

    def mapper(self,_,line):
        rlist = line.split(',')
        if len(rlist)==2:
            income_dict[rlist[0]] = float(rlist[1])
        else:
            taxi_id = rlist[1]
            year = rlist[2].split('-')[0]
    
            actual_dist = float(rlist[5])
            ab_dist = float(rlist[27])
            rrsl = rlist[28]
            rrst = rlist[31]
            wkday = rlist[33]
        
        #print(rrsl)
        #print(type(rrsl))
        #print(rrst)
            if actual_dist!=0.0 and ab_dist!=0.0 and rrst!='':
                if int(wkday)<6:
                    yield (taxi_id,year,'weekday'), (actual_dist/ab_dist,float(rrsl),float(rrst))
                else:
                    yield (taxi_id,year,'weekend'), (actual_dist/ab_dist,float(rrsl),float(rrst))
        
    
    def reducer_init(self):
        
        income_list = np.array(list(income_dict.values()))
        ddev = np.std(np.log(income_list+0.02))
        dmean = np.mean(np.log(income_list+0.02))
        for dID in income_dict:
            if np.log(income_dict[dID]+0.02)>ddev*2+dmean:
                income_class[dID] = 1
            if dmean-2*ddev<=np.log(income_dict[dID]+0.02)<=dmean+2*ddev:
                income_class[dID] = 0

    def reducer(self,key,tuples):
        tuple_list = list(tuples)
        ratio_list = [tuple_list[i][0] for i in range(len(tuple_list))]
        rrsl_list = [tuple_list[i][1] for i in range(len(tuple_list))]
        rrst_list = [tuple_list[i][2] for i in range(len(tuple_list))]
        ave_ratio = sum(ratio_list)/len(ratio_list)
        ave_rrsl  = sum(rrsl_list)/len(rrsl_list)
        ave_rrst  = sum(rrst_list)/len(rrst_list)
        if key[0] in income_class:
            yield (key[1],key[2],income_class[key[0]]),(ave_ratio,ave_rrsl,ave_rrst)
                   
    def reducer_further(self,key,tuples):
        tuple_list = list(tuples)
        ratio_list = [tuple_list[i][0] for i in range(len(tuple_list))]
        rrsl_list = [tuple_list[i][1] for i in range(len(tuple_list))]
        rrst_list = [tuple_list[i][2] for i in range(len(tuple_list))]
        ave_ratio = sum(ratio_list)/len(ratio_list)
        ave_rrsl  = sum(rrsl_list)/len(rrsl_list)
        ave_rrst  = sum(rrst_list)/len(rrst_list)
        #key [0], key[1], key[2]  are year,wkday/wkend, income_level respectively
        yield (key[0],key[1]),(key[2],ave_ratio,ave_rrsl,ave_rrst)
        
    def reducer_final(self,key,ratio_tuple):
        ratio_tuple_list = list(ratio_tuple)
        if len(ratio_tuple_list)==2:
            if ratio_tuple_list[0][0]==1:
                yield (key[0],key[1]),(ratio_tuple_list[0][1]-ratio_tuple_list[1][1],\
                       ratio_tuple_list[0][2]-ratio_tuple_list[1][2],\
                       ratio_tuple_list[0][3]-ratio_tuple_list[1][3])
            else:
                yield (key[0],key[1]),(ratio_tuple_list[1][1]-ratio_tuple_list[0][1],\
                       ratio_tuple_list[1][2]-ratio_tuple_list[0][2],\
                       ratio_tuple_list[1][3]-ratio_tuple_list[0][3])
        
    def steps(self):
        return [MRStep(
                       mapper = self.mapper,
                       reducer_init = self.reducer_init,
                       reducer=self.reducer),
                MRStep(reducer=self.reducer_further),
                MRStep(reducer=self.reducer_final)
                ]
                
if __name__ == '__main__':
    MRIncomeDiff.run()