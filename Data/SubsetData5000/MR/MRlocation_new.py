#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from mrjob.job import MRJob
from mrjob.step import MRStep



class MRRegionDiff(MRJob):
    
    def mapper_init(self):
        
        self.income_class={}
        infile = open('income.csv','r')
        # extract the driver income class dictionary from a input file
        # store it in income_class for later use 
        for line in infile:
            rlist = line.split(',')
            for i in range(1,len(rlist)):
                pair = rlist[i]
                plist = pair.split(':')
                self.income_class[plist[0].strip()]=plist[1].strip()
        infile.close()

    def mapper(self,_,line):
        rlist = line.split(',')
        taxi_id = rlist[1]
        year = '20'+rlist[2].split('/')[2][0:2]
        pick_up = rlist[8]
        drop_off = rlist[9]
        actual_dist = float(rlist[5])
        ab_dist = float(rlist[19])
        rrsl = rlist[20]
        rrst = rlist[22]
       
        if actual_dist!=0.0 and ab_dist!=0.0 and rrst!='' and taxi_id in self.income_class:
            yield (year, pick_up, drop_off, self.income_class[taxi_id]), (actual_dist/ab_dist,float(rrsl),float(rrst))
        
    
    
        

    
    
                   
    def reducer(self,key,tuples):
        tuple_list = list(tuples)
        ratio_list = [tuple_list[i][0] for i in range(len(tuple_list))]
        rrsl_list = [tuple_list[i][1] for i in range(len(tuple_list))]
        rrst_list = [tuple_list[i][2] for i in range(len(tuple_list))]
        # calculate the average rrsl, rrst respectively
        ave_ratio = sum(ratio_list)/len(ratio_list)
        ave_rrsl  = sum(rrsl_list)/len(rrsl_list)
        ave_rrst  = sum(rrst_list)/len(rrst_list)
        #key [0], key[1], key[2] are year, pick_up and drop_off respectively
        yield (key[0],key[1],key[2]),(key[3],ave_ratio,ave_rrsl,ave_rrst)
        
    def reducer_final(self,key,ratio_tuple):
        ratio_tuple_list = list(ratio_tuple)
        if len(ratio_tuple_list)==2:
            # if the first tuple is the rich driver, take the differences of all measures
            # by subtracting the second value from the first value
            if ratio_tuple_list[0][0]=='1':
                yield (key[0],key[1],key[2]),(ratio_tuple_list[0][1]-ratio_tuple_list[1][1],\
                       ratio_tuple_list[0][2]-ratio_tuple_list[1][2],\
                       ratio_tuple_list[0][3]-ratio_tuple_list[1][3])
            else:
                yield (key[0],key[1],key[2]),(ratio_tuple_list[1][1]-ratio_tuple_list[0][1],\
                       ratio_tuple_list[1][2]-ratio_tuple_list[0][2],\
                       ratio_tuple_list[1][3]-ratio_tuple_list[0][3])
        
    def steps(self):
        return [MRStep(mapper_init =self.mapper_init,
                       mapper = self.mapper,
                       reducer=self.reducer),
       
                MRStep(reducer=self.reducer_final)
                ] 
                
if __name__ == '__main__':
    MRRegionDiff.run()

