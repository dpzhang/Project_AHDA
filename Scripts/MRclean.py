'''
The University of Chicago 
CAPP30123: Final Project
Python Version: 3.5
Seed: None
Author: @dpzhang

I. MRjob to produce a clean script
'''

from mrjob.job import MRJob
import numpy as np
import os
import re
from geopy.distance import vincenty
import datetime
import fiona
import shapely
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
import string
from util import *


class MRCleanAndCreate(MRJob):

    def mapper(self, _, line):
        
        all_cols = np.array(line.split(','))
        try:
            # label all elements in one rolumn
            #if len(all_cols) == 24:
            trip_id, taxi_id, \
            pickup_time, dropoff_time, \
            seconds, miles, \
            pickup_census, dropoff_census, \
            pickup_area, dropoff_area, \
            fare, tips, tolls, extras, total,\
            payment,company, \
            pickup_latitude, pickup_longitude, pickup_centroid, \
            dropoff_latitude, dropoff_longitude, dropoff_centroid = all_cols

            # choose to ignore the whole observation if:
                # 1. taxi_id == ''
                # 2. miles == '0' or miles == ''
                # 3. no spatial coordinates
                # 4. seconds == '' or seconds =='0'
            if taxi_id != '' and \
               miles != '0' or miles != '' or miles != '0 ' and \
               pickup_centroid != '' or dropoff_centroid != '' and \
               seconds != '' or seconds !='0':
               
                ################## Process Time Variables ####################
                # process pickup_time, and dropoff_time
                pickup_timeobject = get_time(pickup_time)[0]
                pickup_time = get_time(pickup_time)[1]
                # dropoff_time has NA
                if dropoff_time != '':
                    dropoff_timeobject = get_time(dropoff_time)[0]
                    dropoff_time = get_time(dropoff_time)[1]
                #print('pickup time', pickup_time)                
                #print()
                #print('dropoff time', dropoff_time)
                #print()
                #############################################################

                seconds = get_seconds(seconds)

                # convert miles (miles column has no NAs)
                miles = get_miles(miles)

                # process all money columns: fare, tips, tolls, extras, total
                fare = get_fare(fare)
                tips = get_fare(tips)
                tolls = get_fare(tolls)
                extras = get_fare(extras)
                total = get_fare(total)

                # fill in missing pick up community information
                pickup_centroid = get_centroid(pickup_centroid)
                pickup_manual = get_community(pickup_centroid)
                if pickup_manual != pickup_area:
                    pickup_area = get_area(pickup_area)

                # fill in missing dropoff community information            
                dropoff_centroid = get_centroid(dropoff_centroid)
                dropoff_manual = get_community(dropoff_centroid)
                if dropoff_manual != dropoff_area:
                    dropoff_area = get_area(dropoff_area)

            ######################################################################
                AbsDistance = get_distance(pickup_centroid, dropoff_centroid)

                RRSL = get_RRSL(miles, AbsDistance)
                
                AvgVelocity = 23.7
                AbsTime = get_AbsTime(AbsDistance)

                RRST = get_RRST(seconds, AbsTime)

                pickup_hr = get_timePeriod(pickup_timeobject)
                
                weekday = get_weekday(pickup_timeobject)

                year = get_year(pickup_timeobject)

                month = get_month(pickup_timeobject)

                day = get_day(pickup_timeobject)
                    
                pickup_region = get_region(pickup_area)
                dropoff_region = get_region(pickup_area)

                pickup_lat, pickup_lon = get_latlon(pickup_centroid)
                dropoff_lat, dropoff_lon = get_latlon(dropoff_centroid)

                if RRSL != -1 and RRST != -1 and RRSL != None and RRST != None and\
                   RRSL != 0 and RRST != 0:
                    info = [taxi_id, \
                            pickup_time, dropoff_time, \
                            seconds, miles, \
                            pickup_area, dropoff_area, pickup_region, dropoff_region,\
                            pickup_lat, pickup_lon, dropoff_lat, dropoff_lon, \
                            fare, tips, tolls, extras, total, \
                            AbsDistance, RRSL, AbsTime, RRST, \
                            pickup_hr, weekday, year, month, day]               
                    info.insert(0, trip_id)
                    yield info, None
        except (ValueError, TypeError, UnboundLocalError):
            pass
            

    def reducer(self, trips, _):
        yield trips, None


if __name__ == '__main__':
    MRCleanAndCreate.run()
