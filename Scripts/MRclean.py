'''
The University of Chicago 
CAPP30123: Final Project
Python Version: 3.5
Seed: None
Author: @dpzhang

I. MRjob to produce a clean script
'''

from mrjob.job import MRJob
# from util import *

import pandas as pd
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


def get_fare(a_string):
    '''
    --------------------------------------------------------------------
    This function is created with the following purposes:
        1. extract the fare values from a string of fares
        2. get the rid of the dollor sign
        3. convert to float
    
    Example: "$23.34" to float(23.34)
    --------------------------------------------------------------------
    ''' 
    try:
        if a_string[0] == '$':
            list_of_num =re.findall('\d+', a_string)
            if len(list_of_num) != 0:
                numerized_fare = float('.'.join(list_of_num))
                return numerized_fare
    except ValueError:
        pass


def get_centroid(a_string):
    '''
    --------------------------------------------------------------------
    This function is created with the following purposes:
        1. extract coordinate information: latitude and longitude
        2. convert it to a tuple
    
    Example: "POINT (21.231431 29.3242463)" to (21.231431, 29.3242463) 
    --------------------------------------------------------------------
    '''
    try: 
        centroid = re.findall('(\-*\d+\.\d+)', a_string)
        if not centroid: # if list is emppty
            pass
        else:
            longitude, latitude = centroid
            return float(latitude), float(longitude)
    except ValueError:
        pass   

def get_time(a_string):
    if a_string != '':
        try:
            string_parser = "%m/%d/%y  %H:%M"
            datetime_object = datetime.datetime.strptime(a_string, string_parser)
            datetime_format = datetime.datetime.strftime(datetime_object, string_parser)
            return datetime_object, datetime_format
        except ValueError:
            try:
                string_parser = "%m/%d/%Y %I:%M:%S %p"
                datetime_object = datetime.datetime.strptime(a_string, string_parser)
                datetime_format = datetime.datetime.strftime(datetime_object, string_parser)
                return datetime_object, datetime_format
            except ValueError:
                pass

def get_seconds(a_string):
    try:
        return int(a_string)
    except (ValueError, TypeError):
        pass

def get_miles(a_string):
    try:
        return float(a_string)
    except (ValueError, TypeError):
        pass
        
def get_RRSL(miles, AbsDistance):
    try:
        if miles != 0 and AbsDistance != 0:
            RRSL = miles / AbsDistance
            if RRSL < 1:
                RRSL = -1
            return RRSL
    except (ValueError, TypeError):
        pass
            

def get_AbsTime(AbsDistance):
    try:
        if AbsDistance != 0:
            AbsTime = AbsDistance / 23.7 * 3600
            return AbsTime
    except (ValueError, TypeError):
        pass

def get_RRST(seconds, AbsTime):
    try:
        if seconds != 0 and AbsTime != 0:
            RRST = seconds / AbsTime
            if RRST < 1:
                RRST = -1
            return RRST
    except (ValueError, TypeError):
        pass
           

def get_region(community_id):
    try:
        # create a dictionary
        community_on_region = {'Far North Side': [1, 77, 3, 2, 4, 13, 14, 12, 11, 10, 9, 76],
                               'Northwest Side': [16, 20, 15, 19, 17, 18],
                               'North Side': [6, 7, 5, 21, 22],
                               'West Side': [24, 28, 31, 23, 26, 27, 29, 30, 25],
                               'Central': [8, 32, 33],\
                               'South Side': [35, 34, 60, 36, 37, 38, 39, 40, 41, 42, 43, 69],\
                               'Southwest Side': [56, 57, 58, 59, 62, 63, 61, 64, 65, 66, 67, 68],\
                               'Far Southwest Side': [70, 71, 72, 73, 74, 75],\
                               'Far Southeast Side': [44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55] 
                              }
        # get a clean dictionary that is suitable for mapping the dataframe
        CommunityOnRegion = dict()
        for region_name, communities in community_on_region.items():
            for community in communities:
                CommunityOnRegion[community] = region_name

        if pd.isnull(community_id):
            return 'Outside'
        else: 
            return CommunityOnRegion[int(community_id)]
    except (ValueError, TypeError):
        pass


# unit: mile
def get_distance(origin, destination):
    '''
    Vincenty's formula is doubles the calculation time compared to 
    great-circle, but its accuracy gain at the point tested is ~0.17%
    '''
    try:
        return vincenty(origin, destination).miles
    except (ValueError, TypeError):
        pass


def get_timePeriod(timestamp):
    try:
        hour = timestamp.hour
        if hour in [1, 2, 3]:
            return int(0)
        if hour in [4, 5, 6]:
            return int(1)
        if hour in [7, 8, 9]:
            return int(2)
        if hour in [10, 11, 12]:
            return int(3)
        if hour in [13, 14, 15]:
            return int(4)
        if hour in [16, 17, 18]:
            return int(5)
        if hour in [19, 20, 21]:
            return int(6)
        if hour in [22, 23, 0]:
            return int(7)
    except (ValueError, TypeError):
        pass


def get_weekday(timestamp):
    try:
        weekday = timestamp.weekday()
        if weekday == 0:
            return 1
        if weekday == 1:
            return 2
        if weekday == 2:
            return 3
        if weekday == 3:
            return 4
        if weekday == 4:
            return 5
        if weekday == 5:
            return 6
        if weekday == 6:
            return 7
    except (ValueError, TypeError):
        pass


def get_year(timestamp):
    try:
        year = timestamp.year
        return year
    except (ValueError, TypeError):
        pass


def get_month(timestamp):
    try:
        month = timestamp.month
        return month
    except (ValueError, TypeError):
        pass


def get_day(timestamp):
    try:
        day = timestamp.day
        return day
    except (ValueError, TypeError):
        pass

def get_area(a_string):
    try:
        return int(a_string)
    except (ValueError, TypeError):
        pass

def get_latlon(centroid):
    try:
        lat, lon = centroid
        return lat, lon
    except (ValueError, TypeError):
        return None, None
        

def get_community(coordinate):
    pro_dir = os.path.dirname(os.path.dirname(__file__))
    shp_path = "Data/community_boundaries/geo_export_13b0db68-db36-4972-ab88-61251d23f387.shp"
    shp_file = os.path.join(pro_dir, shp_path)
    try:
        with fiona.open(shp_file) as fiona_collection:
            for i in fiona_collection:
                shape = i['geometry']['coordinates'][0]
                if len(shape) == 1:
                    shape = shape[0]
                polygon = Polygon(shape)

                com = int(i['properties']['area_numbe'])
                lon, lat = coordinate
                point = Point(lat, lon)
                if polygon.contains(point):
                    return com
    except (ValueError, TypeError):
        pass



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
            #elif len(all_cols) == 23:
            #    trip_id, taxi_id, \
            #    pickup_time, dropoff_time, \
            #    seconds, miles, \
            #    pickup_census, dropoff_census, \
            #    pickup_area, dropoff_area, \
            #    fare, tips, tolls, extras, total,\
            #    payment,company, \
            #    pickup_latitude, pickup_longitude, pickup_centroid, \
            #    dropoff_latitude, dropoff_longitude, dropoff_centroid = all_cols
            #else:
            #    pass

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
                #print('seconds', seconds)
                #print()

                # convert miles (miles column has no NAs)
                miles = get_miles(miles)
                #print('miles', miles)
                #print()

                # process all money columns: fare, tips, tolls, extras, total
                fare = get_fare(fare)
                #print('fare', fare)
                #print()
                tips = get_fare(tips)
                #print('tips', tips)
                #print()
                tolls = get_fare(tolls)
                #print('tolls', tolls)
                #print()
                extras = get_fare(extras)
                #print('extras', extras)
                #print()
                total = get_fare(total)
                #print('total', total)
                #print()

                # fill in missing pick up community information
                pickup_centroid = get_centroid(pickup_centroid)
                pickup_manual = get_community(pickup_centroid)
                if pickup_manual != pickup_area:
                    pickup_area = get_area(pickup_area)
                #print('pickup raw', pickup_area)
                #print('pickup manual', pickup_manual)
                #print('final pickup area', pickup_area)
                #print()

                # fill in missing dropoff community information            
                dropoff_centroid = get_centroid(dropoff_centroid)
                dropoff_manual = get_community(dropoff_centroid)
                if dropoff_manual != dropoff_area:
                    dropoff_area = get_area(dropoff_area)
                #print('dropoff raw', dropoff_area)
                #print('dropoff manual', dropoff_manual)
                #print('final dropoff area', dropoff_area)
                #print()

            ######################################################################
                AbsDistance = get_distance(pickup_centroid, dropoff_centroid)
                #print(AbsDistance)
                #print('AbsDistance', AbsDistance)
                #print()

                RRSL = get_RRSL(miles, AbsDistance)
                #print('RRSL', RRSL)
                #print()
                
                AvgVelocity = 23.7
                AbsTime = get_AbsTime(AbsDistance)

                RRST = get_RRST(seconds, AbsTime)
                #print('RRST', RRST)
                #print()

                #print(pickup_time)
                #print(type(pickup_time))
                pickup_hr = get_timePeriod(pickup_timeobject)
                
                weekday = get_weekday(pickup_timeobject)
                #print('weekday')
                #print(weekday)

                year = get_year(pickup_timeobject)
                #print('year')
                #print(year)

                month = get_month(pickup_timeobject)
                #print('month')
                #print(month)

                day = get_day(pickup_timeobject)
                #print('day')
                #print(day)
                    
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
