'''
The University of Chicago 
CAPP30123: Final Project
Python Version: 3.5
Seed: None
Author: @dpzhang

Script Objective: Use MRjob to produce a clean dataset
'''

from mrjob.job import MRJob
import numpy as np
import os
import re
from geopy.distance import vincenty
import datetime
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
    '''
    --------------------------------------------------------------------
    This function is created with the following purposes:
        1. convert time input, such as 'Pickup Timestamp' and 
           'Dropoff Timesteamp' into a datatime object
        2. Use the output file to get the Year, Day, Month, Day of the Week
           of every single trip 
    --------------------------------------------------------------------
    '''
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
    '''
    --------------------------------------------------------------------
    This function is created with the following purposes:
        1. extract trip duration and convert it from a string to an integer
    --------------------------------------------------------------------
    '''
    try:
        return int(a_string)
    except (ValueError, TypeError):
        pass


def get_miles(a_string):
    '''
    --------------------------------------------------------------------
    This function is created with the following purposes:
        1. extract trip length and convert it from a string to a float
    --------------------------------------------------------------------
    '''
    try:
        return float(a_string)
    except (ValueError, TypeError):
        pass
        

# unit: mile
def get_distance(origin, destination):
    '''
    --------------------------------------------------------------------
    This function is created with the following purposes:
        1. Using origin and destination coordinates, compute the absolute
           distance of each trip on the map (a straight line) using 
           Vincenty's formula
    
    Note:
    Vincenty's formula is doubles the calculation time compared to 
    great-circle, but its accuracy gain at the point tested is ~0.17%
    --------------------------------------------------------------------
    '''
    try:
        return vincenty(origin, destination).miles
    except (ValueError, TypeError):
        pass


def get_RRSL(miles, AbsDistance):
    '''
    --------------------------------------------------------------------
    This function is created with the following purposes:
        1. Compute RRSL by dividing miles (relative distance) by Absolute
           distance on the map
    
    Interpretation:
        For the shortest distance possible (a straight line on the map, 
        how many extra mile would a driver take per mile on the map
    --------------------------------------------------------------------
    '''
    try:
        if miles != 0 and AbsDistance != 0:
            RRSL = miles / AbsDistance
            if RRSL < 1:
                RRSL = -1
            return RRSL
    except (ValueError, TypeError):
        pass
            

def get_AbsTime(AbsDistance):
    '''
    --------------------------------------------------------------------
    This function is created with the following purposes:
        1. Compute absolute trip time by dividing miles Absolute distance
           by average velocity
    
    Note:
        Averaged velocity = 23.7
        It is collected by Google and they averaged all speed limit signs
        of Chicago to obtain this statistic
    
    Interpretation:
        What is the shortest time for a driver to complete a trip given
        origin and destination points
    --------------------------------------------------------------------
    '''
    try:
        if AbsDistance != 0:
            AbsTime = AbsDistance / 23.7 * 3600
            return AbsTime
    except (ValueError, TypeError):
        pass


def get_RRST(seconds, AbsTime):
    '''
    --------------------------------------------------------------------
    This function is created with the following purposes:
        1. Compute RRST by dividing actual trip duration (seconds) by 
           absolute time required by a trip given origin and destination    

    Interpretation:
        How many extra seconds would a driver use per second of the shortest
        trip duratiom
    --------------------------------------------------------------------
    '''
    try:
        if seconds != 0 and AbsTime != 0:
            RRST = seconds / AbsTime
            if RRST < 1:
                RRST = -1
            return RRST
    except (ValueError, TypeError):
        pass
           

def get_region(community_id):
    '''
    --------------------------------------------------------------------
    This function is created with the following purposes:
        1. Based on the pickup and dropoff community number, it would 
           return the region information
    --------------------------------------------------------------------
    '''
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

        return CommunityOnRegion[int(community_id)]
    except (ValueError, TypeError):
        pass


def get_timePeriod(timestamp):
    '''
    --------------------------------------------------------------------
    This function is created with the following purposes:
        1. Based on the pickup and dropoff timestamp, it would 
           know the hour of the trip
        2. Use the hour information, it would generate the correct
           time interval of the trip
    --------------------------------------------------------------------
    '''
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
    '''
    --------------------------------------------------------------------
    This function is created with the following purposes:
        1. Based on the pickup and dropoff timestamp, it would 
           know the weekday of the trip
    --------------------------------------------------------------------
    '''
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
    '''
    --------------------------------------------------------------------
    This function is created with the following purposes:
        1. Based on the pickup and dropoff timestamp, it would 
           know the year of the trip
    --------------------------------------------------------------------
    '''
    try:
        year = timestamp.year
        return year
    except (ValueError, TypeError):
        pass


def get_month(timestamp):
    '''
    --------------------------------------------------------------------
    This function is created with the following purposes:
        1. Based on the pickup and dropoff timestamp, it would 
           know the month of the trip
    --------------------------------------------------------------------
    '''
    try:
        month = timestamp.month
        return month
    except (ValueError, TypeError):
        pass


def get_day(timestamp):
    '''
    --------------------------------------------------------------------
    This function is created with the following purposes:
        1. Based on the pickup and dropoff timestamp, it would 
           know the day of the trip
    --------------------------------------------------------------------
    '''
    try:
        day = timestamp.day
        return day
    except (ValueError, TypeError):
        pass


def get_latlon(centroid):
    '''
    --------------------------------------------------------------------
    This function is created with the following purposes:
        1. based on the pickup and dropoff centroid infoamation, it 
           would return the lattitude and longitude of the trip in 
           a tuple format
    --------------------------------------------------------------------
    '''
    try:
        lat, lon = centroid
        return lat, lon
    except (ValueError, TypeError):
        return None, None
        

def get_community(a_string):
    '''
    --------------------------------------------------------------------
    This function is created with the following purposes:
        1. attempt to convert pickup and dropoff community area from a
           string to an integer
    --------------------------------------------------------------------
    '''
    try:
        community = int(a_string)
        return community
    except (ValueError, TypeError):
        pass

class MRCleanAndCreate(MRJob):

    def mapper(self, _, line):
        
        all_cols = np.array(line.split(','))
        try:
            # label all elements in one rolumn
            index, trip_id, taxi_id, \
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
                ##############################################################

                ############# Simple Process of Variables Type ###############
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
                pickup_area = get_community(pickup_area)

                # fill in missing dropoff community information            
                dropoff_centroid = get_centroid(dropoff_centroid)
                dropoff_area = get_community(dropoff_area)
                ##############################################################

                ############ Create Key Trip Analysis Variables ##############
                AbsDistance = get_distance(pickup_centroid, dropoff_centroid)

                RRSL = get_RRSL(miles, AbsDistance)
                
                AvgVelocity = 23.7

                AbsTime = get_AbsTime(AbsDistance)

                RRST = get_RRST(seconds, AbsTime)
                ##############################################################

                ################### Trip Time Variables ######################
                pickup_hr = get_timePeriod(pickup_timeobject) 
                weekday = get_weekday(pickup_timeobject)
                year = get_year(pickup_timeobject)
                month = get_month(pickup_timeobject)
                day = get_day(pickup_timeobject)
                    
                pickup_region = get_region(pickup_area)
                dropoff_region = get_region(pickup_area)

                pickup_lat, pickup_lon = get_latlon(pickup_centroid)
                dropoff_lat, dropoff_lon = get_latlon(dropoff_centroid)
                ##############################################################


                # filter out trips that lacks RRSL and RRST statistics
                if RRSL != -1 and RRST != -1 and \
                   RRSL != None and RRST != None and \
                   RRSL != 0 and RRST != 0:
                # create the info list: these are the returns wanted
                    info = [trip_id, taxi_id, \
                            pickup_time, dropoff_time, \
                            seconds, miles, \
                            pickup_area, dropoff_area, \
                            pickup_region, dropoff_region,\
                            pickup_lat, pickup_lon, \
                            dropoff_lat, dropoff_lon, \
                            fare, tips, tolls, extras, total, \
                            AbsDistance, RRSL, AbsTime, RRST, \
                            pickup_hr, weekday, year, month, day]               

                    yield info, None
        except (ValueError, TypeError, UnboundLocalError):
            pass
            

    def reducer(self, trips, _):
        yield trips, None


if __name__ == '__main__':
    MRCleanAndCreate.run()
