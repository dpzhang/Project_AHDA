'''
The University of Chicago 
CAPP30123: Final Project
Python Version: 3.5
Seed: None

I. Cleaning Data: Loading and cleaning raw taxi trip data
'''

import pandas as pd
import numpy as np
import os
import re
from geopy.distance import vincenty
from geopy.distance import great_circle
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
    if a_string == '':
        return 0
    else:
        list_of_num =re.findall('\d+', a_string)
        if not list_of_num: # if list is empty
            return 0
        else: 
            numerized_fare = float('.'.join(list_of_num))
            return numerized_fare


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
        if a_string == '':
            return 0, 0
        else:
            centroid = re.findall('(\-*\d+\.\d+)', a_string)
            if not centroid: # if list is emppty
                return 0, 0
            else:
                longitude, latitude = centroid
                return float(latitude), float(longitude)
    except ValueError:
        print('WTH coordinate')
        print(a_string)


def clean_census(a_float):
    '''
    --------------------------------------------------------------------
    This function is created with the following purposes:
        1. scan through every pickup/dropoff census
        2. if null return 0
        3. else return a integer
    
    Example: "POINT (21.231431 29.3242463)" to (21.231431, 29.3242463) 
    --------------------------------------------------------------------
    ''' 

    if pd.isnull(a_float):
        return 0
    else:
        return int(a_float) 
    

def get_time(a_string):
    try:
        string_parser = "%m/%d/%y  %H:%M"
        datetime_object = datetime.datetime.strptime(a_string, string_parser)
        datetime_format = datetime.datetime.strftime(datetime_object, string_parser)
        return datetime_object, datetime_format
    #except ValueError:
    #    string_parser = "%m/%d/%y %H%M"
    #    datetime_object = datetime.datetime.strptime(a_string, string_parser)
    #    datetime_format = datetime.datetime.strftime(datetime_object, string_parser)
    #    return datetime_format
    except ValueError:
        print('Wrong format')

      


def clean_data(input_file_name, output = False, \
               output_file_name = 'clean_taxi.csv'):
    '''
    --------------------------------------------------------------------
    This function is used to clean raw Taxi_Trip.csv file downloaded
    from Chicago Open Data Portal. It would
    1. rename all column names
    2. convert trip_id to unique trip id starting from 0
    3. convert 5 money columns from string to float and getting 
       rid of the dollar sign
    4. convert 2 pickup/drop census columns from float to int
    5. convert 2 coordinates from string to tuple and getting
    6. either return cleaned df or write out a cleaned csv file
    --------------------------------------------------------------------
    ''' 
    # new column names     
    col_names = ['trip_id', 'taxi_id', 'pickup_time', 'dropoff_time', \
                 'seconds' , 'miles', 'pickup_census' , 'dropoff_census', \
                 'pickup_area' , 'dropoff_area', 'fare' , 'tips', 'tolls', \
                 'extras', 'total', 'payment', 'company', \
                 'pickup_latitude', 'pickup_longitude',  'pickup_centroid', \
                 'dropoff_latitude', 'dropoff_longitude', 'dropoff_centroid']
    taxi_data= pd.read_csv(input_file_name, header = 0, names = col_names)

    ##### convert 'trip_id' to unique trip id starting from 0 #####
    taxi_data.trip_id = taxi_data.index.values

    ##### simplify 'taxi_id' starting from 0 #####
    #taxi_data.taxi_id = taxi_data.taxi_id.astype('category')
    #taxi_id_relevel = range(1, len(taxi_data.taxi_id.cat.categories) + 1)
    #taxi_data.taxi_id.cat.categories = taxi_id_relevel
    #taxi_data.taxi_id = taxi_data.taxi_id.apply(int)

    ##### process timestamp columns #####
    timedate_cols = ['pickup_time', 'dropoff_time']
    for timedate_col in timedate_cols:
        taxi_data[timedate_col] = \
                                 pd.to_datetime(taxi_data[timedate_col], \
                                                format="%m/%d/%Y %I:%M:%S %p",\
                                                errors = 'ignore')

    ##### process money columns #####
    money_cols = ['fare', 'tips', 'tolls', 'extras', 'total']
    for money_col in money_cols:
        taxi_data[money_col] = taxi_data[money_col].apply(get_fare)

    ##### process centroid columns #####
    centroid_cols = ['pickup_centroid', 'dropoff_centroid']
    for centroid_col in centroid_cols:
        taxi_data[centroid_col] = taxi_data[centroid_col].apply(get_centroid)

    ##### process pickup census and dropoff census #####
    census_cols = ['pickup_census', 'dropoff_census']
    for census_col in census_cols:
        taxi_data[census_col] = taxi_data[census_col].apply(clean_census)
    
    ##### return df ####
    if output == 0:
        return taxi_data
    else:
        #write out a cleaned csv file
        taxi_data.to_csv(output_file_name, index = False)


def get_region(community_id):
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


# unit: mile
def get_distance(origin, destination, vincenty_distance = True):
    '''
    Vincenty's formula is doubles the calculation time compared to 
    great-circle, but its accuracy gain at the point tested is ~0.17%
    '''
    if vincenty_distance:
        distance = vincenty(origin, destination).miles
    else:
        distance = great_circle(origin, destination).miles
    return distance


def get_timePeriod(timestamp):
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


def get_weekday(timestamp):
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


def get_year(timestamp):
    return timestamp.year


def get_month(timestamp):
    return timestamp.month


def get_day(timestamp):
    return timestamp.day


def get_community(coordinate):
    shp_file_vm = "/mnt/storage/Project_AHDA/Data/community_boundaries/geo_export_96616a78-3bcd-4822-be67-fc56301ad13b.shp"
    shp_file_local = "/Users/dongpingzhang/Google Drive/spring2017/macs30200/MACS30200proj/FinalPaper/Data/boundary_files/neighborhood_boundaries/geo_export_96616a78-3bcd-4822-be67-fc56301ad13b.shp"
    with fiona.open(shp_file_local) as fiona_collection:
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
    return 0
