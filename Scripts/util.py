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
    try:
        if a_string[0] == '$':
            list_of_num =re.findall('\d+', a_string)
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
                RRSL = 1
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
                RRST = 1
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
    shp_file_vm = "/mnt/storage/Project_AHDA/Data/community_boundaries/geo_export_96616a78-3bcd-4822-be67-fc56301ad13b.shp"
    shp_file_local = "/Users/dongpingzhang/Google Drive/spring2017/macs30200/MACS30200proj/FinalPaper/Data/boundary_files/neighborhood_boundaries/geo_export_96616a78-3bcd-4822-be67-fc56301ad13b.shp"
    try:
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
    except (ValueError, TypeError):
        pass
