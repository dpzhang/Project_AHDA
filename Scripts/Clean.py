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
    if pd.isnull(a_string):
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
    if pd.isnull(a_string):
        return 0, 0
    else:
        centroid = re.findall('(\-*\d+\.\d+)', a_string)
        if not centroid: # if list is emppty
            return (0, 0)
        else:
            longitude, latitude = centroid
            return float(latitude), float(longitude)


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


# load the data
#staxi_path = os.path.join(os.pardir, "Data/SubsetData5000/STaxiTrips.csv")
#output_path = os.path.join(os.pardir, "Data/SubsetData5000/CSTaxiTrips.csv")
#staxi = clean_data(staxi_path) 
