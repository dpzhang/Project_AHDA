'''
The University of Chicago 
CAPP30123: Final Project
Python Version: 3.5
Seed: None

II. Creating 8 helper variables
'''

import pandas as pd
import numpy as np
import os
import re
from clean import staxi, output_path
from geopy.distance import vincenty
from geopy.distance import great_circle


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

##### 1. pickup region based on community #####
def get_region(community_id):
    if pd.isnull(community_id):
        return 'None'
    else: 
        return CommunityOnRegion[int(community_id)]
staxi['pickup_region'] = staxi.pickup_area.apply(get_region)

##### 2. dropoff region based on community #####
staxi['dropoff_region'] = staxi.dropoff_area.apply(get_region)

##### 3. absolute distance based on pickup and dropoff coordinates #####
# unit: mile
def get_distance(origin_col, destination_col, vincenty_distance = True):
    '''
    Vincenty's formula is doubles the calculation time compared to 
    great-circle, but its accuracy gain at the point tested is ~0.17%
    '''
    absolute_distance = []
    for origin, destination in zip(origin_col, destination_col):
        if vincenty_distance:
            distance = vincenty(origin, destination).miles
        else:
            distance = great_circle(origin, destination).miles
        absolute_distance.append(distance)
       
    return absolute_distance
staxi['AbsDistance'] = get_distance(staxi.pickup_centroid, staxi.dropoff_centroid)

##### 4. ratio of real (actual) path length over shortest path length (RRSL)#####
# explanation: dividing actual distance by absolute distance
staxi['RRSL'] = staxi['miles'] / staxi['AbsDistance']

##### 5. Absolute Velocity: Absolute Distance / Trip Duration #####
# unit: miles / hr
staxi['AbsVelocity'] = staxi['AbsDistance'] / (staxi['seconds']/3600)

##### 6. Actual Velocity: Actual Distance / Trip Duration #####
# unit: miles / hr
staxi['AclVelocity'] = staxi['miles'] / (staxi['seconds']/3600)

##### 7. Ratio of real velocity over relative velocity (RRVV) #####
# dividing relative velocity by absolute velocity
staxi['RRVV'] = staxi['AclVelocity'] / staxi['AbsVelocity']

##### 8. Ratio of real path travel time over shortest path travel time (RRST) #####
# relative (actual) time: in the dataset
# absolute time: absolute_distance / average_velocity
# interpretation: condition on averaged velocity of the whole trip, how many 
# more seconds did the driver waste for every second the trip have to take
staxi['AbsTime'] = staxi['AbsDistance'] / staxi['AbsVelocity'] * 3600
staxi['RRST'] = staxi['AbsTime'] / staxi['seconds']

##### 8. Time Period: 8 levels #####
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
staxi['pickup_hr'] = staxi.pickup_time.apply(get_timePeriod)

##### 9. Day: Indication of if weekday, weekend, or holiday #####
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
staxi['weekday'] = staxi.pickup_time.apply(get_weekday)

# export the processed dataset
staxi.to_csv(output_path, index = False)
