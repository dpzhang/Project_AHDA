'''
The University of Chicago 
CAPP30123: Final Project
Python Version: 3.5
Seed: None

II. Creating 8 helper variables
'''

from util import *


if __name__ == "__main__":
    # load the data
    staxi_path = os.path.join(os.pardir, "Data/SubsetData5000/STaxiTrips.csv")
    output_path = os.path.join(os.pardir, "Data/SubsetData5000/CSTaxiTrips.csv")
    staxi = clean_data(staxi_path) 

    
    ##### 1. pickup region based on community #####
    staxi['pickup_region'] = staxi.pickup_area.apply(get_region)

    ##### 2. dropoff region based on community #####
    staxi['dropoff_region'] = staxi.dropoff_area.apply(get_region)

    ##### 3. absolute distance based on pickup and dropoff coordinates #####
    staxi['AbsDistance'] = get_distance(staxi.pickup_centroid, staxi.dropoff_centroid)

    ##### 4. ratio of real (actual) path length over shortest path length (RRSL)#####

    # explanation: dividing actual distance by absolute distance
    #staxi['RRSL'] = round(staxi['miles'] / staxi['AbsDistance'], 3)
    staxi['RRSL'] = staxi['miles'] / staxi['AbsDistance']
    staxi['RRSL'] = staxi['RRSL'].apply(lambda x: round(x,3))

    ##### 5. Actual Velocity: Actual Distance / Trip Duration #####
    # unit: miles / hr
    # averaging all speed limit sign in the chicago region (2009)
    staxi['AvgVelocity'] = 23.7
    
    ##### 6. Ratio of real path travel time over shortest path travel time (RRST) #####
    #staxi['AbsTime'] = round(staxi['AbsDistance'] / staxi['AvgVelocity'], 3)
    #staxi['RRST'] = round(staxi['seconds'] / staxi['AbsTime'], 3)

    staxi['AbsTime'] = staxi['AbsDistance'] / staxi['AvgVelocity'] * 3600
    staxi['AbsTime']  = staxi['AbsTime'].apply(lambda x: round(x,3))    
    staxi['RRST'] = staxi['seconds'] / staxi['AbsTime']
    staxi['RRST'] = staxi['RRST'].apply(lambda x: round(x,3))
    
    ##### 7. Time Period: 8 levels #####
    staxi['pickup_hr'] = staxi.pickup_time.apply(get_timePeriod)

    ##### 8. Day: Indication of if weekday, weekend, or holiday #####
    staxi['weekday'] = staxi.pickup_time.apply(get_weekday)

    ##### 9. Year: indication of year #####
    staxi['year'] = staxi.pickup_time.apply(get_year)

    ##### 10. Month: indication of month #####
    staxi['month'] = staxi.pickup_time.apply(get_month)

    ##### 11. day: indication of day #####
    staxi['day'] = staxi.pickup_time.apply(get_day)


    # export the processed dataset
    staxi.to_csv(output_path, index = False, float_format='%.3f')
