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
from clean import staxi
import fiona

##### 1. pickup region based on community #####

##### 2. dropoff region based on community #####

##### 3. absolute distance based on pickup and dropoff coordinates #####
def get_distance(pickup_coord, dropoff_coord):

##### 4. ratio of real path length over shortest path length (RRSL)#####
# dividing relative/actual distance by absolute distance

##### 5. Absolute Velocity: Absolute Distance / Trip Duration #####

##### 6. Relative Velocity: Relative Distance / Trip Duration #####

##### 7. Ratio of real velocity over relative velocity (RRVV) #####
# dividing absolute velocity by relative velocity

##### 8. Ratio of real path travel time over shortest path travel time (RRST) #####
# relative/actual time: in the dataset
# absolute time: actual_distance / average_velocity
# interpretation: condition on averaged velocity of the whole trip, how many 
# more seconds did the driver waste for every second the trip have to take

##### 8. Time Period: 8 levels #####

##### 9. Day: Indication of if weekday, weekend, or holiday #####

if __name__ == "__main__":
