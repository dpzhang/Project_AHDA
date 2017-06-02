import pandas as pd
import numpy as np

# read the dataset
raw_df = pd.read_csv('rrsl_analysis.csv')

# remove 'None's
decision_bool = np.array(raw_df['origin'] != 'None').astype(int) + \
                np.array(raw_df['destination'] != 'None').astype(int) == 2
# get the clean df
rrsl_df = raw_df[decision_bool]

# central to central
case1 = rrsl_df[ (rrsl_df['origin'] == 'Central') & (rrsl_df['destination'] == 'Central')]
# we can see for all trips pickup and dropoff in the central regions
# high income drivers have lower RRSL than that of low income drivers
# interpretation: high income drivers tend to find closer route in the city
case1_mean = case1.HL.mean()
# on average, for high inome drivers, they drive 0.45 miles less for every absolute mile

# central to Far Southeast Side
case2 = rrsl_df[ (rrsl_df['origin'] == 'Central') & (rrsl_df['destination'] == 'FarNorthSide')]

# central to Southwest Side
case3 = rrsl_df[ (rrsl_df['origin'] == 'Central') & (rrsl_df['destination'] == 'SouthwestSide')]

# Central to WestSide
case6 = rrsl_df[ (rrsl_df['origin'] == 'Central') & (rrsl_df['destination'] == 'WestSide')]

# Central to NorthSide
case7 = rrsl_df[ (rrsl_df['origin'] == 'Central') & (rrsl_df['destination'] == 'NorthSide')]

# Central to SouthSide
case8 = rrsl_df[ (rrsl_df['origin'] == 'Central') & (rrsl_df['destination'] == 'SouthSide')]






# FarNorthSide to Central
case4 = rrsl_df[ (rrsl_df['origin'] == 'FarNorthSide') & (rrsl_df['destination'] == 'Central')]

# NorthSide to Central
case5 = rrsl_df[ (rrsl_df['origin'] == 'NorthSide') & (rrsl_df['destination'] == 'Central')]


