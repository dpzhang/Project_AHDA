from mrjob.job import MRJob
from util import *

class MRCleanAndCreate(MRJob):

    def mapper(self, _, line):
        all_cols = np.array(line.split(','))
        info_cols = np.array(range(24))
        #print(all_cols[info_cols])
        
        # label all elements in one rolumn
        index, trip_id, taxi_id, \
        pickup_time, dropoff_time, \
        seconds, miles, \
        pickup_census, dropoff_census, \
        pickup_area, dropoff_area, \
        fare, tips, tolls, extras, total,\
        payment,company, \
        pickup_latitude, pickup_longitude, pickup_centroid, \
        dropoff_latitude, dropoff_longitude, dropoff_centroid = all_cols[info_cols]
        #print(pickup_centroid)
        #print(dropoff_centroid)

        # choose to ignore the whole observation if:
            # 1. no spatial coordinates
            # 2. no miles
        if taxi_id != '' and \
           miles != 0 and \
           pickup_centroid != '' or dropoff_centroid != '':
           
            try: 
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

                # seconds has NA
                if seconds == '':
                    seconds == 0
                else:
                    seconds = int(seconds)
                #print('seconds', seconds)
                #print()

                # convert miles (miles column has no NAs)
                miles = float(miles)
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
                    pickup_area = pickup_manual
                #print('pickup raw', pickup_area)
                #print('pickup manual', pickup_manual)
                #print('final pickup area', pickup_area)
                #print()

                # fill in missing dropoff community information            
                dropoff_centroid = get_centroid(dropoff_centroid)
                dropoff_manual = get_community(dropoff_centroid)
                if dropoff_manual != dropoff_area:
                    dropoff_area = dropoff_manual
                #print('dropoff raw', dropoff_area)
                #print('dropoff manual', dropoff_manual)
                #print('final dropoff area', dropoff_area)
                #print()

        ####    ##################################################################
        
                AbsDistance = get_distance(pickup_centroid, dropoff_centroid)
                #print('AbsDistance', AbsDistance)
                #print()

                if AbsDistance != 0:
                    RRSL = miles / AbsDistance
                else:
                    RRSL = 1
                #print('RRSL', RRSL)
                #print()
                
                AvgVelocity = 23.7
                AbsTime = AbsDistance / AvgVelocity

                if AbsTime != 0:
                    RRST = seconds / AbsTime
                else:
                    RRST = 1
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
                
                yield trip_id, (taxi_id, pickup_time, dropoff_time, seconds, miles, \
                                pickup_area, dropoff_area, fare, tips, tolls, extras, \
                                total, pickup_centroid, dropoff_centroid, AbsDistance,\
                                RRSL, AbsTime, RRST, pickup_hr, weekday, year, month,\
                                day)             
            except (TypeError, ValueError):
                print('Rows just remove')


    def reducer(self, trip_id, info):
        yield trip_id, info

if __name__ == '__main__':
    MRCleanAndCreate.run()



