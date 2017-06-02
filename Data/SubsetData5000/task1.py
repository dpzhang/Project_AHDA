import pandas as pd
import numpy as np
from mrjob.job import MRJob

class MRCount(MRJob):

  def mapper(self, _, line):

      #for datarow in csvreader:
      #datarow = line.split(',')
        #print(datarow)
      #trip_id = datarow[0]
        #print((trip_id))
      #total = datarow[14]
      #yield ((trip_id), (total))
      #total = np.float(total)
        #print((total))
        # except TypeError:
        #   print("skipping line with value", datarow[14])
        # try:
        #   print((total))
        #   total = np.float(total)
        # except TypeError:
        #   print(datarow[14])
        #   print("skipping line with value", datarow[14])
        # else:
        #   yield ((trip_id), (total))


  def combiner(self, trip_id, total):
    totalfare = total
    yield trip_id, totalfare


  def reducer(self, name, totalfare):
      lens_list = list(lens)
      sum_counts = list(totalfare)
      yield None, totalfare/len(sum_counts)
          

if __name__ == '__main__':
   MRCount.run()  

