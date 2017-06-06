#!/bin/bash
# file: cs123proj.sh
# cleantaxi.txt will be the clean dataset input
# for this next step, we delete all the " and blankspace
cat cleantaxi.txt | tr -d '" '  > cleantaxi.csv
# (We planned to use package "csv" at first, so turned this file to .csv)

# used to classify high-income drivers and ordinary drivers
python3 MRincome_new.py cleantaxi.csv -r dataproc -c job.conf > income.txt

# cleanning income.txt for later use
cat income.txt |sed 's/^..//'| tr -d ' \t{}"' > income.csv

# mrjob to classify trips by location
python3 MRlocation_new.py cleantaxi.csv -r dataproc -c job1.conf > location.txt

# mrjob to classify trips by time period
python3 MRperiod_new.py cleantaxi.csv -r dataproc -c job1.conf > period.txt

# mrjob to classify trips by weekday
python3 MRwkday_new.py cleantaxi.csv -r dataproc -c job1.conf > weekday.txt

# Note:
    # 2 config files
        # 1. job.conf
        # 2. job1.conf
