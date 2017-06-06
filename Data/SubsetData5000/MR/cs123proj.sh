#!/bin/bash
# file: cs123proj.sh
# clean50000.csv will be the clean dataset processed by dp's code
# it would create a new cleaned dataset called clean500001.csv
cat clean50000.csv | tr -d '" '  > clean500001.csv

# used to classify high-income drivers and ordinary drivers
python3 MRincome_new.py clean500001.csv -r dataproc -c job.conf > income.txt

# cleanning income.txt for later use
cat income.txt |sed 's/^..//'| tr -d ' \t{}"' > income.csv

# mrjob to classify trips by location
python3 MRlocation_new.py clean500001.csv -r dataproc -c job1.conf > location.txt

# mrjob to classify trips by time period
python3 MRperiod_new.py clean500001.csv -r dataproc -c job1.conf > period.txt

# mrjob to classify trips by weekday
python3 MRwkday_new.py clean500001.csv -r dataproc -c job1.conf > weekday.txt

# Note:
    # 2 config files
        # 1. job.conf
        # 2. job1.conf
