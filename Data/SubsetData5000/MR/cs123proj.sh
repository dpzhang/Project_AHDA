#!/bin/bash
# file: cs123proj.sh
cat clean50000.csv | tr -d '" '  > clean500001.csv

python3 MRincome_new.py clean500001.csv -r dataproc -c job.conf > income.txt

cat icome.txt |sed 's/^..//'| tr -d ' \t{}"' > income.csv

python3 MRlocation_new.py clean500001.csv -r dataproc -c job1.conf > location.txt

python3 MRperiod_new.py clean500001.csv -r dataproc -c job1.conf > period.txt

python3 MRwkday_new.py clean500001.csv -r dataproc -c job1.conf > weekday.txt

