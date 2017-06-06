# Data Cleaning on Dataproc
# Ningyin Xu

## Setup instance (Lab 2)
## (For a package called Fiona, which we're managed to delete at the end, we 
##  used Ubuntu 16.04 as instance env.)
sudo apt-get install ipython3
sudo apt-get install python3-pip
sudo pip3 install pandas
sudo pip3 install geopy
sudo apt-get -y install libgeos-dev
sudo pip3 install shapely

## Add additional perminent storage disk (Piazza Post);
## Download data into the disk;
## Setup dataproc on the instance (Lab3);

## Upload data on gcloud storage (bucket)
gsutil /mnt/storage/taxi.csv gs://taxidataahda/taxi1.csv

python3 MRclean_compile.py gs://taxidataahda/taxi1.csv -r dataproc --conf-path = ./mrjob1.conf --no-output --output-dir=gs://taxidataahda/taxiresults

## we wait for 2hrs14mins to finish this work.

