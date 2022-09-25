# Configuring Collectd
sudo vim /etc/collectd/collectd.conf

## Running Collectd
cd test-simdjson/
rm output/* extras/*
source env/bin/activate
sudo rm -rf /var/lib/collectd/csv/
sudo systemctl stop collectd.service
sudo systemctl start collectd.service
python collectd/main.py
sudo systemctl stop collectd.service

## Move Output CSV file
cp /var/lib/collectd/csv/intel-ather-test.c.lucky-rookery-360910.internal/memory/memory-u<tab> ~/test-simdjson/extras/collectd_memory.csv

## Consolidate collectd files
python collectd/parse_results.py
zip -r output.zip output
clear
readlink -f output.zip

## remove all files in csv
sudo rm -rf /var/lib/collectd/csv/

## Remove output.zip file
rm output.zip