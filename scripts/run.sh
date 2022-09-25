### remove all results 
rm output/*

### download output files
readlink -f output/

# Configuring Collectd\
sudo vim /etc/collectd/collectd.conf


## Running Collectd
sudo systemctl stop collectd.service
sudo systemctl start collectd.service
cd test-simdjson/
source env/bin/activate
echo $EPOCHREALTIME
python collectd/main.py
sudo systemctl stop collectd.service

## find output file
readlink -f /var/lib/collectd/csv/intel-ather-test.c.lucky-rookery-360910.internal/memory/memory-u<tab>

## Move Output CSV file
cp /var/lib/collectd/csv/intel-ather-test.c.lucky-rookery-360910.internal/memory/memory-u<tab> ~/test-simdjson/extras/collectd_memory.csv

## Consolidate collectd files



## remove all files in csv
sudo rm -rf /var/lib/collectd/csv/