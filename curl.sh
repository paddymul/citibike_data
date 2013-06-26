#!/bin/bash


while true; do
fname=`date   "+%m-%d-%H_%M_%S"`
fname2="stations-$fname.json"
      curl https://citibikenyc.com/stations/json > $fname2
      sleep 60
done;
