#!/bin/bash

if [ "$#" -ne 3 ]; then
    echo "Usage: ./ingest.sh first_year last_year destination-bucket-name"
    exit
fi

export FIRST_YEAR=$1
export LAST_YEAR=$2
export BUCKET=$3

# get zip files from BTS, extract csv files
for YEAR in `seq $FIRST_YEAR $LAST_YEAR`; do
   for MONTH in `seq 1 12`; do
      bash ./ingest/download.sh $YEAR $MONTH
      # upload the raw CSV files to our GCS bucket
      bash ./ingest/upload.sh $BUCKET
      rm ./ingest/*.csv
   done
   bash ./ingest/bqload.sh $BUCKET $YEAR
done