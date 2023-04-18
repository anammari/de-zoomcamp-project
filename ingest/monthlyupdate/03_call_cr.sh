#!/bin/bash

# same as deploy_cr.sh
NAME=ingest-flights-monthly

BUCKET=dsongcp_data_lake_de-zoomcamp-prj-375800

URL=$(gcloud run services describe ${NAME} --format 'value(status.url)')
echo $URL

# Ingest next month in last available year (2019)
echo {\"bucket\":\"${BUCKET}\"\} > /tmp/message

curl -k -X POST $URL \
   -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
   -H "Content-Type:application/json" --data-binary @/tmp/message
