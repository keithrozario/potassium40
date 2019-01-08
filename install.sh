#!/usr/bin/env bash

UUID=$(uuidgen | tr '[:upper:]' '[:lower:]')  #
BUCKET_NAME=p40.$UUID
echo '{"bucket_name": "'$BUCKET_NAME'"}' | jq '.' > lambda/status.json
cd lambda
sls deploy --bucket_name $BUCKET_NAME
cd ..