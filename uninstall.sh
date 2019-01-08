#!/usr/bin/env bash

cd lambda
sls remove
cd ..
rm lambda/status.json
