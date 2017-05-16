#!/usr/bin/env bash

aws lambda invoke \
--invocation-type RequestResponse \
--function-name 123 \
--region us-east-1 \
--log-type Tail \
--payload file://input.json \
outputfile.txt \
| jq -r .LogResult | base64 --decode