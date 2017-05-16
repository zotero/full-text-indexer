#!/usr/bin/env bash

./build.sh

aws lambda update-function-code --function-name 123 --zip-file fileb://index.zip