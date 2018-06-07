#!/usr/bin/env bash

FUNCTION_NAME="bbb"

rm index.zip
zip -r index.zip index.js config.js node_modules

aws lambda update-function-code --function-name $FUNCTION_NAME --zip-file fileb://index.zip

rm index.zip