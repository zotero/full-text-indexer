#!/usr/bin/env bash

aws lambda create-function \
--region us-east-1 \
--function-name test1 \
--zip-file fileb://index.zip \
--role role-arn \
--handler helloworld.handler \
--runtime nodejs6.10
