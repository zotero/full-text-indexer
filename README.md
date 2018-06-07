Needs at least 8.10 runtime.

Permissions needed for DLQ function:
- lambda:InvokeAsync
- lambda:InvokeFunction
- sqs:DeleteMessage
- sqs:ReceiveMessage

Set 10s max execution time.

```
npm i
cp sample-config.js config.js
# configure FUNCTION_NAME in upload.sh
./upload.sh
```