# batch_processing_lambda
Skeleton code for a simple batch processing lambda that can be triggered by S3 event notification. Once triggered, the code dips into S3 bucket and gets the file specified in the event notification. All the records in the file (CSV) are read and transformed into JSON array. This array is then used to push items into DynamoDB table. Finally, the lambda triggers an Email notification using SNS.

Supports following Environment variables:
- REGION_CODE : AWS region code
- TABLE_NAME : Name of the DynamoDB table where items are inserted
- INBOUND_BUCKET : Name of the S3 bucket where file/s are pushed
- NOTIFICATION_TOPIC_ARN : ARN of the SNS topic

