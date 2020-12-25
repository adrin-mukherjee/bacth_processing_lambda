const uuid   = require('uuid-v4');
const AWS    = require('aws-sdk');
const CSV    = require('csvtojson');
const Ajv    = require('ajv').default;
const Config = require('./config.js');

/**
 * Simple batch processing lambda that gets triggered by S3 event notification when a file is placed
 * in a pre-selected S3 bucket and then attempts to push the data-set to a DynamoDB table
 * 
 * @author adrin.mukherjee
 * @version 1.0.0
 */

exports.handler = async(event)=>{
    const batchId = uuid(); // generate a UUID and associate with all the logs for the batch run   
    console.log(`${batchId} >> Incoming event: ${JSON.stringify(event)}`);
        
    try{
        
        AWS.config.update({region: Config.REGION_CODE}); 

        const bucketAndFile = await extractBucketAndFileName(event);
        
        if(bucketAndFile != null){
            console.log(`${batchId} >> Extracted bucket and file name ${JSON.stringify(bucketAndFile)}`);

            const jsonDataSet = await readFileFromS3(batchId, bucketAndFile);
            console.log(`${batchId} Number of records fetched from file: ${jsonDataSet.length}`);

            const batchRunStats = await persistDataSet(batchId, jsonDataSet);
            console.log(`${batchId} Batch run results: ${JSON.stringify(batchRunStats)}`);

            await sendNotification({batchId: batchId, message: JSON.stringify(batchRunStats)});
        }
        else{
            throw Error(`Unable to extract bucket and file name from S3 event notification >> ${event}`);
        }
    }
    catch(e){
        console.error(`${batchId} >> Error encountered while processing batch`, e);
        await sendNotification({batchId: batchId, message: e});
    }
};

async function extractBucketAndFileName(event){
    let bucketAndFile = null;

    if(event 
        && event.Records 
        && event.Records[0]
        && event.Records[0].s3
        && event.Records[0].s3.bucket.name == Config.S3.INBOUND_BUCKET){

            bucketAndFile = {};
            bucketAndFile.bucketName = event.Records[0].s3.bucket.name;
            // Check fileName pattern, if required
            bucketAndFile.fileName = event.Records[0].s3.object.key;              
    }
    return bucketAndFile;
}

// Retrieve the CSV file from S3 and transform it into a JSON array
async function readFileFromS3(batchId, bucketAndFile){
    try{
        let s3 = new AWS.S3();
        const params = {
            Bucket: bucketAndFile.bucketName,
            Key: bucketAndFile.fileName
        };
        const stream = s3.getObject(params).createReadStream();
        const jsonDataSet = await CSV({
            noheader: false,
            trim: true,
            includeColumns: /(student_id|fname|lname|course)/
        }).fromStream(stream);
        return jsonDataSet;
    }
    catch(e){
        console.error(`${batchId} >> Unable to read CSV file from s3://${bucketAndFile.bucketName}/${bucketAndFile.fileName}`, e);
        throw e;
    }
}

// Persists records into DynamoDB
// For bulk record insertion, use batchWrite() instead of put()
async function persistDataSet(batchId, dataSet){
    let totalRecords = dataSet.length;
    let erroneousRecords = 0;
    let invalidRecords = 0;
    let errorDetailArray = [];

    try{
        if(dataSet instanceof Array && dataSet.length){
            let docClient = new AWS.DynamoDB.DocumentClient();
            if(docClient == null){
                console.error(`${batchId} >> Unable to send requests to DynamoDB`);
                throw Error(`${batchId} >> Unable to send requests to DynamoDB`);
            }
            
            const ajv = new Ajv({allErrors: true});
            let isValid = ajv.compile(Config.FILE_SCHEMA);
            
            for(const item of dataSet){
                if(isValid(item)){  // Validate each record before inserting
                    const params = {
                        TableName: Config.DB.TABLE_NAME,
                        Item: {
                            'student_id': item.student_id,
                            'course': item.course,
                            'fname': item.fname,
                            'lname': item.lname
                        }
                    };
        
                    await docClient.put(params).promise()
                        .then((data)=>{
                            // console.log(`${batchId} >> Record inserted : ${JSON.stringify(data)}`);
                        })
                        .catch((err)=>{
                            let rowNumber = Number(dataSet.indexOf(item)) + 1;
                            console.error(`${batchId} >> Unable to insert item in row ${rowNumber} : ${err}`);
                            erroneousRecords++;        

                            let errorDetail = {
                                row: rowNumber,
                                message: err
                            };
                            errorDetailArray.push(errorDetail);
                        });
                    continue;    
                }
                else{ // Validation failure
                    let err = ajv.errorsText(isValid.errors);
                    let rowNumber = Number(dataSet.indexOf(item)) + 1;
                    console.error(`${batchId} >> Unable to insert item in row ${rowNumber} due to validation failure : ${err}`);
                    erroneousRecords++;
                    invalidRecords++;

                    let errorDetail = {
                        row: rowNumber,
                        message: err
                    };
                    errorDetailArray.push(errorDetail);
                    continue;
                }
            }

            return {
                totalRecordsProcessed: totalRecords,
                totalInsertedRecords: Number(totalRecords-erroneousRecords),
                totalFailedRecords: erroneousRecords,
                recordsFailedDueToValidation: invalidRecords,
                errorDetails: errorDetailArray
            };
        }
        else{
            console.log(`${batchId} >> No records to process in data-set`);
            return {};
        }
    }
    catch(e){
        console.error(`${batchId} >> Unable to persist data to DynamoDB table ${Config.DB.TABLE_NAME}`, e);
        throw e;
    }
}

// Send SNS notifications upon completion of the batch run 
// event: batchId, message
async function sendNotification(event){
    try{
        const params = {
            Message: JSON.stringify(event.message),
            Subject: `Batch processing notification: ${event.batchId}`,
            TopicArn: Config.SNS.NOTIFICATION_TOPIC_ARN
        };

        let msgID = false;
        await new AWS.SNS().publish(params).promise()
            .then((data)=>{
                msgID = data.MessageId;
                console.log(`${event.batchId} >> Notification sent to the topic ${params.TopicArn} with message ID ${msgID}`);
            })
            .catch((err)=>{
                throw err;
            });
    }   
    catch(e){
        console.error(`${event.batchId} >> Unable to publish notification : ${e}`);
    }
}
