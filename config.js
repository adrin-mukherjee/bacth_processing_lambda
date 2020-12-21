/**
 * Configuration module
 * 
 * @author adrin.mukherjee
 * @version 1.0.0
 */
module.exports= {
    REGION_CODE: process.env.REGION_CODE || 'ap-south-1',
    DB: {
        TABLE_NAME: process.env.TABLE_NAME || 'PRELIM_STUDENT_DATA'
    },
    S3: {
        INBOUND_BUCKET: process.env.INBOUND_BUCKET || "inbound-file-drop"
    },
    SNS: {
        NOTIFICATION_TOPIC_ARN: process.env.NOTIFICATION_TOPIC_ARN
    },
    FILE_SCHEMA: {
        "type": "object",
        "properties": {
            "student_id": { "type": "string", "minLength": 5, "maxLength": 20},
            "fname": { "type": "string", "minLength": 1},
            "lname": { "type": "string", "minLength": 1},
            "course": { "type": "string", "minLength": 5},
        },
        "required": ["student_id", "fname", "course"]
    }
};
