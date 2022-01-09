import json
import urllib.parse
import uuid
import boto3


print('Starting Function Trigger')

#Amazon S3 Client
s3 = boto3.client('s3')


#Amazon Textract Client
textract = boto3.client("textract")


def lambda_handler(event, context):

    # Retrive the bucket name and the object key from the event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    params={
        "DocumentLocation": {
            "S3Object": {
                "Bucket": bucket_name,
                "Name": object_key
            }
        },
        "ClientRequestToken": str(uuid.uuid4()),
        "NotificationChannel": {
            "RoleArn": "arn:aws:iam::041364470441:role/TextractRole",
            "SNSTopicArn": "arn:aws:sns:ap-south-1:041364470441:AmazonTextractAudifie"
        }
        
    }
    
    try:
        response = textract.start_document_text_detection(DocumentLocation=params['DocumentLocation'],ClientRequestToken=params['ClientRequestToken'],NotificationChannel=params['NotificationChannel'])
        
        return "Text extraction processing triggered for "+object_key
        
    except Exception as e:
        #Error in getting the object from s3 bucket
        print(e)
        if(not object_key):
            print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(object_key, bucket_name))
        raise e
