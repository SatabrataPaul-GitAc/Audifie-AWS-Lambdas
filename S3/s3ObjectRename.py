import boto3
from botocore.exceptions import BotoCoreError,ClientError
import json
import urllib.parse
from sys import exit

print('Loading function')

#Creating s3 client
s3 = boto3.client('s3')


def renameAudioFile(audio_bucket,object_key,fileName):
    try:
        copy_source = {"Bucket": audio_bucket,"Key": object_key}
        copy_response = s3.copy_object(CopySource=copy_source,Bucket=audio_bucket,Key=fileName+".mp3")
        delete_response = s3.delete_object(Bucket=audio_bucket,Key=object_key)
        return True
    except (BotoCoreError,ClientError) as error:
        #The service returned an error
        print(error)
        exit(-1)
        
    
def renameMarksFile(marks_bucket,object_key,fileName):
    try:
        copy_source = {"Bucket": marks_bucket,"Key": object_key}
        copy_response = s3.copy_object(CopySource=copy_source,Bucket=marks_bucket,Key=fileName+".marks")
        delete_response = s3.delete_object(Bucket=marks_bucket,Key=object_key)
        return True
    except (BotoCoreError,ClientError) as error:
        #The service returned an error
        print(error)
        exit(-1)

def lambda_handler(event, context):

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    fileExtension = key.split('.')[3]
    fileName = key.split('.')[0]
    try:
        if(fileExtension=="mp3"):
            result = renameAudioFile(bucket,key,fileName)
        elif(fileExtension=="marks"):
            result = renameMarksFile(bucket,key,fileName)
        
        if(result==True):
            print("File has been renamed successfully !!!")
    except Exception as e:
        #Error in getting the object from s3 bucket
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
