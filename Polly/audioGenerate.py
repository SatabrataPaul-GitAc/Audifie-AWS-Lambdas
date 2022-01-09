#Importing required methods and libraries
import boto3
from botocore.exceptions import BotoCoreError,ClientError
import json
import urllib.parse
import os
from sys import exit
from tempfile import gettempdir

print('Loading function')

#Creating s3 client
s3_client = boto3.client('s3')

#Creating polly client
polly_client = boto3.client('polly')

def storeText(s3Bucket,objectKey):
    try:
        #Download the text file data from s3 bucket and store it in a temporary file on the system
        output_path = os.path.join(gettempdir(),objectKey)
        response = s3_client.download_file(Bucket=s3Bucket,Key=objectKey,Filename=output_path)
        return [output_path,True]
    
    except (BotoCoreError,ClientError) as error:
        #The service returned an error
        print(error)
        exit(-1)
        
def generateAudio(audioBucket,objectKey,filePath):
    try:
        
        with open(filePath,'r') as fp:
            text = fp.read()
        
        outputObjectKey = objectKey.split('_')[0]+"_audio.mp3"
        response = polly_client.start_speech_synthesis_task(OutputFormat="mp3",OutputS3BucketName=audioBucket,
        OutputS3KeyPrefix=outputObjectKey,SnsTopicArn="arn:aws:sns:ap-south-1:041364470441:AmazonPollyAudifie",Text=text,VoiceId="Salli")
        
        return response['SynthesisTask']['TaskId']
        
    except (BotoCoreError,ClientError) as error:
        #The service returned an error
        print(error)
        exit(-1)
    
def generateSpeechMarks(marksBucket,objectKey,filePath):
    try:
        
        with open(filePath,'r') as fp:
            text = fp.read()
        
        outputObjectKey = objectKey.split('_')[0]+"_speechmarks.marks"
        response = polly_client.start_speech_synthesis_task(OutputFormat="json",OutputS3BucketName=marksBucket,
        OutputS3KeyPrefix=outputObjectKey,SpeechMarkTypes=["sentence"],SnsTopicArn="arn:aws:sns:ap-south-1:041364470441:AmazonPollyAudifie",Text=text,VoiceId="Salli")
        
        return response['SynthesisTask']['TaskId']
        
    except (BotoCoreError,ClientError) as error:
        #The service returned an error
        print(error)
        exit(-1)
    

def lambda_handler(event, context):
    
    try:
        # Get the object from the event and show its content type
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        
        result1 = storeText(bucket,key)
        
        if(result1[1]==True):
            print("Text Data from s3 stored successfully")
        else:
            print("Text Data from s3 could not be stored")
            exit(-1)
        
        result2 = generateAudio("audifyaudioresults",key,result1[0])
        result3 = generateSpeechMarks("audifyspeechmarksresults",key,result1[0])
        
        if(result2 and result3):
            print("Audio generation process and Speech Marks process started sucessfully")
            print("Audio task id : ",result2)
            print("Speech Marks task id : ",result3)
        
    except Exception as e:
        #Error occured in getting the object from s3 bucket
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e


