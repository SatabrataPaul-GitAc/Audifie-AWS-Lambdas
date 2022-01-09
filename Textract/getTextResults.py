import json
import boto3
import os

#Amazon s3 client
s3 = boto3.client("s3")

#Amazon Textract Client
textract = boto3.client("textract")

def getJobResults(jobid):
    pages=[]
    nexttoken=None
    params1= {
        "JobId": jobid
    }
    response = textract.get_document_text_detection(JobId=params1['JobId'])
    
    pages.append(response)
    
    if("NextToken" in response):
        nexttoken = response["NextToken"]
    
    while(nexttoken):
        params2= {
            "JobId": jobid,
            "NextToken": nexttoken
        }
        
        response = textract.get_document_text_detection(JobId=params2['JobId'],NextToken=params2['NextToken'])
        
        pages.append(response)
        nexttoken= None
        
        if("NextToken" in response):
            nexttoken = response["NextToken"]
    
    return pages
    
    


def lambda_handler(event, context):

    notificationMessage = json.loads(json.dumps(event))['Records'][0]['Sns']['Message']
    
    text_extraction_status = json.loads(notificationMessage)['Status']
    
    text_extraction_jobid = json.loads(notificationMessage)['JobId']
    
    document_location = json.loads(notificationMessage)['DocumentLocation']
    
    text_result_bucket = "audifytextractresults"
    
    text_extraction_object_key = json.loads(json.dumps(document_location))['S3ObjectName']
    
    text_result = ""
    
    if(text_extraction_status=="SUCCEEDED"):
        response = getJobResults(text_extraction_jobid)
    
        for resultPage in response:
            for item in resultPage["Blocks"]:
                if(item["BlockType"]=="LINE"):
                    text_result += item["Text"]+"\n"
                    
    outputObjectKey = os.path.splitext(text_extraction_object_key)[0]+"_text"+".txt"
    
    s3.put_object(Body= text_result,Bucket= text_result_bucket,Key= outputObjectKey)
    
    