import os
from typing import Dict
import requests
import config
import chain
import boto3
import csv
import uuid
import re

def getS3Data(bucket_name, object_key):
    #Extract relevant information from the S3 event
    bucket_name = "datalakemkbgo"
    object_key = "details/customer_focus_form - Aug DFR-1.csv"
    aws_access_key_id = ''
    aws_secret_access_key = ''
    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    try:
        #Get the content of the S3 object
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        lines = response['Body'].read().decode('utf-8').splitlines()
        reader = csv.DictReader(lines)
        csv_data=[]
        dict_from_csv = dict(list(reader)[0])
        list_of_column_names = list(dict_from_csv.keys())
        
        # Reset the CSV data by reading it into a list
        reader1 = csv.DictReader(lines)
        for row in reader1:
            
            row_values = [row[column] for column in list_of_column_names]
            print(row_values)
            csv_data.append(row_values)
        # Return the CSV data as JSON
        #print(json.dumps(csv_data))
        
        return list_of_column_names, csv_data
    except Exception as e:
        print(f"Error: {str(e)}")

#This function return column names as a list of column names  
def addSizableColumnData(column_names):
    word_list=[]
    token_size=20000
    words=""
    session_id=''
    for name in column_names:
        words+=name + ","
        if len(words)>token_size:
            column=re.sub(r'^,*(.*?),*$', r'\1', words)
            word_list.append(column)
            words=""
    #word_list.append(words)
    column=re.sub(r'^,*(.*?),*$', r'\1', words)
    word_list.append(column)
    for s in word_list:
        prompt="I am uploading column names for a file: " + s
        response, session_id = chain.run(
            api_key="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",
            session_id=session_id,
            prompt=prompt
            )
    print("response:",response,"session_id:", session_id)
    return session_id   
#getSizableColumnData(list_of_column_names)

 
def getSizeableRows(row_data,session_id):
    row_list=[]
    token_size=10000
    words=""
    word_list=[]
    total=0
    #session_id=''
    for data in row_data:
        print(type(data))
        characters = sum(len(s) for s in data)
        print(characters)
        if total+characters<=token_size:
            row_list.append(data)
            total+=characters
        else:
            prompt="Hey I am uplaoding rows for a file " + str(row_list)
            response, session_id = chain.run(
            api_key="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",
            session_id=session_id,
            prompt=prompt
            )
            print("response:",response,"session_id:", session_id)
            row_list=[]
            row_list.append(data)
            total=characters
    #if len(rowlist)>0:
        #prompt="Hey I am uplaoding rows for a file " + str(row_list)
        #response, session_id =5481ff25-2a6c-42e1-bd38-01d388f379d2 chain.run(
        #    api_key="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",
        #    session_id=session_id,
        #    prompt=prompt
        #    )
        #print("response:",response,"session_id:", session_id)
#rows(data,"613d5c7c-2340-4f2e-9249-7d9ac32a9c19")

def addRows(row_data, session_id):
    print(len(row_data))
    for data in row_data:
        print(data)
        prompt = "I am uploaded the column names now i am uploading rows corresponding         to them for a file " + str(data)
        response, session_id = chain.run(
            api_key="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",
            session_id=session_id,
            prompt=prompt
            )
        print("response:", response, "session_id:", session_id)
    return session_id    

def startChat():
    #This function will act as bridge between user and our ml model
    
    #This is a work around for getting data and displaying user which file are we using
    #Once we go ahead we can check in login credentials and 
    
    prompt=""

    session_id=''
    print("Hello user, Do you wish to upload a new database or work on a previous one?")
    print("kindly choose 'yes' for New User or 'change source' for existing user to process new data and 'no' for the existing user")
    prompt=input()
    if prompt.lower() == 'yes':
        print("Please enter bucket name (for example: datalakemkbgo")
        bucket_name = input()
        print("Please enter object_key. For example object key: Details/customer_focus_form - Aug DFR-1 (13).csv")
        object_key=input()
        list_of_column_names, csv_data = getS3Data(bucket_name, object_key)
        session_id=addSizableColumnData(list_of_column_names)
        prompt="I uploaded the column names of the file. Next i will be uploading each row in form of a list. Please remember it will be row data so the first value will be mapped to first column name, second value will be mapped to second column name and so on."
        response, session_id = chain.run(
                api_key="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",
                session_id=session_id,
                prompt=prompt
            )
        data=[]
        for i in range(2):
            data.append(csv_data[i])

        session_id=addRows(data,session_id)

    if prompt.lower() == 'change source':
        print("Please enter your session id from where you want to continue:")
        session_id=input()
        print("Please enter bucket name (for example: datalakemk2")
        bucket_name = input()
        print("Please enter object_key. For example object key: Details/customer_focus_form - Aug DFR-1.csv")
        object_key=input()
        csv_data = getS3Data(bucket_name, object_key)
        prompt="Hi, this is data of a file. Please read it and remember the file data. The data is as follows: " + str(csv_data)
        response, session_id = chain.run(
                api_key="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",
                session_id=session_id,
                prompt=prompt
            )
        print("The data has been read successfully.")
    if prompt.lower() == 'no':
        print("Please enter your session id from where you want to continue: ")
        session_id=input()

    #getS3Data()
    print("Hello user, you are inside the chat")
    print("This is a program in which you can either ask the user to remember formulas related to data or ask questions to the chat bot.")
    
    #session_id = '27c6af49-cc66-4dee-843b-b19765a7d51a'
    while prompt.lower() != 'quit':
        prompt = input("Enter your query or enter 'quit' to exit the program: ")
        
        if prompt.lower() != 'quit':
            response, session_id = chain.run(
                api_key="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",
                session_id=session_id,
                prompt=prompt
            )
            print("Response:", response)
            # You can do more with the response if needed
    print("Goodbye!")
    print("Your final session id is: " , session_id)

startChat()
def validate_inputs(body: Dict):
    for input_name in ['prompt', 'session_id']:
        if input_name not in body:
            return build_response({
                "status": "error",
                "message": f"{input_name} missing in payload"
            })
    return "data"

def build_response(body: Dict):
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(body)
    }

def get_new_api_key():
    client_ssm=boto3.client("ssm", region_name='ap-south-1',aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    response = client_ssm.get_parameter(Name='/chatbot/api-key',WithDecryption=True)
    print("teh pai response key",response)
    return response

def get_api_key():
    """Fetches the api keys saved in Secrets Manager"""

    headers = {"X-Aws-Parameters-Secrets-Token": os.environ.get('AWS_SESSION_TOKEN')}
    secrets_extension_endpoint = "http://localhost:2773" + \
    "/secretsmanager/get?secretId=" + \
    config.config.API_KEYS_SECRET_NAME
  
    r = requests.get(secrets_extension_endpoint, headers=headers)
    secret = json.loads(json.loads(r.text)["SecretString"])

    return secret["openai-api-key"]

