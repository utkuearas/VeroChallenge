from fastapi import FastAPI, UploadFile, File
from io import BytesIO
import os
import pandas as pd
import requests
from pprint import pprint
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
load_dotenv()

app = FastAPI()

CURRENT_TOKEN = {'token':None,
                 'expires_at':datetime.now()}

# Get private data from .env file. It's not included in github
DATA_URI = os.getenv('DATA_URI')
LOGIN_URI = os.getenv('LOGIN_URI') 
TOKEN = os.getenv('TOKEN')
USERNAME = os.getenv('USERNAME')
PASSWORD = os.getenv('PASSWORD')

async def request_data(token):

    headers = {'Authorization': 'Bearer '+ token}
    data = requests.get(DATA_URI, headers=headers)
    return json.loads(data.text)


async def get_token():

    #Check current token
    if CURRENT_TOKEN['token'] and CURRENT_TOKEN['expires_at'] > datetime.now():
        print('Returning current token')
        return CURRENT_TOKEN['token']
    
    headers = {'Authorization' : TOKEN,
                'Content-Type': 'application/json' }
    data = {'username': USERNAME,
            'password': PASSWORD}
    
    # Get Token
    try:
        print('Requesting token')
        res = requests.post(LOGIN_URI, headers=headers, json=data)
    except:
        print('Request token failed')
        return None
    
    token = json.loads(res.text)['oauth']['access_token']

    CURRENT_TOKEN['token'] = token
    CURRENT_TOKEN['expires_at'] = datetime.now() + timedelta(minutes=20)
    
    return token



@app.post('/uploadCSV')
async def uploadCSV(file: UploadFile=File(...)):

    # Check the file
    if file.content_type != 'text/csv':
        return {"Bad Request"}

    # Reading file
    content = file.file.read()
    buffer = BytesIO(content)
    
    # Creating pandas dataframe object
    df = pd.read_csv(buffer, sep=';')

    token = await get_token()
    data = await request_data(token)

    # Filter data
    filteredData = [i for i in data if i['hu']]
    print(filteredData)
    filteredData = [i for i in filteredData if i['labelIds']]
    print(filteredData)
    return {"Success"}

    # TODO Modify this function
