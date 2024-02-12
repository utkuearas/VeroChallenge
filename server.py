from fastapi import FastAPI, UploadFile, File
from io import BytesIO
import numpy as np
import pandas as pd
import requests
import aiohttp
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import asyncio
load_dotenv()

app = FastAPI()

CURRENT_TOKEN = {'token':None,
                 'expires_at':datetime.now()}

# Get private data from .env file. It's not included in github
DATA_URI = os.getenv('DATA_URI')
LOGIN_URI = os.getenv('LOGIN_URI') 
TOKEN = os.getenv('TOKEN')
USERNAME = os.getenv('LOGIN_USERNAME')
PASSWORD = os.getenv('PASSWORD')
LABEL_ID = os.getenv('LABEL_ID')

async def request_data(token, URI, session=None):

    headers = {'Authorization': 'Bearer '+ token}
    if session is not None:
        data = await session.get(URI, headers=headers)
        return json.loads(await data.text())
    
    data = requests.get(URI, headers=headers)
    return json.loads(data.text)


def merge_two_column(cols):
    col1,col2 = cols
    null1,null2 = cols.isnull()
    if not null1 and null2:
        return col1
    elif not null2 and null1:
        return col2
    elif null2 == null1:
        if not null1:
            return col1
        else:
            return ""
    else:
        return str(col1) + "," + str(col2)

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
    
    print(res.text)
    
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
    client_request = pd.read_csv(buffer, sep=';')
    client_request.to_excel('./client.xlsx')

    token = await get_token()
    data = await request_data(token,DATA_URI)

    api_response = pd.DataFrame.from_records(data)
    api_response.to_excel('./api.xlsx')

    api_columns = set(api_response.columns)
    client_columns = set(client_request.columns)

    if len(api_columns) > len(client_columns):
        large = api_response
        small = client_request
    else:
        large = client_request
        small = api_response
    
    large['kurzname'] = large['kurzname'].astype(str)
    small['kurzname'] = small['kurzname'].astype(str)
    large = large.merge(small, on=['kurzname','gruppe','lagerort'], how='outer')
    large = large.fillna(np.nan)
    large = large.replace('NULL',np.nan)

    cols = [i for i in large.columns if i.endswith('_x')]
    for col in cols:
        col_name = col[:len(col)-2]
        large[col_name] = large[[col_name+'_x',col_name+'_y']].apply(merge_two_column, axis=1)
        large = large.drop([col_name+'_x',col_name+'_y'], axis=1)

    large = large.drop_duplicates()

    labels = set()

    for _, i in large.iterrows():
        if i['labelIds']:
           labels.add(str(int(float(str(i['labelIds']).split(',')[0]))))

    large = large[large['hu'].notna()]

    color_codes = dict()
    async with aiohttp.ClientSession() as session:
        labels_f = [asyncio.create_task(request_data(token,LABEL_ID+uri,session=session)) for uri in list(labels)]
        responses = await asyncio.gather(*labels_f)
        for res in responses:
            color_code = res[0]['colorCode']
            if color_code is not '':
                color_codes[res[0]['id']] = color_code

    large['colorCode'] = pd.DataFrame([np.nan]*len(large))
    
    for i in range(len(large)):
        try:
            label = int(float(large.iloc[i]['labelIds']))
        except:
            continue
        if label in color_codes:
            large.loc[i,'colorCode'] = color_codes[label]
    large = large.replace('',np.nan)

    result = large.to_json(orient='records')
    
    return result
