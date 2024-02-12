import argparse
import pandas as pd
import json
import requests
from datetime import datetime, timedelta, date
import numpy as np
parser = argparse.ArgumentParser()
parser.add_argument('-k','--keys',default='')
parser.add_argument('-c','--colored',default='True')
parser.add_argument('-f','--file',default='./vehicles.csv')


def labelMatch(row):
    labels = [''] * len(row)
    if not row.isnull()['colorCode']:
        index = row.index.get_loc('labelIds')
        color = row['colorCode']
        labels[index] = f'color: {color}'
    return labels

def styleRow(row):
    date = datetime.strptime(row['hu'],'%Y-%m-%d')
    now = datetime.now()
    if now - timedelta(days=90) < date:
        return ['background-color: green'] * len(row)
    elif now - timedelta(days=30*12) < date:
        return ['background-color: orange'] * len(row)
    else:
        return ['background-color: red'] * len(row)

if __name__ == '__main__':

    args = parser.parse_args()
    
    keys = args.keys
    colored = args.colored
    if colored == 'True':
        colored = True
    else:
        colored = False
    csv_file_name = args.file

    files = {'file': (csv_file_name,open(csv_file_name, 'rb'),'text/csv')}
    res = requests.post('http://localhost:8000/uploadCSV', files=files)
    
    data = json.loads(res.text)
    data = pd.read_json(data)
    
    data = data[data['rnr'].notna()]
    data = data.sort_values('gruppe')

    columns = set(data.columns)
    columns.add('colorCode')
    required_columns = set(keys.split(','))
    hide_col = list(columns-required_columns)

    
    if colored and keys != '':
        if 'labelIds' in keys.split(','):
            data = data.style.apply(styleRow,axis=1).apply(labelMatch,axis=1).hide(hide_col,axis='columns')
        else:
            data = data.style.apply(styleRow,axis=1).hide(hide_col,axis='columns')
    elif colored:
        data = data.style.hide(['colorCode'],axis='columns').apply(styleRow,axis=1).apply(labelMatch,axis=1)
    elif keys != '':
        if 'labelIds' in keys.split(','):
            data = data.style.apply(labelMatch,axis=1).hide(hide_col,axis='columns')
        else:
            data = data.style.hide(hide_col,axis='columns')
    else:
        data = data.style.apply(labelMatch,axis=1).hide(['colorCode'],axis='columns')

    data.to_excel(f'./vehicles-{date.today()}.xlsx')