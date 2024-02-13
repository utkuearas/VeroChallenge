import argparse
import pandas as pd
import json
import requests
from datetime import datetime, timedelta, date
import numpy as np
import math

parser = argparse.ArgumentParser()
parser.add_argument('-k','--keys',default='')
parser.add_argument('-c','--colored',default='True')
parser.add_argument('-f','--file',default='./vehicles.csv')

# Embed rows with required row background color
def embed_rows(df):
    embeds = []
    for ind, row in df.iterrows():
        date = datetime.strptime(row['hu'],'%Y-%m-%d')
        now = datetime.now()
        if now - timedelta(days=90) < date:
            embeds.append("green")
        elif now - timedelta(days=30*12) < date:
            embeds.append("orange")
        else:
            embeds.append("red")
    return embeds

if __name__ == '__main__':

    # Checking arguments
    args = parser.parse_args()
    keys = args.keys
    colored = args.colored
    if colored == 'True':
        colored = True
    else:
        colored = False
    csv_file_name = args.file

    # Sending request via CSV file
    files = {'file': (csv_file_name,open(csv_file_name, 'rb'),'text/csv')}
    res = requests.post('http://localhost:8000/uploadCSV', files=files)
    
    data = json.loads(res.text)
    data = pd.read_json(data)

    # Sorting and removing rows which doesn't include rnr value
    data = data[data['rnr'].notna()]
    data = data.sort_values('gruppe')
    data = data.fillna(np.nan)

    # Checking color codes
    required_columns = keys.split(',')
    row_colors = embed_rows(data)
    colorCodes = data['colorCode'].to_list()
    colors = ["black" if math.isnan(i) else i for i in colorCodes]
    
    data = data.drop(columns=['colorCode'])
    data = data.reset_index(drop=True)

    # Pandas Dataframe style function for labelIds
    def apply_label_id(val):
        return [f'color: {color}' for color in colors]
    
    # Pandas Dataframe style function for colored attribute
    def apply_row(val):
        index = val.name
        color = row_colors[index]
        return [f"background-color: {color}"] * len(val)
    
    # Returning result based on the given input parameters
    if colored and keys != '':
        data = data[required_columns]
        if 'labelIds' in keys.split(','):
            data = data.style.apply(apply_row, axis=1)
            data = data.apply(apply_label_id, subset=['labelIds'])
        else:
            data = data.style.apply(apply_row, axis=1)
    elif colored:
        data = data.style.apply(apply_row, axis=1)
        data = data.apply(apply_label_id, subset=['labelIds'])
    elif keys != '':
        data = data[required_columns]
        if 'labelIds' in keys.split(','):
            data = data.style.apply(apply_label_id, subset=['labelIds'])
    else:
       data = data.style.apply(apply_label_id, subset=['labelIds'])

    data.to_excel(f'./vehicles-{date.today()}.xlsx')