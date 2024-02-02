from fastapi import FastAPI, UploadFile, File
from io import BytesIO
import pandas as pd


app = FastAPI()


@app.post('/uploadCSV')
async def uploadCSV(file: UploadFile=File(...)):

    # Reading file
    content = file.file.read()
    buffer = BytesIO(content)
    
    # Creating pandas dataframe object
    df = pd.read_csv(buffer, sep=';')
    print(df.head)
    return {"Success"}

    # TODO Modify this function
