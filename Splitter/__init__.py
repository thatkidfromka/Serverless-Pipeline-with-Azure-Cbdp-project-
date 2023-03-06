import logging
import azure.functions as func
from azure.storage.blob import BlobClient, ContainerClient
import pandas as pd
from io import StringIO
from azure.storage.blob import BlobServiceClient
from datetime import datetime


def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.read} bytes")

    blob_text = myblob.read()
    # print("blob_text:", blob_text)

    # Convert Bytes into Pandas DF
    s = str(blob_text, 'utf-8')
    # print("s", s)
    data = StringIO(s)
    # print("data:", data)
    df = pd.read_csv(data,  sep='|', header=None)
    # print(df)
    print(df.columns)
    country_codes = df[3].unique()

    now = datetime.now()
    # current_time = StringIO(now)
    blob_service_client = BlobServiceClient.from_connection_string(
        "DefaultEndpointsProtocol=https;AccountName=342798357878325789238553;AccountKey=8O/UZhm2m2m/yM5+3LomoMl12+jaHLDPQBNDAQMzsQLTIOmuQWlJxamjuqNHQ9GXIkTRmVKh3MAZ+AStZiDvSg==;EndpointSuffix=core.windows.net")
    blob_service_client.get_blob_client(
        container='time', blob="currentime").upload_blob(str(now), overwrite=True)

    for country in country_codes:
        # Save file name in variable split_target_file
        split_target_file = f"_{country}.csv"

        # create df per country (filtered on country number)
        df_country = df[df[3] == country]

        # create a stream to save the csv file into
        stream = StringIO()
        stream = df_country.to_csv(
            index=False, header=True, mode='a', encoding='utf-8')

        # inputs for upload_blob are io or strings
        blob_service_client = BlobServiceClient.from_connection_string(
            "DefaultEndpointsProtocol=https;AccountName=342798357878325789238553;AccountKey=8O/UZhm2m2m/yM5+3LomoMl12+jaHLDPQBNDAQMzsQLTIOmuQWlJxamjuqNHQ9GXIkTRmVKh3MAZ+AStZiDvSg==;EndpointSuffix=core.windows.net")
        blob_service_client.get_blob_client(
            container='countries', blob="country"+str(country)+".csv").upload_blob(stream, overwrite=True)
