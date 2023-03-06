import logging
import azure.functions as func
from azure.storage.blob import BlobClient, ContainerClient
import pandas as pd
from io import StringIO
import os
import time
from azure.storage.blob import BlobServiceClient


def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.read} bytes")

    blob_text = myblob.read()

    # Convert Bytes into Pandas DF 
    s=str(blob_text,'utf-8')
    data = StringIO(s) 
    df = pd.read_csv(data,  sep= '|', header = None)
    print(df)
    country_codes = df[3].unique()


    for country in country_codes: 
        # Save file name in variable split_target_file
        split_target_file = f"{'customer.csv'.replace('.csv', '')}_{country}.csv"

        # create df per country (filtered on country number)
        df_country = df[df[3] == country]
        """
        country_upload = StringIO()
        blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=342798357878325789238553;AccountKey=8O/UZhm2m2m/yM5+3LomoMl12+jaHLDPQBNDAQMzsQLTIOmuQWlJxamjuqNHQ9GXIkTRmVKh3MAZ+AStZiDvSg==;EndpointSuffix=core.windows.net")
        blob_service_client.get_blob_client(container='countries', blob=country+"nach_country").upload_blob(data,overwrite=True)
        """
    

        #change path for every user
        path = r'/Users/lukasdrobig/Documents/Master_TUM/cloud data/git_repository_project/serverless-distributed-data-processing-group-44/Splitter'
        # create csv file from df
        df_country.to_csv(os.path.join(path, split_target_file), index = False, header = True, mode = 'a')

        # Upload the csv for each blob inside a new directory
        print("Aktuelles Country: ", country)

    # Provide the connection string for your storage account
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=342798357878325789238553;AccountKey=8O/UZhm2m2m/yM5+3LomoMl12+jaHLDPQBNDAQMzsQLTIOmuQWlJxamjuqNHQ9GXIkTRmVKh3MAZ+AStZiDvSg==;EndpointSuffix=core.windows.net")

    # Get the name of the container
    container_name = 'countries'
        # Open file with list of files to upload
    with open('Splitter/filelist.txt', 'r') as f:
        file_list = f.read().splitlines()
        # Iterate through files and upload them to the container
        for file_name in file_list:
            with open(os.path.join('Splitter', file_name), 'rb') as data:
                blob_service_client.get_blob_client(container=container_name, blob="country"+file_name).upload_blob(data,overwrite=True)
            print(f'Uploaded {file_name} to {container_name}')
    # Wait 10 seconds before uploading the next set of files
    time.sleep(10)

  