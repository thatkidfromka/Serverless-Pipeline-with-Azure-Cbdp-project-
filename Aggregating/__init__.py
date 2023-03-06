import logging
import azure.functions as func
from azure.storage.blob import BlobClient, ContainerClient
import pandas as pd
from io import StringIO
from azure.storage.blob import BlobServiceClient

# trigger=countries container
# output = upload to means container


def main(myblob: func.InputStream):
    logging.info(f"------Aggregating started------\n"
                 f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")

    blob_text = myblob.read()

    # Convert Bytes into Pandas DF
    s = str(blob_text, 'utf-8')
    data = StringIO(s)
    df = pd.read_csv(data,  sep=',', header=None)
    # print(df)
    result = df[5].sum()
    # print(result)
    # print("Result is", result)
    rows_count = len(df.index)

    countryname = myblob.name.replace('countries/', '').replace('.csv', '')
    # print("countryname", countryname)

    # example 1: init a dataframe by dict without index
    d = {"countryname": [countryname], "Amount_of_items_used": [
        rows_count], "Sum": [result]}
    df = pd.DataFrame(d)
    print("The DataFrame ")
    # print(df)
    print("---------------------")

    # create a stream to save the csv file into
    stream = StringIO()
    stream = df.to_csv(index=False, header=True, mode='a', encoding='utf-8')

    # inputs for upload_blob are io or strings
    blob_service_client = BlobServiceClient.from_connection_string(
        "DefaultEndpointsProtocol=https;AccountName=342798357878325789238553;AccountKey=8O/UZhm2m2m/yM5+3LomoMl12+jaHLDPQBNDAQMzsQLTIOmuQWlJxamjuqNHQ9GXIkTRmVKh3MAZ+AStZiDvSg==;EndpointSuffix=core.windows.net")
    blob_service_client.get_blob_client(
        container='means', blob="means_"+str(countryname)+".csv").upload_blob(stream, overwrite=True)
