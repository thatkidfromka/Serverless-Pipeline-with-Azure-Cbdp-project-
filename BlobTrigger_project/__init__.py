import logging
import azure.functions as func
from azure.storage.blob import BlobClient, ContainerClient
from io import StringIO
from io import BytesIO
import pandas as pd
from azure.storage.blob import BlobServiceClient

def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.read} bytes")

    """"
    blob_text = myblob.read()
    

    # Count the number of lines in the blob
    line_count = len(blob_text.splitlines())
    print("The file", {myblob.name}, "contains", line_count, "lines")

    # Provide the connection string for your storage account
    connection_string = "DefaultEndpointsProtocol=https;AccountName=342798357878325789238553;AccountKey=8O/UZhm2m2m/yM5+3LomoMl12+jaHLDPQBNDAQMzsQLTIOmuQWlJxamjuqNHQ9GXIkTRmVKh3MAZ+AStZiDvSg==;EndpointSuffix=core.windows.net"
    
    # Upload the line count for each blob inside a new directory
    container_client = ContainerClient.from_connection_string(connection_string, "testcontainer")
    result_blob = container_client.get_blob_client(myblob.name+"-result")
    result_blob.upload_blob(str(line_count),overwrite=True)#overwrite=True was added
    
    #upload result also into countries Container

    """
    """
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=342798357878325789238553;AccountKey=8O/UZhm2m2m/yM5+3LomoMl12+jaHLDPQBNDAQMzsQLTIOmuQWlJxamjuqNHQ9GXIkTRmVKh3MAZ+AStZiDvSg==;EndpointSuffix=core.windows.net")
    blob_service_client.get_blob_client(container="countries",blob=myblob.name+"-result").upload_blob(str(line_count),overwrite=True)
    """

    