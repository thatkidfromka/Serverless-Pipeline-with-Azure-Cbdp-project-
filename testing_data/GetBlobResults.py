import pandas as pd
import os
import time
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import BlobClient
import sys
from io import StringIO
from azure.storage.queue import (
    QueueClient,
    BinaryBase64EncodePolicy,
    BinaryBase64DecodePolicy,
    TextBase64DecodePolicy
)
from io import StringIO
import io
import base64
import azure.functions as func

import os
import uuid




blob_service_client_instance = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=342798357878325789238553;AccountKey=8O/UZhm2m2m/yM5+3LomoMl12+jaHLDPQBNDAQMzsQLTIOmuQWlJxamjuqNHQ9GXIkTRmVKh3MAZ+AStZiDvSg==;EndpointSuffix=core.windows.net")
#blob_service_client_instance = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)
blob_client_instance = blob_service_client_instance.get_blob_client(container='results', blob="result.csv", snapshot=None)
    
blob_data_stream = blob_client_instance.download_blob()
blob_data = blob_data_stream.read()
result_stream=str(blob_data,'utf-8')
result = StringIO(result_stream) 

df_results = pd.read_csv(result,  sep= ',')
print("result"+str(df_results))