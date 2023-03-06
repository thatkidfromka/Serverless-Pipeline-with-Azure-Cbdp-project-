import pandas as pd
import os
import time
from azure.storage.blob import BlobServiceClient
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




connect_str = "DefaultEndpointsProtocol=https;AccountName=342798357878325789238553;AccountKey=8O/UZhm2m2m/yM5+3LomoMl12+jaHLDPQBNDAQMzsQLTIOmuQWlJxamjuqNHQ9GXIkTRmVKh3MAZ+AStZiDvSg==;EndpointSuffix=core.windows.net"

# Create a unique name for the queue
q_name = "results"
# Instantiate a QueueClient object which will
# be used to create and manipulate the queue
print("Queue name: " + q_name)
queue_client = QueueClient.from_connection_string(connect_str, q_name)
message = queue_client.receive_message()
message_content = message.content
pop_receit = message.pop_receipt
message_id = message.id
print(pop_receit)
print(message_id)



base64_bytes = message_content.encode('ascii')
message_bytes = base64.b64decode(base64_bytes)
text = message_bytes.decode('ascii')

print(text)
print(type(text))
csvString = text
csvStringIO = StringIO(csvString)
df_country = pd.read_csv(csvStringIO, sep=",", lineterminator="|")
print(str(df_country))


"""
def upload_message(message):
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')
    print("Adding base64_message: " + base64_message)
    # queue_client_result.send_message(base64_message)
    queue_client.send_message(base64_message)


def pandas_to_String(df):
    result = df.to_csv(lineterminator="|", index=False, header=True)
    return result


csvString = text
csvStringIO = StringIO(csvString)
df_country = pd.read_csv(csvStringIO, sep=",", lineterminator="|")
print(str(df_country))
#queue_client.delete_message()

df_country['mean'] = df_country['Amount_of_items_used']/df_country['Sum']
upload_string = pandas_to_String(df_country)
upload_message(upload_string)

"""
""""
# Connect to the Azure storage account
blob_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=342798357878325789238553;AccountKey=8O/UZhm2m2m/yM5+3LomoMl12+jaHLDPQBNDAQMzsQLTIOmuQWlJxamjuqNHQ9GXIkTRmVKh3MAZ+AStZiDvSg==;EndpointSuffix=core.windows.net")
blob_client_instance = blob_client.get_blob_client(container='queuestorage', blob="0.csv", snapshot=None)
blob_data_stream = blob_client_instance.download_blob()
blob_data = blob_data_stream.read()
result_stream=str(blob_data,'utf-8')
result = StringIO(result_stream) 
df_exist = pd.read_csv(result,  sep= ',')
print("df_exist"+str(df_exist))
sum_exist = df_exist.iloc[0,2]

print("sum_exist"+str(sum_exist))

"""