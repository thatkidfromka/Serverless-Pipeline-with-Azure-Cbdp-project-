import pandas as pd
import os
import time
from azure.storage.blob import BlobServiceClient
import sys
from io import StringIO
from azure.storage.queue import (
    QueueClient,
    BinaryBase64EncodePolicy,
    BinaryBase64DecodePolicy
)
from io import StringIO
import io
import base64
import azure.functions as func

import os
import uuid

# Retrieve the connection string from an environment
# variable named AZURE_STORAGE_CONNECTION_STRING
connect_str = "DefaultEndpointsProtocol=https;AccountName=342798357878325789238553;AccountKey=8O/UZhm2m2m/yM5+3LomoMl12+jaHLDPQBNDAQMzsQLTIOmuQWlJxamjuqNHQ9GXIkTRmVKh3MAZ+AStZiDvSg==;EndpointSuffix=core.windows.net"

# Create a unique name for the queue
q_name = "results"

# Instantiate a QueueClient object which will
# be used to create and manipulate the queue
print("Queue name: " + q_name)
queue_client_result = QueueClient.from_connection_string(connect_str, q_name)
# Setup Base64 encoding and decoding functions
base64_queue_client = QueueClient.from_connection_string(
    conn_str=connect_str, queue_name=q_name,
    message_encode_policy=BinaryBase64EncodePolicy(),
    message_decode_policy=BinaryBase64DecodePolicy()
)


def upload_message(message):
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')
    print("Adding base64_message: " + base64_message)
    # queue_client_result.send_message(base64_message)
    queue_client_result.send_message(base64_message)


def pandas_to_String(df):
    result = df.to_csv(lineterminator="|", index=False, header=True)
    return result



def main(msg: func.QueueMessage):
 
    # fetch Queue Message and save it in a df_country
    print("MERGING QUEUE Triggered:")
    # logging.info('Python queue trigger function processed a queue item.')
    print(msg.get_body().decode('utf-8'))

    csvString = msg.get_body().decode('utf-8')
    csvStringIO = StringIO(csvString)
    df_country = pd.read_csv(csvStringIO, sep=",", lineterminator="|")

    print("incoming country"+str(df_country))

    # read current state from results queue

    message = queue_client_result.receive_message()
    message_content = message.content
    message_id = message.id
    pop_receit = message.pop_receipt
    # delete this message so only one message will be queued
    queue_client_result.delete_message(message_id,pop_receipt=pop_receit)
    base64_bytes = message_content.encode('ascii')
    message_bytes = base64.b64decode(base64_bytes)
    text = message_bytes.decode('ascii')
    csvString = text
    csvStringIO = StringIO(csvString)
    df_results = pd.read_csv(csvStringIO, sep=",", lineterminator="|")
    print(str(df_results))


    """
    #get results file from container
    #download from blob
    blob_service_client_instance = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=342798357878325789238553;AccountKey=8O/UZhm2m2m/yM5+3LomoMl12+jaHLDPQBNDAQMzsQLTIOmuQWlJxamjuqNHQ9GXIkTRmVKh3MAZ+AStZiDvSg==;EndpointSuffix=core.windows.net")
    #blob_service_client_instance = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)
    blob_client_instance = blob_service_client_instance.get_blob_client(container='queueresult', blob="result.csv", snapshot=None)
    blob_data_stream = blob_client_instance.download_blob()
    blob_data = blob_data_stream.read()
    result_stream=str(blob_data,'utf-8')
    result = StringIO(result_stream) 
    df_results = pd.read_csv(result,  sep= ',') 
    """


    # update results file like in blob trigger
    countries_in_results = df_results['countryname'].tolist()
    print("countries_in_results"+str(countries_in_results))

    country = df_country['countryname'].tolist()
    print("country"+str(country))
    print("county[0]"+str(country[0]))
    if country[0] in countries_in_results:
        # set the new value for amount_of_items_used
        amount_of_items_used = df_results.loc[df_results['countryname'] == country[0], ['Amount_of_items_used']]
        amount = amount_of_items_used['Amount_of_items_used'].sum()
        amount_country = df_country['Amount_of_items_used'].sum()
        df_results.loc[df_results['countryname'] == country[0], ['Amount_of_items_used']] = amount + amount_country
        # set new value of sum
        sum_already = df_results.loc[df_results['countryname'] == country[0], ['Sum']]
        sum = sum_already['Sum'].sum()
        sum_country = df_country['Sum'].sum()
        df_results.loc[df_results['countryname'] == country[0], ['Sum']] = sum + sum_country
    else:
        #pd.concat([df_results,df_country], ignore_index=True)
        df_results = df_results.append(df_country, ignore_index=True)
    
    df_results['mean'] = df_results['Sum'] / df_results ['Amount_of_items_used']
    print("df_results"+str(df_results))
    stream = StringIO()
    stream = df_results.to_csv(index = False, header = True, mode = 'a', encoding='utf-8')
       
        # inputs for upload_blob are io or strings
    """
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=342798357878325789238553;AccountKey=8O/UZhm2m2m/yM5+3LomoMl12+jaHLDPQBNDAQMzsQLTIOmuQWlJxamjuqNHQ9GXIkTRmVKh3MAZ+AStZiDvSg==;EndpointSuffix=core.windows.net")
    blob_service_client.get_blob_client(container='queueresult', blob="result.csv").upload_blob(stream,overwrite=True)
    """

    # upload to results queue
    upload_string = pandas_to_String(df_results)
    print(upload_string)
    upload_message(upload_string)


