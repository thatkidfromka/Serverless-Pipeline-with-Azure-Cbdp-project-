import logging
import json
import pandas as pd
from io import StringIO
import base64

import azure.functions as func
from azure.storage.queue import (
    QueueClient,
    BinaryBase64EncodePolicy,
    BinaryBase64DecodePolicy
)
import os
import uuid
from azure.storage.blob import BlobServiceClient


def main(msg: func.QueueMessage):
    print("Aggregating Queue Triggered:")

    
    # logging.info('Python queue trigger function processed a queue item.')

    csvString = msg.get_body().decode('utf-8')
    csvStringIO = StringIO(csvString)
    df = pd.read_csv(csvStringIO, sep=",", lineterminator="|")
    
    sum = df["Balance"].sum()

    rows_count = len(df.index)
      
    countryname= df.iloc[1,1]
    
    d = {"countryname": countryname, "Amount_of_items_used": [
        rows_count], "Sum": [sum]}
    
    df = pd.DataFrame(d)

    # enter here the logic for the reupload of the aggregated filelds: 
    
    stream = StringIO()

    blob_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=342798357878325789238553;AccountKey=8O/UZhm2m2m/yM5+3LomoMl12+jaHLDPQBNDAQMzsQLTIOmuQWlJxamjuqNHQ9GXIkTRmVKh3MAZ+AStZiDvSg==;EndpointSuffix=core.windows.net")
    """
    exists = blob_client.get_blob_client(container='queuestorage', blob=str(countryname)+".csv").exists()
    print(exists)
    
    if(exists == False):
        stream = df.to_csv(index = False, header = True, mode = 'a', encoding='utf-8')
        blob_client.get_blob_client(container='queuestorage', blob=str(countryname)+".csv").upload_blob(stream,overwrite=True)
    else:
        blob_client_instance = blob_client.get_blob_client(container='queuestorage', blob=str(countryname)+".csv", snapshot=None)
        blob_data_stream = blob_client_instance.download_blob()
        blob_data = blob_data_stream.read()
        result_stream=str(blob_data,'utf-8')
        result = StringIO(result_stream) 
        df_exist = pd.read_csv(result,  sep= ',')
        print("df_exist"+str(df_exist))
        sum_exist = df_exist.iloc[0,2]
        
        sum_new = sum_exist + sum
        print("sum_exist"+str(sum_exist))
        row_exist = df_exist.iloc[0,1]
        row_new = row_exist + rows_count
        e = {"countryname": countryname, "Amount_of_items_used": [
        row_new], "Sum": [sum_new]}
        df_exist = pd.DataFrame(e)
        df = pd.DataFrame(e)
        stream = df_exist.to_csv(index = False, header = True, mode = 'a', encoding='utf-8')
        blob_client.get_blob_client(container='queuestorage', blob=str(countryname)+".csv").upload_blob(stream,overwrite=True)
    """


    connect_str = "DefaultEndpointsProtocol=https;AccountName=342798357878325789238553;AccountKey=8O/UZhm2m2m/yM5+3LomoMl12+jaHLDPQBNDAQMzsQLTIOmuQWlJxamjuqNHQ9GXIkTRmVKh3MAZ+AStZiDvSg==;EndpointSuffix=core.windows.net"

    # Create a unique name for the queue
    q_name = "means"

    # Instantiate a QueueClient object which will
    # be used to create and manipulate the queue
    print("Queue name: " + q_name)
    queue_client_messages = QueueClient.from_connection_string(connect_str, q_name)
    

    # method to upload to queue: csv-headers: countrycode, Amount_of_items_used, Sum
    def upload_message(message):
        message_bytes = message.encode('ascii')
        base64_bytes = base64.b64encode(message_bytes)
        base64_message = base64_bytes.decode('ascii')
        print("Adding base64_message: " + base64_message)
        # queue_client_messages.send_message(base64_message)
        queue_client_messages.send_message(base64_message)

    # string for upload
    def pandas_to_String(df):
        result = df.to_csv(lineterminator="|", index=False, header=True)
        return result


    #upload 
    upload_string = pandas_to_String(df)
    blob_client.get_blob_client(container='queuestorage', blob=str(countryname)).upload_blob(str(upload_string),overwrite=True)
    print("Adding upload_string: " + str(upload_string))
    # queue_client_messages.send_message(upload_string)
    upload_message(upload_string)
    print("upload from messages to means queue")


   