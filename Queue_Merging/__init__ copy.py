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
 
    print("MERGING QUEUE Triggered:")
    # logging.info('Python queue trigger function processed a queue item.')
    print(msg.get_body().decode('utf-8'))

    csvString = msg.get_body().decode('utf-8')
    csvStringIO = StringIO(csvString)
    df_country = pd.read_csv(csvStringIO, sep=",", lineterminator="|")


    
    messages = queue_client_result.receive_messages()
    
    # create results dataframe to store messages
    df_results = pd.DataFrame(columns=['countryname', 'Amount_of_items_used', 'Sum'])

    for message in messages:
        m = base64.b64decode(message.content).decode("utf-8").replace("|", ",").split(',')
        m.pop()
        data = m[3:]

        # Load messages into Dataframe
        df_results.loc[len(df_results)] = data

        print("Dequeueing message: " + message.content)
        #queue_client_result.delete_message(message.id, message.pop_receipt)
        print(base64.b64decode(message.content).decode("utf-8"))

            

    print("df_results with messages:"+ str(df_results))
    # Sum up means if country already exists in queue

    countries_in_results = df_results['countryname'].tolist()
    print("countries_in_results"+str(countries_in_results))

    country = df_country['countryname'].tolist()
    print("country"+str(country))
    print("county[0]"+str(country[0]))
    if country[0] in countries_in_results:
        df_results.loc[df_results['countryname'] == country[0], ['Amount_of_items_used']] = df_results['Amount_of_items_used'] + df_country['Amount_of_items_used']
        df_results.loc[df_results['countryname'] == country[0], ['Sum']] = df_results['Sum'] + df_country['Sum']
    else:
        df_results = df_results.append(df_country, ignore_index=True)
    
    df_results['mean'] = df_results['Sum'] / df_results ['Amount_of_items_used']

    # Upload queue
    upload_string = pandas_to_String(df_results)
    print("Adding upload_string: " + str(upload_string))
    # queue_client_result.send_message(upload_string)
    upload_message(upload_string)
