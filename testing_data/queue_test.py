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

import os
import uuid

## THIS FILE IS USED TO UPLOAD THE CUSTOMER DATA INTO THE QUEUE
## RETRIEVE THE RESULT BY EXECUTING GetQueueResults.py after the processing of the queue is finished


# Retrieve the connection string from an environment
# variable named AZURE_STORAGE_CONNECTION_STRING
connect_str = "DefaultEndpointsProtocol=https;AccountName=342798357878325789238553;AccountKey=8O/UZhm2m2m/yM5+3LomoMl12+jaHLDPQBNDAQMzsQLTIOmuQWlJxamjuqNHQ9GXIkTRmVKh3MAZ+AStZiDvSg==;EndpointSuffix=core.windows.net"

# Create a unique name for the queue
q_name = "messages"

# Instantiate a QueueClient object which will
# be used to create and manipulate the queue
print("Queue name: " + q_name)
queue_client_messages = QueueClient.from_connection_string(connect_str, q_name)
# Setup Base64 encoding and decoding functions
base64_queue_client = QueueClient.from_connection_string(
    conn_str=connect_str, queue_name=q_name,
    message_encode_policy=BinaryBase64EncodePolicy(),
    message_decode_policy=BinaryBase64DecodePolicy()
)

# How to add messages to a queue
"""
message = "Python is fun"
message = "['Customer_Key,Country_Code,Balance', '2608301,15,-429.74', '2613401,15,7880.35', '2615701,15,9711.00', '2616201,15,5743.80', '2620101,15,6630.75', '2621101,15,8606.34', '2621401,15,3726.36', '2622201,15,9201.43', '2626901,15,5273.57', '2628301,15,9023.09', '2628601,15,3472.23', '2629401,15,8929.60', '2637001,15,6669.80', '2640401,15,6463.36', '2641301,15,-938.06', '2641901,15,594.46', '2644001,15,6174.62', '2651601,15,1664.23', '2652601,15,5129.85', '2654201,15,4523.77', '2654501,15,5928.97', '2654701,15,2780.66', '2657301,15,7141.38', '2658501,15,2465.82','2659301,15,5623.53', '2661001,15,6735.69', '2661701,15,7818.07', '2662601,15,3756.38', '2664101,15,6126.06', '2664801,15,8127.98', '2667001,15,3423.39', '2667201,15,3697.03', '2667901,15,4283.11', '2673201,15,6273.21', '2673301,15,2560.72', '2674601,15,7427.17', '2675001,15,7307.02', '2675401,15,8926.53', '2676101,15,4967.60', '2676801,15,6002.11', '2683201,15,5936.34', '2684901,15,364.38', '2686601,15,-444.18', '2690801,15,5945.08', '2694701,15,3678.00', '2696401,15,6711.15', '2703501,15,1007.39', '2705101,15,1990.41', '2705701,15,2897.52', '2709501,15,3336.71']"
"""


def upload_message(message):
    print("--------------------------")
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')
    # print("Adding base64_message: " + base64_message)
    queue_client_messages.send_message(base64_message)
    # queue_client_messages.send_message(base64_message)


def pandas_to_String(df):
    result = df.to_csv(lineterminator="|", index=False, header=True)
    return result


def calculate_uploads(df):
    print("Size in bytes", sys.getsizeof(df),
          "Kilobytes", sys.getsizeof(df) * 0.001)

    max_kb_size = 60
    necessary_uploads = round((sys.getsizeof(df) * 0.001)/max_kb_size, 2)

    print("Necessary uploads:", necessary_uploads)
    return necessary_uploads


# file_name = "customer.00.csv"
# queue_client_messages.send_message(string)

with open('filelist.txt', 'r') as f:
    file_list = f.read().splitlines()
    # Iterate through files and upload them to the container
    for file_name in file_list:
        print(file_name)

        if True:
            with open(file_name, 'rb') as data:
                # preparation
                df = pd.read_csv(data,  sep='|', header=None)
                # drop unneccessary columns
                df = df.drop([1, 2, 4, 6, 7, 8,], axis=1)
                df.columns = ['Customer_Key ', 'Country_Code', 'Balance']
                print("DF1", df)

                # print(pandas_to_String(df))

                country_codes = df["Country_Code"].unique()
                print(country_codes)

                for country in country_codes:
                    print("--------------", country)
                    # create df per country (filtered on country number)
                    df_country = df[df["Country_Code"] == country]
                    print("Current country:", country)
                    df_country_rows = df_country.shape[0]
                    print("lines in country:", df_country_rows)
                    # Iterate through df country and split it up into smaller messages

                    i = 0
                    step_size = 1000
                    full_thousand = df_country_rows - \
                        (df_country_rows % step_size)
                    print("full_thousand", full_thousand)

                    while i <= full_thousand:
                        # less than thousand --> Upload rest
                        if i + step_size > df_country_rows:
                            print("start", i, "End is df_country_rows",
                                  df_country_rows)
                            df_upload_rest = df_country.iloc[i:, :]
                            print("Adding df_upload_rest\n: " +
                                  str(df_upload_rest))
                            print(
                                "Size of df_upload_rest.shape[0]", df_upload_rest.shape[0])
                            upload_string = pandas_to_String(df_upload_rest)
                            print("Uploading")
                            print(upload_string)
                            upload_message(upload_string)
                            break

                        # full thousand -->DF Upload
                        else:
                            print("i_start", i, "i_ende", i+step_size)
                            print("i", i, "df_country_rows", df_country_rows)
                            df_upload = df_country.iloc[i:i+step_size, :]
                            print("Adding df_upload\n: " + str(df_upload))

                            upload_string = pandas_to_String(df_upload)
                            print("Uploading")
                            upload_message(upload_string)

                        i += step_size

                    # Wait 10 seconds before uploading the next set of files
                        time.sleep(1)
