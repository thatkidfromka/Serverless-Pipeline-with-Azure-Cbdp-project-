import os
import time
from azure.storage.blob import BlobServiceClient
from datetime import datetime

## THIS FILE IS USED TO UPLOAD THE CUSTOMER DATA INTO THE BLOB Container
## RETRIEVE THE RESULT BY EXECUTING GetBlobResults.py after the processing is completely finished

# Connect to the Azure storage account
blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=342798357878325789238553;AccountKey=8O/UZhm2m2m/yM5+3LomoMl12+jaHLDPQBNDAQMzsQLTIOmuQWlJxamjuqNHQ9GXIkTRmVKh3MAZ+AStZiDvSg==;EndpointSuffix=core.windows.net")

#upload start_time for the time measurement




# Get the name of the container
container_name = 'testcontainer'

while True:
    # Open file with list of files to upload
    with open('customer.csv', 'rb') as data:
        blob_service_client.get_blob_client(container=container_name, blob="customer.csv").upload_blob(data,overwrite=True)

        print("Uploaded customer.csv to"+str(container_name))
    

    break
start_time = datetime.now()
while True:
    print("refresh response time every 10 seconds")
    time.sleep(10)

    blob_client_instance1 = blob_service_client.get_blob_client(container='time', blob="end_time", snapshot=None)
    print(str(start_time))
    time_read = blob_client_instance1.download_blob()
    time_text = time_read.read()
    time_string = str(time_text,'utf-8')
    end_time = datetime.strptime(time_string, '%Y-%m-%d %H:%M:%S.%f')
    delta_time = end_time - start_time
    print("time passed since first upload: "+str(delta_time))
    blob_service_client.get_blob_client(container='time', blob="delta_time").upload_blob(str(delta_time),overwrite=True)
    #start_time = datetime.strptime(time_string, '%Y-%m-%d %H:%M:%S.%f')


