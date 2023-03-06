import os
import time
from azure.storage.blob import BlobServiceClient


# Connect to the Azure storage account
blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=342798357878325789238553;AccountKey=8O/UZhm2m2m/yM5+3LomoMl12+jaHLDPQBNDAQMzsQLTIOmuQWlJxamjuqNHQ9GXIkTRmVKh3MAZ+AStZiDvSg==;EndpointSuffix=core.windows.net")

# Get the name of the container
container_name = 'testcontainer'

while True:
    # Open file with list of files to upload
    with open('filelist.txt', 'r') as f:
        file_list = f.read().splitlines()
        # Iterate through files and upload them to the container
        for file_name in file_list:
            with open(file_name, 'rb') as data:
                blob_service_client.get_blob_client(container=container_name, blob=file_name).upload_blob(data,overwrite=True)
            print(f'Uploaded {file_name} to {container_name}')
    # Wait 10 seconds before uploading the next set of files
    time.sleep(10)

