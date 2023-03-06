import logging
import azure.functions as func
from azure.storage.blob import BlobClient, ContainerClient


##press f5 to start the function
def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.read} bytes")

    blob_text = myblob.read()

    # Count the number of lines in the blob
    line_count = len(blob_text.splitlines())
    print("The file", {myblob.name}, "contains", line_count, "lines")

    # Provide the connection string for your storage account
    connection_string = "DefaultEndpointsProtocol=https;AccountName=cbdpstoragename;AccountKey=v/qQuRZkJjpMiN+w1qd38nCAWyS0ONZvuPp2cCMT+WKlUPb1cICCZF2fXQ9CErWxj/djMi4L+WT4+AStJjv/eQ==;EndpointSuffix=core.windows.net"

    # Upload the line count for each blob inside a new directory
    container_client = ContainerClient.from_connection_string(connection_string, "testcontainer")
    result_blob = container_client.get_blob_client(myblob.name+"-result")
    result_blob.upload_blob(str(line_count))


    
