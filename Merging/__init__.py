import logging
import pandas as pd
import azure.functions as func
from io import StringIO
from azure.storage.blob import BlobServiceClient
from datetime import datetime

def main(myblob: func.InputStream):
    #print ("Merging started")
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")

    #download from blob
    blob_service_client_instance = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=342798357878325789238553;AccountKey=8O/UZhm2m2m/yM5+3LomoMl12+jaHLDPQBNDAQMzsQLTIOmuQWlJxamjuqNHQ9GXIkTRmVKh3MAZ+AStZiDvSg==;EndpointSuffix=core.windows.net")
    #blob_service_client_instance = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)
    blob_client_instance = blob_service_client_instance.get_blob_client(container='results', blob="result.csv", snapshot=None)
    
    blob_data_stream = blob_client_instance.download_blob()
    blob_data = blob_data_stream.read()
    print("blob data"+str(blob_data))
    result_stream=str(blob_data,'utf-8')
    result = StringIO(result_stream) 
    print("result"+str(result))
    df_results = pd.read_csv(result,  sep= ',')

    print("df_results"+str(df_results))
    # Convert Bytes into Pandas DF 

    #### ---------- #####
    ### OLD Appraoch with Local Saving ###
    #with open('Merging/result.csv', "wb") as results_blob:
    #    blob_data = blob_client_instance.download_blob()
    #    blob_data.readinto(results_blob)

    # LOCALFILE is the file path
    #df_results = pd.read_csv('Merging/result.csv')
    #### --------- #####


    # Load Stream
    blob_text = myblob.read()
    # Convert Bytes into Pandas DF 
    s=str(blob_text,'utf-8')
    data = StringIO(s) 
    df_country = pd.read_csv(data,  sep= ',')
    
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
        df_results = df_results.append(df_country, ignore_index=True)
    
    df_results['mean'] = df_results['Sum'] / df_results ['Amount_of_items_used']
    print("df_results"+str(df_results))
    stream = StringIO()
    stream = df_results.to_csv(index = False, header = True, mode = 'a', encoding='utf-8')
       
        # inputs for upload_blob are io or strings
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=342798357878325789238553;AccountKey=8O/UZhm2m2m/yM5+3LomoMl12+jaHLDPQBNDAQMzsQLTIOmuQWlJxamjuqNHQ9GXIkTRmVKh3MAZ+AStZiDvSg==;EndpointSuffix=core.windows.net")
    blob_service_client.get_blob_client(container='results', blob="result.csv").upload_blob(stream,overwrite=True)
    
    print ("Merging finihed successfully")

    #download time from blob
    blob_client_instance1 = blob_service_client_instance.get_blob_client(container='time', blob="currentime", snapshot=None)
    
    time_read = blob_client_instance1.download_blob()
    time_text = time_read.read()
    time_string = str(time_text,'utf-8')
    start_time = datetime.strptime(time_string, '%Y-%m-%d %H:%M:%S.%f')
    end_time = datetime.now()
    delta_time = end_time - start_time
    blob_service_client.get_blob_client(container='time', blob="delta_time").upload_blob(str(delta_time),overwrite=True)
    blob_service_client.get_blob_client(container='time', blob="end_time").upload_blob(str(end_time),overwrite=True)

    """
    with open('Merging/time', "wb") as time_blob:
        time_data = blob_client_instance1.download_blob()
        time_data.readinto(time_blob)

    with open('Merging/time','r') as file:
        get_start_time = file.read()
        start_time = datetime.strptime(get_start_time, '%Y-%m-%d %H:%M:%S.%f')
        now = datetime.now()
        delta_time = now - start_time
        blob_service_client.get_blob_client(container='time', blob="delta_time").upload_blob(str(delta_time),overwrite=True)
    """

    '''
    Step 1: read newly uploaded file
        The csv has the following structure:
        countryname | Amount_of_items_used | Sum |
        country0     | x                   | y   |
    Step 2: open and read shared state with mean for all countries (results. csv in container xxx)
        The csv has the following structure:
        countryname | Amount_of_items_used | Sum |
        country0     | x                   | y   |
    
    Step 3: At the line of results.csv where the country is equal to the country in line 1 (without header)
    update the 

    Step 4: store updated results.csv
    
    '''

