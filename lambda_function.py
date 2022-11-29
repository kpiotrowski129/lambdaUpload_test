import json
import boto3
import requests
import pandas as pd
import time
from datetime import datetime

s3 = boto3.client('s3')

def lambda_handler(event, context):
    
    file = 'dbw_api_calls.csv'
    
    df = pd.read_csv(filepath_or_buffer = file \
                     #,delimiter = '"' \
                     )
    print(df)
    
    for index, row in df.iterrows():
        
        bucket = get_df_element(row, 'bucket', True)
        print(bucket)
        environment = get_df_element(row, 'environment', True)
        print(environment)
        catalog = get_df_element(row, 'catalog', True)
        print(catalog)
        api_catalog = get_df_element(row, 'api_catalog', True)
        print(api_catalog)
        api_endpoint = get_df_element(row, 'api_endpoint', True)
        print(api_endpoint)
        api_version = get_df_element(row, 'api_version', True)
        print(api_version)
        file_extension = get_df_element(row, 'file_extension', True)
        print(file_extension)
        language = get_df_element(row, 'language', True)
        print(language)
        file_encoding = get_df_element(row, 'file_encoding', True)
        print(file_encoding)
    
        r = call_api(api_catalog, api_endpoint, api_version, language)
        fileName = get_filename(api_catalog, api_endpoint, file_extension)
        uploadByteStream = bytes(json.dumps(r, ensure_ascii=False).encode(file_encoding))
        s3.put_object(Bucket = compose_bucket_name(bucket, environment), Key = catalog + '/'+fileName, Body = uploadByteStream)
        time.sleep(3)
    

def call_api(api_catalog, api_endpoint, api_version, language):
    endpoint = ('https://api-dbw.stat.gov.pl/api/'+api_version+'/'+api_catalog+'/'+api_endpoint+'?lang='+language).replace(' ','')
    print('endpoint: '+endpoint) #debug
    r = requests.get(endpoint).json()
    return r
    
def tstamp():
    return datetime.now().strftime("%Y%m%d-%H%M%S")
    
def get_filename(api_catalog, api_endpoint, file_extension):
    fileName = api_catalog+'-'+api_endpoint+'_'+tstamp()+'.'+file_extension
    return fileName
    
def compose_bucket_name(bucket, environment):
    bn = bucket + '-'+ environment
    return bn

def get_df_element(row, elm, strip_quotes):
    re = ''
    if(strip_quotes):
        re = row[elm].replace('"','').replace(' ','')
    else:
        re = row[elm].replace(' ','')
        
    return re
    
