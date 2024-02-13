import logging
import boto3
from botocore.exceptions import ClientError
import json 
import awswrangler as wr
import pandas as pd
import datetime 

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)
logger.info('THIS IS B4 EVERITHING')

def parquet_to_json(event, context):
    
    try:
         
         s3_bucket_name, s3_object_name = get_object_path(event['Records'])
         logger.info(f'Bucket is {s3_bucket_name}')
         logger.info(f'Object key is {s3_object_name}')

         if s3_object_name[-7:] != 'parquet':
                 raise InvalidFileTypeError 

         s3 = boto3.client('s3')
         logger.info('THIS IS B4 GET TEXT FROM FILE')
         df = wr.s3.read_parquet(path='s3://'+s3_bucket_name + '/'+s3_object_name)        
         logger.info('Parquet DATA INSIDE LA')
         ct = datetime.datetime.now()
         ts = str(ct.timestamp())
         s3_url = 's3://PROCESZONE/'
         s3_url += ts +'.json' 
         logger.info('B4 CALL TO_PARQUET')
         json = df.to_json(orient="split")
         logger.info(json)
         logger.info('Parquet converted to json uploaded to s3 bucket CHEEARS')
         
    except KeyError as k:
        logger.error(f'Error retrieving data, {k}')
    except ClientError as c:
        if c.response['Error']['Code'] == 'NoSuchKey':
            logger.error(f'No object found - {s3_object_name}')
        elif c.response['Error']['Code'] == 'NoSuchBucket':
            logger.error(f'No such bucket - {s3_bucket_name}')
        else:
            raise
    except UnicodeError:
        logger.error(f'File {s3_object_name} is not a valid text file')
    except InvalidFileTypeError:
        logger.error(f'File {s3_object_name} is not a valid text file')
    except Exception as e:
        logger.error(e)
        raise RuntimeError
    

def get_object_path(records):
    """Extracts bucket and object references from Records field of event."""
    return records[0]['s3']['bucket']['name'], \
    records[0]['s3']['object']['key']


def get_text_from_file(client, bucket, object_key):
    """Reads text from specified file in S3."""
    data = client.get_object(Bucket=bucket, Key=object_key)
    contents = data['Body'].read()
    return contents.decode('utf-8')

class InvalidFileTypeError(Exception):
    """Traps error where file type is not txt."""
    pass