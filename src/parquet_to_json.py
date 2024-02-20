import logging
import boto3
from botocore.exceptions import ClientError
import json 
import awswrangler as wr
import pandas as pd
import datetime
from awswrangler import exceptions

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)
logger.info('THIS IS B4 EVERITHING')

s3_procesed_zone_url = 's3://processed-zone-895623xx35/'

def parquet_to_json(event, context):
    try:
         
         
        s3_bucket_name, s3_object_name = get_object_path(event['Records'])
        if s3_object_name[-7:] != 'parquet':
                 raise InvalidFileTypeError 

        s3 = boto3.client('s3', region_name='eu-west-2')
        df = wr.s3.read_parquet(path=s3_procesed_zone_url+s3_object_name)
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
    except exceptions.NoFilesFound as e:
        logger.error(e)
        raise(e)
    except UnicodeError:
        logger.error(f'File {s3_object_name} is not a valid parquet file')
    except InvalidFileTypeError:
        logger.error(f'File {s3_object_name} is not a valid parquet file')
    except Exception as e:
        logger.error(e)
        raise RuntimeError
    

def get_object_path(records):
    return records[0]['s3']['bucket']['name'], \
    records[0]['s3']['object']['key']


def get_text_from_file(client, bucket, object_key):
    data = client.get_object(Bucket=bucket, Key=object_key)
    contents = data['Body'].read()
    return contents.decode('utf-8')

class InvalidFileTypeError(Exception):
    """Traps error where file type is not txt."""
    pass