import logging
import boto3
from botocore.exceptions import ClientError
import json 
import awswrangler as wr
import pandas as pd
import datetime 

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)

s3_procesed_zone_url = 's3://processed-zone-895623xx35/'


def json_to_parquet(event, context):
    """
    Args:
        param1: aws event obj
        param2: aws context obj

    Returns:
        JSON object 

    Raises:
        RuntimeError

    Logs:
        InvalidConnection: logs warning to CloudWatch
        ParamValidationError: logs error to CloudWatch
        ClientError: logs error to CloudWatch

    """    
    try:
         
         
         s3_bucket_name, s3_object_name = get_object_path(event['Records'])
         logger.info(f'Bucket is {s3_bucket_name}')
         logger.info(f'Object key is {s3_object_name}')

         if s3_object_name[-4:] != 'json':
                 raise InvalidFileTypeError 

         s3 = boto3.client('s3')
         data_json = get_text_from_file(s3, s3_bucket_name, s3_object_name)
         json_data = json.loads(data_json)
         df = pd.DataFrame.from_records(json_data)

         bucket_key =datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S.%f')
         s3_url = s3_procesed_zone_url
         s3_url += bucket_key +'.parquet' 

         wr.s3.to_parquet(df=df, path=s3_url, index=False)
         logger.info(f'JSON converted to parquet writen into {s3_bucket_name}')
         
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
    return records[0]['s3']['bucket']['name'], \
        records[0]['s3']['object']['key']


def get_text_from_file(client, bucket, object_key):
    """Reads text from specified file in S3."""
    data = client.get_object(Bucket=bucket, Key=object_key)
    contents = data['Body'].read()
    return contents.decode('utf-8')

class InvalidFileTypeError(Exception):
    """Traps error where file type is not .json"""
    pass