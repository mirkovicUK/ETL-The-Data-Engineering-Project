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
def json_to_parquet(event, context):
    
    try:
         
         s3_bucket_name, s3_object_name = get_object_path(event['Records'])
         logger.info(f'Bucket is {s3_bucket_name}')
         logger.info(f'Object key is {s3_object_name}')

         if s3_object_name[-4:] != 'json':
                 raise InvalidFileTypeError 

         s3 = boto3.client('s3')
         logger.info('THIS IS B4 GET TEXT FROM FILE')
         data_json = get_text_from_file(s3, s3_bucket_name, s3_object_name)
         json_data = json.loads(data_json)
         logger.info('JSON DATA INSIDE LA')
         df = pd.DataFrame.from_records(json_data, index=[0])
         ct = datetime.datetime.now()
         ts = str(ct.timestamp())
         #Hard coded s3 bucket, change to new bucket name after every build
         s3_url = 's3://processed-zone-895623xx35/' 
         s3_url += ts +'.parquet' 
         logger.info('B4 CALL TO_PARQUET')
         wr.s3.to_parquet(df=df, path=s3_url, index=False)
         logger.info('JSON converted to parquet and logged to CloudWatch CHEEARS')
         
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