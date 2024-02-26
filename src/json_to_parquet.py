import logging
import boto3
from botocore.exceptions import ClientError
import json 
import awswrangler as wr
import pandas as pd
import datetime 

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)

def read_processed_bucket_name():
    s3 = boto3.client('s3')
    bucket_name = "terraform-12345" 
    object_key = "tf-state"   
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    data = json.loads(response['Body'].read().decode('utf-8'))
    processed_bucket_name = data["outputs"]["parquet_bucket"]["value"]
           
    return f's3://{processed_bucket_name}'


# def read_ingested_bucket_name():
#     s3 = boto3.client('s3')
#     bucket_name = "terraform-12345" 
#     object_key = "tf-state"   
#     response = s3.get_object(Bucket=bucket_name, Key=object_key)
#     data = json.loads(response['Body'].read().decode('utf-8'))
#     ingested_bucket_name = data["outputs"]["ingested_bucket"]["value"]
           
#     return ingested_bucket_name
# INGESTION_BUCKET = read_ingested_bucket_name()
# s3_bucket_name= INGESTION_BUCKET

s3_procesed_zone_url = read_processed_bucket_name() 




def json_to_parquet(event, context):
    logger.info(event)
    """
    Args:
        param1: aws event obj
        param2: aws context obj

    Returns:
        None 

    Raises:
        RuntimeError
        UnicodeError
        InvalidFileTypeError
        KeyError

    Logs:
        InvalidConnection: logs warning to CloudWatch
        ParamValidationError: logs error to CloudWatch
        ClientError: logs error to CloudWatch

    Lambda that's trigered with data obj landing in s3,
    convert it to parquet and write to s3 bucket
    """    
    try:

        s3_bucket_name, s3_object_name = get_object_path(event['Records'])
        if s3_object_name[-4:] != 'json':
                raise InvalidFileTypeError 

        s3 = boto3.client('s3', region_name='eu-west-2')
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
        logger.error(f'File {s3_object_name} is not a valid json file')
    except InvalidFileTypeError:
        logger.error(f'File {s3_object_name} is not a valid json file')
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
    """Traps error where file type is not .json"""
    pass