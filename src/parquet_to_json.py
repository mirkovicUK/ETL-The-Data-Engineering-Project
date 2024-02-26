import logging
import boto3
from botocore.exceptions import ClientError
import json 
import pandas as pd
from datetime import datetime as dt

from awswrangler import exceptions
import awswrangler as wr
from awswrangler import _utils

pg8000 = _utils.import_optional_dependency("pg8000")
pg8000_native = _utils.import_optional_dependency("pg8000.native")
from pg8000.native import Connection, literal, identifier, DatabaseError


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

s3_procesed_zone_url = read_processed_bucket_name()




def read_ingested_bucket_name():
    s3 = boto3.client('s3')
    bucket_name = "terraform-12345" 
    object_key = "tf-state"   
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    data = json.loads(response['Body'].read().decode('utf-8'))
    ingested_bucket_name = data["outputs"]["ingested_bucket"]["value"]
           
    return ingested_bucket_name
INGESTION_BUCKET = read_ingested_bucket_name()
s3_bucket_name= INGESTION_BUCKET



DB = 'data_warehouse'

def parquet_to_json(event, context):
    try:
         
        s3_bucket_name, s3_object_name = get_object_path(event['Records'])
        if s3_object_name[-7:] != 'parquet':
                 raise InvalidFileTypeError 

        s3 = boto3.client('s3', region_name='eu-west-2')
        df = wr.s3.read_parquet(path=s3_procesed_zone_url+s3_object_name)
        json_str = df.to_json(orient="split")
        json_obj = json.loads(json_str)

        secret = get_secret(DB)
        con = Connection(secret['username'], host = secret['host'], 
                         database = secret['dbname'],password = secret['password'])
        
        
        last_update = dt.now()
        #writing data
        write_dim_staff(con, json_obj['data'][2][0]['dim_staff'], last_update)
        write_dim_counterparty(con, json_obj['data'][3][0]['dim_counterparty'], last_update)
        write_dim_currency(con, json_obj['data'][4][0]['dim_currency'], last_update)
        write_dim_design(con, json_obj['data'][5][0]['dim_design'], last_update)

        #write to db sales
        #write_to_dim_fact_sales_order(con, json['data'][0]['fact_sales_order'])
        con.close()
         
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


class InvalidConnection(Exception):
    """Traps error where db connection is not pg8000."""
    pass


def write_dim_staff(con, data, updated=dt.now()):
    dim_staf_column = ['staff_record_id', 'first_name', 'last_name', 'department_name',
                           'location', 'email_address', 'last_updated_date', 'last_updated_time']
    for data_point in data:
        values = [
            data_point['staff_record_id'],
            data_point['staff_record_id'],
            data_point['first_name'],
            data_point['last_name'],
            data_point['department_name'],
            data_point['location'],
            data_point['email_address'],
            updated.date(),
            updated.time()
        ]

        dim_staff_query = f"""
        INSERT INTO dim_staff
        VALUES
        ({literal(values[0])},{literal(values[1])},
        {literal(values[2])},{literal(values[3])},
        {literal(values[4])},{literal(values[5])},
        {literal(values[6])},{literal(values[7])},
        {literal(values[8])})
        ON CONFLICT DO NOTHING;
        """ 
        con.run(dim_staff_query)


def get_secret(secret_name):
    secret_name = secret_name
    region_name = "eu-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
    service_name='secretsmanager',
    region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']
    return  json.loads(secret)


def write_dim_counterparty(con, data, updated=dt.now()):
    dim_counterparty_column = ['counterparty_record_id', 
    'counterparty_id', 'counterparty_legal_name',
    'counterparty_legal_address_line_1', 'counterparty_legal_address_line_2',
    'counterparty_legal_district', 'counterparty_legal_city', 'counterparty_legal_postal_code',
    'counterparty_legal_country', 'counterparty_legal_phone_number', 'last_updated_date',
    'last_updated_time']

    for data_point in data:
        values = [
            data_point['counterparty_id'],
            data_point['counterparty_id'],
            data_point['counterparty_legal_name'],
            data_point['counterparty_legal_address_line_1'],
            data_point['counterparty_legal_address_line_2'],
            data_point['counterparty_legal_district'],
            data_point['counterparty_legal_city'],
            data_point['counterparty_legal_postal_code'],
            data_point['counterparty_legal_country'],
            data_point['counterparty_legal_phone_number'],
            updated.date(),
            updated.time()
        ]

        dim_counterparty_query = f"""
        INSERT INTO dim_counterparty
        VALUES
        ({literal(values[0])},{literal(values[1])},
        {literal(values[2])},{literal(values[3])},
        {literal(values[4])},{literal(values[5])},
        {literal(values[6])},{literal(values[7])},
        {literal(values[8])},{literal(values[9])},
        {literal(values[10])},{literal(values[11])})
        ON CONFLICT DO NOTHING;
        """ 
        con.run(dim_counterparty_query)


def write_dim_currency(con, data, updated=dt.now()):
    dim_currency_colums = ['currency_record_id', 'currency_id', 'currency_code', 
                           'currency_name', 'last_updated_date', 'last_updated_time']
    for data_point in data:
        values = [
            data_point['currency_id'],
            data_point['currency_id'],
            data_point['currency_code'],
            data_point['currency_name'],
            updated.date(),
            updated.time()
        ]
        dim_courrency_query = f"""
        INSERT INTO dim_currency
        VALUES
        ({literal(values[0])},{literal(values[1])},
        {literal(values[2])},{literal(values[3])},
        {literal(values[4])},{literal(values[5])})
        ON CONFLICT DO NOTHING;
        """ 
        con.run(dim_courrency_query)


def write_dim_design(con, data, updated=dt.now()):
    dim_design_columns = [
        'design_record_id', 'design_id', 'design_name', 'file_location', 
        'file_name', 'last_updated_date', 'last_updated_time']
    for data_point in data:
        values = [
            data_point['design_id'],
            data_point['design_id'],
            data_point['design_name'],
            data_point['file_location'],
            data_point['file_name'],
            updated.date(),
            updated.time()
        ]
    
    
        dim_design_query = f"""
            INSERT INTO dim_design
            VALUES
            ({literal(values[0])},{literal(values[1])},
            {literal(values[2])},{literal(values[3])},
            {literal(values[4])},{literal(values[5])},
            {literal(values[6])})
            ON CONFLICT DO NOTHING;
            """ 
        con.run(dim_design_query)


if __name__ == "__main__":
        
    event = {'Records': [{'eventVersion': '2.1', 'eventSource': 'aws:s3', 'awsRegion': 'eu-west-2', 'eventTime': '2024-02-22T18:21:52.322Z', 'eventName': 'ObjectCreated:Put', 'userIdentity': {'principalId': 'AWS:AROAVRUVV7VBPFRJIPWUL:json_to_parquet_lambda'}, 'requestParameters': {'sourceIPAddress': '13.40.13.112'}, 'responseElements': {'x-amz-request-id': 'S0X52HRJ6KZVF6N6', 'x-amz-id-2': 'oiZ1hzwyd82P6ZqxEr7InxOBnxAHb8MDO05wbx8qqT/x3tA0lHyzmzy5zh0lum1XZQbejljwfQ7Di3ubJ3pyd7SmKC7EGOPo'}, 's3': {'s3SchemaVersion': '1.0', 'configurationId': 'tf-s3-lambda-20240215205827601100000009', 'bucket': {'name': 'processed-zone-895623xx35', 'ownerIdentity': {'principalId': 'AJQM6R72WQQXD'}, 'arn': 'arn:aws:s3:::processed-zone-895623xx35'}, 'object': {'key': '2024-02-22-18-21-48.776406.parquet', 'size': 302961, 'eTag': '2b63dfeaa14a411ad0108e19ea02c187', 'sequencer': '0065D790C03CE23C1A'}}}]}
    print(parquet_to_json(event, None))