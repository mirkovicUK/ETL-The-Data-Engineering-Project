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

s3_procesed_zone_url = 's3://processed-zone-895623xx35/'
DB = 'DB_write'
def parquet_to_json(event, context):
    try:
         
        s3_bucket_name, s3_object_name = get_object_path(event['Records'])
        if s3_object_name[-7:] != 'parquet':
                 raise InvalidFileTypeError 

        s3 = boto3.client('s3', region_name='eu-west-2')
        df = wr.s3.read_parquet(path=s3_procesed_zone_url+s3_object_name)
        json = df.to_json(orient="split")
        logger.info(json)
        logger.info('Parquet converted to json logget to CloudWatch')

        secret = get_secret(DB)
        con = Connection(secret['username'], host = secret['host'], 
                         database = secret['dbname'],password = secret['password'])
        if not isinstance(con, pg8000.Connection):
            raise InvalidConnection()
        
        last_update = dt.now()
        #writing data
        write_dim_staff(con, json['data'][2]['dim_staff'], last_update)
        write_dim_counterparty(con, json['data'][3]['dim_counterparty'], last_update)
        
        

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