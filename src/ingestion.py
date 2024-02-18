import logging
import boto3
from botocore.exceptions import ClientError, ParamValidationError
import json 
import datetime 
from decimal import Decimal

import awswrangler as wr
from awswrangler import _utils

pg8000 = _utils.import_optional_dependency("pg8000")
pg8000_native = _utils.import_optional_dependency("pg8000.native")
from pg8000.native import Connection, literal, identifier, DatabaseError


logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)

DB = "totesys_db"
INGESTION_BUCKET = 'ingestion-zone-895623xx35'


def ingestion(event, context):
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
        con = wr.postgresql.connect(secret_id = DB)
        if not isinstance(con, pg8000.Connection):
            raise InvalidConnection()

        time_of_last_query = get_time_of_last_query()
        set_time_of_the_last_query(datetime.datetime.now())
        bucket_key = time_of_last_query.strftime('%Y-%m-%d-%H-%M-%S.%f')

        #get data  
        sales={'sales':[]}
        sales['sales'].append(get_fact_sales_order(con, time_of_last_query))
        sales['sales'].append(get_dim_location(con,time_of_last_query))


        con.close()

        ###############################################
        #ADD CHECKS FOR EMPTY DICT , NO DATA NO WRITINIG IN S3 <<<------ TOMOROW
        #############################################
        print(sales)
        put_object_into_s3_bucket(data=sales,
                                  bucket_name=INGESTION_BUCKET,
                                  key=bucket_key)
        

    except InvalidConnection:
        logger.warning('Not pg8000 connection')
    except ParamValidationError as e:
        logger.error(e)
    except ClientError as e:
        logger.error(e)
    except Exception as e:
        logger.error(e)
        raise RuntimeError
    

def put_object_into_s3_bucket(data, bucket_name, key):

    try:
        s3 = boto3.client('s3')
        s3.put_object(
            Body=json.dumps(data, indent=2, default=str),
            Bucket=bucket_name,
            Key = key+'.json',
            )
    except Exception as e:
        raise RuntimeError(e)
    except ClientError as e:
        logging.error(e.response['Error']['Message'])
        raise ClientError(e)
    

def set_time_of_the_last_query(time):
    try: 
        client = boto3.client('ssm')
        client.put_parameter(
        Name = 'time',
        Value=time.strftime('%Y-%m-%d %H:%M:%S.%f'),
        Type='String',
        Overwrite=True)
    except Exception as e:
        raise RuntimeError(e)
    

def get_time_of_last_query():
    try:
        client = boto3.client('ssm')
        time = client.get_parameter(Name='time')['Parameter']['Value']
        return datetime.datetime.strptime(time , '%Y-%m-%d %H:%M:%S.%f')
    except Exception as e:
        raise RuntimeError(e)


def get_fact_sales_order(con, time_of_last_query):
    try:
        table = 'sales_order'
        keys = ['sales_order_id', 'created_at', 'last_updated',
                'design_id', 'staff_id', 'counterparty_id', 'units_sold',
                'unit_price', 'currency_id', 'agreed_delivery_date',
                'agreed_payment_date', 'agreed_delivery_location_id']

        query = f"""SELECT * FROM {identifier(table)} 
                WHERE last_updated>{literal(time_of_last_query)};"""
        rows = con.run(query)

        fact_sales_order={'fact_sales_order':[]}
        for row in rows:
            data_point = {}
            for ii,(k,v) in enumerate(zip(keys, row)):
                if ii==1:
                    data_point['created_date'] = v.date()
                    data_point['created_time'] = v.time()
                elif ii==2:
                    data_point['last_updated_date'] = v.date()
                    data_point['last_updated_time'] = v.time()
                elif ii==4:
                    data_point['sales_staff_id'] = v
                elif ii==7:
                    data_point[k] = Decimal(round(v,2))
                elif ii==9:
                    data_point[k]=datetime.datetime.strptime(v,'%Y-%m-%d').date()
                elif ii==10:
                    data_point[k]=datetime.datetime.strptime(v,'%Y-%m-%d').date()
                else:
                    data_point[k] = v
            fact_sales_order['fact_sales_order'].append(data_point)
        return fact_sales_order
    except Exception as e:
        logger.error(e)
        
def get_dim_location(con, time_of_last_query):
    try:
        table = 'address'
        keys = ['address_id', 'address_line_1', 'address_line_2',
                'district', 'city', 'postal_code', 'country', 'phone',
                'created_at', 'last_updated']
        query = f"""SELECT * FROM {identifier(table)} 
                WHERE last_updated>{literal(time_of_last_query)};"""
        rows = con.run(query)

        dim_location={'dim_location':[]}
        for row in rows:
            data_point={}
            for ii,(k,v) in enumerate(zip(keys,row)):
                if ii==0:
                    data_point['location_id'] = v
                elif ii==8 or ii==9:
                    pass
                else:
                    data_point[k] = v
            dim_location['dim_location'].append(data_point)
        return dim_location
    except Exception as e:
        logger.error(e)


class InvalidConnection(Exception):
    """Traps error where db connection is not pg8000."""
    pass

if __name__ == "__main__":
    print(ingestion(None, None))