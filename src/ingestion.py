from pg8000.native import Connection, literal, identifier, DatabaseError
import logging
import boto3
import boto3
from botocore.exceptions import ClientError, ParamValidationError
import json
import datetime
import time
from decimal import Decimal

import awswrangler as wr
from awswrangler import _utils
import json

pg8000 = _utils.import_optional_dependency("pg8000")
pg8000_native = _utils.import_optional_dependency("pg8000.native")


logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)

DB = "new_tote"


def read_ingested_bucket_name():
    s3 = boto3.client('s3')
    bucket_name = "terraform-12345"
    object_key = "tf-state"
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    data = json.loads(response['Body'].read().decode('utf-8'))
    ingested_bucket_name = data["outputs"]["ingested_bucket"]["value"]

    return ingested_bucket_name


INGESTION_BUCKET = read_ingested_bucket_name()
bucket_name = INGESTION_BUCKET


def ingestion(event, context):
    """
    Args:
        param1: aws event obj
        param2: aws context obj

    Returns:
        None, 
        write JSON object to s3

    Raises:
        RuntimeError

    Logs:
        InvalidConnection: logs warning to CloudWatch
        ParamValidationError: logs error to CloudWatch
        ClientError: logs error to CloudWatch

    Lambda that query db and manipulate data,
    write JSON into ingestion zone
    """

    try:
        con = wr.postgresql.connect(secret_id=DB)
        if not isinstance(con, pg8000.Connection):
            raise InvalidConnection()

        time_of_last_query = get_time_of_last_query()
        set_time_of_the_last_query(datetime.datetime.now())
        bucket_key = time_of_last_query.strftime('%Y-%m-%d-%H-%M-%S.%f')

        # get data for sales schema
        sales = {'sales': []}
        sales['sales'].append(get_fact_sales_order(con, time_of_last_query))
        sales['sales'].append(get_dim_location(con, time_of_last_query))
        sales['sales'].append(get_dim_staff(con, time_of_last_query))
        sales['sales'].append(get_counterparty(con, time_of_last_query))
        sales['sales'].append(get_dim_currency(con, time_of_last_query))
        sales['sales'].append(get_dim_design(con, time_of_last_query))
        con.close()

        # ingestion write only JSON with data
        for table in sales['sales']:
            for k, v in table.items():
                if len(v) > 0:
                    put_object_into_s3_bucket(data=sales,
                                              bucket_name=INGESTION_BUCKET,
                                              key=bucket_key)
                    break
            else:
                continue
            break

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
            Key=key+'.json',
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
            Name='time',
            Value=time.strftime('%Y-%m-%d %H:%M:%S.%f'),
            Type='String',
            Overwrite=True)
    except Exception as e:
        raise RuntimeError(e)


def get_time_of_last_query():
    try:
        client = boto3.client('ssm')
        time = client.get_parameter(Name='time')['Parameter']['Value']
        return datetime.datetime.strptime(time, '%Y-%m-%d %H:%M:%S.%f')
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

        fact_sales_order = {'fact_sales_order': []}
        for row in rows:
            data_point = {}
            for ii, (k, v) in enumerate(zip(keys, row)):
                if ii == 1:
                    data_point['created_date'] = v.date()
                    data_point['created_time'] = v.time()
                elif ii == 2:
                    data_point['last_updated_date'] = v.date()
                    data_point['last_updated_time'] = v.time()
                elif ii == 4:
                    data_point['sales_staff_id'] = v
                elif ii == 7:
                    data_point[k] = Decimal(round(v, 2))
                elif ii == 9:
                    data_point[k] = datetime.datetime.strptime(
                        v, '%Y-%m-%d').date()
                elif ii == 10:
                    data_point[k] = datetime.datetime.strptime(
                        v, '%Y-%m-%d').date()
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

        dim_location = {'dim_location': []}
        for row in rows:
            data_point = {}
            for ii, (k, v) in enumerate(zip(keys, row)):
                if ii == 0:
                    data_point['location_id'] = v
                elif ii == 8 or ii == 9:
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


def get_dim_staff(con, time_of_last_query):
    try:
        query = f"""SELECT 
                       {identifier('staff')}.{identifier('staff_id')},
                        {identifier('staff')}.{identifier('first_name')},
                        {identifier('staff')}.{identifier('last_name')},
                        {identifier('department')}.{identifier('department_name')},
                        {identifier('department')}.{identifier('location')},
                        {identifier('staff')}.{identifier('email_address')},
                        {identifier('staff')}.{identifier('last_updated')},
                        {identifier('staff')}.{identifier('department_id')},
                        {identifier('department')}.{identifier('department_id')}
                    FROM {identifier('staff')}
                    LEFT JOIN {identifier('department')}
                    ON staff.department_id = department.department_id
                    WHERE staff.last_updated>{literal(time_of_last_query)};
                    """
        rows = con.run(query)

        dim_staff = {'dim_staff': []}
        for row in rows:
            data_point = {}
            for ii, value in enumerate(row):
                if ii == 0:
                    data_point['staff_record_id'] = value
                elif ii == 1:
                    data_point['first_name'] = value
                elif ii == 2:
                    data_point['last_name'] = value
                elif ii == 3:
                    data_point['department_name'] = value
                elif ii == 4:
                    data_point['location'] = value
                elif ii == 5:
                    data_point['email_address'] = value
                else:
                    pass
            dim_staff['dim_staff'].append(data_point)
        return dim_staff
    except Exception as e:
        logger.error(e)


def get_counterparty(con, time_of_last_query):
    try:
        query = f"""
                SELECT 
                    counterparty.counterparty_id,
                    counterparty.counterparty_legal_name,
                    address.address_line_1,
                    address.address_line_2,
                    address.district,
                    address.city,
                    address.postal_code,
                    address.country,
                    address.phone,
                    counterparty.legal_address_id,
                    counterparty.last_updated,
                    address.address_id
                FROM counterparty
                LEFT JOIN address 
                ON counterparty.legal_address_id = address.address_id
                WHERE counterparty.last_updated > {literal(time_of_last_query)}
                """
        rows = con.run(query)

        dim_counterparty = {'dim_counterparty': []}
        for row in rows:
            data_point = {}
            for ii, value in enumerate(row):
                if ii == 0:
                    data_point['counterparty_id'] = value
                if ii == 1:
                    data_point['counterparty_legal_name'] = value
                if ii == 2:
                    data_point['counterparty_legal_address_line_1'] = value
                if ii == 3:
                    data_point['counterparty_legal_address_line_2'] = value
                if ii == 4:
                    data_point['counterparty_legal_district'] = value
                if ii == 5:
                    data_point['counterparty_legal_city'] = value
                if ii == 6:
                    data_point['counterparty_legal_postal_code'] = value
                if ii == 7:
                    data_point['counterparty_legal_country'] = value
                if ii == 8:
                    data_point['counterparty_legal_phone_number'] = value
            dim_counterparty['dim_counterparty'].append(data_point)
        return dim_counterparty
    except Exception as e:
        logger.error(e)


def get_dim_currency(con, time_of_last_query):
    try:
        currency = {
            'GBP': 'British Pound',
            'USD': 'US Dollar',
            'EUR': 'Euro'
        }
        query = f"""
                SELECT * FROM currency
                WHERE last_updated > {literal(time_of_last_query)};
                """
        rows = con.run(query)

        dim_currency = {'dim_currency': []}
        for row in rows:
            data_point = {}
            for ii, v in enumerate(row):
                if ii == 0:
                    data_point['currency_id'] = v
                if ii == 1:
                    data_point['currency_code'] = v
                    currency_name = currency[v]
                if ii == 2:
                    data_point['currency_name'] = currency_name
            dim_currency['dim_currency'].append(data_point)
        return dim_currency
    except Exception as e:
        logger.error(e)


def get_dim_design(con, time_of_last_query):
    try:
        keys = ['design_id', 'design_name', 'file_location', 'file_name']
        query = f"""
                SELECT design_id, design_name, file_location, file_name
                FROM design
                WHERE last_updated > {literal(time_of_last_query)};
                """
        rows = con.run(query)

        dim_design = {'dim_design': []}
        for row in rows:
            data_point = {}
            for k, v in zip(keys, row):
                data_point[k] = v
            dim_design['dim_design'].append(data_point)
        return dim_design
    except Exception as e:
        logger.error(e)


if __name__ == "__main__":
    print(ingestion(None, None))
    # print(get_time_of_last_query(),'get')
    # set_time_of_the_last_query(datetime.datetime(2020, 2, 20, 18, 14, 14))
