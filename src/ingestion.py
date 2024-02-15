import logging
import boto3
from botocore.exceptions import ClientError, ParamValidationError
import json 
import datetime 

import awswrangler as wr
from awswrangler import _utils

pg8000 = _utils.import_optional_dependency("pg8000")
pg8000_native = _utils.import_optional_dependency("pg8000.native")
from pg8000.native import Connection, literal, identifier, DatabaseError


logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)

DB_credentials = "dtotesys_db"


def ingestion(event, context):

    logger.info(event)
    logger.info(context)
    logger.info(DB_credentials)

    try:
        con = wr.postgresql.connect(secret_id = DB_credentials)
        if not isinstance(con, pg8000.Connection):
            raise InvalidConnection()
    


    except InvalidConnection:
        logger.warning('Not pg8000 connection')

    except ParamValidationError as e:
        logger.error(e)

    except ClientError as e:
        logger.error(e)

    except Exception as e:
        logger.error(e)
        raise RuntimeError
    
   



    # table = 'sales_order'

    # df = wr.postgresql.read_sql_table(
    # table=table
    # schema="public",
    # con=con
    # )
    # print(df)

    # query = f"SELECT * FROM {identifier(table)};"
    # rows = con.run(query)
    # one_data_point = rows[0]
    # print(one_data_point)
    # created_at = one_data_point[1]
    # print(created_at)

    # con.close()
    

    # data_dict = df.to_dict(orient='records')
        



class InvalidConnection(Exception):
    """Traps error where db connection is not pg8000."""
    pass

if __name__ == "__main__":
    ingestion('s','s')