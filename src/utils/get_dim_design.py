import datetime
from decimal import Decimal
import logging

import awswrangler as wr
from awswrangler import _utils
pg8000_native = _utils.import_optional_dependency("pg8000.native")
from pg8000.native import literal, identifier, DatabaseError


logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)

def get_dim_design(con, time_of_last_query):
    """
    Args:
        param1: pg8000 conection obj
        param2: time of last db query
    Returns:
        {'dim_design':[data_pooint1, data_point2...]}
    Raises:
        Does not raises an exception.
    Logs:
        Logs error to cloud watch 
    """
    try:
        keys = ['design_id', 'design_name', 'file_location', 'file_name']
        query =f"""
                SELECT design_id, design_name, file_location, file_name
                FROM design
                WHERE last_updated > {literal(time_of_last_query)}
                ;
                """
        rows = con.run(query)

        dim_design={'dim_design':[]}
        for row in rows:
            data_point={}
            for k,v in zip(keys, row):
                data_point[k]=v
            dim_design['dim_design'].append(data_point)
        return dim_design
    except Exception as e:
        logger.error(e)


if __name__ == "__main__":
    print(*get_dim_design(wr.postgresql.connect(secret_id = "totesys_db"), 
            datetime.datetime.strptime('2024-02-02 18:32:09.709000',
                                       '%Y-%m-%d %H:%M:%S.%f'))['dim_design'], sep='\n\n')