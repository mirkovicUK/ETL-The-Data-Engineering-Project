from datetime import datetime as dt
from datetime import timedelta
import math

from src.utils.writing_utils.get_secret import get_secret 

from awswrangler import _utils
pg8000 = _utils.import_optional_dependency("pg8000")
from pg8000.native import Connection, literal, identifier, DatabaseError


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