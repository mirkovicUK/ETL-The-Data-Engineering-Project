import datetime
from decimal import Decimal
import logging

import awswrangler as wr
from awswrangler import _utils
pg8000_native = _utils.import_optional_dependency("pg8000.native")
from pg8000.native import literal, identifier, DatabaseError


logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)

def get_dim_currency(con, time_of_last_query):
    """
    Args:
        param1: pg8000 conection obj
        param2: time of last db query
    Returns:
        {'counterparty':[data_pooint1, data_point2...]}
    Raises:
        Does not raises an exception.
    Logs:
        Logs error to cloud watch 
    """
    try:
        currency ={
            'GBP' : 'British Pound',
            'USD' : 'US Dollar',
            'EUR' : 'Euro'
        }
        query =f"""
                SELECT * FROM currency
                WHERE last_updated > {literal(time_of_last_query)};
                """
        rows = con.run(query)

        dim_currency = {'dim_currency':[]}
        for row in rows:
            data_point={}
            for ii, v in enumerate(row):
                if ii==0:
                    data_point['currency_id'] = v
                if ii==1:
                    data_point['currency_code'] = v
                    currency_name = currency[v]
                if ii==2:
                    data_point['currency_name'] = currency_name
            dim_currency['dim_currency'].append(data_point) 
        return dim_currency
    except Exception as e:
        logger.error(e)


if __name__ == "__main__":
    print(*get_dim_currency(wr.postgresql.connect(secret_id = "totesys_db"), 
            datetime.datetime.strptime('2020-09-10 18:32:09.709000',
                                       '%Y-%m-%d %H:%M:%S.%f'))['dim_currency'], sep='\n\n')