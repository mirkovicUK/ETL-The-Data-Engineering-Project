import datetime
from decimal import Decimal
import logging

import awswrangler as wr
from awswrangler import _utils
pg8000_native = _utils.import_optional_dependency("pg8000.native")
from pg8000.native import literal, identifier, DatabaseError

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)

def get_counterparty(con, time_of_last_query):
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
        
        dim_counterparty= {'dim_counterparty':[]}
        for row in rows:
            data_point={}
            for ii, value in enumerate(row):
                if ii==0:
                    data_point['counterparty_id'] = value
                if ii==1:
                    data_point['counterparty_legal_name'] = value
                if ii==2:
                    data_point['counterparty_legal_address_line_1'] = value
                if ii==3:
                    data_point['counterparty_legal_address_line_2'] = value
                if ii==4:
                    data_point['counterparty_legal_district'] = value
                if ii==5:
                    data_point['counterparty_legal_city'] = value
                if ii==6:
                    data_point['counterparty_legal_postal_code'] = value
                if ii==7:
                    data_point['counterparty_legal_country'] = value
                if ii==8:
                    data_point['counterparty_legal_phone_number'] = value
            dim_counterparty['dim_counterparty'].append(data_point)
        return dim_counterparty
    except Exception as e:
        logger.error(e)

if __name__ == "__main__":
    print(*get_counterparty(wr.postgresql.connect(secret_id = "totesys_db"), 
            datetime.datetime.strptime('2022-09-10 18:32:09.709000',
                                       '%Y-%m-%d %H:%M:%S.%f'))['dim_counterparty'], sep='\n\n')