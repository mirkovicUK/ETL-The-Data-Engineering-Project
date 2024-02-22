import awswrangler as wr
from awswrangler import _utils
pg8000_native = _utils.import_optional_dependency("pg8000.native")
from pg8000.native import literal, identifier, DatabaseError
import datetime
from decimal import Decimal
import logging

logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)

def get_fact_sales_order(con, time_of_last_query):
    """
    Args:
        param1: pg8000 conection obj
        param2: time of last db query

    Returns:
        {'fact_sales_order':[data_pooint1, data_point2...]}

    Raises:
        KeyError: Does not raises an exception.

    Logs:
        Logs error to cloud watch 
    """
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

    

if __name__ == "__main__":
    print(*get_fact_sales_order(wr.postgresql.connect(secret_id = "totesys_db"), 
                               datetime.datetime.strptime('0001-02-16 18:32:09.709000', '%Y-%m-%d %H:%M:%S.%f'))['fact_sales_order'], sep='\n')