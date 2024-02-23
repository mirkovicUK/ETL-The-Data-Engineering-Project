from datetime import datetime as dt
from datetime import timedelta
import math

from src.utils.writing_utils.get_secret import get_secret 

from awswrangler import _utils
pg8000 = _utils.import_optional_dependency("pg8000")
from pg8000.native import Connection, literal, identifier, DatabaseError


def write_fact_sales_order(con, data, updated=dt.now()):
    fact_sales_order_columns = [
        'sales_record_id', 'sales_order_id', 'created_date', 
        'created_time', 'last_updated_date', 'last_updated_time',
        'sales_staff_id', 'counterparty_record_id', 'units_sold',
        'unit_price', 'currency_record_id', 'design_record_id',
        'agreed_payment_date', 'agreed_delivery_date', 
        'agreed_delivery_location_id']

    for data_point in data:
        values = [
            data_point['sales_order_id'],
            data_point['sales_order_id'],
            data_point['created_date'],
            data_point['created_time'],
            updated.date(),
            updated.time(),
            data_point['sales_staff_id'],
            data_point['counterparty_id'],
            data_point['units_sold'],
            data_point['unit_price'],
            data_point['currency_id'],
            data_point['design_id'],
            data_point['agreed_payment_date'],
            data_point['agreed_delivery_date'],
            data_point['agreed_delivery_location_id'],
        ]
        fact_sales_order_query = f"""
            INSERT INTO fact_sales_order
            
            VALUES
            ({literal(values[0])},{literal(values[1])},{literal(values[2])},
            {literal(values[3])},{literal(values[4])},{literal(values[5])},
            {literal(values[6])},{literal(values[7])},{literal(values[8])},
            {literal(values[9])},{literal(values[10])},{literal(values[11])},
            {literal(values[12])},{literal(values[13])},{literal(values[14])})
            ON CONFLICT DO NOTHING;"""
        print(fact_sales_order_query)
        con.run(fact_sales_order_query)