import awswrangler as wr
from awswrangler import _utils
pg8000_native = _utils.import_optional_dependency("pg8000.native")
from pg8000.native import literal, identifier, DatabaseError
from datetime import datetime as dt
from decimal import Decimal
import logging
import math


def write_to_fact_sales_order(con, data):
    for data_point in data:
        for key, value in data_point.items():
            if key == "agreed_delivery_date":
                data_point[key] = dt.strptime(value, '%Y-%m-%d').date()
            elif key == "agreed_payment_date":
                data_point[key] = dt.strptime(value, '%Y-%m-%d').date()
            elif key == "created_date":
                data_point[key] = dt.strptime(value, '%Y-%m-%d').date()
            elif key == "created_time":
                data_point[key] = dt.strptime(value, '%H:%M:%S.%f').time()
            elif key == "last_updated_date":
                data_point[key] = dt.strptime(value, '%Y-%m-%d').date()
            elif key == "last_updated_time":
                data_point[key] = dt.strptime(value, '%H:%M:%S.%f').time()


#dim_date
##############################################################################
        
        dim_date_columns = ['date_id', 'year', 'month', 'day', 'day_of_week', 'day_name', 
                            'month_name', 'quarter', 'last_updated_date', 
                            'last_updated_time']
        updated = dt.now()
        dim_date_values  = [data_point['last_updated_date'],
                            data_point['last_updated_date'].year,
                            data_point['last_updated_date'].month,
                            data_point['last_updated_date'].day,
                            data_point['last_updated_date'].weekday()+1,
                            data_point['last_updated_date'].strftime("%A"),
                            data_point['last_updated_date'].strftime("%B"),
                            math.ceil(data_point['last_updated_date'].month /3.),
                            updated.date(),
                            updated.time()
                            ]
        dim_date_query = f"""
                    INSERT INTO dim_date 
                    ({identifier(dim_date_columns[0])},{identifier(dim_date_columns[1])},{identifier(dim_date_columns[2])},
                    {identifier(dim_date_columns[3])},{identifier(dim_date_columns[4])},{identifier(dim_date_columns[5])},
                    {identifier(dim_date_columns[6])},{identifier(dim_date_columns[7])},{identifier(dim_date_columns[8])},
                    {identifier(dim_date_columns[9])})
                    VALUES
                    ({literal(dim_date_values[0])},{literal(dim_date_values[1])},{literal(dim_date_values[2])},
                    {literal(dim_date_values[3])},{literal(dim_date_values[4])},{literal(dim_date_values[5])},
                    {literal(dim_date_values[6])},{literal(dim_date_values[7])},{literal(dim_date_values[8])},
                    {literal(dim_date_values[9])})
                    ;
                    """
        

#dim_staff
##############################################################################

        
        dim_staf_column = ['staff_record_id', 'first_name', 'last_name', 'department_name',
                           'location', 'email_address', 'last_updated_date', 'last_updated_time']
        
        dim_staff_query = f"""
                    INSERT INTO dim_staff
                    (staff_record_id, staff_id, first_name, last_name, department_name,
                    location, email_address, last_updated_date, last_updated_time)
                    VALUES
                    (20, 20, 'Ur', 'Mir', 'DE', 'Lon', 'something@gmail.com', {literal(updated.date())}, {literal(updated.time())});
                    """
        
#dim_counterparty
###############################################################################################
        dim_counterparty_column = ['counterparty_record_id', 'counterparty_id', 'counterparty_legal_name',
                                   'counterparty_legal_address_line_1', 'counterparty_legal_address_line_2',
                                   'counterparty_legal_district', 'counterparty_legal_city', 'counterparty_legal_postal_code',
                                   'counterparty_legal_country', 'counterparty_legal_phone_number', 'last_updated_date',
                                   'last_updated_time']
        
        dim_counterparty_query = f"""
                        INSERT INTO dim_counterparty
                        (counterparty_record_id, counterparty_id, counterparty_legal_name, counterparty_legal_address_line_1,
                        counterparty_legal_address_line_2, counterparty_legal_district, counterparty_legal_city, 
                        counterparty_legal_postal_code,counterparty_legal_country, counterparty_legal_phone_number, 
                        last_updated_date,last_updated_time)
                        VALUES
                        (12, 20, 'UR', 'BAR bb', 'something', 
                        'BROMLEY', 'LON', 'SEYYY', 'UK','22222',{literal(updated.date())}, {literal(updated.time())});
                        """
        

#fact_sales_order
###############################################################################################
        fact_sales_order_columns = ['sales_order_id', 'created_date', 'created_time', 
                   'last_updated_date', 'last_updated_time', 'sales_staff_id', 
                   'counterparty_record_id', 'units_sold', 'unit_price', 
                   'currency_record_id', 'design_record_id', 'agreed_payment_date', 
                   'agreed_delivery_date', 'agreed_delivery_location_id']
        values = [
            6882,
            dt.strptime("2024-02-20", '%Y-%m-%d').date(),
            dt.strptime("15:31:10.295000", '%H:%M:%S.%f').time(),
            dt.strptime("2024-02-20", '%Y-%m-%d').date(),
            dt.strptime("15:31:10.295000", '%H:%M:%S.%f').time(),
            20,
            12,
            44148,
            3.74,
            1,
            19,
            dt.strptime("2024-02-20", '%Y-%m-%d').date(),
            dt.strptime("2024-02-20", '%Y-%m-%d').date(),
            28]
        
        fact_sales_order_query = f"""INSERT INTO fact_sales_order
                    (sales_order_id, 
                    created_date,
                    created_time, 
                    last_updated_date, 
                    last_updated_time, 
                    sales_staff_id, 
                    counterparty_record_id, 
                    units_sold, 
                    unit_price, 
                    currency_record_id,
                    design_record_id, 
                    agreed_payment_date, 
                    agreed_delivery_date, 
                    agreed_delivery_location_id)
                VALUES ({literal(values[0])},{literal(values[1])},{literal(values[2])},
                    {literal(values[3])},{literal(values[4])},{literal(values[5])},
                    {literal(values[6])},{literal(values[7])},{literal(values[8])},
                    {literal(values[9])},{literal(values[10])},{literal(values[11])},
                    {literal(values[12])},{literal(values[13])});"""

#dim_currency
###########################################################################################
        dim_currency_colums = ['currency_record_id', 'currency_id', 'currency_code', 
                           'currency_name', 'last_updated_date', 'last_updated_time']

        dim_currency_query = f"""INSERT INTO dim_currency
                                VALUES
                                (1, 1, 'usd', 'dolar', {literal(updated.date())}, {literal(updated.time())});
                            """

#dim_design
###########################################################################################
        dim_design_columns = ['design_record_id', 'design_id', 'design_name', 'file_location', 
                              'file_name', 'last_updated_date', 'last_updated_time']

        dim_design_query = f"""
                            INSERT INTO dim_design
                            VALUES
                            (19, 19, 'name', 'location/../', 'file_name.json',{literal(updated.date())}, {literal(updated.time())});
                            """

#dim_location
#######################################################################################
        dim_location_columns = ['location_record_id', 'address_id', 'address_line_1', 
                                'address_line_2', 'district', 'city', 'postal_code', 
                                'country', 'phone', 'last_updated_date', 'last_updated_time']

        dim_location_query = f"""
                            INSERT INTO dim_location
                            VALUES
                            (28,28,'address1','address2','district','city', 'posta_code',
                            'country', 'phone',{literal(updated.date())}, {literal(updated.time())});
                            """

########################################################################################
        
        rows = con.run("""SELECT *
            FROM information_schema.columns
            WHERE table_schema = 'project_team_5'
            AND table_name   = 'dim_location';""")
        print([row[3] for row in rows])
        

        con.run(dim_date_query)
        con.run(dim_staff_query)
        con.run(dim_counterparty_query)
        con.run(dim_currency_query)
        con.run(dim_design_query)
        con.run(dim_location_query)
        con.run(fact_sales_order_query)
    print()
    print(con.run("SELECT * FROM dim_date;"), '<----------DIM_DATE')
    print(con.run("SELECT * FROM fact_sales_order;"), '<----------FACT SALES ORDER')
    print(con.run("SELECT * FROM dim_staff;"), '<----------DIM_STAFF')
    print(con.run("SELECT * FROM dim_counterparty;"), '<----------DIM_COUNTERPARTY')
    print(con.run("SELECT * FROM dim_currency;"), '<----------DIM_CURRENCY')
    print(con.run("SELECT * FROM dim_design;"), '<----------DIM_Design')
    print(con.run("SELECT * FROM dim_location ;"), '<----------DIM_LOCATION') 



    
