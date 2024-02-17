from src.utils.get_fact_sales_order import get_fact_sales_order as gfso 
from decimal import Decimal
import pytest
import datetime
from unittest.mock import Mock

import awswrangler as wr
from awswrangler import _utils
pg8000_native = _utils.import_optional_dependency("pg8000.native")
from pg8000.native import literal, identifier, DatabaseError


@pytest.mark.describe('get_fact_sales_order()')
@pytest.mark.it('query select all data points after given date')
def test_seletc_data_after_given_date():
    con = Mock()
    query = tuple([[6807, datetime.datetime(2024, 2, 15, 10, 44, 10, 192000), 
                   datetime.datetime(2024, 2, 15, 10, 44, 10, 192000), 130, 20, 20, 74284, 
                   Decimal('3.60'), 3, '2024-02-18', '2024-02-19', 30],
                   [6808, datetime.datetime(2024, 2, 15, 11, 57, 10, 123000), 
                    datetime.datetime(2024, 2, 15, 11, 57, 10, 123000), 
                    288, 12, 11, 74326, Decimal('3.88'), 1, '2024-02-19', '2024-02-17', 23]
                   ])
    con.run.return_value=query
    time_of_last_query = datetime.datetime.strptime('2024-2-15 10:44:10.192011', '%Y-%m-%d %H:%M:%S.%f')
    data = gfso(con, time_of_last_query)
    assert len(data['fact_sales_order']) == 2


@pytest.mark.describe('get_fact_sales_order()')
@pytest.mark.it('function returns dict')
def test_function_returns_dict():
    con = Mock()
    query = tuple([[6807, datetime.datetime(2024, 2, 15, 10, 44, 10, 192000), 
                   datetime.datetime(2024, 2, 15, 10, 44, 10, 192000), 130, 20, 20, 74284, 
                   Decimal('3.60'), 3, '2024-02-18', '2024-02-19', 30],
                   [6808, datetime.datetime(2024, 2, 15, 11, 57, 10, 123000), 
                    datetime.datetime(2024, 2, 15, 11, 57, 10, 123000), 
                    288, 12, 11, 74326, Decimal('3.88'), 1, '2024-02-19', '2024-02-17', 23]
                   ])
    con.run.return_value=query
    time_of_last_query = datetime.datetime.strptime('2024-2-15 10:44:10.192011', '%Y-%m-%d %H:%M:%S.%f')
    data = gfso(con, time_of_last_query)
    assert isinstance(data, dict)


@pytest.mark.describe('get_fact_sales_order()')
@pytest.mark.it('function returns dict with key of fact_sales_order')
def test_function_returns_dict_with_correct_key():
    con = Mock()
    query = tuple([[6807, datetime.datetime(2024, 2, 15, 10, 44, 10, 192000), 
                   datetime.datetime(2024, 2, 15, 10, 44, 10, 192000), 130, 20, 20, 74284, 
                   Decimal('3.60'), 3, '2024-02-18', '2024-02-19', 30],
                   [6808, datetime.datetime(2024, 2, 15, 11, 57, 10, 123000), 
                    datetime.datetime(2024, 2, 15, 11, 57, 10, 123000), 
                    288, 12, 11, 74326, Decimal('3.88'), 1, '2024-02-19', '2024-02-17', 23]
                   ])
    con.run.return_value=query
    time_of_last_query = datetime.datetime.strptime('2024-2-15 10:44:10.192011', '%Y-%m-%d %H:%M:%S.%f')
    data = gfso(con, time_of_last_query)
    assert 'fact_sales_order' in data


@pytest.mark.describe('get_fact_sales_order()')
@pytest.mark.it('function returns data on corect keys')
def test_function_returns_data_on_correct_key():
    con = Mock()
    query = tuple([[6807, datetime.datetime(2024, 2, 15, 10, 44, 10, 192000), 
                   datetime.datetime(2024, 2, 15, 10, 44, 10, 192000), 130, 20, 20, 74284, 
                   Decimal('3.60'), 3, '2024-02-18', '2024-02-19', 30],
                   ])
    con.run.return_value=query
    time_of_last_query = datetime.datetime.strptime('2024-2-15 10:44:10.192011', '%Y-%m-%d %H:%M:%S.%f')
    data = gfso(con, time_of_last_query)
    keys = ['sales_order_id', 'created_date', 'created_time', 'last_updated_date',
                'design_id', 'sales_staff_id', 'counterparty_id', 'units_sold', 'last_updated_time',
                'unit_price', 'currency_id', 'agreed_delivery_date',
                'agreed_payment_date', 'agreed_delivery_location_id']
    for x in keys:
        assert x in data['fact_sales_order'][0]
   