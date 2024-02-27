from src.utils.get_dim_currency import get_dim_currency as gc
from decimal import Decimal
import pytest
import datetime
from unittest.mock import Mock


@pytest.mark.describe('get_dim_currency()')
@pytest.mark.it('query select all data points after given date')
def test_seletc_data_after_given_date():
    con = Mock()
    query = tuple([[1, 'GBP', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
                    datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)],
                   [2, 'USD', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
                    datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)]])
    con.run.return_value = query
    time_of_last_query = datetime.datetime.strptime(
        '2024-2-15 10:44:10.192011', '%Y-%m-%d %H:%M:%S.%f')
    data = gc(con, time_of_last_query)
    assert len(data['dim_currency']) == 2


@pytest.mark.describe('get_dim_currency()')
@pytest.mark.it('function returns dict with key of dim_location')
def test_function_returns_dict_with_correct_key():
    con = Mock()
    query = tuple([[1, 'GBP', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
                    datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)],
                   [2, 'USD', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
                    datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)]])
    con.run.return_value = query
    time_of_last_query = datetime.datetime.strptime(
        '2024-2-15 10:44:10.192011', '%Y-%m-%d %H:%M:%S.%f')
    data = gc(con, time_of_last_query)
    assert 'dim_currency' in data


@pytest.mark.describe('get_dim_currency()')
@pytest.mark.it('function returns dict')
def test_function_returns_dict():
    con = Mock()
    query = tuple([[1, 'GBP', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
                    datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)],
                   [2, 'USD', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
                    datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)]])
    con.run.return_value = query
    time_of_last_query = datetime.datetime.strptime(
        '2024-2-15 10:44:10.192011', '%Y-%m-%d %H:%M:%S.%f')
    data = gc(con, time_of_last_query)
    assert isinstance(data, dict)


@pytest.mark.describe('get_dim_currency()')
@pytest.mark.it('function returns data on correct keys')
def test_function_returns_data_on_correct_key():
    con = Mock()
    query = tuple([[1, 'GBP', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
                    datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)],
                   [2, 'USD', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
                    datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)]])
    con.run.return_value = query
    time_of_last_query = datetime.datetime.strptime(
        '2024-2-15 10:44:10.192011', '%Y-%m-%d %H:%M:%S.%f')
    data = gc(con, time_of_last_query)
    keys = ['currency_id', 'currency_code', 'currency_name']
    for x in keys:
        assert x in data['dim_currency'][0]
