from src.utils.get_counterparty import get_counterparty as cp
from decimal import Decimal
import pytest
import datetime
from unittest.mock import Mock

@pytest.mark.describe('get_counterparty()')
@pytest.mark.it('query select all data points after given date')
def test_seletc_data_after_given_date():
    con = Mock()
    query = tuple([[4, 'Kohler Inc', '37736 Heathcote Lock', 'Noemy Pines',
                    None, 'Bartellview', '42400-5199', 'Congo', 
                    '1684 702261', 29, datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), 29],
                    [5, 'Frami, Yundt and Macejkovic', '364 Goodwin Streets',
                      None, None, 'Sayreville', '85544-4254', 
                      'Svalbard & Jan Mayen Islands', '0847 468066', 22, 
                      datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), 22]])
    con.run.return_value=query
    time_of_last_query = datetime.datetime.strptime('2024-2-15 10:44:10.192011', '%Y-%m-%d %H:%M:%S.%f')
    data = cp(con, time_of_last_query)
    assert len(data['dim_counterparty']) == 2


@pytest.mark.describe('get_counterparty()')
@pytest.mark.it('function returns dict with key of dim_location')
def test_function_returns_dict_with_correct_key():
    con = Mock()
    query = tuple([[4, 'Kohler Inc', '37736 Heathcote Lock', 'Noemy Pines',
                    None, 'Bartellview', '42400-5199', 'Congo', 
                    '1684 702261', 29, datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), 29],
                    [5, 'Frami, Yundt and Macejkovic', '364 Goodwin Streets',
                      None, None, 'Sayreville', '85544-4254', 
                      'Svalbard & Jan Mayen Islands', '0847 468066', 22, 
                      datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), 22]])
    con.run.return_value=query
    time_of_last_query = datetime.datetime.strptime('2024-2-15 10:44:10.192011', '%Y-%m-%d %H:%M:%S.%f')
    data = cp(con, time_of_last_query)
    assert 'dim_counterparty' in data


@pytest.mark.describe('get_counterparty()')
@pytest.mark.it('function returns dict')
def test_function_returns_dict():
    con = Mock()
    query = tuple([[4, 'Kohler Inc', '37736 Heathcote Lock', 'Noemy Pines',
                    None, 'Bartellview', '42400-5199', 'Congo', 
                    '1684 702261', 29, datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), 29],
                    [5, 'Frami, Yundt and Macejkovic', '364 Goodwin Streets',
                      None, None, 'Sayreville', '85544-4254', 
                      'Svalbard & Jan Mayen Islands', '0847 468066', 22, 
                      datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), 22]])
    con.run.return_value=query
    time_of_last_query = datetime.datetime.strptime('2024-2-15 10:44:10.192011', '%Y-%m-%d %H:%M:%S.%f')
    data = cp(con, time_of_last_query)
    assert isinstance(data, dict)


@pytest.mark.describe('get_counterparty()')
@pytest.mark.it('function returns data on correct keys')
def test_function_returns_data_on_correct_key():
    con = Mock()
    query = tuple([[4, 'Kohler Inc', '37736 Heathcote Lock', 'Noemy Pines',
                    None, 'Bartellview', '42400-5199', 'Congo', 
                    '1684 702261', 29, datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), 29],
                    [5, 'Frami, Yundt and Macejkovic', '364 Goodwin Streets',
                      None, None, 'Sayreville', '85544-4254', 
                      'Svalbard & Jan Mayen Islands', '0847 468066', 22, 
                      datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), 22]])
    con.run.return_value=query
    time_of_last_query = datetime.datetime.strptime('2024-2-15 10:44:10.192011', '%Y-%m-%d %H:%M:%S.%f')
    data = cp(con, time_of_last_query)
    keys = ['counterparty_id', 'counterparty_legal_name', 'counterparty_legal_address_line_1',
            'counterparty_legal_address_line_2', 'counterparty_legal_district', 
            'counterparty_legal_city', 'counterparty_legal_postal_code', 'counterparty_legal_country',
            'counterparty_legal_phone_number']
    for x in keys:
        assert x in data['dim_counterparty'][0]