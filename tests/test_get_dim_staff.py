from src.utils.get_dim_staff import get_dim_staff as get_staff
from decimal import Decimal
import pytest
import datetime
from unittest.mock import Mock

@pytest.mark.describe('get_dim_staff()')
@pytest.mark.it('query select all data points after given date')
def test_seletc_data_after_given_date():
    con = Mock()
    query = tuple([[19, 'Pierre', 'Sauer', 'Purchasing', 'Manchester', 
                    'pierre.sauer@terrifictotes.com', 
                    datetime.datetime(2022, 11, 3, 14, 20, 51, 563000),
                 2, 2],
                    [20, 'Flavio', 'Kulas', 'Production', 'Leeds', 
                     'flavio.kulas@terrifictotes.com', 
                     datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), 3, 3]])
    con.run.return_value=query
    time_of_last_query = datetime.datetime.strptime('2024-2-15 10:44:10.192011', '%Y-%m-%d %H:%M:%S.%f')
    data = get_staff(con, time_of_last_query)
    assert len(data['dim_staff']) == 2


@pytest.mark.describe('get_dim_staff()')
@pytest.mark.it('function returns dict with key of dim_location')
def test_function_returns_dict_with_correct_key():
    con = Mock()
    query = tuple([[19, 'Pierre', 'Sauer', 'Purchasing', 'Manchester', 
                    'pierre.sauer@terrifictotes.com', 
                    datetime.datetime(2022, 11, 3, 14, 20, 51, 563000),
                 2, 2],
                    [20, 'Flavio', 'Kulas', 'Production', 'Leeds', 
                     'flavio.kulas@terrifictotes.com', 
                     datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), 3, 3]])
    con.run.return_value=query
    time_of_last_query = datetime.datetime.strptime('2024-2-15 10:44:10.192011', '%Y-%m-%d %H:%M:%S.%f')
    data = get_staff(con, time_of_last_query)
    assert 'dim_staff' in data


@pytest.mark.describe('get_dim_staff()')
@pytest.mark.it('function returns dict')
def test_function_returns_dict():
    con = Mock()
    query = tuple([[19, 'Pierre', 'Sauer', 'Purchasing', 'Manchester', 
                    'pierre.sauer@terrifictotes.com', 
                    datetime.datetime(2022, 11, 3, 14, 20, 51, 563000),
                 2, 2],
                    [20, 'Flavio', 'Kulas', 'Production', 'Leeds', 
                     'flavio.kulas@terrifictotes.com', 
                     datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), 3, 3]])
    con.run.return_value=query
    time_of_last_query = datetime.datetime.strptime('2024-2-15 10:44:10.192011', '%Y-%m-%d %H:%M:%S.%f')
    data = get_staff(con, time_of_last_query)
    assert isinstance(data, dict)


@pytest.mark.describe('get_dim_staff()')
@pytest.mark.it('function returns data on correct keys')
def test_function_returns_data_on_correct_key():
    con = Mock()
    query = tuple([[19, 'Pierre', 'Sauer', 'Purchasing', 'Manchester', 
                    'pierre.sauer@terrifictotes.com', 
                    datetime.datetime(2022, 11, 3, 14, 20, 51, 563000),
                 2, 2],
                    [20, 'Flavio', 'Kulas', 'Production', 'Leeds', 
                     'flavio.kulas@terrifictotes.com', 
                     datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), 3, 3]])
    con.run.return_value=query
    time_of_last_query = datetime.datetime.strptime('2024-2-15 10:44:10.192011', '%Y-%m-%d %H:%M:%S.%f')
    data = get_staff(con, time_of_last_query)
    keys = ['staff_id', 'first_name', 'last_name', 'department_name', 'location', 'email_address']
    for x in keys:
        assert x in data['dim_staff'][0]