from src.utils.get_dim_design import get_dim_design as design
from decimal import Decimal
import pytest
import datetime
from unittest.mock import Mock

@pytest.mark.describe('get_dim_design()')
@pytest.mark.it('query select all data points after given date')
def test_seletc_data_after_given_date():
    con = Mock()
    query = tuple([[311, 'Steel', '/lost+found', 'steel-20220407-1se7.json'],
                  [312, 'Concrete', '/lib', 'concrete-20220529-7tii.json']])
    con.run.return_value=query
    time_of_last_query = datetime.datetime.strptime('2024-2-15 10:44:10.192011', '%Y-%m-%d %H:%M:%S.%f')
    data = design(con, time_of_last_query)
    assert len(data['dim_design']) == 2


@pytest.mark.describe('get_dim_design()')
@pytest.mark.it('function returns dict with key of dim_location')
def test_function_returns_dict_with_correct_key():
    con = Mock()
    query = tuple([[311, 'Steel', '/lost+found', 'steel-20220407-1se7.json'],
                  [312, 'Concrete', '/lib', 'concrete-20220529-7tii.json']])
    con.run.return_value=query
    time_of_last_query = datetime.datetime.strptime('2024-2-15 10:44:10.192011', '%Y-%m-%d %H:%M:%S.%f')
    data = design(con, time_of_last_query)
    assert 'dim_design' in data


@pytest.mark.describe('get_dim_design()')
@pytest.mark.it('function returns dict')
def test_function_returns_dict():
    con = Mock()
    query = tuple([[311, 'Steel', '/lost+found', 'steel-20220407-1se7.json'],
                  [312, 'Concrete', '/lib', 'concrete-20220529-7tii.json']])
    con.run.return_value=query
    time_of_last_query = datetime.datetime.strptime('2024-2-15 10:44:10.192011', '%Y-%m-%d %H:%M:%S.%f')
    data = design(con, time_of_last_query)
    assert isinstance(data, dict)


@pytest.mark.describe('get_dim_design()')
@pytest.mark.it('function returns data on correct keys')
def test_function_returns_data_on_correct_key():
    con = Mock()
    query = tuple([[311, 'Steel', '/lost+found', 'steel-20220407-1se7.json'],
                  [312, 'Concrete', '/lib', 'concrete-20220529-7tii.json']])
    con.run.return_value=query
    time_of_last_query = datetime.datetime.strptime('2024-2-15 10:44:10.192011', '%Y-%m-%d %H:%M:%S.%f')
    data = design(con, time_of_last_query)
    keys = ['design_id', 'design_name', 'file_location', 'file_name']
    for x in keys:
        assert x in data['dim_design'][0]