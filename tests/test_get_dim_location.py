from src.utils.get_dim_location import get_dim_location as get_location 
from decimal import Decimal
import pytest
import datetime
from unittest.mock import Mock

@pytest.mark.describe('get_dim_location()')
@pytest.mark.it('query select all data points after given date')
def test_seletc_data_after_given_date():
    con = Mock()
    query = tuple([[29, '37736 Heathcote Lock', 'Noemy Pines', None,
                    'Bartellview', '42400-5199', 'Congo', '1684 702261',
                     datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), 
                     datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)],
                [30, '0336 Ruthe Heights', None, 'Buckinghamshire', 
                 'Lake Myrlfurt', '94545-4284', 'Falkland Islands (Malvinas)', 
                 '1083 286132', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
                 datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)]])
    con.run.return_value=query
    time_of_last_query = datetime.datetime.strptime('2024-2-15 10:44:10.192011', '%Y-%m-%d %H:%M:%S.%f')
    data = get_location(con, time_of_last_query)
    assert len(data['dim_location']) == 2


@pytest.mark.describe('get_dim_location()')
@pytest.mark.it('function returns dict')
def test_function_returns_dict():
    con = Mock()
    query = tuple([[29, '37736 Heathcote Lock', 'Noemy Pines', None,
                    'Bartellview', '42400-5199', 'Congo', '1684 702261',
                     datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), 
                     datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)],
                [30, '0336 Ruthe Heights', None, 'Buckinghamshire', 
                 'Lake Myrlfurt', '94545-4284', 'Falkland Islands (Malvinas)', 
                 '1083 286132', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
                 datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)]])
    con.run.return_value=query
    time_of_last_query = datetime.datetime.strptime('2024-2-15 10:44:10.192011', '%Y-%m-%d %H:%M:%S.%f')
    data = get_location(con, time_of_last_query)
    assert isinstance(data, dict)


@pytest.mark.describe('get_dim_location()')
@pytest.mark.it('function returns dict with key of dim_location')
def test_function_returns_dict_with_correct_key():
    con = Mock()
    query = tuple([[29, '37736 Heathcote Lock', 'Noemy Pines', None,
                    'Bartellview', '42400-5199', 'Congo', '1684 702261',
                     datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), 
                     datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)],
                [30, '0336 Ruthe Heights', None, 'Buckinghamshire', 
                 'Lake Myrlfurt', '94545-4284', 'Falkland Islands (Malvinas)', 
                 '1083 286132', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
                 datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)]])
    con.run.return_value=query
    time_of_last_query = datetime.datetime.strptime('2024-2-15 10:44:10.192011', '%Y-%m-%d %H:%M:%S.%f')
    data = get_location(con, time_of_last_query)
    assert 'dim_location' in data


@pytest.mark.describe('get_dim_location()')
@pytest.mark.it('function returns data on corect keys')
def test_ffunction_returns_data_on_correct_key():
    con = Mock()
    query = tuple([[29, '37736 Heathcote Lock', 'Noemy Pines', None,
                    'Bartellview', '42400-5199', 'Congo', '1684 702261',
                     datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), 
                     datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)],
                [30, '0336 Ruthe Heights', None, 'Buckinghamshire', 
                 'Lake Myrlfurt', '94545-4284', 'Falkland Islands (Malvinas)', 
                 '1083 286132', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000),
                 datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)]])
    con.run.return_value=query
    time_of_last_query = datetime.datetime.strptime('2024-2-15 10:44:10.192011', '%Y-%m-%d %H:%M:%S.%f')
    data = get_location(con, time_of_last_query)
    keys = ['location_id', 'address_line_1', 'address_line_2',
                'district', 'city', 'postal_code', 'country', 'phone']
    for x in keys:
        assert x in data['dim_location'][0]