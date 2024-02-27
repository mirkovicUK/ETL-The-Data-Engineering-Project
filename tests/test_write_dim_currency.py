from src.utils.writing_utils.write_dim_currency import write_dim_currency 
# from src.utils.writing_utils.get_secret import get_secret 
from datetime import datetime as dt
import pytest
from unittest.mock import Mock
from awswrangler import _utils
pg8000 = _utils.import_optional_dependency("pg8000")
from pg8000.native import Connection, literal, identifier, DatabaseError


@pytest.mark.describe('write_dim_currency()')
@pytest.mark.it('test_number_of_times_connection_to_DB_is_made')
def test_number_of_connections_to_DB():
    data = [
        {'currency_id': 1, 'currency_code': 'GBP', 'currency_name': 'British Pound'},
        {'currency_id': 2, 'currency_code': 'USD', 'currency_name': 'US Dollar'},
        {'currency_id': 3, 'currency_code': 'EUR', 'currency_name': 'Euro'}
    ]
    con = Mock()
    mock_datetime = dt(2024, 2, 26, 10, 30, 0)
    write_dim_currency(con, data, mock_datetime)

    assert con.run.call_count == len(data)
    
@pytest.mark.describe('write_dim_currency()')
@pytest.mark.it('test_if_correct_data_is_being_inserted')
def test_data_insertion():
    data = [
        {'currency_id': 1, 'currency_code': 'GBP', 'currency_name': 'British Pound'}]
    con = Mock()
    mock_datetime = dt(2024, 2, 26, 10, 30, 0)
    write_dim_currency(con, data, mock_datetime)
    expected = f"""
        INSERT INTO dim_currency
        VALUES
        (1,1,
        'GBP','British Pound',
        '2024-02-26','10:30:00')
        ON CONFLICT DO NOTHING;"""
    assert con.run.call_args[0][0].strip() == expected.strip()

@pytest.mark.describe('write_dim_currency()')
@pytest.mark.it('test_if_correct_data_is_being_inserted_for_multiple_data')
def test_data_insertion_for_multiple_data():
    data = [
        {'currency_id': 1, 'currency_code': 'GBP', 'currency_name': 'British Pound'},
        {'currency_id': 2, 'currency_code': 'USD', 'currency_name': 'US Dollar'},
        {'currency_id': 3, 'currency_code': 'EUR', 'currency_name': 'Euro'}
    ]
    con = Mock()
    mock_datetime = dt(2024, 2, 26, 10, 30, 0)
    write_dim_currency(con, data, mock_datetime)
    expected = [f"""
        INSERT INTO dim_currency
        VALUES
        (1,1,
        'GBP','British Pound',
        '2024-02-26','10:30:00')
        ON CONFLICT DO NOTHING;""",
        """INSERT INTO dim_currency
        VALUES
        (2,2,
        'USD','US Dollar',
        '2024-02-26','10:30:00')
        ON CONFLICT DO NOTHING;""",
        """INSERT INTO dim_currency
        VALUES
        (3,3,
        'EUR','Euro',
        '2024-02-26','10:30:00')
        ON CONFLICT DO NOTHING;
        """]
    for i,data_point in enumerate(data):
        assert con.run.call_args_list[i][0][0].strip() == expected[i].strip()
