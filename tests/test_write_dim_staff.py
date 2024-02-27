from src.utils.writing_utils.write_dim_staff import write_dim_staff 
from src.utils.writing_utils.get_secret import get_secret 
from datetime import datetime as dt
import pytest
from unittest.mock import Mock
from awswrangler import _utils
pg8000 = _utils.import_optional_dependency("pg8000")
from pg8000.native import Connection, literal, identifier, DatabaseError


@pytest.mark.describe('write_dim_staff()')
@pytest.mark.it('correct_data_is_written_to_DB')
def test_correct_data_is_written_to_DB():
    data = [
        {'staff_record_id': 1, 'first_name': 'Jeremie', 'last_name': 'Franey', 'department_name': 'Purchasing', 'location': 'Manchester', 'email_address': 'jeremie.franey@terrifictotes.com'},
        {'staff_record_id': 2, 'first_name': 'Deron', 'last_name': 'Beier', 'department_name': 'Facilities', 'location': 'Manchester', 'email_address': 'deron.beier@terrifictotes.com'},
        {'staff_record_id': 3, 'first_name': 'Jeanette', 'last_name': 'Erdman', 'department_name': 'Facilities', 'location': 'Manchester', 'email_address': 'jeanette.erdman@terrifictotes.com'},
        {'staff_record_id': 4, 'first_name': 'Ana', 'last_name': 'Glover', 'department_name': 'Production', 'location': 'Leeds', 'email_address': 'ana.glover@terrifictotes.com'},
        {'staff_record_id': 5, 'first_name': 'Magdalena', 'last_name': 'Zieme', 'department_name': 'HR', 'location': 'Leeds', 'email_address': 'magdalena.zieme@terrifictotes.com'}
    ]
    con = Mock()
    mock_datetime = dt(2024, 2, 26, 10, 30, 0)
    write_dim_staff(con, data, mock_datetime)

    assert con.run.call_count == len(data)
    
@pytest.mark.describe('write_dim_staff()')
@pytest.mark.it('test_if_correct_data_is_being_inserted')
def test_data_insertion():
    data = [
        {'staff_record_id': 1, 'first_name': 'Jeremie', 'last_name': 'Franey', 'department_name': 'Purchasing', 'location': 'Manchester', 'email_address': 'jeremie.franey@terrifictotes.com'}]
    con = Mock()
    mock_datetime = dt(2024, 2, 26, 10, 30, 0)
    write_dim_staff(con, data, mock_datetime)
    expected = f"""
    INSERT INTO dim_staff
        VALUES
        (1,1,
        'Jeremie','Franey',
        'Purchasing','Manchester',
        'jeremie.franey@terrifictotes.com','2024-02-26',
        '10:30:00')
        ON CONFLICT DO NOTHING;"""
    assert con.run.call_args[0][0].strip() == expected.strip()

@pytest.mark.describe('write_dim_staff()')
@pytest.mark.it('test_if_correct_data_is_being_inserted_for_multiple_data')
def test_data_insertion_for_multiple_data():
    data = [
        {'staff_record_id': 1, 'first_name': 'Jeremie', 'last_name': 'Franey', 'department_name': 'Purchasing', 'location': 'Manchester', 'email_address': 'jeremie.franey@terrifictotes.com'},
        {'staff_record_id': 2, 'first_name': 'Deron', 'last_name': 'Beier', 'department_name': 'Facilities', 'location': 'Manchester', 'email_address': 'deron.beier@terrifictotes.com'},
        {'staff_record_id': 3, 'first_name': 'Jeanette', 'last_name': 'Erdman', 'department_name': 'Facilities', 'location': 'Manchester', 'email_address': 'jeanette.erdman@terrifictotes.com'},
        {'staff_record_id': 4, 'first_name': 'Ana', 'last_name': 'Glover', 'department_name': 'Production', 'location': 'Leeds', 'email_address': 'ana.glover@terrifictotes.com'},
        {'staff_record_id': 5, 'first_name': 'Magdalena', 'last_name': 'Zieme', 'department_name': 'HR', 'location': 'Leeds', 'email_address': 'magdalena.zieme@terrifictotes.com'}
    ]
    con = Mock()
    mock_datetime = dt(2024, 2, 26, 10, 30, 0)
    write_dim_staff(con, data, mock_datetime)
    expected = [f"""
    INSERT INTO dim_staff
        VALUES
        ({data_point['staff_record_id']},{data_point['staff_record_id']},
        '{data_point['first_name']}','{data_point['last_name']}',
        '{data_point['department_name']}','{data_point['location']}',
        '{data_point['email_address']}','2024-02-26',
        '10:30:00')
        ON CONFLICT DO NOTHING;""" for data_point in data]

    for i, data_point in enumerate(data):
        assert con.run.call_args_list[i][0][0].strip() == expected[i].strip()