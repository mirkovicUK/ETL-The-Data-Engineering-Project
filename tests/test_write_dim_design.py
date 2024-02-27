
from pg8000.native import Connection, literal, identifier, DatabaseError
from src.utils.writing_utils.write_dim_design import write_dim_design as wdd
from src.utils.writing_utils.get_secret import get_secret

from src.utils.writing_utils.write_dim_design import write_dim_design
from src.utils.writing_utils.get_secret import get_secret 

from datetime import datetime as dt
import pytest
from unittest.mock import Mock

from awswrangler import _utils
pg8000 = _utils.import_optional_dependency("pg8000")


@pytest.mark.describe('write_dim_design()')
@pytest.mark.it('correct_data_is_written_to_DB')
def test_correct_data_is_written_to_DB():
    data = [{'design_id': 311,
             'design_name': 'Steel',
             'file_location': '/lost+found',
             'file_name': 'steel-20220407-1se7.json'},
            {'design_id': 312,
             'design_name': 'Concrete',
             'file_location': '/lib',
             'file_name': 'concrete-20220529-7tii.json'},
            {'design_id': 313,
             'design_name': 'Plastic',
             'file_location': '/Users',
             'file_name': 'plastic-20231231-fsdr.json'},
            {'design_id': 314,
             'design_name': 'Cotton',
             'file_location': '/usr/src',
             'file_name': 'cotton-20220926-rgqy.json'},
            {'design_id': 315,
             'design_name': 'Concrete',
             'file_location': '/System',
             'file_name': 'concrete-20230717-u6p2.json'}]

    # secret = get_secret('DB_write')
    # con = Connection(secret['username'],
    #                 host = secret['host'],
    #                 database = secret['dbname'],
    #                 password = secret['password'])
    # wdd(con, data, dt.now() )
    # rows = con.run("DELETE FROM dim_design; ;")
    # rows = con.run("SELECT * FROM dim_design LIMIT 10;")
    # print(*rows, '<----------dim_design', sep='\n')

    con = Mock()
    mock_datetime = dt(2024, 2, 26, 10, 30, 0)
    write_dim_design(con, data, mock_datetime)

    assert con.run.call_count == len(data)
    
@pytest.mark.describe('write_dim_design()')
@pytest.mark.it('test_if_correct_data_is_being_inserted')
def test_data_insertion():
    data = [
        {'design_id': 311, 'design_name': 'Steel', 'file_location': '/lost+found', 'file_name': 'steel-20220407-1se7.json'}]
    con = Mock()
    mock_datetime = dt(2024, 2, 26, 10, 30, 0)
    write_dim_design(con, data, mock_datetime)
    expected = f"""
        INSERT INTO dim_design
            VALUES
            (311,311,
            'Steel','/lost+found',
            'steel-20220407-1se7.json','2024-02-26',
            '10:30:00')
            ON CONFLICT DO NOTHING;"""
    assert con.run.call_args[0][0].strip() == expected.strip()

@pytest.mark.describe('write_dim_design()')
@pytest.mark.it('test_if_correct_data_is_being_inserted_for_multiple_data')
def test_data_insertion_for_multiple_data():
    data = [
        {'design_id': 311, 'design_name': 'Steel', 'file_location': '/lost+found', 'file_name': 'steel-20220407-1se7.json'},
        {'design_id': 312, 'design_name': 'Concrete', 'file_location': '/lib', 'file_name': 'concrete-20220529-7tii.json'},
        {'design_id': 313, 'design_name': 'Plastic', 'file_location': '/Users', 'file_name': 'plastic-20231231-fsdr.json'},
        {'design_id': 314, 'design_name': 'Cotton', 'file_location': '/usr/src', 'file_name': 'cotton-20220926-rgqy.json'},
        {'design_id': 315, 'design_name': 'Concrete', 'file_location': '/System', 'file_name': 'concrete-20230717-u6p2.json'}
    ]
    con = Mock()
    mock_datetime = dt(2024, 2, 26, 10, 30, 0)
    write_dim_design(con, data, mock_datetime)
    expected = [f"""
        INSERT INTO dim_design
            VALUES
            ({data_point['design_id']},{data_point['design_id']},
            '{data_point['design_name']}','{data_point['file_location']}',
            '{data_point['file_name']}','2024-02-26',
            '10:30:00')
            ON CONFLICT DO NOTHING;""" for data_point in data]

    for i, data_point in enumerate(data):
        assert con.run.call_args_list[i][0][0].strip() == expected[i].strip()
