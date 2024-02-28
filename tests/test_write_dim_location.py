from pg8000.native import Connection, literal, identifier, DatabaseError

from src.utils.writing_utils.write_dim_location import write_dim_location
from src.utils.writing_utils.get_secret import get_secret 
from datetime import datetime as dt
import pytest
from unittest.mock import Mock

from awswrangler import _utils

pg8000 = _utils.import_optional_dependency("pg8000")


@pytest.mark.describe("write_dim_location()")
@pytest.mark.it("correct_data_is_written_to_DB")
def test_correct_data_is_written_to_DB():
    data = [
    {
        "location_id": 1,
        "address_line_1": "6826 Herzog Via",
        "address_line_2": None,
        "district": "Avon",
        "city": "New Patienceburgh",
        "postal_code": "28441",
        "country": "Turkey",
        "phone": "1803 637401",
    },
    {
        "location_id": 2,
        "address_line_1": "179 Alexie Cliffs",
        "address_line_2": None,
        "district": None,
        "city": "Aliso Viejo",
        "postal_code": "99305-7380",
        "country": "San Marino",
        "phone": "9621 880720",
    },
    {
        "location_id": 3,
        "address_line_1": "148 Sincere Fort",
        "address_line_2": None,
        "district": None,
        "city": "Lake Charles",
        "postal_code": "89360",
        "country": "Samoa",
        "phone": "0730 783349",
    },
    {
        "location_id": 4,
        "address_line_1": "6102 Rogahn Skyway",
        "address_line_2": None,
        "district": "Bedfordshire",
        "city": "Olsonside",
        "postal_code": "47518",
        "country": "Republic of Korea",
        "phone": "1239 706295",
    },
    {
        "location_id": 5,
        "address_line_1": "34177 Upton Track",
        "address_line_2": None,
        "district": None,
        "city": "Fort Shadburgh",
        "postal_code": "55993-8850",
        "country": "Bosnia and Herzegovina",
        "phone": "0081 009772",
    },
]

    con = Mock()
    mock_datetime = dt(2024, 2, 26, 10, 30, 0)
    write_dim_location(con, data, mock_datetime)

    assert con.run.call_count == len(data)
    
@pytest.mark.describe('write_dim_location()')
@pytest.mark.it('test_if_correct_data_is_being_inserted')
def test_data_insertion():
    data = [
        {'location_id': 1, 'address_line_1': '6826 Herzog Via', 'address_line_2': None, 'district': 'Avon', 'city': 'New Patienceburgh', 'postal_code': '28441', 'country': 'Turkey', 'phone': '1803 637401'}]
    con = Mock()
    mock_datetime = dt(2024, 2, 26, 10, 30, 0)
    write_dim_location(con, data, mock_datetime)
    expected = f"""
        INSERT INTO dim_location
            VALUES
            (1,1,
            '6826 Herzog Via',NULL,
            'Avon','New Patienceburgh',
            '28441','Turkey',
            '1803 637401','2024-02-26',
            '10:30:00')
            ON CONFLICT DO NOTHING;"""
    assert con.run.call_args[0][0].strip() == expected.strip()

@pytest.mark.describe('write_dim_location()')
@pytest.mark.it('test_if_correct_data_is_being_inserted_for_multiple_data')
def test_data_insertion_for_multiple_data():
    data = [
        {'location_id': 1, 'address_line_1': '6826 Herzog Via', 'address_line_2': 'NULL', 'district': 'Avon', 'city': 'New Patienceburgh', 'postal_code': '28441', 'country': 'Turkey', 'phone': '1803 637401'},
        {'location_id': 2, 'address_line_1': '179 Alexie Cliffs', 'address_line_2': 'NULL', 'district': 'NULL', 'city': 'Aliso Viejo', 'postal_code': '99305-7380', 'country': 'San Marino', 'phone': '9621 880720'},
        {'location_id': 3, 'address_line_1': '148 Sincere Fort', 'address_line_2': 'NULL', 'district': 'NULL', 'city': 'Lake Charles', 'postal_code': '89360', 'country': 'Samoa', 'phone': '0730 783349'},
        {'location_id': 4, 'address_line_1': '6102 Rogahn Skyway', 'address_line_2': 'NULL', 'district': 'Bedfordshire', 'city': 'Olsonside', 'postal_code': '47518', 'country': 'Republic of Korea', 'phone': '1239 706295'},
        {'location_id': 5, 'address_line_1': '34177 Upton Track', 'address_line_2': 'NULL', 'district': 'NULL', 'city': 'Fort Shadburgh', 'postal_code': '55993-8850', 'country': 'Bosnia and Herzegovina', 'phone': '0081 009772'}
    ]
    con = Mock()
    mock_datetime = dt(2024, 2, 26, 10, 30, 0)
    write_dim_location(con, data, mock_datetime)
    expected = [f"""
        INSERT INTO dim_location
            VALUES
            ({data_point['location_id']},{data_point['location_id']},
            '{data_point['address_line_1']}','{data_point['address_line_2']}',
            '{data_point['district']}','{data_point['city']}',
            '{data_point['postal_code']}','{data_point['country']}',
            '{data_point['phone']}','2024-02-26',
            '10:30:00')
            ON CONFLICT DO NOTHING;""" for data_point in data]

    for i, data_point in enumerate(data):
        assert con.run.call_args_list[i][0][0].strip() == expected[i].strip()