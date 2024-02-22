from src.utils.writing_utils.write_dim_counterparty import write_dim_counterparty as wdcp
from src.utils.writing_utils.get_secret import get_secret 
from datetime import datetime as dt
import pytest

from awswrangler import _utils
pg8000 = _utils.import_optional_dependency("pg8000")
from pg8000.native import Connection, literal, identifier, DatabaseError


@pytest.mark.describe('write_dim_counterparty()')
@pytest.mark.it('correct_data_is_written_to_DB')
def test_correct_data_is_written_to_DB():
    data = [
        {'counterparty_id': 1, 'counterparty_legal_name': 'Fahey and Sons', 'counterparty_legal_address_line_1': '605 Haskell Trafficway', 'counterparty_legal_address_line_2': 'Axel Freeway', 'counterparty_legal_district': None, 'counterparty_legal_city': 'East Bobbie', 'counterparty_legal_postal_code': '88253-4257', 'counterparty_legal_country': 'Heard Island and McDonald Islands', 'counterparty_legal_phone_number': '9687 937447'},
        {'counterparty_id': 2, 'counterparty_legal_name': 'Leannon, Predovic and Morar', 'counterparty_legal_address_line_1': '079 Horacio Landing', 'counterparty_legal_address_line_2': None, 'counterparty_legal_district': None, 'counterparty_legal_city': 'Utica', 'counterparty_legal_postal_code': '93045', 'counterparty_legal_country': 'Austria', 'counterparty_legal_phone_number': '7772 084705'},
        {'counterparty_id': 3, 'counterparty_legal_name': 'Armstrong Inc', 'counterparty_legal_address_line_1': '179 Alexie Cliffs', 'counterparty_legal_address_line_2': None, 'counterparty_legal_district': None, 'counterparty_legal_city': 'Aliso Viejo', 'counterparty_legal_postal_code': '99305-7380', 'counterparty_legal_country': 'San Marino', 'counterparty_legal_phone_number': '9621 880720'},
        {'counterparty_id': 4, 'counterparty_legal_name': 'Kohler Inc', 'counterparty_legal_address_line_1': '37736 Heathcote Lock', 'counterparty_legal_address_line_2': 'Noemy Pines', 'counterparty_legal_district': None, 'counterparty_legal_city': 'Bartellview', 'counterparty_legal_postal_code': '42400-5199', 'counterparty_legal_country': 'Congo', 'counterparty_legal_phone_number': '1684 702261'},
        {'counterparty_id': 5, 'counterparty_legal_name': 'Frami, Yundt and Macejkovic', 'counterparty_legal_address_line_1': '364 Goodwin Streets', 'counterparty_legal_address_line_2': None, 'counterparty_legal_district': None, 'counterparty_legal_city': 'Sayreville', 'counterparty_legal_postal_code': '85544-4254', 'counterparty_legal_country': 'Svalbard & Jan Mayen Islands', 'counterparty_legal_phone_number': '0847 468066'}
    ]

    # secret = get_secret('DB_write')
    # con = Connection(secret['username'], 
    #                 host = secret['host'],
    #                 database = secret['dbname'],
    #                 password = secret['password'])
    # wdcp(con, data, dt.now() )
    # rows = con.run("DELETE FROM dim_counterparty; ;")
    # rows = con.run("SELECT * FROM dim_counterparty ;")
    # print(*rows, '<----------dim_counterparty', sep='\n')