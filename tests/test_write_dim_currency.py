from src.utils.writing_utils.write_dim_currency import write_dim_currency as wdc
from src.utils.writing_utils.get_secret import get_secret 
from datetime import datetime as dt
import pytest

from awswrangler import _utils
pg8000 = _utils.import_optional_dependency("pg8000")
from pg8000.native import Connection, literal, identifier, DatabaseError


@pytest.mark.describe('write_dim_currency()')
@pytest.mark.it('correct_data_is_written_to_DB')
def test_correct_data_is_written_to_DB():
    data = [
        {'currency_id': 1, 'currency_code': 'GBP', 'currency_name': 'British Pound'},
        {'currency_id': 2, 'currency_code': 'USD', 'currency_name': 'US Dollar'},
        {'currency_id': 3, 'currency_code': 'EUR', 'currency_name': 'Euro'}
    ]
    # secret = get_secret('DB_write')
    # con = Connection(secret['username'], 
    #                 host = secret['host'],
    #                 database = secret['dbname'],
    #                 password = secret['password'])
    # wdc(con, data, dt.now() )
    # rows = con.run("DELETE FROM dim_currency;")
    # rows = con.run("SELECT * FROM dim_currency ;")
    # print(*rows, '<----------dim_currency', sep='\n')