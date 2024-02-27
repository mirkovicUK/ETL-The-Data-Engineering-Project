from pg8000.native import Connection, literal, identifier, DatabaseError
from src.utils.writing_utils.write_dim_staff import write_dim_staff as wds
from src.utils.writing_utils.get_secret import get_secret
from datetime import datetime as dt
import pytest

from awswrangler import _utils
pg8000 = _utils.import_optional_dependency("pg8000")

# @pytest.mark.skip
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
    secret = get_secret('data_warehouse')
    con = Connection(secret['username'],
                    host = secret['host'],
                    database = secret['dbname'],
                    password = secret['password'])
    wds(con, data, dt.now() )
    # rows = con.run("DELETE FROM dim_staff; ;")
    rows = con.run("SELECT * FROM dim_staff ;")
    print(*rows, '<----------DIM_STAFF', sep='\n')
