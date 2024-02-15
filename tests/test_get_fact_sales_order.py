from src.utils.get_fact_sales_order import get_fact_sales_order as gfso 
import pytest
import datetime

import awswrangler as wr
from awswrangler import _utils
pg8000_native = _utils.import_optional_dependency("pg8000.native")
from pg8000.native import literal, identifier, DatabaseError


@pytest.mark.describe('get_fact_sales_order()')
@pytest.mark.it('query select all data points after given date')
def test_something():
    con = wr.postgresql.connect(secret_id = "totesys_db")
    time_of_last_query = datetime.datetime.strptime('2024-02-15 10:32:09.709000', '%Y-%m-%d %H:%M:%S.%f')
    data = gfso(con, time_of_last_query)
    con.close()
    assert len(data['fact_sales_order']) == 12 

@pytest.mark.describe('get_fact_sales_order()')
@pytest.mark.it('query select all data points after given date')
def test_something_():
    con = wr.postgresql.connect(secret_id = "totesys_db")
    time_of_last_query = datetime.datetime.strptime('2024-02-15 10:32:09.709000', '%Y-%m-%d %H:%M:%S.%f')
    data = gfso(con, time_of_last_query)
    con.close()
    assert len(data['fact_sales_order']) == 12