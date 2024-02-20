from src.ingestion import ingestion
from unittest.mock import Mock, patch
import pytest
from src.ingestion import ingestion, InvalidConnection
import logging

@pytest.mark.describe('ingestion()')
@pytest.mark.it('test that we are using PG8000 to connect to db')
def test_ingestion_uses_pg8000_to_conect_to_DB(caplog):
    with patch('src.ingestion.wr.postgresql') as connection:
        with caplog.at_level(logging.WARNING):
            connection.connect.return_value = list()
            ingestion('event', 'context')
            assert 'Not pg8000 connection' in caplog.text


@pytest.mark.describe('ingestion()')
@pytest.mark.it('loggs_if_incorect_db_credentials')
@patch('src.ingestion.wr.postgresql')
@patch('src.ingestion.DB')
def test_ingestion_loggs_if_incorect_db_credentials(con, db, caplog):
    db = 'incorect secret'
    con.connect.return_value = list()
    ingestion('event', 'context')
    assert 'Not pg8000 connection' in caplog.text


@pytest.mark.describe('ingestion()')
@pytest.mark.it('ingestion write only JSON with data to s3')
@patch('src.ingestion.wr.postgresql')
@patch('src.ingestion.put_object_into_s3_bucket')
def test_ingestion_write_only_JSON_with_data_in_it(con, put_obj_into_s3_bucket):
    con.connect.return_value = list()
    con.run.return_value = list()
    con.close.return_value = 'nothing'
    put_obj_into_s3_bucket.assert_not_called()

