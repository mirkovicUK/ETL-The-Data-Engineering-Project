import boto3
from moto import mock_aws
import pytest
import datetime

from src.utils.get_time_of_last_query import get_time_of_last_query as get_time

@pytest.mark.describe('get_time_of_last_query()')
@pytest.mark.it('test function returns correct datetime obj')
@mock_aws
def test_func_returns_datetime_object():
    time = datetime.datetime.now()
    client = boto3.client('ssm')
    client.put_parameter(
    Name = 'time',
    Value=time.strftime('%Y-%m-%d %H:%M:%S.%f'),
    Overwrite=True)
    assert get_time() == time


@pytest.mark.describe('get_time_of_last_query()')
@pytest.mark.it('test function raise Runtime error no time param')
@mock_aws
def test_func_raise_run_timer_err():
    with pytest.raises(RuntimeError, match='ParameterNotFound'):
        get_time()