import boto3
from moto import mock_aws
import pytest
import datetime

from src.utils.set_time_of_the_last_query import set_time_of_the_last_query as set_time
from src.utils.get_time_of_last_query import get_time_of_last_query as get_time


@pytest.mark.describe('set_time_of_the_last_query as set_time()')
@pytest.mark.it('test function sets time time to aws ssm')
@mock_aws
def test_func_set_datetime_object():
    time = datetime.datetime.strptime('2024-2-15-10:44:10.192011', '%Y-%m-%d-%H:%M:%S.%f')
    set_time(time)
    assert get_time() == time


@pytest.mark.describe('set_time_of_the_last_query as set_time()')
@pytest.mark.it('test function raise TypeError if time wrong time object')
@mock_aws
def test_func_raise_run_timer_err():
    with pytest.raises(RuntimeError):
        set_time('sda')