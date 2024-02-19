from src.utils.get_object_path import get_object_path as op 
import pytest 
from unittest.mock import Mock

@pytest.mark.describe('get_object_path()')
@pytest.mark.it('function returns correct bucket name')
def test_returns_correct_bucket_name():
    records = [{'s3':{'bucket':{'name':'YO IM S3'},'object':{'key': 'YO IM KEY'}}}]
    bucket,_ = op(records)
    assert bucket == 'YO IM S3'


@pytest.mark.describe('get_object_path()')
@pytest.mark.it('function returns correct key')
def test_returns_correct_key():
    records = [{'s3':{'bucket':{'name':'YO IM S3'},'object':{'key': 'YO IM KEY'}}}]
    _, key = op(records)
    assert key == 'YO IM KEY'