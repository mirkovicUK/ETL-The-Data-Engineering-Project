from src.utils.get_text_from_file import get_text_from_file as gtff
import pytest
from unittest.mock import Mock
from moto import mock_aws
import boto3
import json


@pytest.mark.describe('get_text_from_file()')
@pytest.mark.it('function fetch corect data')
@mock_aws
def test_retreive_correct_data():
    data = {'some': 'data', 'to': 'test'}
    s3 = boto3.resource('s3', region_name='eu-west-2')
    s3.create_bucket(Bucket='mybucket',
                     CreateBucketConfiguration={
                         'LocationConstraint': 'eu-west-2'})

    s3 = boto3.client('s3', region_name='eu-west-2')
    s3.put_object(
        Body=json.dumps(data, indent=2, default=str),
        Bucket='mybucket',
        Key='some_key' + '.json',
    )
    retrieved_data = gtff(s3, 'mybucket', 'some_key.json')
    retrieved_data = json.loads(retrieved_data)
    assert data == retrieved_data


@pytest.mark.describe('get_text_from_file()')
@pytest.mark.it('it is pure function')
@mock_aws
def test_doesnot_modify_data():
    data = {'some': 'data', 'to': 'test'}
    s3 = boto3.resource('s3', region_name='eu-west-2')
    s3.create_bucket(Bucket='mybucket',
                     CreateBucketConfiguration={
                         'LocationConstraint': 'eu-west-2'})

    s3 = boto3.client('s3', region_name='eu-west-2')
    s3.put_object(
        Body=json.dumps(data, indent=2, default=str),
        Bucket='mybucket',
        Key='some_key' + '.json',
    )
    retrieved_data = gtff(s3, 'mybucket', 'some_key.json')
    retrieved_data = json.loads(retrieved_data)
    assert data == {'some': 'data', 'to': 'test'}
