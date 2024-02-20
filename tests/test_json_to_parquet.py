from src.ingestion import ingestion
from unittest.mock import Mock, patch
import pytest
from src.json_to_parquet import json_to_parquet as jtp,  InvalidFileTypeError
import logging
from moto import mock_aws
import boto3
import json

@pytest.mark.describe('json_to_parquet()')
@pytest.mark.it('test function logs if it gets invalid file name')
def test_function_logs_if_invalid_file_name(caplog):
    with caplog.at_level(logging.ERROR):
        event = {'Records':[{'s3':{'bucket':{'name':'YO IM S3'},
                          'object':{'key': 'YO IM KEY'}
                          }
                    }]}
        context = 'no context'
        jtp(event, context)
        assert 'File YO IM KEY is not a valid json file' in caplog.text


@pytest.mark.describe('json_to_parquet()')
@pytest.mark.it('test function logs if it there is no file to fatch')
@mock_aws
def test_function_logs_client_error_if_there_is_no_json_to_fatch(caplog):
    with caplog.at_level(logging.ERROR):
        event = {'Records':[{'s3':{'bucket':{'name':'YO_IM_S3'},
                          'object':{'key': 'YO_IM_KEY.json'}
                          }
                    }]}
        context = 'no context'
        s3 = boto3.resource('s3', region_name='eu-west-2')
        s3.create_bucket(Bucket='YO_IM_S3', 
                    CreateBucketConfiguration={
                    'LocationConstraint': 'eu-west-2'})
        jtp(event, context)
        assert 'No object found - YO_IM_KEY.json' in caplog.text


@pytest.mark.describe('json_to_parquet()')
@pytest.mark.it('test function logs if it gets invalid bucket name')
@mock_aws
def test_function_logs_client_error_if_not_existing_bucket(caplog):
    with caplog.at_level(logging.ERROR):
        event = {'Records':[{'s3':{'bucket':{'name':'YO_IM_S3'},
                          'object':{'key': 'YO_IM_KEY.json'}
                          }
                    }]}
        context = 'no context'
        jtp(event, context)
        assert 'No such bucket - YO_IM_S3' in caplog.text


@pytest.mark.describe('json_to_parquet()')
@pytest.mark.it('test function loggs parquet in procest bucket')
@patch('src.json_to_parquet.s3_procesed_zone_url', 's3://processed-zone/')
@mock_aws
def test_function_loggs_parquet_in_processed_bucket(caplog):
        data = {'some' : ['data'], 'to' : ['test']}
        event = {'Records':[{'s3':{'bucket':{'name':'YO_IM_S3'},
                          'object':{'key': 'YO_IM_KEY.json'}
                          }
                    }]}
        context = 'no context'

        s3 = boto3.resource('s3', region_name='eu-west-2')
        s3.create_bucket(Bucket='YO_IM_S3', 
                    CreateBucketConfiguration={
                    'LocationConstraint': 'eu-west-2'})
        s3.create_bucket(Bucket='processed-zone', 
                    CreateBucketConfiguration={
                    'LocationConstraint': 'eu-west-2'})
        
        s3 = boto3.client('s3', region_name='eu-west-2')
        s3.put_object(
            Body=json.dumps(data, indent=2, default=str),
            Bucket='YO_IM_S3',
            Key = 'YO_IM_KEY.json',
            )
        
        jtp(event, context)
        assert 'JSON converted to parquet writen into YO_IM_S3' in caplog.text


@pytest.mark.describe('json_to_parquet()')
@pytest.mark.it('test function write parquet in procest bucket')
@patch('src.json_to_parquet.s3_procesed_zone_url', 's3://processed-zone/')
@mock_aws
def test_function_write_parquet_in_processed_bucket(caplog):
        data = {'some' : ['data'], 'to' : ['test']}
        event = {'Records':[{'s3':{'bucket':{'name':'YO_IM_S3'},
                          'object':{'key': 'YO_IM_KEY.json'}
                          }
                    }]}
        context = 'no context'

        s3 = boto3.resource('s3', region_name='eu-west-2')
        s3.create_bucket(Bucket='YO_IM_S3', 
                    CreateBucketConfiguration={
                    'LocationConstraint': 'eu-west-2'})
        s3.create_bucket(Bucket='processed-zone', 
                    CreateBucketConfiguration={
                    'LocationConstraint': 'eu-west-2'})
        
        s3 = boto3.client('s3', region_name='eu-west-2')
        s3.put_object(
            Body=json.dumps(data, indent=2, default=str),
            Bucket='YO_IM_S3',
            Key = 'YO_IM_KEY.json',
            )
        
        s3 = boto3.resource('s3')
        my_bucket = s3.Bucket('processed-zone')        
        jtp(event, context)
        for object in my_bucket.objects.all():
            assert object.key[-8:] == '.parquet'
        