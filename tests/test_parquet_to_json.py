from src.ingestion import ingestion
from unittest.mock import Mock, patch
import pytest
import logging
from moto import mock_aws
import boto3
import json
from awswrangler import exceptions

from src.parquet_to_json import parquet_to_json as ptj,  InvalidFileTypeError

@pytest.mark.describe('parquet_to_json()')
@pytest.mark.it('test function logs if it gets invalid file name')
def test_function_logs_if_invalid_file_name(caplog):
    with caplog.at_level(logging.ERROR):
        event = {'Records':[{'s3':{'bucket':{'name':'YO IM S3'},
                          'object':{'key': 'YO IM KEY'}
                          }
                    }]}
        context = 'no context'
        ptj(event, context)
        assert 'File YO IM KEY is not a valid parquet file' in caplog.text


# @pytest.mark.describe('parquet_to_json()')
# @pytest.mark.it('test function raise NoFilesFound if it there is no file to fatch')
# @mock_aws
# def test_function_raise_if_there_is_no_parque_to_fatch(caplog):
#     with caplog.at_level(logging.ERROR):
#         with pytest.raises(exceptions.NoFilesFound, match='No files Found on: s3://processed-zone-895623xx35/YO_IM_KEY.parquet.'):
#             event = {'Records':[{'s3':{'bucket':{'name':'YO_IM_S3'},
#                                 'object':{'key': 'YO_IM_KEY.parquet'}
#                                 }
#                         }]}
#             context = 'no context'
#             s3 = boto3.resource('s3', region_name='eu-west-2')
#             s3.create_bucket(Bucket='processed-zone-895623xx35', 
#                         CreateBucketConfiguration={
#                         'LocationConstraint': 'eu-west-2'})
#             ptj(event, context)


@pytest.mark.describe('parquet_to_json()')
@pytest.mark.it('test function logs if it gets invalid bucket name')
@mock_aws
def test_function_logs_client_error_if_not_existing_bucket(caplog):
    with caplog.at_level(logging.ERROR):
        event = {'Records':[{'s3':{'bucket':{'name':'YO_IM_S3'},
                          'object':{'key': 'YO_IM_KEY.json'}
                          }
                    }]}
        context = 'no context'
        ptj(event, context)
        assert 'File YO_IM_KEY.json is not a valid parquet file' in caplog.text