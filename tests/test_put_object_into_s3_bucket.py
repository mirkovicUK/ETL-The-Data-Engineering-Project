import boto3
from moto import mock_aws
import pytest
import json

from src.utils.put_object_into_s3_bucket import put_object_into_s3_bucket as put_s3


@pytest.mark.describe("put_object_into_s3_bucket()")
@pytest.mark.it("test function write into s3")
@mock_aws
def test_func_write_into_s3():
    data = {"some": "data", "to": "test"}
    s3 = boto3.resource("s3")
    s3.create_bucket(
        Bucket="mybucket", CreateBucketConfiguration={"LocationConstraint": "eu-west-2"}
    )
    put_s3(data, "mybucket", "some_key")
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket="mybucket", Key="some_key.json")
    assert json.loads(obj["Body"].read().decode("utf-8")) == data


@pytest.mark.describe("put_object_into_s3_bucket()")
@pytest.mark.it("test function raise RuntimeError when NoSuchBucket")
@mock_aws
def test_RuntimeError_NoSuchBucket():
    with pytest.raises(RuntimeError, match="NoSuchBucket"):
        put_s3(None, "mybucket", "some_key")
