import boto3
import json
import logging
from botocore.exceptions import ClientError

def put_object_into_s3_bucket(data, bucket_name, key):
    """
    Args:
        param1: aws bucket name
        param2: aws object key

    Returns:
        None

    Raises:
        RuntimeError
        ClientError

    Logs: error msg for botocore.exceptions.ClientError

    """
    try:
        s3 = boto3.client('s3')
        s3.put_object(
            Body=json.dumps(data, indent=2, default=str),
            Bucket=bucket_name,
            Key = key+'.json',
            )
    except Exception as e:
        raise RuntimeError(e)
    except ClientError as e:
        logging.error(e.response['Error']['Message'])
        raise ClientError(e)