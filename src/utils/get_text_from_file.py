def get_text_from_file(client, bucket, object_key):
    """
    Args:
        param1: event['Records']; records from aws event object

    Returns:
        str: bucke_name, object_name

    Raises:
        None

    Logs:
        None

    Reads text from specified file in S3.
    """
    data = client.get_object(Bucket=bucket, Key=object_key)
    contents = data["Body"].read()
    return contents.decode("utf-8")
