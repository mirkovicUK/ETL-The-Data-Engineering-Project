import boto3
import datetime


def get_time_of_last_query():
    """
    Args:
        None
    Returns:
        Python datetime object

    Raises:
        Runtime error

    Logs:
        None
    """
    try:
        client = boto3.client("ssm", region_name="eu-west-2")
        time = client.get_parameter(Name="time")["Parameter"]["Value"]
        return datetime.datetime.strptime(time, "%Y-%m-%d %H:%M:%S.%f")
    except Exception as e:
        raise RuntimeError(e)


if __name__ == "__main__":
    print(get_time_of_last_query())
