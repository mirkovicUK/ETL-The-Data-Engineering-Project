import boto3
import datetime


def set_time_of_the_last_query(time):
    """
    Args:
        python datetime object
        format: '%Y-%m-%d-%H:%M:%S.%f''
    Returns:
        None

    Raises:
        Runtime error

    Logs:
        None
    """
    try:
        client = boto3.client("ssm", region_name="eu-west-2")
        client.put_parameter(
            Name="time",
            Value=time.strftime("%Y-%m-%d %H:%M:%S.%f"),
            Type="String",
            Overwrite=True,
        )

    except Exception as e:
        raise RuntimeError(e)


if __name__ == "__main__":
    print(set_time_of_the_last_query(datetime.datetime.now()))
