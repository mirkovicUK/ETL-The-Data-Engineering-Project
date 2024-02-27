from pg8000.native import literal, identifier
import datetime
import logging

import awswrangler as wr
from awswrangler import _utils

pg8000_native = _utils.import_optional_dependency("pg8000.native")

logger = logging.getLogger("MyLogger")
logger.setLevel(logging.INFO)


def get_dim_location(con, time_of_last_query):
    """
    Args:
        param1: pg8000 conection obj
        param2: time of last db query
    Returns:
        {'dim_location':[data_pooint1, data_point2...]}
    Raises:
        Does not raises an exception.
    Logs:
        Logs error to cloud watch
    """
    try:
        table = "address"
        keys = [
            "address_id",
            "address_line_1",
            "address_line_2",
            "district",
            "city",
            "postal_code",
            "country",
            "phone",
            "created_at",
            "last_updated",
        ]
        query = f"""SELECT * FROM {identifier(table)}
                WHERE last_updated>{literal(time_of_last_query)}
                ;"""
        rows = con.run(query)

        dim_location = {"dim_location": []}
        for row in rows:
            data_point = {}
            for ii, (k, v) in enumerate(zip(keys, row)):
                if ii == 0:
                    data_point["location_id"] = v
                elif ii == 8 or ii == 9:
                    pass
                else:
                    data_point[k] = v
            dim_location["dim_location"].append(data_point)
        return dim_location
    except Exception as e:
        logger.error(e)


if __name__ == "__main__":
    print(
        *get_dim_location(
            wr.postgresql.connect(secret_id="totesys_db"),
            datetime.datetime.strptime(
                "2022-09-10 18:32:09.709000", "%Y-%m-%d %H:%M:%S.%f"
            ),
        )["dim_location"],
        sep="\n",
    )
