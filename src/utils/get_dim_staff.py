from pg8000.native import literal, identifier
import datetime

import logging

import awswrangler as wr
from awswrangler import _utils

pg8000_native = _utils.import_optional_dependency("pg8000.native")

logger = logging.getLogger("MyLogger")
logger.setLevel(logging.INFO)


def get_dim_staff(con, time_of_last_query):
    """
    Args:
        param1: pg8000 conection obj
        param2: time of last db query
    Returns:
        {'dim_staff':[data_pooint1, data_point2...]}
    Raises:
        Does not raises an exception.
    Logs:
        Logs error to cloud watch
    """
    try:
        query = f"""SELECT
                       {identifier('staff')}.{identifier('staff_id')},
                        {identifier('staff')}.{identifier('first_name')},
                        {identifier('staff')}.{identifier('last_name')},
                        {identifier('department')}.{identifier('department_name')},
                        {identifier('department')}.{identifier('location')},
                        {identifier('staff')}.{identifier('email_address')},
                        {identifier('staff')}.{identifier('last_updated')},
                        {identifier('staff')}.{identifier('department_id')},
                        {identifier('department')}.{identifier('department_id')}
                    FROM {identifier('staff')}
                    LEFT JOIN {identifier('department')}
                    ON staff.department_id = department.department_id
                    WHERE staff.last_updated>{literal(time_of_last_query)}
                    ;
                    """
        rows = con.run(query)

        dim_staff = {"dim_staff": []}
        for row in rows:
            data_point = {}
            for ii, value in enumerate(row):
                if ii == 0:
                    data_point["staff_record_id"] = value
                elif ii == 1:
                    data_point["first_name"] = value
                elif ii == 2:
                    data_point["last_name"] = value
                elif ii == 3:
                    data_point["department_name"] = value
                elif ii == 4:
                    data_point["location"] = value
                elif ii == 5:
                    data_point["email_address"] = value
                else:
                    pass
            dim_staff["dim_staff"].append(data_point)
        return dim_staff
    except Exception as e:
        logger.error(e)


if __name__ == "__main__":
    print(
        *get_dim_staff(
            wr.postgresql.connect(secret_id="totesys_db"),
            datetime.datetime.strptime(
                "2022-09-10 18:32:09.709000", "%Y-%m-%d %H:%M:%S.%f"
            ),
        )["dim_staff"],
        sep="\n",
    )
