from pg8000.native import Connection, literal, identifier, DatabaseError
from datetime import datetime as dt
from datetime import timedelta
import math

from src.utils.writing_utils.get_secret import get_secret

from awswrangler import _utils

pg8000 = _utils.import_optional_dependency("pg8000")


def write_dim_staff(con, data, updated=dt.now()):
    dim_staf_column = [
        "staff_record_id",
        "first_name",
        "last_name",
        "department_name",
        "location",
        "email_address",
        "last_updated_date",
        "last_updated_time",
    ]
    for data_point in data:
        values = [
            data_point["staff_record_id"],
            data_point["staff_record_id"],
            data_point["first_name"],
            data_point["last_name"],
            data_point["department_name"],
            data_point["location"],
            data_point["email_address"],
            updated.date(),
            updated.time(),
        ]

        dim_staff_query = f"""
        INSERT INTO dim_staff
        VALUES
        ({literal(values[0])},{literal(values[1])},
        {literal(values[2])},{literal(values[3])},
        {literal(values[4])},{literal(values[5])},
        {literal(values[6])},{literal(values[7])},
        {literal(values[8])})
        ON CONFLICT DO NOTHING;
        """
        con.run(dim_staff_query)


if __name__ == "__main__":
    secret = get_secret("DB_write")
    con = Connection(
        secret["username"],
        host=secret["host"],
        database=secret["dbname"],
        password=secret["password"],
    )
    # write_dim_date(con)
    rows = con.run("SELECT * FROM staff ;")
    print(len(rows), "<----------DIM_STAFF", sep="\n")
