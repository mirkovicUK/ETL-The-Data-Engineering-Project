from pg8000.native import Connection, literal
from datetime import datetime as dt
from datetime import timedelta
import math

from src.utils.writing_utils.get_secret import get_secret

from awswrangler import _utils

pg8000 = _utils.import_optional_dependency("pg8000")


def write_dim_date(con, starting_date="2021-01-01", updated=dt.now()):
    dim_date_columns = [
        "date_id",
        "year",
        "month",
        "day",
        "day_of_week",
        "day_name",
        "month_name",
        "quarter",
        "last_updated_date",
        "last_updated_time",
    ]
    starting_date = dt.strptime(starting_date, "%Y-%m-%d").date()

    for ii in range(1460):
        starting_date += timedelta(1)
        dim_date_values = [
            starting_date,
            starting_date.year,
            starting_date.month,
            starting_date.day,
            starting_date.weekday() + 1,
            starting_date.strftime("%A"),
            starting_date.strftime("%B"),
            math.ceil(starting_date.month / 3.0),
            updated.date(),
            updated.time(),
        ]

        dim_date_query = f"""
                    INSERT INTO dim_date
                    VALUES
                    ({literal(dim_date_values[0])},{literal(dim_date_values[1])},{literal(dim_date_values[2])},
                    {literal(dim_date_values[3])},{literal(dim_date_values[4])},{literal(dim_date_values[5])},
                    {literal(dim_date_values[6])},{literal(dim_date_values[7])},{literal(dim_date_values[8])},
                    {literal(dim_date_values[9])})
                    ON CONFLICT DO NOTHING;
                    """
        con.run(dim_date_query)


if __name__ == "__main__":
    secret = get_secret("DB_write")
    con = Connection(
        secret["username"],
        host=secret["host"],
        database=secret["dbname"],
        password=secret["password"],
    )
    # write_dim_date(con)
    rows = con.run("SELECT * FROM dim_date ;")
    print(len(rows), "<----------DIM_DATE", sep="\n")
