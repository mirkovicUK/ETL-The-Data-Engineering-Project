from pg8000.native import literal
from datetime import datetime as dt

from awswrangler import _utils

pg8000 = _utils.import_optional_dependency("pg8000")


def write_dim_design(con, data, updated=dt.now()):
    # dim_design_columns = [
    #     "design_record_id",
    #     "design_id",
    #     "design_name",
    #     "file_location",
    #     "file_name",
    #     "last_updated_date",
    #     "last_updated_time",
    # ]
    for data_point in data:
        values = [
            data_point["design_id"],
            data_point["design_id"],
            data_point["design_name"],
            data_point["file_location"],
            data_point["file_name"],
            updated.date(),
            updated.time(),
        ]

        dim_design_query = f"""
            INSERT INTO dim_design
            VALUES
            ({literal(values[0])},{literal(values[1])},
            {literal(values[2])},{literal(values[3])},
            {literal(values[4])},{literal(values[5])},
            {literal(values[6])})
            ON CONFLICT DO NOTHING;
            """
        con.run(dim_design_query)
