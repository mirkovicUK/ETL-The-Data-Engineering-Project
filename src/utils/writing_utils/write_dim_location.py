from pg8000.native import literal
from datetime import datetime as dt
from awswrangler import _utils

pg8000 = _utils.import_optional_dependency("pg8000")


def write_dim_location(con, data, updated=dt.now()):
    # dim_location_columns = [
    #     "location_record_id",
    #     "address_id",
    #     "address_line_1",
    #     "address_line_2",
    #     "district",
    #     "city",
    #     "postal_code",
    #     "country",
    #     "phone",
    #     "last_updated_date",
    #     "last_updated_time",
    # ]

    for data_point in data:
        values = [
            data_point["location_id"],
            data_point["location_id"],
            data_point["address_line_1"],
            data_point["address_line_2"],
            data_point["district"],
            data_point["city"],
            data_point["postal_code"],
            data_point["country"],
            data_point["phone"],
            updated.date(),
            updated.time(),
        ]

        dim_location_query = f"""
            INSERT INTO dim_location
            VALUES
            ({literal(values[0])},{literal(values[1])},
            {literal(values[2])},{literal(values[3])},
            {literal(values[4])},{literal(values[5])},
            {literal(values[6])},{literal(values[7])},
            {literal(values[8])},{literal(values[9])},
            {literal(values[10])})
            ON CONFLICT DO NOTHING;
            """
        con.run(dim_location_query)
