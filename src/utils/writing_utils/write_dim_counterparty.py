from pg8000.native import literal
from datetime import datetime as dt
from awswrangler import _utils

pg8000 = _utils.import_optional_dependency("pg8000")


def write_dim_counterparty(con, data, updated=dt.now()):
    # dim_counterparty_column = [
    #     'counterparty_record_id',
    #     'counterparty_id',
    #     'counterparty_legal_name',
    #     'counterparty_legal_address_line_1',
    #     'counterparty_legal_address_line_2',
    #     'counterparty_legal_district',
    #     'counterparty_legal_city',
    #     'counterparty_legal_postal_code',
    #     'counterparty_legal_country',
    #     'counterparty_legal_phone_number',
    #     'last_updated_date',
    #     'last_updated_time']
    for data_point in data:
        values = [
            data_point["counterparty_id"],
            data_point["counterparty_id"],
            data_point["counterparty_legal_name"],
            data_point["counterparty_legal_address_line_1"],
            data_point["counterparty_legal_address_line_2"],
            data_point["counterparty_legal_district"],
            data_point["counterparty_legal_city"],
            data_point["counterparty_legal_postal_code"],
            data_point["counterparty_legal_country"],
            data_point["counterparty_legal_phone_number"],
            updated.date(),
            updated.time(),
        ]

        dim_counterparty_query = f"""
        INSERT INTO dim_counterparty
        VALUES
        ({literal(values[0])},{literal(values[1])},
        {literal(values[2])},{literal(values[3])},
        {literal(values[4])},{literal(values[5])},
        {literal(values[6])},{literal(values[7])},
        {literal(values[8])},{literal(values[9])},
        {literal(values[10])},{literal(values[11])})
        ON CONFLICT DO NOTHING;
        """
        con.run(dim_counterparty_query)
