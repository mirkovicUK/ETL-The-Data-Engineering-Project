import datetime
import pytest

from awswrangler import _utils

pg8000 = _utils.import_optional_dependency("pg8000")


@pytest.mark.describe("write_fact_sales_order()")
@pytest.mark.it("correct_data_is_written_to_DB")
def test_correct_data_is_written_to_DB():
    print(datetime.date(2022, 11, 3))
    # data = [
    #     {
    #         "sales_order_id": 1,
    #         "created_date": datetime.date(2022, 11, 3),
    #         "created_time": datetime.time(14, 20, 52, 186000),
    #         "last_updated_date": datetime.date(2022, 11, 3),
    #         "last_updated_time": datetime.time(14, 20, 52, 186000),
    #         "design_id": 9,
    #         "sales_staff_id": 16,
    #         "counterparty_id": 18,
    #         "units_sold": 84754,
    #         "unit_price": Decimal("2.43"),
    #         "currency_id": 3,
    #         "agreed_delivery_date": datetime.date(2022, 11, 10),
    #         "agreed_payment_date": datetime.date(2022, 11, 3),
    #         "agreed_delivery_location_id": 4,
    #     },
    #     {
    #         "sales_order_id": 2,
    #         "created_date": datetime.date(2022, 11, 3),
    #         "created_time": datetime.time(14, 20, 52, 186000),
    #         "last_updated_date": datetime.date(2022, 11, 3),
    #         "last_updated_time": datetime.time(14, 20, 52, 186000),
    #         "design_id": 3,
    #         "sales_staff_id": 19,
    #         "counterparty_id": 8,
    #         "units_sold": 42972,
    #         "unit_price": Decimal("3.94"),
    #         "currency_id": 2,
    #         "agreed_delivery_date": datetime.date(2022, 11, 7),
    #         "agreed_payment_date": datetime.date(2022, 11, 8),
    #         "agreed_delivery_location_id": 8,
    #     },
    #     {
    #         "sales_order_id": 3,
    #         "created_date": datetime.date(2022, 11, 3),
    #         "created_time": datetime.time(14, 20, 52, 188000),
    #         "last_updated_date": datetime.date(2022, 11, 3),
    #         "last_updated_time": datetime.time(14, 20, 52, 188000),
    #         "design_id": 4,
    #         "sales_staff_id": 10,
    #         "counterparty_id": 4,
    #         "units_sold": 65839,
    #         "unit_price": Decimal("2.91"),
    #         "currency_id": 3,
    #         "agreed_delivery_date": datetime.date(2022, 11, 6),
    #         "agreed_payment_date": datetime.date(2022, 11, 7),
    #         "agreed_delivery_location_id": 19,
    #     },
    #     {
    #         "sales_order_id": 4,
    #         "created_date": datetime.date(2022, 11, 3),
    #         "created_time": datetime.time(14, 20, 52, 188000),
    #         "last_updated_date": datetime.date(2022, 11, 3),
    #         "last_updated_time": datetime.time(14, 20, 52, 188000),
    #         "design_id": 4,
    #         "sales_staff_id": 10,
    #         "counterparty_id": 16,
    #         "units_sold": 32069,
    #         "unit_price": Decimal("3.89"),
    #         "currency_id": 2,
    #         "agreed_delivery_date": datetime.date(2022, 11, 5),
    #         "agreed_payment_date": datetime.date(2022, 11, 7),
    #         "agreed_delivery_location_id": 15,
    #     },
    #     {
    #         "sales_order_id": 5,
    #         "created_date": datetime.date(2022, 11, 3),
    #         "created_time": datetime.time(14, 20, 52, 186000),
    #         "last_updated_date": datetime.date(2022, 11, 3),
    #         "last_updated_time": datetime.time(14, 20, 52, 186000),
    #         "design_id": 7,
    #         "sales_staff_id": 18,
    #         "counterparty_id": 4,
    #         "units_sold": 49659,
    #         "unit_price": Decimal("2.41"),
    #         "currency_id": 3,
    #         "agreed_delivery_date": datetime.date(2022, 11, 5),
    #         "agreed_payment_date": datetime.date(2022, 11, 8),
    #         "agreed_delivery_location_id": 25,
    #     },
    # ]

    # secret = get_secret('DB_write')
    # con = Connection(secret['username'],
    #                 host = secret['host'],
    #                 database = secret['dbname'],
    #                 password = secret['password'])
    # wfso(con, data, datetime.datetime.now() )
    # rows = con.run("DELETE FROM fact_sales_order; ;")
    # rows = con.run("SELECT * FROM fact_sales_order LIMIT 10;")
    # print(*rows, '<----------fact_sales_order', sep='\n')
