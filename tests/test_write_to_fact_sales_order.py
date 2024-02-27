from src.utils.writing_utils.write_to_fact_sales_order import write_to_fact_sales_order as write_fso
import pytest
import awswrangler as wr
@pytest.mark.skip
def test_shape():
    data = [{
            "agreed_delivery_date": "2024-02-25",
            "agreed_delivery_location_id": 28,
            "agreed_payment_date": "2024-02-23",
            "counterparty_record_id": 12, #"counterparty_id": 12,
            "created_date": "2024-02-20",
            "created_time": "15:31:10.295000",
            "currency_record_id": 1, #"currency_id": 1,
            "design_record_id": 19, #"design_id": 19,
            "last_updated_date": "2024-02-20",
            "last_updated_time": "15:31:10.295000",
            "sales_order_id": 6882,
            "sales_staff_id": 20,
            "unit_price": "3.47",
            "units_sold": 44148}]
    con = wr.postgresql.connect(secret_id = 'data_warehouse')
    
    write_fso(con, data)
    con.close()