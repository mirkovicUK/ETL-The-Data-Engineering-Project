from pg8000.native import Connection, literal, identifier, DatabaseError
import pytest

from awswrangler import _utils

pg8000 = _utils.import_optional_dependency("pg8000")


# @pytest.mark.describe("write_dim_location()")
# @pytest.mark.it("correct_data_is_written_to_DB")
# def test_correct_data_is_written_to_DB():
# data = [
#     {
#         "location_id": 1,
#         "address_line_1": "6826 Herzog Via",
#         "address_line_2": None,
#         "district": "Avon",
#         "city": "New Patienceburgh",
#         "postal_code": "28441",
#         "country": "Turkey",
#         "phone": "1803 637401",
#     },
#     {
#         "location_id": 2,
#         "address_line_1": "179 Alexie Cliffs",
#         "address_line_2": None,
#         "district": None,
#         "city": "Aliso Viejo",
#         "postal_code": "99305-7380",
#         "country": "San Marino",
#         "phone": "9621 880720",
#     },
#     {
#         "location_id": 3,
#         "address_line_1": "148 Sincere Fort",
#         "address_line_2": None,
#         "district": None,
#         "city": "Lake Charles",
#         "postal_code": "89360",
#         "country": "Samoa",
#         "phone": "0730 783349",
#     },
#     {
#         "location_id": 4,
#         "address_line_1": "6102 Rogahn Skyway",
#         "address_line_2": None,
#         "district": "Bedfordshire",
#         "city": "Olsonside",
#         "postal_code": "47518",
#         "country": "Republic of Korea",
#         "phone": "1239 706295",
#     },
#     {
#         "location_id": 5,
#         "address_line_1": "34177 Upton Track",
#         "address_line_2": None,
#         "district": None,
#         "city": "Fort Shadburgh",
#         "postal_code": "55993-8850",
#         "country": "Bosnia and Herzegovina",
#         "phone": "0081 009772",
#     },
# ]

# secret = get_secret('DB_write')
# con = Connection(secret['username'],
#                 host = secret['host'],
#                 database = secret['dbname'],
#                 password = secret['password'])
# wdl(con, data, dt.now() )
# rows = con.run("DELETE FROM dim_location; ;")
# rows = con.run("SELECT * FROM dim_location LIMIT 10;")
# print(*rows, '<----------dim_locatiion', sep='\n')
