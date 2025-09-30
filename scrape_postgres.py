import sqlalchemy as db
from enum import Enum
from sqlalchemy.dialects.postgresql import insert


engine = db.create_engine('postgresql://postgres:1234@localhost:5432/maybeland')
connection = engine.connect()
metadata = db.MetaData()
metadata.reflect(bind=engine)

table_flats = db.Table('flats', metadata, autoload_with=engine)
table_job_results = db.Table('job_results', metadata, autoload_with=engine)


class RealtyClass(Enum):
    UNKNOWN = -1
    STANDARD = 0
    COMFORT = 1
    COMFORT_PLUS = 2
    BUSINESS = 3
    BUSINESS_PLUS = 4
    PREMIUM = 5
    PREMIUM_PLUS = 6
    ELITE = 7


class Furnish(Enum):
    UNKNOWN = -1
    NOTHING = 0
    WHITEBOX = 1
    FURNITURE = 2
    WHITEBOX_PLUS = 3


def safe_insert_job_result(result):
    try:
        insert_stmt = insert(table_job_results).values(
            func_name = result[0],
            start_time = result[1],
            end_time = result[2],
            status = result[3],
            result = result[4]
        )
        connection.execute(insert_stmt)
        connection.commit()
    except Exception as e:
        print(e)
