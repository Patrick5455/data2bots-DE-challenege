import psycopg2
from psycopg2._psycopg import connection
from constants import *


def connect_to_postgres() -> connection:
    try:
        data2bots_db_connection = psycopg2.connect(host=DB_HOST, port=DB_PORT,
                                                   database=DB_NAME, user=DB_USER,
                                                   password=DB_PASSWORD)
        print('connected to DB successfully')
        return data2bots_db_connection
    except Exception as db_error:
        print(f"something went wrong while trying to connect with "
              f"{DB_NAME} DB", str(db_error))

