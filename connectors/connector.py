from .engine import *
import sqlalchemy

def sql_execute_to_db(query: str, engine, *parameters):
    conn: sqlalchemy.Connection = engine.connect()
    try:
        conn.execute(sqlalchemy.text(query), parameters)
        conn.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
    conn.close()

