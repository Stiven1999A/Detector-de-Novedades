"""database/connection.py"""
import psycopg2

def connect_to_db(db_name, user, password, host, port):
    """_summary_

    Args:
        db_name (_type_): _description_
        user (_type_): _description_
        password (_type_): _description_
        host (_type_): _description_
        port (_type_): _description_

    Returns:
        _type_: _description_
    """
    conn = psycopg2.connect(
        dbname=db_name,
        user=user,
        password=password,
        host=host,
        port=port
    )
    return conn
