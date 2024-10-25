"""main.py"""
import os
import psycopg2
from dotenv import load_dotenv
from scripts.insertingdata import fetch_new_data, update_database
from database.connection import connect_to_db
from database.dataframe_utils import label_atypical_consumptions

load_dotenv()

def main():
    """
    Main function to connect to the database, fetch new data, update the database, 
    label atypical consumptions, and print the labeled data.
    """
    db_name = os.getenv('DB_NAME')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')

    try:
        conn = connect_to_db(db_name, user, password, host, port)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM public."Fechas"')
        count = cursor.fetchone()[0]

        file_path = (
            "C:\\Users\\Estiben\\OneDrive\\Escritorio\\aprendizaje\\Input\\data.csv"
            if count == 0 else
            "C:\\Users\\Estiben\\OneDrive\\Escritorio\\aprendizaje\\Input\\data_16_20_oct.csv"
        )

        new_data = fetch_new_data(file_path)
        updated_data = update_database(conn, new_data)
        labeled_data = label_atypical_consumptions(conn, updated_data)
        print(labeled_data)

    except (psycopg2.DatabaseError, FileNotFoundError) as e:
        print(f"A database or file error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
