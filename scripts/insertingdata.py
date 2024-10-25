"""script/insertingdata.py"""
import pandas as pd
from database.dataframe_utils import (
    update_processes,
    update_groups,
    update_procesos_grupos,
    update_fechas,
    add_day_of_week_id,
    filter_existing_rows
)

def fetch_new_data(file_path):
    """
    Reads a CSV file, processes the data, and returns a DataFrame with additional transformations.
    
    Args:
        file_path (str): Path to the CSV file.
    
    Returns:
        pd.DataFrame: The processed DataFrame with the 'IdDiaSemana' column added.
    """
    df = pd.read_csv(file_path, sep='|')
    df['Fecha'] = pd.to_datetime(df['Fecha'])
    df = df.drop(df.columns[0], axis=1)
    df = df.sort_values(['Fecha', 'NombreProceso'], ascending=True)
    df = df.reset_index(drop=True)
    df = add_day_of_week_id(df)
    return df

def update_database(conn, df):
    """
    Updates the database with the provided DataFrame.

    Args:
        conn: A database connection object.
        df: A pandas DataFrame containing the data to be updated.

    Returns:
        pd.DataFrame: A DataFrame with the updated data.
    """
    df = update_processes(conn, df)
    df = update_groups(conn, df)
    df = update_fechas(conn, df)
    update_procesos_grupos(conn, df)
    df = filter_existing_rows(df, conn)
    return df
