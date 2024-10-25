"scripts/forecasting.py"
import pandas as pd
from prophet import Prophet
from datetime import datetime, timedelta
from database.dataframe_utils import add_day_of_week_id
from database.connection import connect_to_db

def fit_predictive_model(df):
    # Connect to the database
    conn = connect_to_db()
    
    # Identify unique IdProceso
    unique_id_proceso = df['IdProceso'].unique()
    
    # Prepare the dataframe to store predictions
    predictions = []

    for id_proceso in unique_id_proceso:
        # Fetch data for the current IdProceso
        query = f"""
        SELECT * FROM public."ConsumoMIPS"
        WHERE "IdProceso" = {id_proceso} AND "IdAtipico" IN (-1, 0, 1);
        """
        data = pd.read_sql(query, conn)
        unique_id_fecha = data['IdFecha'].unique()
        # Fetch corresponding dates from the public."Fechas" table
        date_query = f"""
        SELECT "IdFecha", "Fecha" FROM public."Fechas"
        WHERE "IdFecha" IN ({','.join(map(str, unique_id_fecha))});
        """
        date_data = pd.read_sql(date_query, conn)
        
        # Merge the date data with the main data
        data = data.merge(date_data, on='IdFecha', how='left')
        
        # Prepare data for Prophet
        data['Fecha'] = pd.to_datetime(data['Fecha'], format='%Y-%m-%d')
        prophet_df = data[['Fecha', 'ConsumoMIPS']].rename(columns={'Fecha': 'ds', 'ConsumoMIPS': 'y'})
        
        # Fit the model
        model = Prophet()
        model.add_country_holidays(country_name='CO')
        model.fit(prophet_df)
        # Draw the maximum value of "IdFecha" from the table public."ConsumoMIPS"
        max_date_query = f"""
        SELECT MAX("IdFecha") as max_id_fecha FROM public."ConsumoMIPS";
        """
        max_date_result = pd.read_sql(max_date_query, conn)
        max_id_fecha = max_date_result['max_id_fecha'].iloc[0]
        # Create future dataframe
        future_dates = model.make_future_dataframe(periods=(datetime.now().days_in_month - datetime.now().day) + 30)
        
        # Predict
        forecast = model.predict(future_dates)
        
        # Merge forecast with original data to get additional columns
        forecast = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
        forecast = forecast.merge(data[['IdGrupo', 'IdProceso']], left_on='ds', right_on='Fecha', how='left')
        
        # Add IdFecha and IdDiaSemana
        forecast['IdFecha'] = forecast['ds'].dt.strftime('%Y-%m-%d')
        forecast = add_day_of_week_id(forecast)
        
        # Prepare predictions for insertion
        for _, row in forecast.iterrows():
            predictions.append({
                'IdPrediccion': len(predictions) + 1,
                'IdProceso': id_proceso,
                'IdGrupo': row['IdGrupo'],
                'IdFecha': row['IdFecha'],
                'IdDiaSemana': row['IdDiaSemana'],
                'Prediccion': row['yhat'],
                'LimInf': row['yhat_lower'],
                'LimSup': row['yhat_upper']
            })
    
    # Convert predictions to DataFrame
    predictions_df = pd.DataFrame(predictions)
    
    # Insert predictions into the database
    predictions_df.to_sql('PrediccionesMIPS', conn, if_exists='append', index=False)
    
    # Close the database connection
    conn.close()
    
    return predictions_df