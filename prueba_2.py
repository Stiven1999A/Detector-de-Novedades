import pandas as pd
import numpy as np
from datetime import datetime
from Utils.funciones import contaminacion
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
import matplotlib.pyplot as plt

fecha = '2024-09-10'

fecha_dia_atras = datetime.strptime(fecha, '%Y-%m-%d')

df = pd.read_csv('C:\\Users\\Estiben\\OneDrive\\Escritorio\\aprendizaje\\Outcome\\data2024-09-09.csv', sep='|')
df['Fecha'] = pd.to_datetime(df['Fecha'])
df = df.drop(df.columns[0], axis=1)
df = df.sort_values('total_mipsFecha', ascending=False)
df = df.reset_index()
df = df.drop(df.columns[0], axis=1)

#print(df.head())

# Valores únicos de dataframe
Nombres_Procesos = df['NombreProceso'].unique()
#print(Nombres_Procesos, len(Nombres_Procesos))
#Creación del dataframe con los inliers de todos los proceso
inliers = pd.DataFrame(columns=['NombreProceso', 'total_mipsFecha'])

for nombre in Nombres_Procesos:
    df_Proceso = df[df['NombreProceso'] == nombre]
    if len(df_Proceso) > 5:
        X = df_Proceso[['total_mipsFecha']].values 
        n_neighbors, contamination = contaminacion(df_Proceso, "total_mipsFecha")
        lof = LocalOutlierFactor(n_neighbors=n_neighbors, contamination=contamination)
        y_pred = lof.fit_predict(X)
        df_Proceso['Es_Tipico'] = y_pred
        inliers_Proceso = pd.DataFrame({
            'NombreProceso': df_Proceso[df_Proceso['Es_Tipico'] == 1]['NombreProceso'],
            'total_mipsFecha': df_Proceso[df_Proceso['Es_Tipico'] == 1]['total_mipsFecha']
        })
        inliers = pd.concat([inliers, inliers_Proceso], ignore_index=True)

inliers.to_csv('C:\\Users\\Estiben\\OneDrive\\Escritorio\\aprendizaje\\Outcome\\inliers_hasta_sep_2.csv', sep='|')
