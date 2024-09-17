import pyodbc
import math
import pandas as pd
from urllib import parse
from datetime import datetime
from sklearn.neighbors import LocalOutlierFactor
from sqlalchemy import create_engine
from decouple import config
import matplotlib.pyplot as plt

conn_str = 'mssql+pyodbc://usrCargaMips:clvC4rG4M1p5PZM77@296COSC01\SEGUNDA/WLMDW_TEMPO?driver=ODBC+Driver+17+for+SQL+Server'

engine = create_engine(conn_str)

fecha = '2024-09-02'

fecha_dt = datetime.strptime(fecha, '%Y-%m-%d')

fecha_un_anio_antes = fecha_dt.replace(year=(fecha_dt.year - 1))

fecha_un_anio_antes_str = fecha_un_anio_antes.strftime('%Y-%m-%d')

query = f"SELECT * FROM dbo.refrescarprocesos_10dias WHERE Fecha BETWEEN '{fecha_un_anio_antes_str}' AND '{fecha}';"  # Cambia por tu tabla

df = pd.read_sql(query, engine)

fecha_dia_atras = datetime.strptime(fecha, '%Y-%m-%d')

dfProceso = df[df['NombreProceso'] == '(SINCO)PSINCO/IRBORIEMUE ON EXF']
print(dfProceso)

X = dfProceso[['total_mipsFecha']].values  # Convertir la columna 'y' a una matriz 2D
print(X)

n_neighbors = math.ceil(0.1 * (len(X)))

# Crear el modelo LOF
lof = LocalOutlierFactor(n_neighbors=n_neighbors, contamination="auto")

# Ajustar el modelo y predecir outliers
y_pred = lof.fit_predict(X)  # 1 para inliers, -1 para outliers
print(y_pred)
# Agregar la columna de predicciones al DataFrame
dfProceso['Outlier'] = y_pred
print(dfProceso)
conteo = dfProceso['Outlier'].value_counts()
print(conteo[1] / conteo.sum(), conteo[-1] / conteo.sum())
# 3. Visualizar los resultados
# Puntos normales
normal_points = dfProceso[dfProceso['Outlier'] == 1]['total_mipsFecha']
print(normal_points.max())
# Outliers
outlier_points = dfProceso[dfProceso['Outlier'] == -1]['total_mipsFecha']

plt.figure(figsize=(10, 6))
plt.plot(normal_points, 'bo', label='Normal')
plt.plot(outlier_points, 'ro', label='Outlier')
plt.title('Detección de Outliers usando LocalOutlierFactor (LOF)')
plt.xlabel('Índice')
plt.ylabel('Valor de y')
plt.legend()

# Guardar la figura como un archivo de imagen (por ejemplo, PNG)
plt.savefig('outliers_detection.png')
