import pandas as pd
import numpy as np
import seaborn as sns
import math
from Utils.funciones import medidas_tendencia_central
from Utils.funciones import contaminacion
from datetime import datetime
from sklearn.neighbors import LocalOutlierFactor
import matplotlib.pyplot as plt

fecha = '2024-09-02'

fecha_dia_atras = datetime.strptime(fecha, '%Y-%m-%d')
# 1) Se obtiene el dataframe con todos los datos.
df = pd.read_csv('C:\\Users\\Estiben\\OneDrive\\Escritorio\\aprendizaje\\data2024-09-09.csv', sep='|')
df['Fecha'] = pd.to_datetime(df['Fecha'])
df = df.drop(df.columns[0], axis=1)
df = df.sort_values('total_mipsFecha', ascending=False)
df = df.reset_index()
df = df.drop(df.columns[0], axis=1)
print(df.head()) #Este es el dataframe con todos los datos del histórico.

#2) Se debe de hacer un modelo para cada proceso del histórico

name_pro = 'ASSISTANT/WORKER'
dfProceso = df[df['NombreProceso'] == name_pro]

print(len(dfProceso))
print(dfProceso)
#medidas = medidas_tendencia_central(dfProceso, "total_mipsFecha")
#print(medidas)

sns.boxplot(y=dfProceso['total_mipsFecha'])
plt.savefig('boxplot.png')

X = dfProceso[['total_mipsFecha']].values  # Convertir la columna 'y' a una matriz 2D
#print(X)

#n_neighbors = math.ceil(0.1 * (len(X)))
n_neighbors, contamination = contaminacion(dfProceso, "total_mipsFecha")
print(n_neighbors, contamination)
# Crear el modelo LOF
lof = LocalOutlierFactor(n_neighbors=n_neighbors, contamination=contamination)

# Ajustar el modelo y predecir outliers
y_pred = lof.fit_predict(X)  # 1 para inliers, -1 para outliers
#print(y_pred)
# Agregar la columna de predicciones al DataFrame
dfProceso['Outlier'] = y_pred
#print(dfProceso)
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
plt.ylabel('Valor de MIPS')
plt.legend()

# Guardar la figura como un archivo de imagen (por ejemplo, PNG)
plt.savefig('outliers_detection.png')
