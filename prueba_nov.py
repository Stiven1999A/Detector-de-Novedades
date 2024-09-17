import math
import pandas as pd
import numpy as np
import seaborn as sns
from datetime import datetime
from Utils.funciones import medidas_tendencia_central
from Utils.funciones import contaminacion
from sklearn.svm import OneClassSVM
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt

fecha = '2024-09-02'

fecha_dia_atras = datetime.strptime(fecha, '%Y-%m-%d')

df = pd.read_csv('C:\\Users\\Estiben\\OneDrive\\Escritorio\\aprendizaje\\Outcome\\data2024-09-12.csv', sep='|')
df['Fecha'] = pd.to_datetime(df['Fecha'])
df = df.drop(df.columns[0], axis=1)
df = df.sort_values('total_mipsFecha', ascending=False)
df = df.reset_index()
df = df.drop(df.columns[0], axis=1)
#print(df.head())
#Estos son los nuevos datos que van a ingresar diariamente.
novadata = df
#Ahora debo de entrenar el modelo con los datos que son inliers
df_inliers = pd.read_csv('C:\\Users\\Estiben\\OneDrive\\Escritorio\\aprendizaje\\Outcome\\inliers_hasta_sep_2.csv', sep='|')
v = 0
n = 0
i = 0
novedades = pd.DataFrame(columns=['NombreProceso', 'total_mipsFecha'])

for nombre in novadata['NombreProceso']:
    i = i + 1
    mips_inliers = pd.DataFrame(df_inliers[df_inliers['NombreProceso'] == nombre]['total_mipsFecha'])
    mediana_mips_inlier = mips_inliers.median()
    X = mips_inliers[['total_mipsFecha']]
    if len(X) == 0:
        v = v + 1
    elif any(isinstance(x, (int, float)) and x < 0 for x in X):
        n = n + 1
    else: 
        clf = OneClassSVM(kernel='rbf', gamma=0.99, nu=0.01).fit(X)
        mips_novadata = pd.DataFrame(novadata[novadata['NombreProceso'] == nombre]['total_mipsFecha'])
        X_nuevo = mips_novadata[['total_mipsFecha']]
        X_nuevo = X_nuevo.reset_index(drop=True)
        pred = clf.predict(X_nuevo)
        if len(pred) == 1:
            if pred == -1 and X_nuevo[0,0] > mediana_mips_inlier:
                print('True')
                outlier_proceso = pd.DataFrame({
                    'NombreProceso': nombre,
                    'total_mipsFecha': mips_novadata['total_mipsFecha']
                })
                novedades = pd.concat([novedades, outlier_proceso], ignore_index=True)
            else:
                pass
        else:
            if -1 in pred:
                indices = [j for j, y in enumerate(pred) if y == -1]
                outliers_vector = []
                for j in indices:
                    print(X_nuevo['total_mipsFecha'].loc[j])
                    if X_nuevo['total_mipsFecha'].loc[j] > mediana_mips_inlier:
                        outliers_vector.append(X_nuevo['total_mipsFecha'].loc[j])
                        outlier_proceso = pd.DataFrame({
                            'NombreProceso': nombre * len(outliers_vector),
                            'total_mipsFecha': outliers_vector
                        })
                novedades = pd.concat([novedades, outlier_proceso], ignore_index=True)
            else:
                pass
print(novedades)
print(f'La cantidad de novedades encontradas es:{len(novedades)}')
print(f'La cantidad de filas vacias es: {v}')
print(f'La cantidad de valores negativos es: {n}')
