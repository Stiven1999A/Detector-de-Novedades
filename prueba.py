import pyodbc
import pandas as pd
from urllib import parse
from datetime import datetime
from sklearn.neighbors import LocalOutlierFactor
from sqlalchemy import create_engine
from decouple import config

conn_str = 'mssql+pyodbc://usrCargaMips:clvC4rG4M1p5PZM77@296COSC01\SEGUNDA/WLMDW_TEMPO?driver=ODBC+Driver+17+for+SQL+Server'

# Crear el engine
engine = create_engine(conn_str)

fecha = '2024-09-09'

fecha_dt = datetime.strptime(fecha, '%Y-%m-%d')

fecha_un_anio_antes = fecha_dt.replace(year=(fecha_dt.year - 1))

fecha_un_anio_antes_str = fecha_un_anio_antes.strftime('%Y-%m-%d')


query = f"SELECT * FROM dbo.refrescarprocesos_10dias WHERE Fecha BETWEEN '{fecha_un_anio_antes_str}' AND '{fecha}';"  # Cambia por tu tabla

df = pd.read_sql(query, engine)

#path_df = f'/mnt/c/Users/egonzalez570/OneDrive - Grupo-exito.com/Escritorio/mips-sinco-estadisticas/app/outcome/data{fecha}.csv'
#df.to_csv(path_df, sep='|')

fecha_dia_atras = datetime.strptime(fecha, '%Y-%m-%d')

dfDiaEvaluar = df[df['Fecha'] == fecha_dia_atras] # dia prueba 2024-04-02 %fecha_dia_atras.strftime("%Y-%m-%d")
#print(dfDiaEvaluar.head())
dfAtipicos = []

for index, row in dfDiaEvaluar.iterrows():
    proceso = row['NombreProceso']
    grupo = row['NombreGrupo']
        # print(proceso)
    dfProceso = df[df['NombreProceso'] == proceso]
    #print(dfProceso)

    if len(dfProceso)>=2:
        dfProceso = dfProceso.sort_values(by='total_mipsFecha')
        dfProceso.reset_index(inplace=True)

            # Entrenamiento a partir del proceso que se tomo
        xModel = pd.DataFrame()
        n_muestras = len(dfProceso)
        n_neighbors = max(1, int(0.2 * n_muestras))
        clf = LocalOutlierFactor(n_neighbors=n_neighbors, contamination="auto")
        #print(clf)
        xModel['total_mipsFecha'] = dfProceso['total_mipsFecha']
        #xModel['indice'] = dfProceso.index
            # y_pred = clf.fit(xModel)
        preds_lof = clf.fit_predict(xModel)
        #print(preds_lof)
            #se agrega a una columna del dataframe si es considerado outlier o no
        dfProceso['is_anomaly'] = preds_lof.tolist()
        X_scores = clf.negative_outlier_factor_
        dfProceso['Scores'] = X_scores

            # consideremos los limites de un dato
        inliers = dfProceso[dfProceso['is_anomaly']==1]
        MaxInlier = inliers['total_mipsFecha'].max()
        #print(dfProceso)
        #print(MaxInlier)
        filaProceso = dfDiaEvaluar[(dfDiaEvaluar['NombreProceso'] == proceso) & (dfDiaEvaluar['NombreGrupo'] == grupo)]
        #print(filaProceso)
        if filaProceso['total_mipsFecha'].values>=MaxInlier:
            dfAtipicos.append({
                'Proceso':proceso,
                'grupo': grupo,
                'Consumo actual': float(filaProceso['total_mipsFecha'].values),
                'Costo Maximo Identificado':MaxInlier,
                'Diferencia': float(filaProceso['total_mipsFecha'].values-MaxInlier),
                '%Crecimiento': float(((filaProceso['total_mipsFecha'].values-MaxInlier)/MaxInlier)*100)  
            })

dfAtipicospd = pd.DataFrame(dfAtipicos)
dfAtipicospd.reset_index(inplace=True)

dfAtipicospd.sort_values('%Crecimiento',inplace=True, ascending=False)
print(dfAtipicospd.head(15))

dfAtipicospd.sort_values('Consumo actual',inplace=True, ascending=False)
print(dfAtipicospd)

pathComplete = f'/mnt/c/Users/egonzalez570/OneDrive - Grupo-exito.com/Escritorio/mips-sinco-estadisticas/app/outcome/DatosAtipicosClass{fecha}.csv'
dfAtipicospd.to_csv(pathComplete, sep='|')



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

#############################################################################################################################
dfDiaEvaluar = df[df['Fecha'] == fecha_dia_atras] # dia prueba 2024-04-02 %fecha_dia_atras.strftime("%Y-%m-%d")
dfDiaEvaluar.sort_values('total_mipsFecha',)
print(dfDiaEvaluar, len(dfDiaEvaluar))

dfAtipicos = []

for index, row in dfDiaEvaluar.iterrows():
    proceso = row['NombreProceso']
    grupo = row['NombreGrupo']
        # print(proceso)
    dfProceso = df[df['NombreProceso'] == proceso]
    #print(dfProceso)

    if len(dfProceso)>=2:
        dfProceso = dfProceso.sort_values(by='total_mipsFecha')
        dfProceso.reset_index(inplace=True)

            # Entrenamiento a partir del proceso que se tomo
        xModel = pd.DataFrame()
        n_muestras = len(dfProceso)
        n_neighbors = max(1, int(0.2 * n_muestras))
        clf = LocalOutlierFactor(n_neighbors=n_neighbors, contamination="auto")
        #print(clf)
        xModel['total_mipsFecha'] = dfProceso['total_mipsFecha']
        #xModel['indice'] = dfProceso.index
            # y_pred = clf.fit(xModel)
        preds_lof = clf.fit_predict(xModel)
        #print(preds_lof)
            #se agrega a una columna del dataframe si es considerado outlier o no
        dfProceso['is_anomaly'] = preds_lof.tolist()
        X_scores = clf.negative_outlier_factor_
        dfProceso['Scores'] = X_scores

            # consideremos los limites de un dato
        inliers = dfProceso[dfProceso['is_anomaly']==1]
        MaxInlier = inliers['total_mipsFecha'].max()
        #print(dfProceso)
        #print(MaxInlier)
        filaProceso = dfDiaEvaluar[(dfDiaEvaluar['NombreProceso'] == proceso) & (dfDiaEvaluar['NombreGrupo'] == grupo)]
        #print(filaProceso)
        if filaProceso['total_mipsFecha'].values>=MaxInlier:
            dfAtipicos.append({
                'Proceso':proceso,
                'grupo': grupo,
                'Consumo actual': float(filaProceso['total_mipsFecha'].values),
                'Costo Maximo Identificado':MaxInlier,
                'Diferencia': float(filaProceso['total_mipsFecha'].values-MaxInlier),
                '%Crecimiento': float(((filaProceso['total_mipsFecha'].values-MaxInlier)/MaxInlier)*100)  
            })

dfAtipicospd = pd.DataFrame(dfAtipicos)
dfAtipicospd.reset_index(inplace=True)

dfAtipicospd.sort_values('%Crecimiento',inplace=True, ascending=False)
print(dfAtipicospd.head(15))

dfAtipicospd.sort_values('Consumo actual',inplace=True, ascending=False)
print(dfAtipicospd)
