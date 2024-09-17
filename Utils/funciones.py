import pandas as pd

def medidas_tendencia_central(df, columna):
    """
    Calcula las medidas de tendencia central (media, mediana, moda) 
    y los cuartiles (primer y tercer cuartil), además de la varianza 
    y la desviación estándar de una columna de un DataFrame.
    
    Parámetros:
    df (pd.DataFrame): El DataFrame que contiene los datos.
    columna (str): El nombre de la columna de la cual se desean calcular las medidas.
    
    Retorna:
    dict: Un diccionario con la media, mediana, moda, primer cuartil, 
    tercer cuartil, varianza y desviación estándar de la columna.
    """
    try:
        media = df[columna].mean()
        mediana = df[columna].median()
        moda = df[columna].mode().values[0]
        primer_cuantil = df[columna].quantile(0.25)
        tercer_cuantil = df[columna].quantile(0.75)
        varianza = df[columna].var()
        desviacion_estandar = df[columna].std()
        return {
            "media": media,
            "mediana": mediana,
            "moda": moda,
            "primer_cuantil": primer_cuantil,
            "tercer_cuantil": tercer_cuantil,
            "varianza": varianza,
            "desviacion_estandar": desviacion_estandar
        }    
    except KeyError:
        return f"La columna '{columna}' no existe en el DataFrame."
    except Exception as e:
        return f"Error: {e}"

# Ejemplo de uso:
#df = pd.DataFrame({"columna1": [1, 2, 2, 3, 4, 4, 4, 5]})
#resultado = medidas_tendencia_central(df, "columna1")
#print(resultado)

def contaminacion(df, columna):
    """
    Calcula el máximo de los extremos de los bigotes para una columna específica 
    de un DataFrame, basándose en el cálculo del boxplot.

    Parámetros:
    df (pd.DataFrame): El DataFrame que contiene los datos.
    columna (str): El nombre de la columna para la cual se desea calcular el máximo de los bigotes.

    Retorna:
    float: El valor máximo de los extremos de los bigotes para la columna especificada.
    """
    if columna not in df.columns:
        raise ValueError(f"La columna '{columna}' no existe en el DataFrame.")
    datos = df[columna]
    if not pd.api.types.is_numeric_dtype(datos):
        raise ValueError("Los datos en la columna deben ser numéricos.")
    Q1 = datos.quantile(0.25)
    Q3 = datos.quantile(0.75)
    IQR = Q3 - Q1
    if len(datos) > 20:
        limite_superior = Q3 + 1.5 * IQR
        max_extremo = datos[datos <= limite_superior].max()
        conteo = (datos > max_extremo).sum()

        if conteo == 0:
            contamination = 'auto'
        else:
            contamination = conteo / len(datos)

        n_neighbors = 20
    else:
        limite_superior = Q3
        max_extremo = datos[datos <= limite_superior].max()
        conteo = (datos > max_extremo).sum()

        if conteo == 0:
            contamination = 'auto'
        else:
            contamination = conteo / len(datos)

        n_neighbors = conteo + 1
    return n_neighbors, contamination
