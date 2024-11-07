import pandas as pd

def contar_nulos():
    # Cargar el CSV
    df = pd.read_csv('meteo24.csv')
    
    # Contar los valores nulos por columna
    nulos_por_columna = df.isnull().sum()
    
    # Filtrar solo las columnas que tienen valores nulos
    columnas_con_nulos = nulos_por_columna[nulos_por_columna > 0]
    
    # Imprimir las columnas con valores nulos y la cantidad de nulos por columna
    print("Columnas con valores nulos:")
    print(columnas_con_nulos)

# Ejecutar la funciÃ³n
contar_nulos()