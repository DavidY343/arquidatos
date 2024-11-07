import pandas as pd

def analizar_datos(nombre_archivo):
    # Leer el archivo CSV
    df = pd.read_csv(nombre_archivo)
    
    # Mostrar las primeras filas del DataFrame
    print("Primeras filas del DataFrame:")
    print(df.head(10))
    
    # Mostrar información general del DataFrame
    print("\nInformación general del DataFrame:")
    print(df.info())
    
    # Mostrar estadísticas descriptivas del DataFrame
    print("\nEstadísticas descriptivas del DataFrame:")
    print(df.describe())
    
    # Mostrar el número total de filas
    print("\nNúmero total de filas en el DataFrame:")
    print(len(df))
    
    # Mostrar el número de valores únicos por columna
    print("\nNúmero de valores únicos por columna:")
    print(df.nunique())
    
    # Verificar si hay combinaciones duplicadas de FECHA y DISTRITO
    print("\nNúmero de combinaciones duplicadas de FECHA y DISTRITO:")
    duplicados = df.duplicated(subset=['FECHA', 'DISTRITO']).sum()
    print(duplicados)
    
    # Mostrar algunos ejemplos de combinaciones duplicadas de FECHA y DISTRITO
    if duplicados > 0:
        print("\nEjemplos de combinaciones duplicadas de FECHA y DISTRITO:")
        duplicados_df = df[df.duplicated(subset=['FECHA', 'DISTRITO'], keep=False)]
        print(duplicados_df.head(20))
    
    # Guardar los registros duplicados en un nuevo archivo CSV si existen
    if duplicados > 0:
        duplicados_df.to_csv('meteo24_duplicados.csv', index=False)
        print("\nRegistros duplicados guardados en 'meteo24_duplicados.csv'")

if __name__ == "__main__":
    analizar_datos('meteo24_limpio.csv')