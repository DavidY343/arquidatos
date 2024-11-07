import pandas as pd

def mostrar_datos(nombre_archivo):
    # Leer el archivo CSV
    df = pd.read_csv(nombre_archivo, sep=';')
    
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
    """
    # Contar el número de pares únicos de FECHA y DISTRITO
    print("\nNúmero de pares únicos de FECHA y ESTACION:")
    pares_unicos = set()
    for index, row in df.iterrows():
        ano = row['ANO']
        mes = row['MES']
        estacion = row['ESTACION']
        for dia in range(1, 32):
            dia_col = f'D{dia:02d}'
            val_col = f'V{dia:02d}'
            if val_col in row and row[val_col] == 'V':
                fecha = f"{ano}-{mes:02d}-{dia:02d}"
                pares_unicos.add((fecha, estacion))
    print(len(pares_unicos))"""

if __name__ == "__main__":
    mostrar_datos('../AreasSucio.csv')