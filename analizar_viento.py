import pandas as pd

def analizar_viento(nombre_archivo):
    # Leer el archivo CSV
    df = pd.read_csv(nombre_archivo, sep=';')
    
    # Filtrar los datos de viento (magnitud 81)
    df_viento = df[df['MAGNITUD'] == 81]
    
    # Crear una lista para almacenar los valores de viento
    valores_viento = []

    # Iterar sobre cada fila del DataFrame filtrado
    for index, row in df_viento.iterrows():
        # Iterar sobre los días del mes
        for dia in range(1, 32):
            dia_col = f'D{dia:02d}'
            val_col = f'V{dia:02d}'
            
            # Verificar si el dato es válido
            if row[val_col] == 'V':
                # Agregar el valor de viento a la lista
                valores_viento.append(row[dia_col])
    
    # Convertir la lista a un DataFrame para análisis
    df_valores_viento = pd.DataFrame(valores_viento, columns=['VIENTO'])
    
    # Mostrar estadísticas descriptivas de los valores de viento
    print("\nEstadísticas descriptivas de los valores de viento:")
    print(df_valores_viento.describe())
    
    # Calcular y mostrar la media del viento
    media_viento = df_valores_viento['VIENTO'].mean()
    print(f"\nMedia del viento: {media_viento}")

if __name__ == "__main__":
    analizar_viento('meteo24.csv')