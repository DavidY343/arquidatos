import pandas as pd
import numpy as np
import re
from pymongo import MongoClient

def limpiar_datos_meteo24(nombre_archivo, estaciones_archivo):
    # Leer el archivo CSV
    df = pd.read_csv(nombre_archivo, sep=';')
    
    # Leer el archivo de estaciones
    estaciones_df = pd.read_csv(estaciones_archivo, sep=';')
    estaciones_df['CÓDIGO'] = estaciones_df['CÓDIGO'].astype(str)
    
    # Crear un diccionario para almacenar los registros limpios
    registros_limpios = {}

    temperaturas = df[df['MAGNITUD'] == 83].filter(regex='^D\d{2}$').values.flatten()
    temperaturas = temperaturas[~np.isnan(temperaturas)]
    mediana_temperatura = np.median(temperaturas)

    print(f'Mediana de temperatura: {mediana_temperatura}')

    # Iterar sobre cada fila del DataFrame
    for index, row in df.iterrows():
        # Extraer la fecha base
        ano = row['ANO']
        mes = row['MES']
        
        # Iterar sobre los días del mes
        for dia in range(1, 32):
            dia_col = f'D{dia:02d}'
            val_col = f'V{dia:02d}'
            
            # Verificar si el dato es válido
            if row[val_col] == 'V':
                # Crear la fecha
                fecha = f'{ano}-{mes:02d}-{dia:02d}'
                distrito = row['ESTACION']
                
                # Inicializar las variables si no existen
                if (fecha, distrito) not in registros_limpios:
                    registros_limpios[(fecha, distrito)] = {
                        'TEMPERATURA': np.nan,
                        'PRECIPITACION': np.nan,
                        'VIENTO': np.nan,
                        'CÓDIGO': row['PUNTO_MUESTREO'][:8],
                        'DIRECCION': '',
                        'Codigo Postal': ''
                    }
                
                # Extraer los valores según la magnitud
                if row['MAGNITUD'] == 83:
                    registros_limpios[(fecha, distrito)]['TEMPERATURA'] = row[dia_col]
                elif row['MAGNITUD'] == 89:
                    registros_limpios[(fecha, distrito)]['PRECIPITACION'] = row[dia_col]
                elif row['MAGNITUD'] == 81:
                    registros_limpios[(fecha, distrito)]['VIENTO'] = row[dia_col] > 10  # Ejemplo: considerar viento fuerte si > 10
    
    # Agregar la información de dirección y código postal
    for key, datos in registros_limpios.items():
        codigo = datos['CÓDIGO']
        estacion_info = estaciones_df[estaciones_df['CÓDIGO'] == codigo]
        if not estacion_info.empty:
            datos['DIRECCION'] = estacion_info.iloc[0]['DIRECCION']
            datos['Codigo Postal'] = estacion_info.iloc[0]['Codigo Postal']
    
    # Convertir el diccionario a una lista de registros
    registros_limpios_list = [
        {
            'FECHA': fecha,
            'TEMPERATURA': float(datos['TEMPERATURA']) if not pd.isna(datos['TEMPERATURA']) else float(mediana_temperatura),
            'PRECIPITACION': float(datos['PRECIPITACION']) if not pd.isna(datos['PRECIPITACION']) else 0.0,
            'VIENTO': bool(datos['VIENTO']) if not pd.isna(datos['VIENTO']) else False,
            'Codigo Postal': str(datos['Codigo Postal'])
        }
        for (fecha, distrito), datos in registros_limpios.items()
    ]
    
    # Crear un DataFrame con los registros limpios
    df_limpio = pd.DataFrame(registros_limpios_list, columns=['FECHA', 'TEMPERATURA', 'PRECIPITACION', 'VIENTO', 'Codigo Postal'])
   
    # Guardar el DataFrame limpio en un nuevo archivo CSV
    df_limpio.to_csv('meteo24.csv', index=False)

        # Conectar a MongoDB y seleccionar la base de datos y colección
    client = MongoClient('mongodb://localhost:27017/')
    db = client['arqui2']
    
    # Eliminar la colección si ya existe
    if 'meteo24' in db.list_collection_names():
        db.drop_collection('meteo24')

    # Aplicar la validación del esquema
    db.create_collection('meteo24', validator={
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["FECHA", "TEMPERATURA", "PRECIPITACION", "VIENTO", "Codigo Postal"],
            "properties": {
                "FECHA": {
                    "bsonType": "string",
                    "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
                    "description": "Debe ser una cadena en formato de fecha (YYYY-MM-DD) y es obligatorio"
                },
                "TEMPERATURA": {
                    "bsonType": "double",
                    "description": "Debe ser un número y es obligatorio"
                },
                "PRECIPITACION": {
                    "bsonType": "double",
                    "description": "Debe ser un número y es obligatorio"
                },
                "VIENTO": {
                    "bsonType": "bool",
                    "description": "Debe ser un booleano y es obligatorio"
                },
                "Codigo Postal": {
                    "bsonType": "string",
                    "pattern": "^\\d{5}$",
                    "description": "Debe ser una cadena de 5 dígitos y es obligatorio"
                }
            }
        }
    })
    
    collection = db['meteo24']
    
    # Insertar los registros en la colección
    collection.insert_many(registros_limpios_list)
    
    print("Datos insertados en MongoDB con éxito.")

if __name__ == "__main__":
    limpiar_datos_meteo24('meteo24Sucio.csv', 'estaciones_meteo_CodigoPostal.csv')