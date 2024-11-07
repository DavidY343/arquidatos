import pandas as pd
import numpy as np
import re
from pymongo import MongoClient



def limpiar_usuarios(nombre_archivo):
    # Leer el archivo CSV
    df = pd.read_csv(nombre_archivo)
    
    # Eliminar la columna duplicada 'Email'
    if 'Email' in df.columns:
        df.drop(columns=['Email'], inplace=True)
    
    # Revisar y corregir valores nulos
    df.fillna({
        'NIF': 'DESCONOCIDO',
        'NOMBRE': 'DESCONOCIDO',
        'EMAIL': 'desconocido@example.com',
        'TELEFONO': '000000000'
    }, inplace=True)
    
    # Identificar y consolidar registros duplicados
    df.drop_duplicates(inplace=True)
    
    # Convertir todos los valores a minúsculas
    df['NIF'] = df['NIF'].str.lower()
    df['NOMBRE'] = df['NOMBRE'].str.lower()
    df['EMAIL'] = df['EMAIL'].str.lower()
    df['TELEFONO'] = df['TELEFONO'].str.lower()

    # Eliminar espacios adicionales y caracteres especiales
    df['NIF'] = df['NIF'].str.strip()
    df['NOMBRE'] = df['NOMBRE'].str.strip()
    df['EMAIL'] = df['EMAIL'].str.strip()
    df['TELEFONO'] = df['TELEFONO'].str.replace(' ', '').str.replace('^(\+34|34)', '', regex=True)
    
    # Validar el formato del número de teléfono
    telefono_regex = re.compile(r'^\d{9}$')
    telefonos_invalidos = df[~df['TELEFONO'].apply(lambda x: bool(telefono_regex.match(x)))]
    
    # Validar el formato del email
    email_regex = re.compile(r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$')
    emails_invalidos = df[~df['EMAIL'].apply(lambda x: bool(email_regex.match(x)))]
    
    # Imprimir mensajes si hay valores inválidos
    if not telefonos_invalidos.empty:
        print("Hay números de teléfono inválidos:")
        print(telefonos_invalidos[['NIF', 'NOMBRE', 'TELEFONO']])
    
    if not emails_invalidos.empty:
        print("Hay emails inválidos:")
        print(emails_invalidos[['NIF', 'NOMBRE', 'EMAIL']])
    
    
    # Conectar a MongoDB y seleccionar la base de datos y colección
    client = MongoClient('mongodb://localhost:27017/')
    db = client['arqui2']
    
    # Eliminar la colección si ya existe
    if 'Usuarios' in db.list_collection_names():
        db.drop_collection('Usuarios')

    # Aplicar la validación del esquema
    db.create_collection('Usuarios', validator={
        '$jsonSchema': {
                'bsonType': 'object',
                'required': ['NIF', 'NOMBRE', 'EMAIL', 'TELEFONO'],
                'properties': {
                    'NIF': {
                        'bsonType': 'string',
                        'pattern': '^[0-9]{3}-[0-9]{2}-[0-9]{4}$',
                        'description': 'Debe ser una cadena con el formato XXX-XX-XXXX y es obligatorio'
                    },
                    'NOMBRE': {
                        'bsonType': 'string',
                        'description': 'Debe ser una cadena en minúsculas y es obligatorio'
                    },
                    'EMAIL': {
                        'bsonType': 'string',
                        'pattern': '^[a-z0-9._%+-]+@[a-z0-9.-]+\\.[a-z]{2,}$',
                        'description': 'Debe ser una cadena con formato de email válido y es obligatorio'
                    },
                    'TELEFONO': {
                        'bsonType': 'string',
                        'pattern': '^\\d{9}$',
                        'description': 'Debe ser una cadena de 9 dígitos y es obligatorio'
                    }
                }
            }
        })
    
    collection = db['Usuarios']
    
    # Convertir el DataFrame a una lista de diccionarios
    registros_limpios_list = df.to_dict('records')
    
    # Insertar los registros en la colección
    collection.insert_many(registros_limpios_list)
    
    print("Datos insertados en MongoDB con éxito.")

if __name__ == "__main__":
    limpiar_usuarios('UsuariosSucio.csv')