import pandas as pd
from pymongo import MongoClient

def insertar_csv_en_mongodb(nombre_archivo_csv, nombre_bd, nombre_coleccion, validador):
    # Leer el archivo CSV
    df = pd.read_csv(nombre_archivo_csv)
    
    # Convertir el DataFrame a una lista de diccionarios
    registros = df.to_dict('records')
    
    # Conectar a MongoDB y seleccionar la base de datos y colección
    client = MongoClient('mongodb://localhost:27017/')
    db = client[nombre_bd]
    
    # Eliminar la colección si ya existe
    if nombre_coleccion in db.list_collection_names():
        db.drop_collection(nombre_coleccion)
    
    # Crear la colección con el validador de esquema
    db.create_collection(nombre_coleccion, validator=validador)
    
    # Insertar los registros en la colección
    collection = db[nombre_coleccion]
    collection.insert_many(registros)
    
    print("Datos insertados en MongoDB con éxito.")

if __name__ == "__main__":
    validador_areas_recreativas = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["ID", "DESC_CLASIFICACION", "COD_BARRIO", "BARRIO", "COD_DISTRITO", "DISTRITO", "ESTADO", "COORD_GIS_X", "COORD_GIS_Y", "SISTEMA_COORD", "LATITUD", "LONGITUD", "TIPO_VIA", "NOM_VIA", "NUM_VIA", "COD_POSTAL", "DIRECCION_AUX", "NDP", "FECHA_INSTALACION", "CODIGO_INTERNO", "CONTRATO_COD", "TOTAL_ELEM", "tipo", "capacidadMax", "estadoGlobalArea"],
            "properties": {
                "ID": {
                    "bsonType": "int",
                    "description": "Debe ser un entero y es obligatorio"
                },
                "DESC_CLASIFICACION": {
                    "bsonType": "string",
                    "description": "Debe ser una cadena y es obligatorio"
                },
                "COD_BARRIO": {
                    "bsonType": "int",
                    "description": "Debe ser un entero y es obligatorio"
                },
                "BARRIO": {
                    "bsonType": "string",
                    "description": "Debe ser una cadena y es obligatorio"
                },
                "COD_DISTRITO": {
                    "bsonType": "int",
                    "description": "Debe ser un entero y es obligatorio"
                },
                "DISTRITO": {
                    "bsonType": "string",
                    "description": "Debe ser una cadena y es obligatorio"
                },
                "ESTADO": {
                    "bsonType": "string",
                    "description": "Debe ser una cadena y es obligatorio"
                },
                "COORD_GIS_X": {
                    "bsonType": "double",
                    "description": "Debe ser un número y es obligatorio"
                },
                "COORD_GIS_Y": {
                    "bsonType": "double",
                    "description": "Debe ser un número y es obligatorio"
                },
                "SISTEMA_COORD": {
                    "bsonType": "string",
                    "description": "Debe ser una cadena y es obligatorio"
                },
                "LATITUD": {
                    "bsonType": "double",
                    "description": "Debe ser un número y es obligatorio"
                },
                "LONGITUD": {
                    "bsonType": "double",
                    "description": "Debe ser un número y es obligatorio"
                },
                "TIPO_VIA": {
                    "bsonType": "string",
                    "description": "Debe ser una cadena y es obligatorio"
                },
                "NOM_VIA": {
                    "bsonType": "string",
                    "description": "Debe ser una cadena y es obligatorio"
                },
                "NUM_VIA": {
                    "bsonType": "string",
                    "description": "Debe ser una cadena y es obligatorio"
                },
                "COD_POSTAL": {
                    "bsonType": "int",
                    "description": "Debe ser un entero y es obligatorio"
                },
                "DIRECCION_AUX": {
                    "bsonType": "string",
                    "description": "Debe ser una cadena y es obligatorio"
                },
                "NDP": {
                    "bsonType": "int",
                    "description": "Debe ser un entero y es obligatorio"
                },
                "FECHA_INSTALACION": {
                    "bsonType": "string",
                    "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
                    "description": "Debe ser una cadena en formato de fecha (YYYY-MM-DD) y es obligatorio"
                },
                "CODIGO_INTERNO": {
                    "bsonType": "string",
                    "description": "Debe ser una cadena y es obligatorio"
                },
                "CONTRATO_COD": {
                    "bsonType": "string",
                    "description": "Debe ser una cadena y es obligatorio"
                },
                "TOTAL_ELEM": {
                    "bsonType": "int",
                    "description": "Debe ser un entero y es obligatorio"
                },
                "tipo": {
                    "bsonType": "string",
                    "description": "Debe ser una cadena y es obligatorio"
                },
                "capacidadMax": {
                    "bsonType": "int",
                    "description": "Debe ser un entero y es obligatorio"
                },
                "estadoGlobalArea": {
                    "bsonType": "string",
                    "description": "Debe ser una cadena y es obligatorio"
                }
            }
        }
    }

    insertar_csv_en_mongodb('AreasLimpio.csv', 'arqui2', 'AreasRecreativa', validador_areas_recreativas)