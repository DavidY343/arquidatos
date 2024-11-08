import pandas as pd
from pymongo import MongoClient

def insertar_csv_en_mongodb(nombre_archivo_csv, nombre_bd, nombre_coleccion):
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
    
    # Crear la colección
    collection = db[nombre_coleccion]
    
    # Insertar los registros en la colección
    collection.insert_many(registros)
    
    print("Datos insertados en MongoDB con éxito.")

if __name__ == "__main__":
    insertar_csv_en_mongodb('AreasLimpio.csv', 'arqui2', 'AreasRecreativa')