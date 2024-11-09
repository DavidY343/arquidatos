import pandas as pd
from pymongo import MongoClient

def insertar_csv_en_mongodb(nombre_archivo_csv, nombre_bd, nombre_coleccion, validador):
    # Leer el archivo CSV
    df = pd.read_csv(nombre_archivo_csv)
    
    if nombre_coleccion == "AreaRecreativa":
        df['nombre'] = df['nombre'].astype(str)
        df['fechaInstalacion'] = pd.to_datetime(df['fechaInstalacion'], errors='coerce')

    if nombre_coleccion == "EncuestaSatisfaccion":
        df['id'] = df['id'].astype(str)
        df['fechaEncuesta'] = pd.to_datetime(df['fechaEncuesta'], errors='coerce')
        df['AreaRecreativaID'] = df['AreaRecreativaID'].astype(str)

    if nombre_coleccion == "IncidenteSeguridad":
        df['id'] = df['id'].astype(str)
        df['fechaReporte'] = pd.to_datetime(df['fechaReporte'], errors='coerce')
        df['AreaRecreativaID'] = df['AreaRecreativaID'].astype(str)

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

def transformar_y_subir_datos():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['arqui']
    
    # Cargar la colección original
    incidencias = db['IncidenciasUsuario']
    
    # Crear un cursor para iterar sobre todos los documentos
    for incidencia in incidencias.find():
        usuario_id = incidencia.get('UsuarioID', "")
        
        # Verificar si el UsuarioID es una cadena (si es una cadena, convertirla a lista)
        if isinstance(usuario_id, str):
            # Eliminar los corchetes y las comillas, luego dividir por la coma
            usuario_id_lista = usuario_id.strip("[]").replace("'", "").split(", ")
        else:
            usuario_id_lista = usuario_id
        
        # Actualizar el documento con el UsuarioID en formato lista
        incidencias.update_one(
            {'_id': incidencia['_id']},
            {'$set': {'UsuarioID': usuario_id_lista}}
        )
    
    print("Datos transformados y subidos con éxito.")

if __name__ == "__main__":
    validador_areas_recreativas = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": [
            "nombre", "DESC_CLASIFICACION", "COD_BARRIO", "barrio", "COD_DISTRITO", "distrito", "estadoOperativo", "COORD_GIS_X", 
            "COORD_GIS_Y", "SISTEMA_COORD", "LATITUD", "LONGITUD", "TIPO_VIA", "NOM_VIA", "NUM_VIA", "COD_POSTAL", 
            "DIRECCION_AUX", "NDP", "fechaInstalacion", "CODIGO_INTERNO", "CONTRATO_COD", "TOTAL_ELEM", "tipo", 
            "capacidadMax", "estadoGlobalArea", "coordenadasGPS"
            ],
            "properties": {
            "nombre": {
                "bsonType": "string",
                "description": "Identificador único de cada área recreativa."
            },
            "DESC_CLASIFICACION": {
                "bsonType": "string",
                "description": "Descripción de la clasificación del área recreativa."
            },
            "COD_BARRIO": {
                "bsonType": "int",
                "description": "Código identificador del barrio."
            },
            "barrio": {
                "bsonType": "string",
                "description": "Nombre del barrio."
            },
            "COD_DISTRITO": {
                "bsonType": "int",
                "description": "Código identificador del distrito."
            },
            "distrito": {
                "bsonType": "string",
                "description": "Nombre del distrito."
            },
            "estadoOperativo": {
                "bsonType": "string",
                "enum": ["operativo", "no operativo"],
                "description": "Estado actual del área recreativa."
            },
            "COORD_GIS_X": {
                "bsonType": "double",
                "description": "Coordenada GIS X (Este/Oeste) en el sistema especificado."
            },
            "COORD_GIS_Y": {
                "bsonType": "double",
                "description": "Coordenada GIS Y (Norte/Sur) en el sistema especificado."
            },
            "SISTEMA_COORD": {
                "bsonType": "string",
                "description": "Sistema de coordenadas utilizado (por ejemplo, ETRS89)."
            },
            "LATITUD": {
                "bsonType": "double",
                "description": "Latitud de la ubicación en grados decimales."
            },
            "LONGITUD": {
                "bsonType": "double",
                "description": "Longitud de la ubicación en grados decimales."
            },
            "TIPO_VIA": {
                "bsonType": "string",
                "description": "Tipo de vía (calle, avenida, etc.)."
            },
            "NOM_VIA": {
                "bsonType": "string",
                "description": "Nombre de la vía donde se ubica el área recreativa."
            },
            "NUM_VIA": {
                "bsonType": "string",
                "description": "Número de la vía en la dirección."
            },
            "COD_POSTAL": {
                "bsonType": "string",
                "description": "Código postal de la ubicación."
            },
            "DIRECCION_AUX": {
                "bsonType": "string",
                "description": "Descripción auxiliar de la dirección"
            },
            "NDP": {
                "bsonType": "string",
                "description": "Número de identificación propio del área recreativa."
            },
            "fechaInstalacion": {
                "bsonType": "date",
                "description": "Fecha de instalación del área recreativa."
            },
            "CODIGO_INTERNO": {
                "bsonType": "string",
                "description": "Código interno para la gestión del área recreativa."
            },
            "CONTRATO_COD": {
                "bsonType": "string",
                "description": "Código de contrato asociado al mantenimiento o gestión del área recreativa."
            },
            "TOTAL_ELEM": {
                "bsonType": "int",
                "description": "Número total de elementos dentro del área recreativa."
            },
            "tipo": {
                "bsonType": "string",
                "description": "Tipo de elementos presentes (por ejemplo, deportivos, recreativos)."
            },
            "capacidadMax": {
                "bsonType": "int",
                "description": "Capacidad máxima de usuarios del área recreativa."
            },
            "estadoGlobalArea": {
                "bsonType": "string",
                "description": "Condición general del área recreativa (malo, medio o bueno)."
            },
            "coordenadasGPS": {
                "bsonType": "string",
                "description": "Coordenadas GPS combinadas en formato 'latitud longitud'."
            }
            }
        }
    }

    validador_incidente_seguridad = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["id", "fechaReporte", "tipoIncidente", "gravedad", "AreaRecreativaID"],
            "properties": {
                "id": {
                "bsonType": "string",
                "description": "Debe ser un número entero, identificador único para cada incidente."
                },
                "fechaReporte": {
                "bsonType": "date",
                "description": "Fecha en la que se reportó el incidente. Formato de fecha requerido."
                },
                "tipoIncidente": {
                "enum": ["Caida", "Vandalismo", "Robo", "Daño estructural", "Accidente"],
                "description": "Tipo de incidente reportado. Valores permitidos: Caida, Vandalismo, Robo, Daño estructural, Accidente."
                },
                "gravedad": {
                "enum": ["Media", "Baja", "Alta", "Critica"],
                "description": "Gravedad del incidente. Valores permitidos: Media, Baja, Alta, Critica."
                },
                "AreaRecreativaID": {
                "bsonType": "string",
                "description": "ID del área recreativa asociada al incidente. Debe ser un número entero."
                }
            }
        }
    }

    validador_encuestas_satisfaccion = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["id", "puntuacionCalidad", "puntuacionAccesibilidad", "AreaRecreativaID", "fechaEncuesta"],
            "properties": {
                "id": {
                "bsonType": "string",
                "description": "Debe ser un entero único que identifica cada evaluación"
                },
                "puntuacionCalidad": {
                "bsonType": "int",
                "minimum": 1,
                "maximum": 5,
                "description": "Puntuación de calidad, debe ser un entero entre 1 y 5"
                },
                "puntuacionAccesibilidad": {
                "bsonType": "int",
                "minimum": 1,
                "maximum": 5,
                "description": "Puntuación de accesibilidad, debe ser un entero entre 1 y 5"
                },
                "comentarios": {
                "bsonType": "string",
                "description": "Comentarios opcionales sobre la evaluación"
                },
                "AreaRecreativaID": {
                "bsonType": "string",
                "description": "Debe ser un entero que referencia el área recreativa"
                },
                "fechaEncuesta": {
                "bsonType": "date",
                "description": "Fecha de realización de la encuesta"
                }
            }
        }
    }

    insertar_csv_en_mongodb('AreasLimpio.csv', 'arqui2', 'AreaRecreativa', validador_areas_recreativas)
    insertar_csv_en_mongodb('EncuestasSatisfaccionLimpia.csv', 'arqui2', 'EncuestaSatisfaccion', validador_encuestas_satisfaccion)
    insertar_csv_en_mongodb('IncidentesSeguridadLimpio.csv', 'arqui2', 'IncidenteSeguridad', validador_incidente_seguridad)
    transformar_y_subir_datos()