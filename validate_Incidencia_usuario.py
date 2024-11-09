from pymongo import MongoClient

def establecer_validaciones():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['arqui']
    
    # Validación para la colección Usuarios
    usuario_schema = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["NIF", "NOMBRE", "EMAIL", "TELEFONO"],
            "properties": {
                "NIF": {
                    "bsonType": "string",
                    "description": "Debe ser un string y es requerido"
                },
                "NOMBRE": {
                    "bsonType": "string",
                    "description": "Debe ser un string y es requerido"
                },
                "EMAIL": {
                    "bsonType": "string",
                    "pattern": "^.+@.+\..+$",
                    "description": "Debe ser un correo válido y es requerido"
                },
                "TELEFONO": {
                    "bsonType": "string",
                    "description": "Debe ser un string y es requerido"
                }
            }
        }
    }

    # Validación para la colección IncidenciasUsuario
    incidencia_schema = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["id", "TIPO_INCIDENCIA", "fechaReporte", "estado", "UsuarioID", "MantenimientoID", "nivelEscalamiento"],
            "properties": {
                "id": {
                    "bsonType": "int",
                    "description": "Debe ser un entero y es requerido"
                },
                "TIPO_INCIDENCIA": {
                    "enum": ["desgaste", "rotura", "vandalismo", "mal funcionamiento"],
                    "description": "Debe ser un string y es requerido"
                },
                "fechaReporte": {
                    "bsonType": "date",
                    "description": "Debe ser una fecha y es requerido"
                },
                "estado": {
                    "enum": ["abierta", "cerrada"],
                    "description": "Debe ser un string y es requerido"
                },
                "UsuarioID": {
                    "bsonType": "array",
                    "items": {
                        "bsonType": "string"
                    },
                    "description": "Debe ser un array de strings y es requerido"
                },
                "MantenimientoID": {
                    "bsonType": "array",
                    "items": {
                        "bsonType": "string"
                    },
                    "description": "Debe ser un array de strings y es requerido"
                },
                "nivelEscalamiento": {
                    "enum": ["alto", "bajo", "medio"],
                    "description": "Debe ser un string y es requerido"
                },
                "tiempoResolucion": {
                    "bsonType": ["double", "null"],
                    "description": "Debe ser un double o null"
                }
            }
        }
    }

    # Aplicar validaciones
    db.command("collMod", "Usuarios", validator=usuario_schema)
    db.command("collMod", "IncidenciasUsuario", validator=incidencia_schema)
    
    print("Validaciones establecidas para las colecciones.")

if __name__ == "__main__":
    establecer_validaciones()
