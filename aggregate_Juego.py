from pymongo import MongoClient
def crear_agregado():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['arqui2']
    
    pipeline = [
        {
            #Primer paso: unimos los juegos con los registros de mantenimiento
            '$lookup': {
                'from': "Mantenimiento", #Colección de mantenimiento
                'localField': "id", #Campo en la colección de juegos que hace referencia a Mantenimiento
                'foreignField': "JuegoID", #Campo en la colección de mantenimiento que debe coincidir
                'as': "Mantenimientos" #Alias para los resultados combinados
            }
        },
        {
            # Unimos las incidencias asociadas a cada mantenimiento
            '$lookup': {
                'from': "IncidenciasUsuario",  # Colección de incidencias
                'let': { 'mantenimiento_ids': "$Mantenimientos.id" },  # Variable que contiene el array de IDs de mantenimientos
                'pipeline': [
                    { 
                        '$match': { 
                            '$expr': { 
                                '$in': [ "$MantenimientoID", "$$mantenimiento_ids" ]  # Coincide con cualquier MantenimientoID dentro de mantenimiento_ids
                            } 
                        }    
                    },
                { 
                    '$project': {  # Selecciona solo los campos que necesitas de cada incidencia
                        'id': 1,
                        'fechaReporte': 1,
                        'estado': 1,
                        'UsuarioID': 1,
                        'tiempoResolucion': 1
                    } 
                }
        
            ],
            'as': 'IncidenciasUsuarios'  # Nombre del campo resultante que contendrá las incidencias relacionadas
            }
        },
        {
            '$project': {
                'id': 1,
                'nombre': 1,
                'modelo': 1,
                'estadoOperativo': 1,
                'accesibilidad': 1,
                'fechaInstalacion': 1,
                'tipo': 1,
                'desgasteAcumulado': 1,
                'indicadorExposicion': 1,
                'ultimaFechaMantenimiento': 1,  # Atributo ya existente en el CSV de juegos
                'IncidenciasUsuarios': 1,  # Embebemos las incidencias relacionadas
                'Mantenimientos': { '$map': {
                    'input': '$Mantenimientos',
                    'as': 'mantenimiento',
                    'in': '$$mantenimiento.id'  # Aquí extraemos solo los IDs de los mantenimientos
                    }
                }
            }
        },
        {
            #Guardamos el resultado en una nueva colección
            '$out': 'Juego'
        }
    ]
    db['Juegos'].aggregate(pipeline)
    
    print("Agregado Juego creado con éxito.")

if __name__ == "__main__":
    crear_agregado() 
