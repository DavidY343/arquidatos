from pymongo import MongoClient
def crear_agregado():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['arqui2']
    
    pipeline = [
        {
            # Unir juegos con registros de mantenimiento
            '$lookup': {
                'from': "Mantenimiento",
                'localField': "id",
                'foreignField': "JuegoID",
                'as': "Mantenimientos"
            }
        },
        {
            # Convertir IDs de mantenimiento en un array separado
            '$set': {
                'mantenimiento_ids': {
                    '$map': {
                        'input': '$Mantenimientos',
                        'as': 'mantenimiento',
                        'in': '$$mantenimiento.id'
                    }
                }
            }
        },
        {
            # Descomponer mantenimiento_ids en elementos individuales
            '$unwind': '$mantenimiento_ids'
        },
        {
            # Unir incidencias asociadas a cada mantenimiento individual
            '$lookup': {
                'from': "IncidenciasUsuario",
                'let': { 'mantenimiento_id': "$mantenimiento_ids" },
                'pipeline': [
                    { 
                        '$match': { 
                            '$expr': { 
                                '$in': [ "$mantenimiento_id", "$MantenimientoID" ]  # Asegura coincidencia en el array de MantenimientoID
                            } 
                        }    
                    },
                    { 
                        '$project': {
                            'id': 1,
                            'fechaReporte': 1,
                            'estado': 1,
                            'UsuarioID': 1,
                            'tiempoResolucion': 1
                        } 
                    }
                ],
                'as': 'IncidenciasUsuarios'
            }
        },
        {
            # Agrupamos nuevamente después del unwind
            '$group': {
                '_id': "$id",
                'nombre': { '$first': "$nombre" },
                'modelo': { '$first': "$modelo" },
                'estadoOperativo': { '$first': "$estadoOperativo" },
                'accesibilidad': { '$first': "$accesibilidad" },
                'fechaInstalacion': { '$first': "$fechaInstalacion" },
                'tipo': { '$first': "$tipo" },
                'desgasteAcumulado': { '$first': "$desgasteAcumulado" },
                'indicadorExposicion': { '$first': "$indicadorExposicion" },
                'ultimaFechaMantenimiento': { '$first': "$ultimaFechaMantenimiento" },
                'IncidenciasUsuarios': { '$push': "$IncidenciasUsuarios" },
                'Mantenimientos': { '$push': "$mantenimiento_ids" }
            }
        },
        {
            '$project': {
                'id': '$_id',
                'nombre': 1,
                'modelo': 1,
                'estadoOperativo': 1,
                'accesibilidad': 1,
                'fechaInstalacion': 1,
                'tipo': 1,
                'desgasteAcumulado': 1,
                'indicadorExposicion': 1,
                'ultimaFechaMantenimiento': 1,
                'IncidenciasUsuarios': 1,
                'Mantenimientos': 1
            }
        },
        {
            '$out': 'Juego'
        }
    ]
    
    db['Juegos'].aggregate(pipeline)
    print("Agregado Juego creado con éxito.")

if __name__ == "__main__":
    crear_agregado()
