from pymongo import MongoClient
def crear_agregado():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['arqui2']
    
    pipeline = [
        {
            '$lookup': {
                'from': 'Mantenimiento',
                'localField': 'id',
                'foreignField': 'JuegoID',
                'as': 'mantenimientos'
            }
        },
        {
            '$unwind': {
                'path': '$mantenimientos',
                'preserveNullAndEmptyArrays': True
            }
        },
        {
            '$addFields': {
                'mantenimientos._id': '$mantenimientos.id'
            }
        },
        {
            '$lookup': {
                'from': 'IncidenciasUsuario',
                'let': {'mantenimientoId': '$mantenimientos.id'},
                'pipeline': [
                    {'$unwind': '$MantenimientoID'},
                    {'$match': {'$expr': {'$eq': ['$MantenimientoID', '$$mantenimientoId']}}}
                ],
                'as': 'incidencias'
            }
        },
        {
            '$group': {
                '_id': '$id',
                'nombre': {'$first': '$nombre'},
                'modelo': {'$first': '$modelo'},
                'estadoOperativo': {'$first': '$estadoOperativo'},
                'accesibilidad': {'$first': '$accesibilidad'},
                'fechaInstalacion': {'$first': '$fechaInstalacion'},
                'tipo': {'$first': '$tipo'},
                'desgasteAcumulado': {'$first': '$desgasteAcumulado'},
                'indicadorExposicion': {'$first': '$indicadorExposicion'},
                'ultimaFechaMantenimiento': {'$first': '$ultimaFechaMantenimiento'},
                'mantenimientos': {'$push': '$mantenimientos'},
                'incidencias': {'$push': '$incidencias'}
            }
        },
        {
            '$project': {
                'id': 1,
                'nombre': 1,
                'estadoOperativo': 1,
                'fechaInstalacion': 1,
                'modelo': 1,
                'tipo': 1,
                'accesibilidad': 1,
                'indicadorExposicion': 1,
                'desgasteAcumulado': 1,
                'ultimaFechaMantenimiento': 1,
                'mantenimientos': {
                    '$map': {
                        'input': '$mantenimientos',
                        'as': 'mantenimiento',
                        'in': {
                            '_id': '$$mantenimiento._id',
                        }
                    }
                },
                'incidencias': {
                    '$map': {
                        'input': '$incidencias',
                        'as': 'incidencia',
                        'in': {
                            '_id': '$$incidencia.id',
                            'mantenimientoId': '$$incidencia.MantenimientoID',
                            'tipoIncidencia': '$$incidencia.tipoIncidencia',
                            'estado': '$$incidencia.estado',
                            'fechaReporte': '$$incidencia.fechaReporte'
                        }
                    }
                },
            }
        },
        {
            '$out': 'Juego'
        }
    ]
    
    db['Juegos'].aggregate(pipeline)
    print("Agregado Juego creado con Ã©xito.")

if __name__ == "__main__":
    crear_agregado()