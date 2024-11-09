from pymongo import MongoClient

def crear_agregado_juego():
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
                '_id': '$_id',
                'id': {'$first': '$id'},
                'nombre': {'$first': '$nombre'},
                'COD_BARRIO': {'$first': '$COD_BARRIO'},
                'BARRIO': {'$first': '$BARRIO'},
                'COD_DISTRITO': {'$first': '$COD_DISTRITO'},
                'DISTRITO': {'$first': '$DISTRITO'},
                'estadoOperativo': {'$first': '$estadoOperativo'},
                'COORD_GIS_X': {'$first': '$COORD_GIS_X'},
                'COORD_GIS_Y': {'$first': '$COORD_GIS_Y'},
                'SISTEMA_COORD': {'$first': '$SISTEMA_COORD'},
                'LATITUD': {'$first': '$LATITUD'},
                'LONGITUD': {'$first': '$LONGITUD'},
                'TIPO_VIA': {'$first': '$TIPO_VIA'},
                'NOM_VIA': {'$first': '$NOM_VIA'},
                'NUM_VIA': {'$first': '$NUM_VIA'},
                'COD_POSTAL': {'$first': '$COD_POSTAL'},
                'DIRECCION_AUX': {'$first': '$DIRECCION_AUX'},
                'NDP': {'$first': '$NDP'},
                'fechaInstalacion': {'$first': '$fechaInstalacion'},
                'CODIGO_INTERNO': {'$first': '$CODIGO_INTERNO'},
                'CONTRATO_COD': {'$first': '$CONTRATO_COD'},
                'modelo': {'$first': '$modelo'},
                'tipo': {'$first': '$tipo'},
                'accesibilidad': {'$first': '$accesibilidad'},
                'indicadorExposicion': {'$first': '$indicadorExposicion'},
                'coordenadasGPS': {'$first': '$coordenadasGPS'},
                'desgasteAcumulado': {'$first': '$desgasteAcumulado'},
                'mantenimientos': {'$push': '$mantenimientos'},
                'incidencias': {'$push': '$incidencias'}
            }
        },
        {
            '$project': {
                'id': 1,
                'nombre': 1,
                'COD_BARRIO': 1,
                'BARRIO': 1,
                'COD_DISTRITO': 1,
                'DISTRITO': 1,
                'estadoOperativo': 1,
                'COD_POSTAL': 1,
                'DIRECCION_AUX': 1,
                'NDP': 1,
                'fechaInstalacion': 1,
                'CODIGO_INTERNO': 1,
                'CONTRATO_COD': 1,
                'modelo': 1,
                'tipo': 1,
                'accesibilidad': 1,
                'indicadorExposicion': 1,
                'coordenadasGPS': 1,
                'desgasteAcumulado': 1,
                'mantenimientos': 1,
                'incidencias': {
                    '$map': {
                        'input': '$incidencias',
                        'as': 'incidencia',
                        'in': {
                            '_id': '$$incidencia._id',
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
            '$out': 'JuegoAgregado'
        }
    ]
    
    db['Juegos'].aggregate(pipeline)
    
    print("Agregado Juego creado con éxito.")

if __name__ == "__main__":
    crear_agregado_juego()