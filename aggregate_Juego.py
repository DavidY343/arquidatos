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
            '$project': {
                'id': 1,
                'nombre': 1,
                'COD_BARRIO': 1,
                'BARRIO': 1,
                'COD_DISTRITO': 1,
                'DISTRITO': 1,
                'estadoOperativo': 1,
                'COORD_GIS_X': 1,
                'COORD_GIS_Y': 1,
                'SISTEMA_COORD': 1,
                'LATITUD': 1,
                'LONGITUD': 1,
                'TIPO_VIA': 1,
                'NOM_VIA': 1,
                'NUM_VIA': 1,
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