from pymongo import MongoClient

def crear_agregado():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['arqui2']
    
    pipeline = [
        {
            '$lookup': {
                'from': 'Juego',
                'localField': 'NDP',
                'foreignField': 'NDP',
                'as': 'juegos'
            }
        },
        {
            '$lookup': {
                'from': 'IncidentesSeguridad',
                'localField': 'ID',
                'foreignField': 'AreaRecreativaID',
                'as': 'incidentes'
            }
        },
        {
            '$lookup': {
                'from': 'RegistroClima',
                'localField': 'COD_POSTAL',
                'foreignField': 'Codigo Postal',
                'as': 'clima'
            }
        },
        {
            '$lookup': {
                'from': 'EncuestaSatisfaccion',
                'localField': 'ID',
                'foreignField': 'AreaRecreativaID',
                'as': 'encuestas'
            }
        },
        {
            '$project': {
                'ID': 1,
                'DESC_CLASIFICACION': 1,
                'COD_BARRIO': 1,
                'BARRIO': 1,
                'COD_DISTRITO': 1,
                'DISTRITO': 1,
                'ESTADO': 1,
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
                'FECHA_INSTALACION': 1,
                'CODIGO_INTERNO': 1,
                'CONTRATO_COD': 1,
                'TOTAL_ELEM': 1,
                'tipo': 1,
                'capacidadMax': 1,
                'estadoGlobalArea': 1,
                'juegos': 1,
                'incidentes': {
                    '$map': {
                        'input': '$incidentes',
                        'as': 'incidente',
                        'in': {
                            'TIPO_INCIDENTE': '$$incidente.TIPO_INCIDENTE',
                            'GRAVEDAD': '$$incidente.GRAVEDAD',
                            'FECHA_REPORTE': '$$incidente.FECHA_REPORTE'
                        }
                    }
                },
                'clima': 1,
                'encuestas': 1
            }
        },
        {
            '$out': 'AreaRecreativa_Clima'
        }
    ]
    
    db['AreasRecreativa'].aggregate(pipeline)
    
    print("Agregado ÁreaRecreativa_Clima creado con éxito.")

if __name__ == "__main__":
    crear_agregado()