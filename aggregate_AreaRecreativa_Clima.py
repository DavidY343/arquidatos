from pymongo import MongoClient

def crear_agregado():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['arqui2']
    
    pipeline = [
        {
            '$lookup': {
                'from': 'Juegos',
                'localField': 'NDP',
                'foreignField': 'NDP',
                'as': 'juegos'
            }
        },
        {
            '$lookup': {
                'from': 'IncidenteSeguridad',
                'localField': 'nombre',
                'foreignField': 'AreaRecreativaID',
                'as': 'incidentes'
            }
        },
        {
            '$lookup': {
                'from': 'RegistroClima',
                'localField': 'COD_POSTAL',
                'foreignField': 'codigoPostal',
                'as': 'clima'
            }
        },
        {
            '$lookup': {
                'from': 'EncuestaSatisfaccion',
                'localField': 'nombre',
                'foreignField': 'AreaRecreativaID',
                'as': 'encuestas'
            }
        },
        {
            '$project': {
                'nombre': 1,
                'barrio': 1,
                'distrito': 1,
                'estadoOperativo': 1,
                'coordenadasGPS': 1,
                'COD_POSTAL': 1,
                'fechaInstalacion': 1,
                'capacidadMax': 1,
                'estadoGlobalArea': 1,
                'juegos': 1,
                'incidentes': {
                    '$map': {
                        'input': '$incidentes',
                        'as': 'incidente',
                        'in': {
                            'tipoIncidente': '$$incidente.tipoIncidente',
                            'gravedad': '$$incidente.gravedad',
                            'fechaReporte': '$$incidente.fechaReporte'
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
    
    db['AreaRecreativa'].aggregate(pipeline)
    
    print("Agregado ÁreaRecreativa_Clima creado con éxito.")

if __name__ == "__main__":
    crear_agregado()