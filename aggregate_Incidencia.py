from pymongo import MongoClient

def crear_agregado():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['arqui2']
    
    pipeline = [
        # Convertir UsuarioID (lista de IDs) en una relación uno a muchos con la colección Usuarios
        {
            '$lookup': {
                'from': 'Usuarios',  # Nombre de la colección de usuarios
                'let': { 'usuario_ids': "$UsuarioID" },  # IDs de usuarios en la incidencia como lista
                'pipeline': [
                    { '$match': { '$expr': { '$in': [ "$NIF", "$$usuario_ids" ] } } },
                    { '$project': {'nombre': 1, 'email': 1, 'telefono': 1 } }
                ],
                'as': 'usuarios'
            }
        },
        # Proyectamos los campos necesarios y embebemos el nivelEscalamiento
        {
            '$project': {
                'tipoIncidencia': 1,
                'fechaReporte': 1,
                'estado': 1,
                'MantenimientoID': 1,
                'nivelEscalamiento': 1,
                'tiempoResolucion': 1,
                'usuarios': 1  # Embebemos la lista de usuarios
            }
        },
        # (Opcional) Aquí podrías agregar lógica para modificar el nivelEscalamiento, si es necesario
        {
            '$out': 'Incidencia'  # Guardamos el resultado en una nueva colección
        }
    ]
    
    db['IncidenciasUsuario'].aggregate(pipeline)  # Verifica que el nombre de la colección sea correcto
    
    print("Agregado Incidencia_Agregado creado con éxito.")

if __name__ == "__main__":
    crear_agregado()