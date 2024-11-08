from pymongo import MongoClient


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
    transformar_y_subir_datos()
