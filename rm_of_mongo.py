from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['arqui2']

# Obtener la lista de todas las colecciones en la base de datos
colecciones = db.list_collection_names()

# Iterar sobre cada colección y eliminarla
for coleccion in colecciones:
    db.drop_collection(coleccion)
    print(f'Colección {coleccion} eliminada.')