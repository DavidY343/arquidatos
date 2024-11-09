import pandas as pd

def comprobar_ids_repetidos():
    # Cargar los CSV
    df_incidencias_usuario = pd.read_csv('AreasLimpio.csv')
    df_mantenimiento = pd.read_csv('EncuestasSatisfaccionLimpia.csv')
    df_juegos = pd.read_csv('IncidentesSeguridadLimpio.csv')
    # Comprobar IDs repetidos en el dataset de incidencias
    ids_incidencias = df_incidencias_usuario['nombre']
    ids_repetidos_incidencias = ids_incidencias[ids_incidencias.duplicated()]

    if not ids_repetidos_incidencias.empty:
        print("IDs repetidos en el dataset de incidencias:")
        print(ids_repetidos_incidencias)
    else:
        print("No hay IDs repetidos en el dataset de incidencias.")

    # Comprobar IDs repetidos en el dataset de mantenimientos
    ids_mantenimiento = df_mantenimiento['id']
    ids_repetidos_mantenimiento = ids_mantenimiento[ids_mantenimiento.duplicated()]

    if not ids_repetidos_mantenimiento.empty:
        print("IDs repetidos en el dataset de mantenimientos:")
        print(ids_repetidos_mantenimiento)
    else:
        print("No hay IDs repetidos en el dataset de mantenimientos.")

    # Comprobar IDs repetidos en el dataset de juegos
    ids_juegos = df_juegos['id']
    ids_repetidos_juegos = ids_juegos[ids_juegos.duplicated()]

    if not ids_repetidos_juegos.empty:
        print("IDs repetidos en el dataset de juegos:")
        print(ids_repetidos_juegos)
    else:
        print("No hay IDs repetidos en el dataset de juegos.")
    
# Ejecutar la función
comprobar_ids_repetidos()