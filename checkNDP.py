import pandas as pd
import re
import numpy as np

def limpiar_datasets():
	# Cargar el CSV
	df_areas = pd.read_csv('AreasSucio.csv')
	df_juegos = pd.read_csv('JuegosSucio.csv')
	df_mantenimiento = pd.read_csv('MantenimientoSucio.csv')
	df_incidencias_usuario = pd.read_csv('IncidenciasUsuariosSucio.csv')

	# Convertir todas las columnas de texto a minúsculas
	def normalizar_dataset(df):
		def quitar_tildes(texto):
			if isinstance(texto, str):
				texto = re.sub(r'[áÁ]', 'a', texto)
				texto = re.sub(r'[éÉ]', 'e', texto)
				texto = re.sub(r'[íÍ]', 'i', texto)
				texto = re.sub(r'[óÓ]', 'o', texto)
				texto = re.sub(r'[úÚ]', 'u', texto)
			return texto

		for col in df.select_dtypes(include=['object']).columns:
			df[col] = df[col].str.lower()
			df[col] = df[col].apply(quitar_tildes)
		return df

	df_areas = normalizar_dataset(df_areas)
	df_juegos = normalizar_dataset(df_juegos)
	df_mantenimiento = normalizar_dataset(df_mantenimiento)
	df_incidencias_usuario = normalizar_dataset(df_incidencias_usuario)

	def convertir_a_cadena_sin_decimales(df):
		columnas_a_convertir = ['COD_DISTRITO', 'COD_POSTAL', 'NDP']
		
		for columna in columnas_a_convertir:
			df[columna] = df[columna].apply(lambda x: str(int(x)) if pd.notnull(x) else "")
		return df

	# Cargar el DataFrame y aplicar el método
	df_juegos = convertir_a_cadena_sin_decimales(df_juegos)
	df_areas = convertir_a_cadena_sin_decimales(df_areas)

	# Guardar el DataFrame limpio en un nuevo archivo CSV
	df_areas.to_csv('areasLimpio.csv', index=False)
	df_juegos.to_csv('juegosLimpio.csv', index=False)
	df_mantenimiento.to_csv('mantenimientoLimpio.csv', index=False)
	df_incidencias_usuario.to_csv('incidenciasUsuarioLimpio.csv', index=False)

# Ejecutar la función
limpiar_datasets()