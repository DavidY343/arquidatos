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
        # insertamos las nuevas columnas
        # df = validar_coordenadas(df)
        return df
    df_areas = normalizar_dataset(df_areas)
    df_juegos = normalizar_dataset(df_juegos)
    df_mantenimiento = normalizar_dataset(df_mantenimiento)
    df_incidencias_usuario = normalizar_dataset(df_incidencias_usuario)
    """def limpiar_ceros():
        # Eliminar los .0 de la columna NDP si están presentes
        df_areas['NDP'] = df_areas['NDP'].apply(lambda x: str(x).replace(r'\.0$', '') if pd.notna(x) and x != '' else x)
        df_juegos['NDP'] = df_juegos['NDP'].apply(
            lambda x: str(x).replace(r'\.0$', '') if pd.notna(x) and x != '' else x)
    """
    # Ejecutar la función
    #limpiar_ceros()

    def rellenar_nulos_via(df, df_aux):
        # Diccionario de mapeo para abreviaturas
        abreviaturas_via = {
            'c': 'calle',
            'avda': 'avenida',
            'av': 'avenida',
            'pza': 'plaza',
            'pq': 'parque',
            'pº': 'paseo',
            'pje': 'pasaje',
            'v': 'vía'
        }

        # Posibles nombres completos de tipo de vía
        tipos_via = ['parque', 'avenida', 'plaza', 'calle', 'paseo', 'vía', 'pasaje']

        for index, row in df.iterrows():
            # Obtener el NDP para buscar en el dataset auxiliar
            ndp = row['NDP']
            aux_rows = df_aux[df_aux['NDP'] == ndp]

            # Relleno desde el dataset auxiliar si aún están vacíos
            if pd.isnull(row['TIPO_VIA']):
                tipo_via_aux = aux_rows['TIPO_VIA'].dropna().values
                df.at[index, 'TIPO_VIA'] = tipo_via_aux[0] if tipo_via_aux.size > 0 else None

            if pd.isnull(row['NOM_VIA']):
                nom_via_aux = aux_rows['NOM_VIA'].dropna().values
                df.at[index, 'NOM_VIA'] = nom_via_aux[0] if nom_via_aux.size > 0 else None

            if pd.isnull(row['NUM_VIA']):
                num_via_aux = aux_rows['NUM_VIA'].dropna().values
                df.at[index, 'NUM_VIA'] = num_via_aux[0] if num_via_aux.size > 0 else None

            # Si después de revisar df_aux aún hay campos nulos, intenta extraer desde DIRECCION_AUX
            if pd.isnull(df.at[index, 'TIPO_VIA']) or pd.isnull(df.at[index, 'NOM_VIA']) or pd.isnull(
                    df.at[index, 'NUM_VIA']):
                direccion_aux = row['DIRECCION_AUX']

                if pd.notnull(direccion_aux):
                    # Reemplazar abreviaturas en DIRECCION_AUX con nombres completos
                    for abbr, full in abreviaturas_via.items():
                        direccion_aux = re.sub(r'\b' + re.escape(abbr) + r'\b', full, direccion_aux,
                                               flags=re.IGNORECASE)

                    # Detectar y limpiar TIPO_VIA
                    tipo_via_match = next((via for via in tipos_via if via in direccion_aux.lower()), None)

                    # Solo continuar si tipo_via_match tiene un valor
                    if tipo_via_match:
                        # Solo rellenar si el campo sigue vacío
                        if pd.isnull(df.at[index, 'TIPO_VIA']):
                            df.at[index, 'TIPO_VIA'] = tipo_via_match  # Sin capitalización
                        # Extraer NOM_VIA sin incluir el tipo de vía ni el punto inicial
                        nombre_via_match = re.search(
                            r"(?<=\b" + re.escape(tipo_via_match) + r"\b\s)[^.]+?(?=\s*(?:nº|#|\d+|,|$))",
                            direccion_aux, re.IGNORECASE)
                        numero_via_match = re.search(r"(?:nº|#|\s)(\d+)", direccion_aux, re.IGNORECASE)

                        if nombre_via_match and pd.isnull(df.at[index, 'NOM_VIA']):
                            # Guardamos solo el nombre sin el tipo de vía y sin punto inicial
                            nombre_via = nombre_via_match.group().strip().lstrip("· ")

                            # Si el nombre de la vía comienza con el tipo de vía, lo eliminamos
                            if nombre_via.lower().startswith(tipo_via_match.lower()):
                                nombre_via = re.sub(rf"^{re.escape(tipo_via_match)}\s*", "", nombre_via,
                                                    flags=re.IGNORECASE)

                            # Asignar el nombre de vía limpio a NOM_VIA
                            df.at[index, 'NOM_VIA'] = nombre_via
                        if numero_via_match and pd.isnull(df.at[index, 'NUM_VIA']):
                            df.at[index, 'NUM_VIA'] = numero_via_match.group(1).strip()

                    # Manejo automático de ambiguos
                    # Limpiar la dirección de cualquier referencia posterior a un punto
                    direccion_limpia = re.sub(r"\s*\.\s*.*", "", direccion_aux).strip()
                    direccion_limpia = re.sub(r"ascendente\s*\d*|,.*", "", direccion_limpia,
                                              flags=re.IGNORECASE).strip()

                    if pd.isnull(df.at[index, 'NOM_VIA']):
                        df.at[index, 'NOM_VIA'] = direccion_limpia

        return df

    df_areas = rellenar_nulos_via(df_areas, df_juegos)
    df_juegos = rellenar_nulos_via(df_juegos, df_areas)
    # Método para completar valores nulos en COD_DISTRITO y DISTRITO
    def llenar_cod_distrito_distrito(df):
        # Crear un diccionario para hacer la correspondencia entre COD_DISTRITO y DISTRITO existentes
        mapa_distrito = df.dropna(subset=['COD_DISTRITO', 'DISTRITO']).set_index('DISTRITO')['COD_DISTRITO'].to_dict()

        # Llenar los valores de COD_DISTRITO usando DISTRITO si falta el código
        df['COD_DISTRITO'] = df.apply(lambda row: mapa_distrito.get(row['DISTRITO'], row['COD_DISTRITO'])
        if pd.isnull(row['COD_DISTRITO']) else row['COD_DISTRITO'], axis=1)

        # Llenar los valores de DISTRITO usando COD_DISTRITO si falta el distrito
        # Invertir el diccionario para buscar por COD_DISTRITO
        mapa_cod_distrito = {v: k for k, v in mapa_distrito.items()}
        df['DISTRITO'] = df.apply(lambda row: mapa_cod_distrito.get(row['COD_DISTRITO'], row['DISTRITO'])
        if pd.isnull(row['DISTRITO']) else row['DISTRITO'], axis=1)

    # Ejecutar la función para llenar COD_DISTRITO y DISTRITO nulos
    llenar_cod_distrito_distrito(df_areas)
    llenar_cod_distrito_distrito(df_juegos)
    def actualizar_vias_desconocidas():
        # Función para reemplazar los valores nulos o vacíos de las columnas de vías con el formato adecuado
        def reemplazar_via(row, columna, tipo_via):
            if pd.isna(row[columna]) or row[columna] == "":
                return f"{row['ID']}_{tipo_via}_desconocido"
            return row[columna]

        # Actualizar las columnas TIPO_VIA, NOM_VIA y NUM_VIA
        df_areas['TIPO_VIA'] = df_areas.apply(reemplazar_via, axis=1, columna='TIPO_VIA', tipo_via='TIPO_VIA')
        df_areas['NOM_VIA'] = df_areas.apply(reemplazar_via, axis=1, columna='NOM_VIA', tipo_via='NOM_VIA')
        df_areas['NUM_VIA'] = df_areas.apply(reemplazar_via, axis=1, columna='NUM_VIA', tipo_via='NUM_VIA')
        df_juegos['TIPO_VIA'] = df_juegos.apply(reemplazar_via, axis=1, columna='TIPO_VIA', tipo_via='TIPO_VIA')
        df_juegos['NOM_VIA'] = df_juegos.apply(reemplazar_via, axis=1, columna='NOM_VIA', tipo_via='NOM_VIA')
        df_juegos['NUM_VIA'] = df_juegos.apply(reemplazar_via, axis=1, columna='NUM_VIA', tipo_via='NUM_VIA')
    # Ejecutar la función
    actualizar_vias_desconocidas()

    # Convertir la columna FECHA_INSTALACION al formato yyyy-mm-dd y quitar la hora si está presente
    def convertir_fecha(fecha):
        # Intentar con varios formatos comunes
        for formato in ("%Y/%m/%d", "%d-%m-%Y", "%m-%d-%Y", "%Y-%m-%d", "%d/%m/%Y", "%y/%m/%d", "%y-%m-%d"):
            try:
                fecha_convertida = pd.to_datetime(fecha, format=formato)
                return fecha_convertida.strftime("%Y-%m-%d")  # Formato sin hora
            except ValueError:
                continue
        # Quitar la parte de la hora si tiene el formato "yyyy-mm-dd HH:MM:SS"
        if isinstance(fecha, str) and " " in fecha:
            return fecha.split(" ")[0]
        return fecha  # Retornar la fecha sin cambios si ningún formato funciona

    df_areas['FECHA_INSTALACION'] = df_areas['FECHA_INSTALACION'].apply(convertir_fecha)
    df_juegos['FECHA_INSTALACION'] = df_juegos['FECHA_INSTALACION'].apply(convertir_fecha)
    df_mantenimiento['FECHA_INTERVENCION'] = df_mantenimiento['FECHA_INTERVENCION'].apply(convertir_fecha)
    df_incidencias_usuario['FECHA_REPORTE'] = df_incidencias_usuario['FECHA_REPORTE'].apply(convertir_fecha)

    def actualizar_fecha_desde_aux(df, df_aux):
        df_aux['FECHA_INSTALACION'] = pd.to_datetime(df_aux['FECHA_INSTALACION'], errors='coerce')

        # Agrupar el dataset de juegos por NDP y obtener la fecha mínima para cada NDP
        fechas_minimas = df_aux.groupby('NDP')['FECHA_INSTALACION'].min().dropna()

        # Función para reemplazar la fecha en áreas si está vacía o contiene "fecha_incorrecta"
        def reemplazar_fecha(row):
            # Si la fecha está vacía o contiene "fecha_incorrecta"
            if pd.isna(row['FECHA_INSTALACION']) or row['FECHA_INSTALACION'] == "fecha_incorrecta":
                # Primero intentar asignar la fecha más antigua de los juegos
                nueva_fecha = fechas_minimas.get(row['NDP'], row['FECHA_INSTALACION'])
                # Si la nueva fecha es inválida o NaT, asignar la ID como desconocida
                if pd.isna(nueva_fecha) or row['FECHA_INSTALACION'] == "fecha_incorrecta":
                    return f"{row['ID']}_FECHA_INSTALACION_desconocido"
                return nueva_fecha
            return row['FECHA_INSTALACION']

        # Aplicar la función a cada fila del dataset de áreas
        df['FECHA_INSTALACION'] = df.apply(reemplazar_fecha, axis=1)
        df['FECHA_INSTALACION'] = df['FECHA_INSTALACION'].apply(convertir_fecha)
    # Ejecutar la función
    actualizar_fecha_desde_aux(df_areas, df_juegos)
    actualizar_fecha_desde_aux(df_juegos, df_areas)

    def modificar_cod_interno_incorrecto(df):
        # Filtrar las filas con fechas incorrectas o vacías
        df['CODIGO_INTERNO'] = df.apply(
            lambda row: f"{row['ID']}_CODIGO_INTERNO_desconocido"
            if pd.isnull(row['CODIGO_INTERNO'])
            else row['CODIGO_INTERNO'], axis=1
        )

    # Ejecutar la función
    modificar_cod_interno_incorrecto(df_areas)
    modificar_cod_interno_incorrecto(df_juegos)

    def actualizar_cod_postal_desde_aux(df,df_aux):
        # Buscar el primer COD_POSTAL válido para cada NDP en el dataset de juegos
        cod_postales_aux = df_aux.dropna(subset=['COD_POSTAL']).groupby('NDP')['COD_POSTAL'].first()

        # Función para reemplazar COD_POSTAL nulo con el primer valor encontrado en los juegos o 'ID_COD_POSTAL_desconocido' si no se encuentra
        def reemplazar_cod_postal(row):
            if pd.isna(row['COD_POSTAL']):
                # Buscar el COD_POSTAL más cercano en juegos
                nuevo_cod_postal = cod_postales_aux.get(row['NDP'])

                if pd.isna(nuevo_cod_postal):
                    # Si no se encuentra un COD_POSTAL válido, asignar 'ID_COD_POSTAL_desconocido'
                    return f"{row['ID']}_COD_POSTAL_desconocido"
                else:
                    return nuevo_cod_postal
            return row['COD_POSTAL']

        # Aplicar la función de reemplazo a cada fila en el dataset de áreas
        df['COD_POSTAL'] = df.apply(reemplazar_cod_postal, axis=1)

    # Ejecutar la función
    actualizar_cod_postal_desde_aux(df_areas, df_juegos)
    actualizar_cod_postal_desde_aux(df_juegos, df_areas)


    def insertar_nuevas_columnas_juegos(df, df_mantenimientos):
        # Insertar nuevas columnas
        df.rename(columns={'DESC_CLASIFICACION': 'nombre'}, inplace=True)
        df.rename(columns={'tipo_juego': 'tipo'}, inplace=True)
        df.rename(columns={'ID': 'id'}, inplace=True)
        df.rename(columns={'MODELO': 'modelo'}, inplace=True)
        df.rename(columns={'ESTADO': 'estadoOperativo'}, inplace=True)
        df.rename(columns={'ACCESIBLE': 'accesibilidad'}, inplace=True)
        df.rename(columns={'FECHA_INSTALACION': 'fechaInstalacion'}, inplace=True)
        # Rellenar las líneas vacías de la columna 'accesibilidad' con 'no'
        df_juegos['accesibilidad'] = df_juegos['accesibilidad'].fillna('no')
        if df is None or df_mantenimientos is None:
            raise ValueError("DataFrames cannot be None")
        df['IndicadorExposicion'] = np.random.choice(['bajo', 'medio', 'alto'], len(df))
        # Calcular desgaste acumulado
        for index, row in df.iterrows():
            if 'JuegoID' in df_mantenimientos.columns and 'ID' in row:
                num_mantenimientos = df_mantenimientos[df_mantenimientos['JuegoID'] == row['ID']].shape[0]
                tiempo_de_uso = np.random.randint(1, 16)
                value_exposicion = {'bajo': 1, 'medio': 2, 'alto': 3}[row['IndicadorExposicion']]
                df.at[index, 'desgasteAcumulado'] = ((tiempo_de_uso * value_exposicion) - (num_mantenimientos * 100))
                ultima_fecha = df_mantenimientos[df_mantenimientos['JuegoID'] == row['ID']]['FECHA_INTERVENCION'].max()
                if pd.notnull(ultima_fecha):
                    df.at[index, 'ultimaFechaMantenimiento'] = pd.to_datetime(ultima_fecha).strftime("%Y-%m-%d")
                else:
                    df.at[index, 'ultimaFechaMantenimiento'] = None

        return df

    df_juegos = insertar_nuevas_columnas_juegos(df_juegos, df_mantenimiento)

    def normalizar_mantenimiento(df):
        # cambiamos el formato de los ID para que sean más normales
        df = normalizar_dataset(df)
        df.rename(columns={'ID': 'id'}, inplace=True)
        df.rename(columns={'FECHA_INTERVENCION' : 'fechaIntervencion'}, inplace=True)
        df.rename(columns={'TIPO_INTERVENCION': 'tipoIntervencion'}, inplace=True)
        df.rename(columns={'ESTADO_PREVIO': 'estadoPrevio'}, inplace=True)
        df.rename(columns={'ESTADO_POSTERIOR': 'estadoPosterior'}, inplace=True)
        for index, row in df.iterrows():
            valor = row['id']
            if isinstance(valor, str):
                match = re.match(r'(-?\d+),(\d+)\s*mnt', valor)
                if match:
                    entero = match.group(1)
                    df.at[index, 'id'] = f"mnt{int(entero):06d}"
        return df

    df_mantenimiento = normalizar_mantenimiento(df_mantenimiento)

    def normalizar_incidencias_usuarios(df_user_incidences, df_mantenimientos):
        df_user_incidences = normalizar_dataset(df_user_incidences)
        df_user_incidences.rename(columns={'MantenimeintoID': 'MantenimientoID'}, inplace=True)
        df_user_incidences.rename(columns={'ID': 'id'}, inplace=True)
        df_user_incidences.rename(columns={'FECHA_REPORTE': 'fechaReporte'}, inplace=True)
        df_user_incidences.rename(columns={'ESTADO': 'estado'}, inplace=True)
        df_user_incidences['nivelEscalamiento'] = np.random.choice(['bajo', 'medio', 'alto'],
                                                                   size=len(df_user_incidences))

        for index, row in df_user_incidences.iterrows():
            if row['estado'] == 'cerrada' and pd.notnull(row['MantenimientoID']):
                mantenimiento_ids = row['MantenimientoID'].strip('[]').replace("'", "").split(', ')
                fechas_resolucion = df_mantenimientos[df_mantenimientos['id'].isin(mantenimiento_ids)]['fechaIntervencion']
                ultima_fecha_resolucion = fechas_resolucion.max()
                if pd.notnull(ultima_fecha_resolucion):
                    df_user_incidences.at[index, 'tiempoResolucion'] = \
                        (pd.to_datetime(ultima_fecha_resolucion) - pd.to_datetime(row['fechaReporte'])).days
                else:
                    df_user_incidences.at[index, 'tiempoResolucion'] = None

        return df_user_incidences

    df_incidencias_usuario = normalizar_incidencias_usuarios(df_incidencias_usuario, df_mantenimiento)

    def quitar_decimal_ndp(df):
        df['NDP'] = df['NDP'].apply(lambda x: str(x).replace('.0', '') if pd.notna(x) and x != '' else x)
        return df

    # Aplicar la función a los DataFrames
    df_juegos = quitar_decimal_ndp(df_juegos)
    """def rellenar_nulos_unicos(df, dataset_nombre):
        for index, row in df.iterrows():
            for columna in df.columns:
                if pd.isnull(row[columna]):
                    if dataset_nombre != "Areas":
                        valor_reemplazo = f"{row['id']}-{columna}-desconocido"
                    else:
                        valor_reemplazo = f"{row['ID']}-{columna}-desconocido"

                    # Cast the column to string type before assigning the replacement value
                    df[columna] = df[columna].astype(str)
                    df.at[index, columna] = valor_reemplazo
        return df

    df_areas = rellenar_nulos_unicos(df_areas, "Areas")
    df_juegos = rellenar_nulos_unicos(df_juegos, "Juegos")"""
    def contar_nulos(df, nombre):
        # Cargar el CSV

        # Contar los valores nulos por columna
        nulos_por_columna = df.isnull().sum()

        # Filtrar solo las columnas que tienen valores nulos
        columnas_con_nulos = nulos_por_columna[nulos_por_columna > 0]

        # Imprimir las columnas con valores nulos y la cantidad de nulos por columna
        print(f"Columnas con valores nulos{nombre}:")
        print(columnas_con_nulos)

    contar_nulos(df_areas, "Areas")
    contar_nulos(df_juegos, "Juegos")
    contar_nulos(df_mantenimiento, "Mantenimiento")
    contar_nulos(df_incidencias_usuario, "Incidencias de usuario")
    # Guardar el DataFrame limpio en un nuevo archivo CSV
    df_areas.to_csv('areasLimpio.csv', index=False)
    df_juegos.to_csv('juegosLimpio.csv', index=False)
    df_mantenimiento.to_csv('mantenimientoLimpio.csv', index=False)
    df_incidencias_usuario.to_csv('incidenciasUsuarioLimpio.csv', index=False)

# Ejecutar la función
limpiar_datasets()
