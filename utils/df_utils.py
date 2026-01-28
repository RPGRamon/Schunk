# -*- coding: utf-8 -*-
import os
import re 
import glob
import logging
import pandas as pd
from pathlib import Path
from decimal import Decimal
#from rapidfuzz import process, fuzz


def leer_parquet(folder, nombre_archivo, exact_match=True):
    # """
    # Lee un archivo parquet desde una carpeta específica.

    # Args:
    #     folder (str): Subcarpeta ("raw", "clean", "mix", etc.).
    #     nombre_archivo (str): Nombre del archivo (con o sin extensión .parquet).
    #     exact_match (bool): Indica si la búsqueda del archivo debe ser exacta.

    # Returns:
    #     pd.DataFrame: DataFrame leído del archivo parquet.
    # """
    base_path =  r"C:\Macros_C\Bitron ex\parquets"
    # base_path = r"C:\Users\<SIGNUM>\Downloads\ATT_COP\parquet"

    # Obtener la ruta completa del directorio
    folder_path = os.path.join(base_path, folder)

    try:
        # Iterar sobre archivos en la carpeta especificada
        for filename in os.listdir(folder_path):
            # Verificar si el archivo es un parquet
            if filename.endswith('.parquet'):
                if exact_match:
                    if filename == nombre_archivo or filename == nombre_archivo + ".parquet":
                        # Ruta completa del archivo
                        ruta_completa = os.path.join(folder_path, filename)
                        df = pd.read_parquet(ruta_completa)
                        print(f"Cargado: {ruta_completa}")
                        return df
                else:
                    # Si no es una coincidencia exacta, buscar archivos que contengan el nombre
                    if nombre_archivo in filename.rsplit('.', 1)[0].upper():
                        # Ruta completa del archivo
                        ruta_completa = os.path.join(folder_path, filename)
                        df = pd.read_parquet(ruta_completa)
                        print(f"Cargado: {ruta_completa}")
                        return df
        print(f"  [WARN] No se encontró el archivo: {nombre_archivo}")
        return None

    except Exception as e:
        print(f"  [ERROR] al leer archivos en {folder_path}: {e}")
        return None
    

def varios_parquets(folder, nombre_archivo, exact_match=False):
    # """
    # Lee y combina múltiples archivos Parquet en un directorio que coincidan con un nombre dado.

    # Args:
    # - base_path (str): Ruta base del directorio.
    # - folder (str): Subcarpeta dentro de la ruta base donde buscar los archivos.
    # - nombre_archivo (str): Nombre de archivo o prefijo para buscar.
    # - exact_match (bool): Si True, busca coincidencias <SIGNUM>; si False, busca coincidencias parciales.

    # Returns:
    # - pd.DataFrame: DataFrame combinado de todos los archivos que coinciden con los criterios.
    # """
    # Obtener la ruta completa del directorio
    base_path = r"C:\Users\guil\Desktop\NCO\ATT_NCO\parquet"
    folder_path = os.path.join(base_path, folder)

    try:
        # Lista para almacenar DataFrames
        dataframes = []

        # Crear el patrón de búsqueda para los archivos
        if exact_match:
            file_pattern = os.path.join(folder_path, f'{nombre_archivo}.parquet')
        else:
            file_pattern = os.path.join(folder_path, f'{nombre_archivo}*.parquet')

        # Buscar todos los archivos Parquet que siguen el patrón
        parquet_files = glob.glob(file_pattern)

        # Leer y combinar los DataFrames
        for ruta_completa in parquet_files:
            df = pd.read_parquet(ruta_completa)
            print(f"Cargado: {ruta_completa}")
            dataframes.append(df)

        if dataframes:
            # Combinar todos los DataFrames en uno solo
            combined_df = pd.concat(dataframes, ignore_index=True)
            return combined_df
        else:
            print(f"  [WARN] No se encontraron archivos que coincidan con: {nombre_archivo}")
            return None

    except Exception as e:
        print(f"  [ERROR] al leer archivos en {folder_path}: {e}")
        return None




def renombrar_columnas(df, diccionario_renombres):
    # """
    # Renombra columnas del DataFrame segn un diccionario {original: nuevo}.
    
    # Parmetros:
    #     df (pd.DataFrame): El DataFrame cuyas columnas se quieren renombrar.
    #     diccionario_renombres (dict): Diccionario con nombres actuales como clave y nuevos como valor.
        
    # Retorna:
    #     pd.DataFrame: DataFrame con columnas renombradas.
    # """
    return df.rename(columns=diccionario_renombres)


def reordenar_columnas(df, nuevo_orden):
    # """
    # Reordena las columnas del DataFrame segn la lista dada.
    
    # Parmetros:
    #     df (pd.DataFrame): El DataFrame a modificar.
    #     nuevo_orden (list): Lista con el nuevo orden de columnas.
        
    # Retorna:
    #     pd.DataFrame: DataFrame con columnas reordenadas.
    # """
    columnas_existentes = [col for col in nuevo_orden if col in df.columns]
    columnas_faltantes = list(set(nuevo_orden) - set(columnas_existentes))
    
    if columnas_faltantes:
        print(f"  [WARN] Las columnas no existen y seran ignoradas: {columnas_faltantes}")
    
    return df[columnas_existentes]
    # return columnas_existentes



def unir_dataframes(df_1, df_2, col1, col2, tipo_union="left"):
    # """
    # Une dos DataFrames usando columnas clave.

    # Args:
    #     df_maestro (pd.DataFrame)
    #     df_secundario (pd.DataFrame)
    #     columna_maestro (str)
    #     columna_secundario (str)
    #     tipo_union (str)

    # Returns:
    #     pd.DataFrame
    # """
    try:
        df_unido = pd.merge(
            df_1,
            df_2,
            left_on=col1,
            right_on=col2,
            how=tipo_union,
            suffixes=('', '_sec')
        )
        print(f"  [OK] Union completada. Filas: {len(df_unido)}")
        return df_unido
    except Exception as e:
        print(f"  [Error] al unir: {e}")
        return None


def manejar_columnas(df, columnas, mode="remove"):
    # """
    # Elimina o conserva columnas especficas del DataFrame, segn el modo indicado.

    # Parmetros:
    #     df (pd.DataFrame): DataFrame de entrada.
    #     columnas (list): Lista de nombres de columnas a eliminar o conservar.
    #     mode (str): 'remove' para eliminar, 'keep' para conservar. Por defecto es 'remove'.

    # Retorna:
    #     pd.DataFrame: DataFrame modificado segn el modo.
    # """
    if mode not in ["remove", "keep"]:
        raise ValueError("  [WARN] El parmetro 'mode' debe ser 'remove' o 'keep'.")

    if mode == "remove":
        columnas_existentes = [col for col in columnas if col in df.columns]
        columnas_inexistentes = list(set(columnas) - set(columnas_existentes))

        if columnas_inexistentes:
            print(f"  [WARN] Columnas no encontradas y no se eliminarn: {columnas_inexistentes}")

        return df.drop(columns=columnas_existentes)

    elif mode == "keep":
        columnas_existentes = [col for col in columnas if col in df.columns]
        columnas_faltantes = list(set(columnas) - set(columnas_existentes))

        if columnas_faltantes:
            print(f"  [WARN] Columnas no encontradas y no se incluiran: {columnas_faltantes}")

        return df[columnas_existentes]


def crear_columna(df, nombre_columna, funcion_condicional):
    # """
    # Crea una nueva columna usando una funcin condicional.

    # Args:
    #     df (pd.DataFrame): DataFrame original.
    #     nombre_columna (str): Nombre de la nueva columna.
    #     funcion_condicional (function): Funcin que reciba una fila y devuelva un valor.

    # Returns:
    #     pd.DataFrame: DataFrame con la nueva columna.
    # """
    try:
        df[nombre_columna] = df.apply(funcion_condicional, axis=1)
        print(f" [OK] Columna '{nombre_columna}' creada.")
        return df
    except Exception as e:
        print(f" [Error] creando columna '{nombre_columna}': {e}")
        return df


def guardar_dataframe(df, carpeta_base, subcarpeta, nombre_archivo, formato='parquet'):
    # """
    # Guarda un DataFrame en formato parquet o csv.

    # Parmetros:
    #     df (pd.DataFrame): El DataFrame a guardar.
    #     carpeta_base (str): Ruta base donde guardar.
    #     subcarpeta (str): Carpeta dentro de la base donde se guardar.
    #     nombre_archivo (str): Nombre del archivo sin extensin.
    #     formato (str): 'parquet' o 'csv'. Por defecto 'parquet'.
    # """
    try:
        ruta_carpeta = os.path.join(carpeta_base, subcarpeta)
        os.makedirs(ruta_carpeta, exist_ok=True)
        ruta_completa = os.path.join(ruta_carpeta, f"{nombre_archivo}.{formato}")
        if formato == 'parquet':
            df.to_parquet(ruta_completa, index=False)
        elif formato == 'csv':
            df.to_csv(ruta_completa, index=False)
        else:
            raise ValueError("Formato no soportado. Usa 'parquet' o 'csv'.")

        logging.info(f"Archivo guardado en: {ruta_completa}")
        print(f" [OK] Archivo guardado exitosamente en: {ruta_completa}")
    except Exception as e:
        logging.error(f"Error al guardar el archivo: {str(e)}")
        print(f" [Error] al guardar el archivo: {e}")

def eliminar_duplicados(df, columna_clave):
    # """
    # Elimina duplicados conservando la fila con más datos no nulos.

    # Args:
    #     df (pd.DataFrame): DataFrame de entrada.
    #     columna_clave (str): Nombre de la columna para identificar duplicados.

    # Returns:
    #     pd.DataFrame: DataFrame sin duplicados.
    # """
    try:
        # Reemplazar strings vacíos por NaN (si los hay)
        df.replace(r"^\s*$", pd.NA, regex=True, inplace=True)

        # Contar valores no nulos por fila
        df["_non_null_count"] = df.notnull().sum(axis=1)

        # Ordenar para priorizar filas con más datos
        df = df.sort_values(by=[columna_clave, "_non_null_count"], ascending=[True, False])

        # Eliminar duplicados, conservando el que tenga más datos
        df = df.drop_duplicates(subset=columna_clave, keep="first")

        # Eliminar columna auxiliar
        df.drop(columns=["_non_null_count"], inplace=True)

        print(f" [OK] Duplicados eliminados usando la columna '{columna_clave}'.")
        return df

    except Exception as e:
        print(f" [Error] eliminando duplicados: {e}")
        return df

def eliminar_sufijo(df, sufijo="_sec"):
    # """
    # Elimina columnas cuyo nombre termine con el sufijo indicado.

    # Args:
    #     df (pd.DataFrame): DataFrame original.
    #     sufijo (str): Sufijo de columnas a eliminar. Por defecto '_sec'.

    # Returns:
    #     pd.DataFrame: DataFrame sin las columnas con el sufijo.
    # """
    try:
        columnas_a_eliminar = [col for col in df.columns if col.endswith(sufijo)]
        df = df.drop(columns=columnas_a_eliminar)
        print(f" [OK] Columnas eliminadas: {columnas_a_eliminar}")
        return df
    except Exception as e:
        print(f" [Error] eliminando columnas con sufijo '{sufijo}': {e}")
        return df


def clean_dataframe_headers(df, prefix=""):
    # """
    # Limpia los encabezados de un DataFrame de pandas.
    
    # Args:
    # - df: DataFrame de pandas cuyos encabezados deben ser limpiados.
    # - prefix: String que se agregará como prefijo a cada nombre de columna.
    
    # Returns:
    # - DataFrame con encabezados limpiados.
    # """
    def clean_header(header):
        # Convertir a minúsculas
        header = header.lower()

        # Eliminar espacios en blanco al principio y al final
        header = header.strip()
        
        # Reemplazar múltiples espacios por un único guion bajo
        header = re.sub(r'\s+', '_', header)
        
        # Eliminar caracteres especiales (incluyendo saltos de línea)
        header = re.sub(r'[^\w\s]', '', header)
        
        # Reemplazar múltiples guiones bajos consecutivos por un único guion bajo
        header = re.sub(r'_+', '_', header)
        
        # Agregar prefijo
        if prefix:
            header = f"{prefix}_{header}"
        
        return header
    
    # Limpiar cada columna del DataFrame
    df.columns = [clean_header(col) for col in df.columns]
    
    return df

def convert_to_datetime_with_threshold(df, threshold=0.3):
    try:
        unconverted_columns = []  # Lista para almacenar las columnas que no se convierten
        for column in df.columns:
            # Intentamos convertir todos los valores a datetime, estableciendo errores como 'coerce'.
            converted = pd.to_datetime(df[column], errors='coerce')
            
            # Filtrar los valores nulos, NaN y blancos
            non_null_values = df[column].dropna().apply(lambda x: x.strip() if isinstance(x, str) else x)
            non_null_values = non_null_values[non_null_values != '']

            # Calculamos la proporción de valores convertidos exitosamente, excluyendo nulos, NaN y blancos.
            num_valid_dates = converted[non_null_values.index].notna().sum()
            num_non_null_values = len(non_null_values)
            proportion_valid = num_valid_dates / num_non_null_values if num_non_null_values > 0 else 0

            # Si la proporción de valores convertidos es mayor o igual al umbral, actualizamos la columna.
            if proportion_valid >= threshold:
                # Usamos apply con lambda para normalizar la fecha.
                df[column] = converted.apply(lambda x: x.normalize() if pd.notna(x) else pd.NaT)
            else:
                # Si no se convierte, agregamos el nombre de la columna a la lista.
                unconverted_columns.append(column)
    
        return df, unconverted_columns
    
    except Exception as e:
        print(f"Error convert_to_datetime_with_threshold: {e}")
        # Devuelve el DataFrame original y una lista vacía para evitar errores de desempaquetamiento
        return df, []
    

    
def clean_dataframe_headers(df, prefix=""):
    # """
    # Limpia los encabezados de un DataFrame de pandas.
    
    # Args:
    # - df: DataFrame de pandas cuyos encabezados deben ser limpiados.
    # - prefix: String que se agregará como prefijo a cada nombre de columna.
    
    # Returns:
    # - DataFrame con encabezados limpiados.
    # """
    def clean_header(header):
        # Convertir a minúsculas
        header = header.lower()

        # Eliminar espacios en blanco al principio y al final
        header = header.strip()
        
        # Reemplazar múltiples espacios por un único guion bajo
        header = re.sub(r'\s+', '_', header)
        
        # Eliminar caracteres especiales (incluyendo saltos de línea)
        header = re.sub(r'[^\w\s]', '', header)
        
        # Reemplazar múltiples guiones bajos consecutivos por un único guion bajo
        header = re.sub(r'_+', '_', header)
        
        # Agregar prefijo
        if prefix:
            header = f"{prefix}_{header}"
        
        return header
    
    # Limpiar cada columna del DataFrame
    df.columns = [clean_header(col) for col in df.columns]
    
    return df

def add_prefix(df, prefix=""):
    # """
    # Limpia los encabezados de un DataFrame de pandas.
    
    # Args:
    # - df: DataFrame de pandas cuyos encabezados deben ser limpiados.
    # - prefix: String que se agregará como prefijo a cada nombre de columna.
    
    # Returns:
    # - DataFrame con encabezados limpiados.
    # """
    def prefix_add(header):

        if prefix:
            header = f"{prefix}_{header}"
        
        return header
    
    # Limpiar cada columna del DataFrame
    df.columns = [prefix_add(col) for col in df.columns]
    
    return df


def set_headers_from_parquet(df):
    # """
    # Limpia los encabezados de un DataFrame y establece una fila como encabezado si contiene la mayoría de los nombres de columna especificados.

    # Args:
    # - df (pd.DataFrame): DataFrame que necesita limpieza de encabezados.
    # - cta_col (list): Lista de nombres de columna que deben estar presentes en la fila encabezado.

    # Returns:
    # - pd.DataFrame: DataFrame con los encabezados ajustados.
    # """
    cta_col = [
        "IWM_JOB_NUMBER",
        "PACE",
        "Project Type",
        "FTU Status",
        "CL001 Aging",
        "CI050 Aging",
        "Comments"
        ]

    # Limpiar espacios de los actuales encabezados
    df.columns = df.columns.str.strip()

    # Definir el número de filas a revisar, por ejemplo, las primeras 5 filas
    num_rows_to_check = 5

    # Limpiar espacios de las primeras `num_rows_to_check` filas
    for i in range(num_rows_to_check):
        df.iloc[i] = df.iloc[i].astype(str).str.strip()

    # Verificar cuál tiene más coincidencias: encabezado actual o filas de datos
    header_matches = sum(1 for col in cta_col if col in df.columns)
    
    best_row_index = None
    max_row_matches = 0

    for i in range(num_rows_to_check):
        row_matches = sum(1 for col in cta_col if col in df.iloc[i].values)
        if row_matches > max_row_matches:
            max_row_matches = row_matches
            best_row_index = i

    # Si una de las filas de datos tiene más coincidencias, establecerla como nuevo encabezado
    if best_row_index is not None and max_row_matches > header_matches:
        new_header = df.iloc[best_row_index]
        df = df.drop(best_row_index).reset_index(drop=True)
        df.columns = new_header
        print(f"Encabezado establecido usando la fila de datos index: {best_row_index}")
    else:
        print("El encabezado existente ya contiene la mayoría de las columnas especificadas.")


    return clean_dataframe_headers(df)

def set_headers_by_line(df, hline):
    try:
        # Verifica que el índice de la línea especificada esté dentro del rango
        if hline < 0 or hline >= len(df):
            raise ValueError(f"Índice de línea de encabezado {hline} fuera del rango.")

        # Obtén los nuevos encabezados de la línea especificada
        new_headers = df.iloc[hline].tolist()

        # Reasigna los encabezados y elimina la fila usada como header
        df = df.drop(df.index[hline]).reset_index(drop=True)
        df.columns = new_headers

        return df

    except Exception as e:
        print(f"Error: {e}")
        return None
    


def eliminar_columnas_duplicadas(df):
    # """
    # Elimina columnas duplicadas en un DataFrame, conservando la primera ocurrencia.

    # Args:
    # - df (pd.DataFrame): DataFrame del cual eliminar columnas duplicadas.

    # Returns:
    # - pd.DataFrame: DataFrame con columnas duplicadas eliminadas.
    # """
    # Lista para almacenar columnas únicas
    columnas_vistas = set()
    columnas_unicas = []

    # Iterar sobre cada columna en el DataFrame
    for col in df.columns:
        # Agregar la columna sólo si no ha sido vista antes
        if col not in columnas_vistas:
            columnas_vistas.add(col)
            columnas_unicas.append(col)

    return df[columnas_unicas]

def corregir_columna(df, col_name, valid_list,
                     threshold_auto=90, threshold_flag=70):
    corrections = []
    flagged_rows = []

    for idx, val in df[col_name].fillna("").items():
        # Obtener mejor coincidencia
        match, score, _ = process.extractOne(
            val, valid_list, scorer=fuzz.WRatio
        )
        if score >= threshold_auto:
            df.at[idx, col_name] = match
        elif score >= threshold_flag:
            flagged_rows.append(df.loc[idx].copy())
        # opcional: guardar todo para auditoría
        corrections.append((idx, val, match, score))

    flagged_df = pd.DataFrame(flagged_rows)
    return df, pd.DataFrame(corrections, columns=[
        'index','original','suggested','score'
    ]), flagged_df

# def corregir_columna(df, col_name, valid_list,
#                      threshold_auto=89, threshold_flag=70):
#     corrections = []
#     flagged_rows = []

#     for idx, val in df[col_name].fillna("").items():
#         match, score, _ = process.extractOne(
#             val, valid_list, scorer=fuzz.WRatio
#         )
#         if score >= threshold_auto:
#             df.at[idx, col_name] = match
#         if threshold_flag <= score < 89:
#             flagged_rows.append(df.loc[idx].copy())
#         corrections.append((idx, val, match, score))

#     flagged_df = pd.DataFrame(flagged_rows)
#     audit_df = pd.DataFrame(corrections, columns=['index','original','suggested','score'])

#     return df, audit_df, flagged_df

def quitar_notacion(valor):
    #función para quitar la notación xientifica de un dataframe
    try:
        return format(Decimal(str(valor)).quantize(1), "f")
    except:
        return valor
    
def Guardar_Formato(df, carpeta_base, subcarpeta, nombre_archivo):
#def guardar_dataframe(df, carpeta_base, subcarpeta, nombre_archivo, formato='parquet'):
    """
    Aplica formato numérico de 4 decimales fijos a la columna 'TC Reporte'.
    Se requiere "pip install XlsxWriter"
    """
    # Ruta destino
    try:
        ruta_carpeta = os.path.join(carpeta_base, subcarpeta)
        os.makedirs(ruta_carpeta, exist_ok=True)
        ruta_completa = os.path.join(ruta_carpeta, f"{nombre_archivo}.xlsx")
        # Crear escritor con xlsxwriter
        with pd.ExcelWriter(ruta_completa, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Layout_Cia")

            workbook  = writer.book
            worksheet = writer.sheets["Layout_Cia"]
            worksheet.freeze_panes(1, 0)

            # Crear formato con 4 decimales fijos
            formato_4dec = workbook.add_format({"num_format": "0.0000"})

            formato_general = workbook.add_format({
                "font_name": "Montserrat",
                "font_size": 12,
            })
            if "TC Reporte" in df.columns:
                col_idx = df.columns.get_loc("TC Reporte")
                worksheet.set_column(col_idx, col_idx, None, formato_4dec)
            worksheet.set_column("A:Z", None, formato_general)
            for col_num, col_name in enumerate(df.columns): worksheet.write(0, col_num, col_name, formato_general)
    
        logging.info(f"Archivo guardado en: {ruta_completa}")
        print(f" [OK] Archivo guardado exitosamente en: {ruta_completa}")
    except Exception as e:
        logging.error(f"Error al guardar el archivo: {str(e)}")
        print(f" [Error] al guardar el archivo: {e}")

def multi_excel_to_csv(input_folder, sheet_name=None, backup_folder_name="old_bk"):
    # """
    # Convierte una hoja especfica de todos los archivos Excel en una carpeta a CSV
    # y mueve los originales a una carpeta de respaldo. Usa la primera hoja si no se proporciona nombre de hoja.
 
    # Args:
    # - input_folder (str): Ruta de la carpeta que contiene los archivos Excel.
    # - sheet_name (list, optional): Lista de las hojas que se tienen que convertir. Si no se proporciona, se usa la primera hoja.
    # - backup_folder_name (str): Nombre de la carpeta de respaldo para mover los archivos originales.
 
    # Returns:
    # - None
    # """
    # Crear carpeta de respaldo si no existe
   
    backup_folder_path = os.path.join(input_folder, backup_folder_name)
    os.makedirs(backup_folder_path, exist_ok=True)
 
    excel_extensions = ('.xlsx', '.xls', '.xlsm', '.xlsb')
 
    for filename in os.listdir(input_folder):
        if filename.lower().endswith(excel_extensions):
            file_path = os.path.join(input_folder, filename)
            base_name = os.path.splitext(filename)[0]
           
            try:
                # pyxlsb es vital para archivos .xlsb en Pandas
                engine = 'pyxlsb' if filename.lower().endswith('.xlsb') else None
               
                # Leemos el Excel. Si sheet_name es None, leemos todas ({nombre: df})
                # Si es lista o string, lee solo esas.
                sheets_dict = pd.read_excel(file_path, sheet_name=sheet_name, engine=engine)
 
                # Si solo se pidió una hoja (string), lo convertimos a dict para iterar uniforme
                if isinstance(sheets_dict, pd.DataFrame):
                    # Si no hay nombre de hoja (se usó índice 0), usamos 'default' o el nombre original
                    s_name = sheet_name if sheet_name else "hoja_1"
                    sheets_dict = {s_name: sheets_dict}
 
                for s_name, df in sheets_dict.items():
                    # Nombre: ArchivoOriginal_NombreHoja.csv
                    csv_filename = f"{s_name}.csv"
                    csv_path = os.path.join(input_folder, csv_filename)
                   
                    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                    print(f" [OK] Exportada hoja '{s_name}' de {filename}")
 
                # Mover original al terminar de extraer todas sus hojas
                shutil.move(file_path, os.path.join(backup_folder_path, filename))
 
            except Exception as e:
                print(f" [ERROR] al procesar {filename}: {e}")
 
    print("Proceso completado.")
 
    