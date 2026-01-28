# -*- coding: utf-8 -*-

import os
import time
import shutil
import pandas as pd
from glob import glob
from pathlib import Path
from pyxlsb import open_workbook
from zipfile import ZipFile, BadZipFile


def convert_to_parquet(ruta_origen, ruta_destino):
#     # """
#     # Convierte a parquet los archivos de excel en su diferente formato 

#     # Args:
#     # - ruta_origen (str): Ruta de la carpeta que contiene los archivos Excel.
#     # - ruta_destino (str): Ruta de la carpeta en la que se quiere guardar 

#     # Returns:
#     # - None
#     # """
    os.makedirs(ruta_destino, exist_ok=True)

    for archivo in os.listdir(ruta_origen):
        ruta_archivo = os.path.join(ruta_origen, archivo)

        if os.path.isfile(ruta_archivo):
            nombre_salida = os.path.splitext(archivo)[0] + ".parquet"
            ruta_salida = os.path.join(ruta_destino, nombre_salida)

            try:
                print(f"Leyendo: {archivo}")

                ext = archivo.lower().split('.')[-1]

                if ext in ['xlsx', 'xls', 'xlsm']:
                    # Puedes especificar engine si quieres, por ejemplo openpyxl para xlsx/xlsm
                    df = pd.read_excel(ruta_archivo)
                elif ext == 'csv':
                    df = pd.read_csv(ruta_archivo, encoding='utf-8', on_bad_lines='skip')
                else:
                    print(f"  [WARN] Extensión no soportada: {archivo}")
                    continue

                df = df.astype(str)

                df.to_parquet(ruta_salida, engine="pyarrow", index=False)
                print(f"  [OK] Guardado: {nombre_salida}")

            except Exception as e:
                print(f"  [ERROR] Error en {archivo}: {e}")


def is_valid_xlsx(path):
#     # """
#     # Funcion para validar path del zip
#     # Args:
#     # - path (str): Ruta de la path a validar.
#     # """
    try:
        with ZipFile(path, 'r') as zip_ref:
            zip_ref.namelist()
        return True
    except BadZipFile:
        return False
    

def convert_to_csv(input_folder, sheet_name=None, backup_folder_name="old_bk"):
#     # """
#     # Convierte una hoja especfica de un archivo Excel cuyo nombre contiene una cadena dada a CSV
#     # y mueve el original a una carpeta de respaldo.

#     # Args:
#     # - input_folder (str): Ruta de la carpeta que contiene los archivos Excel.
#     # - sheet_name (str): Nombre de la hoja que se desea convertir.
#     # - backup_folder_name (str): Nombre de la carpeta de respaldo para mover el archivo original.

#     # Returns:
#     # - None
#     # """
    input_folder = Path(input_folder)
    backup_folder = input_folder / backup_folder_name
    backup_folder.mkdir(exist_ok=True)

    engine_map = {
        '.xlsx': 'openpyxl',
        '.xlsm': 'openpyxl',
        '.xls' : 'xlrd',
    }

    for entry in input_folder.iterdir():
        if not entry.is_file():
            continue

        ext = entry.suffix.lower()
        if ext not in engine_map:
            continue

        # Validar XLSX/XLSM
        if ext in ['.xlsx', '.xlsm']:
            if not is_valid_xlsx(entry):
                print(f"  [WARN] {entry.name} no es un archivo XLSX válido (no es ZIP). Se salta.")
                continue

        engine = engine_map[ext]
        print(f"\nProcesando {entry.name} con engine {engine} ...")

        try:
            if sheet_name:
                df = pd.read_excel(entry, sheet_name=sheet_name, engine=engine)
            else:
                df = pd.read_excel(entry, sheet_name=0, engine=engine)

            if df is None:
                print(f"  [WARN] {entry.name} devolvió None al leer con engine={engine}.")
                continue

            csv_name = entry.stem + '.csv'
            csv_path = input_folder / csv_name
            df.to_csv(csv_path, index=False, encoding='utf-8')
            print(f"  [OK] Generado {csv_name}")

            target_backup = backup_folder / entry.name
            shutil.move(str(entry), str(target_backup))
            print(f"  [OK] Movido original a {backup_folder.name}/{entry.name}")

        except Exception as e:
            print(f"  [ERROR] Falló al procesar {entry.name}: {type(e).__name__}: {e}")

    print("\nTodos los archivos procesados.")


def parquet_in_chunks(path_csv, columnas=None, chunk_size=50000, output_prefix="chunk", output_dir="parquet_output",encoding='utf-8'):
    # """
    # Convierte un archivo .csv grande a múltiples archivos Parquet por chunks usando pandas.

    # Parámetros:
    # - path_csv: ruta del archivo .csv
    # - columnas: lista opcional con nombres de columnas a leer (None = todas)
    # - chunk_size: número de filas por chunk (default: 50,000)
    # - output_prefix: prefijo para los archivos generados
    # - output_dir: carpeta donde se guardarán los .parquet (se crea si no existe)
    # - encoding: codificación del archivo CSV (default: utf-8)
    # """

    os.makedirs(output_dir, exist_ok=True)
    start_time = time.time()
    print(f"[OK] Iniciando conversión de '{path_csv}' por chunks de {chunk_size} filas...")

    chunk_id = 0

    try:
        reader = pd.read_csv(path_csv, usecols=columnas, chunksize=chunk_size, encoding=encoding, low_memory=False)

        for chunk in reader:
            try:
                output_path = os.path.join(output_dir, f"{output_prefix}_{chunk_id}.parquet")
                chunk.to_parquet(output_path, index=False, engine='pyarrow')
                print(f"[OK] Chunk {chunk_id} guardado en: {output_path}")
                chunk_id += 1
            except Exception as e:
                print(f"[ERROR] Falló al guardar el chunk {chunk_id}: {e}")
                break

    except Exception as e:
        print(f"[ERROR] No se pudo abrir el archivo CSV: {e}")
        return

    elapsed = time.time() - start_time
    print(f"[OK] Tiempo total: {elapsed:.2f} segundos")


def join_chunks(input_dir="parquet_output", prefix="chunk", output_dir="parquets_unidos", output_name="archivo_final.parquet"):
    # """
    # Une archivos .parquet generados por chunks en un solo archivo Parquet.

    # Parámetros:
    # - input_dir: carpeta donde están los archivos .parquet
    # - prefix: prefijo común de los archivos a unir (ej. "chunk")
    # - output_dir: carpeta donde se guardará el archivo final
    # - output_name: nombre del archivo final .parquet (ej. "clientes_final.parquet")
    # """
    os.makedirs(output_dir, exist_ok=True)
    pattern = os.path.join(input_dir, f"{prefix}_*.parquet")
    files = sorted(glob(pattern))

    if not files:
        print(f"[WARN] No se encontraron archivos con patrón: {pattern}")
        return

    print(f"[OK] Uniendo {len(files)} archivos desde: {input_dir}")

    try:
        df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
        output_path = os.path.join(output_dir, output_name)
        df.to_parquet(output_path, index=False)
        print(f"[OK] Archivo final guardado como: {output_path}")
    except Exception as e:
        print(f"[ERROR] Falló al unir archivos: {e}")

############################################################
###############       DEPRECATED CODE        ###############
############################################################

def convert_single_to_csv(input_folder, search_string, sheet_name, backup_folder_name="old_bk"):
    # """
    # Convierte una hoja especfica de un archivo Excel cuyo nombre contiene una cadena dada a CSV
    # y mueve el original a una carpeta de respaldo.

    # Args:
    # - input_folder (str): Ruta de la carpeta que contiene los archivos Excel.
    # - search_string (str): Cadena que debe estar contenida en el nombre del archivo.
    # - sheet_name (str): Nombre de la hoja que se desea convertir.
    # - backup_folder_name (str): Nombre de la carpeta de respaldo para mover el archivo original.

    # Returns:
    # - None
    # """
    # Crear la carpeta de respaldo si no existe
    backup_folder_path = os.path.join(input_folder, backup_folder_name)
    if not os.path.exists(backup_folder_path):
        os.makedirs(backup_folder_path)

    # Extensiones de archivos Excel
    excel_extensions = ('.xlsx', '.xls', '.xlsm', '.xlsb')

    # Iterar sobre todos los archivos en la carpeta de entrada
    for filename in os.listdir(input_folder):
        # Verificar si el archivo tiene una de las extensiones de Excel
        if filename.lower().endswith(excel_extensions) and search_string in filename:
            file_path = os.path.join(input_folder, filename)
            
            try:
                # Leer la hoja especfica del archivo Excel
                df = pd.read_excel(file_path, sheet_name=sheet_name)
                
                # Convertir a CSV
                csv_filename = filename.rsplit('.', 1)[0] + '.csv'
                csv_path = os.path.join(input_folder, csv_filename)
                df.to_csv(csv_path, index=False)
                print(f"Convertido {filename}: hoja '{sheet_name}' a {csv_filename}")

                # Mover el archivo Excel a la carpeta de respaldo
                shutil.move(file_path, backup_folder_path)
                print(f"Movido {filename} a la carpeta {backup_folder_name}")

            except Exception as e:
                print(f"Error al procesar {filename}: {e}")

            # Terminar despus de encontrar y procesar el primer archivo que coincide
            break

    print("Proceso completado.")


def explorar_archivo(path_archivo, n_preview=100, encoding='utf-8'):
    # """
    # Explora un archivo .xlsx o .csv, mostrando:
    # - Lista de columnas
    # - Número estimado de filas
    # - Primeras filas de ejemplo

    # Parámetros:
    # - path_archivo: ruta al archivo (soporta .xlsx y .csv)
    # - n_preview: número de filas para mostrar como muestra
    # - encoding: codificación usada en CSV (por defecto 'utf-8')
    # """
    ext = os.path.splitext(path_archivo)[1].lower()

    try:
        if ext == ".xlsx":
            df = pd.read_excel(path_archivo, engine="openpyxl", nrows=n_preview)
            total_df = pd.read_excel(path_archivo, engine="openpyxl", usecols=[0])
        elif ext == ".csv":
            df = pd.read_csv(path_archivo, encoding=encoding, nrows=n_preview)
            total_df = pd.read_csv(path_archivo, encoding=encoding, usecols=[0])
        else:
            print(f"[ERROR] Formato no soportado: {ext}")
            return []

        columnas = df.columns.tolist()
        print(f"[OK] Columnas ({len(columnas)}): {columnas}")
        print(f"[OK] Filas estimadas: {len(total_df)}")

        return columnas

    except Exception as e:
        print(f"[ERROR] No se pudo explorar el archivo: {e}")
        return []
    
def csv_to_utf8(ruta_archivo):
    # """
    # Ccambia el encodig de csv a UFT-8
    # Parámetros:
    # - ruta_archivo: ruta al archivo (.csv)
    # """
    try:
        with open(ruta_archivo, 'rb') as f:
            contenido = f.read()
        # Decodificar usando 'latin1' que no falla nunca (map ea byte a unicode)
        texto = contenido.decode('latin1')
        # Guardar sobrescribiendo en UTF-8
        with open(ruta_archivo, 'w', encoding='utf-8') as f:
            f.write(texto)
        print(f"[OK] Convertido a UTF-8 (sin validar): {ruta_archivo}")
    except Exception as e:
        print(f"[ERROR] No se pudo convertir {ruta_archivo}: {e}")

def bulk_csv_to_utf8(ruta_raiz):
    # """
    # Forza el cambio el encodig de csv a UFT-8
    # Parámetros:
    # - ruta_raiz: ruta al la carpeta donde hay harchivos
    # """
    for carpeta, _, archivos in os.walk(ruta_raiz):
        for archivo in archivos:
            if archivo.lower().endswith('.csv'):
                ruta_archivo = os.path.join(carpeta, archivo)
                csv_to_utf8(ruta_archivo)
