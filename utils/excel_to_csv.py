# -*- coding: utf-8 -*-

import os
import pandas as pd
import shutil


def xlsb_convert_to_parquet(ruta_origen, ruta_destino):
    os.makedirs(ruta_destino, exist_ok=True)

    for archivo in os.listdir(ruta_origen):
        ruta_archivo = os.path.join(ruta_origen, archivo)

        # Verificar si es un archivo (no un directorio)
        if os.path.isfile(ruta_archivo):
            nombre_salida = os.path.splitext(archivo)[0] + ".parquet"
            ruta_salida = os.path.join(ruta_destino, nombre_salida)

            try:
                print(f"Leyendo: {archivo}")
                if archivo.endswith(('.xlsx', '.xls', '.xlsm', '.xlsb','.XLSX', '.XLS', '.XLSM', '.XLSB')):
                    df = pd.read_excel(ruta_archivo)
                elif archivo.endswith(".csv"):
                    df = pd.read_csv(ruta_archivo, encoding='utf-8', on_bad_lines='skip')

                df = df.astype(str)
                
                df.to_parquet(ruta_salida, engine="pyarrow", index=False)
                print(f"Guardado: {nombre_salida}")
            except Exception as e:
                print(f"Error en {archivo}: {e}")


def xlsb_convert_to_csv(input_folder, sheet_name=None, backup_folder_name="old_bk"):
    # """
    # Convierte una hoja especfica de todos los archivos Excel en una carpeta a CSV
    # y mueve los originales a una carpeta de respaldo. Usa la primera hoja si no se proporciona nombre de hoja.

    # Args:
    # - input_folder (str): Ruta de la carpeta que contiene los archivos Excel.
    # - sheet_name (str, optional): Nombre de la hoja que se desea convertir. Si no se proporciona, se usa la primera hoja.
    # - backup_folder_name (str): Nombre de la carpeta de respaldo para mover los archivos originales.

    # Returns:
    # - None
    # """
    # Crear carpeta de respaldo si no existe
    backup_folder_path = os.path.join(input_folder, backup_folder_name)
    if not os.path.exists(backup_folder_path):
        os.makedirs(backup_folder_path)

    # Extensiones de archivos Excel
    excel_extensions = ('.xlsx', '.xls', '.xlsm', '.xlsb','.XLSX', '.XLS', '.XLSM', '.XLSB')

    # Iterar sobre todos los archivos en la carpeta de <SIGNUM>
    for filename in os.listdir(input_folder):
        # Verificar si el archivo tiene una de las extensiones de Excel
        if filename.lower().endswith(excel_extensions):
            file_path = os.path.join(input_folder, filename)

            try:
                # Leer el archivo Excel, usar la primera hoja si no se proporciona sheet_name
                if sheet_name is None:
                    df = pd.read_excel(file_path, sheet_name=0)
                else:
                    df = pd.read_excel(file_path, sheet_name=sheet_name)

                # Convertir a CSV
                csv_filename = filename.rsplit('.', 1)[0] + '.csv'
                csv_path = os.path.join(input_folder, csv_filename)
                df.to_csv(csv_path, index=False)
                print(f" [OK] Convertido {filename} a {csv_filename}")

                # Mover el archivo Excel a la carpeta de respaldo
                shutil.move(file_path, backup_folder_path)
                # print(f" [OK] Movido {filename} a la carpeta {backup_folder_name}")

            except Exception as e:
                print(f" [ERROR] al procesar {filename}: {e}")

    print("Proceso completado.")


def xlsb_convert_single_to_csv(input_folder, search_string, sheet_name, backup_folder_name="old_bk"):
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
                df = pd.read_excel(file_path, sheet_name=sheet_name,engine='pyxlsb')
                
                # Convertir a CSV
                csv_filename = filename.rsplit('.', 1)[0] + '.csv'
                csv_path = os.path.join(input_folder, csv_filename)
                df.to_csv(csv_path, index=False)
                print(f" [OK] Convertido {filename}: hoja '{sheet_name}' a {csv_filename}")

                # Mover el archivo Excel a la carpeta de respaldo
                shutil.move(file_path, backup_folder_path)
                # print(f" [OK] Movido {filename} a la carpeta {backup_folder_name}")

            except Exception as e:
                print(f" [ERROR] al procesar {filename}: {e}")

            # Terminar despus de encontrar y procesar el primer archivo que coincide
            break

    print("Proceso completado.")


def xlsb_to_csv(input_folder, search_string, sheet_name, backup_folder_name="old_bk"):
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

    # Extensiones de archivos Excel|
    excel_extensions = ('.XLSB', '.xlsb')

    # Iterar sobre todos los archivos en la carpeta de entrada
    for filename in os.listdir(input_folder):
        # Verificar si el archivo tiene una de las extensiones de Excel
        if filename.lower().endswith(excel_extensions) and search_string in filename:
            file_path = os.path.join(input_folder, filename)
            
            try:
                # Leer la hoja especfica del archivo Excel
                df = pd.read_excel(file_path, sheet_name=sheet_name,engine='pyxlsb')
                
                # Convertir a CSV
                csv_filename = filename.rsplit('.', 1)[0] + '.csv'
                csv_path = os.path.join(input_folder, csv_filename)
                df.to_csv(csv_path, index=False,encoding='utf-8')
                print(f" [OK] Convertido {filename}: hoja '{sheet_name}' a {csv_filename}")

                # Mover el archivo Excel a la carpeta de respaldo
                shutil.move(file_path, backup_folder_path)
                # print(f" [OK] Movido {filename} a la carpeta {backup_folder_name}")

            except Exception as e:
                print(f" [ERROR] al procesar {filename}: {e}")

            # Terminar despus de encontrar y procesar el primer archivo que coincide
            break

    print("Proceso completado.")
