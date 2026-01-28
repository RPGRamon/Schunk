# -*- coding: utf-8 -*-

'''
RAW INGEST
Shunk module Schunk_raw.py
Autor: Ramon
Fecha Inicio: Enero 21 2026
Propósito: Convetir los papeles de trabajo a archivos a parquet
Dependencias: CLIENTES, PROVEEDORES, IVA ACREDITABLE, IVA TRASLADADO
Fecha de modificación: (Razón y fecha)
'''
#____________________________________________
#librerias python
import os 
import pandas
import numpy as np
from pathlib import Path
#____________________________________________
#librerias RPG
from utils.excel_to_csv import xlsb_convert_to_csv
from utils.source_utils import convert_to_parquet
from utils.source_utils import bulk_csv_to_utf8
#____________________________________________ 
#INGESTA DE ARCHIVOS

if __name__ == "__main__":
    #base_path =  Path(os.environ["USERPROFILE"]) / r"Desktop\RPG\INDORAMA\parquet"
    path = Path(os.environ["USERPROFILE"]) / r"OneDrive - CONSULTORIA GLOBAL RPG S.C\Desktop\RPG\SCHUNK\inputs" #Ruta de archivos a convertir
    folder = Path(os.environ["USERPROFILE"]) / r"OneDrive - CONSULTORIA GLOBAL RPG S.C\Desktop\RPG\SCHUNK\parquets\raw" #Ruta de archivos convertid os
    print(path)
    xlsb_convert_to_csv(path)
    bulk_csv_to_utf8(path)
    convert_to_parquet(path, folder)