# -*- coding: utf-8 -*-

'''
MIX
Shunk module Schunk_mix.py
Autor: Ramon
Fecha Inicio: Enero 26 2026
Propósito: Unir los formatos para generar el layout de compañia
Dependencias: CLIENTES, PROVEEDORES, IVA ACREDITABLE, IVA TRASLADADO
Fecha de modificación: (Razón y fecha)
'''
#____________________________________________#____________________________________________
#librerias python
import os 
import pandas as pd
import numpy as np
from pathlib import Path
#____________________________________________
#librerias RPG
from utils.df_utils import leer_parquet
from utils.df_utils import guardar_dataframe
from utils.df_utils import Guardar_Formato
from utils.df_utils import unir_dataframes
#____________________________________________ 
#INGESTA DE ARCHIVOS

if __name__ == "__main__":
    path = Path(os.environ["USERPROFILE"]) / r"OneDrive - CONSULTORIA GLOBAL RPG S.C\Desktop\RPG\SCHUNK\parquets" #Ruta para guardar archivos limpios
    folder = Path(os.environ["USERPROFILE"]) / r"OneDrive - CONSULTORIA GLOBAL RPG S.C\Desktop\RPG\SCHUNK\parquets\clean" #Ruta de archivos crudos

proveedores = leer_parquet(folder, "PROVEEDORES", exact_match=False)
clientes = leer_parquet(folder, "CLIENTES", exact_match=False)
acreditable = leer_parquet(folder, "ACREDITABLE", exact_match=False)
cobrado = leer_parquet(folder, "COBRANZA", exact_match=False)
bancos = leer_parquet(folder, "BANCOS", exact_match=False)
cfdi_emitidos = leer_parquet(folder, "EMITIDOS", exact_match=False)
cfdi_recibidos = leer_parquet(folder, "RECIBIDOS", exact_match=False)

layout_depos = unir_dataframes(cobrado, clientes, "Merge_Key_Aux", "Merge_Key", tipo_union="left")
layout_depos = unir_dataframes(layout_depos, bancos, "Merge_Key_Bank", "Merge_Key", tipo_union="left")

guardar_dataframe(layout_depos, path, "mix", "Layout_Depósitos", formato='csv')
Guardar_Formato(layout_depos, path, "mix", "Layout_Depósitos")