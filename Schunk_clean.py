# -*- coding: utf-8 -*-

'''
CLEANING
Shunk module Schunk_clean.py
Autor: Ramon
Fecha Inicio: Enero 21 2026
Propósito: Leer los parquets y limpiar los formatos
Dependencias: CLIENTES, PROVEEDORES, IVA ACREDITABLE, IVA TRASLADADO
Fecha de modificación: (Razón y fecha)
'''
#____________________________________________
#librerias python
import os 
import pandas as pd
import numpy as np
from pathlib import Path
#____________________________________________
#librerias RPG
from utils.df_utils import leer_parquet
from utils.df_utils import guardar_dataframe
from utils.df_utils import manejar_columnas
from utils.df_utils import renombrar_columnas
from utils.df_utils import quitar_notacion
#____________________________________________ 
#INGESTA DE ARCHIVOS

if __name__ == "__main__":
    path = Path(os.environ["USERPROFILE"]) / r"OneDrive - CONSULTORIA GLOBAL RPG S.C\Desktop\RPG\SCHUNK\parquets" #Ruta para guardar archivos limpios
    folder = Path(os.environ["USERPROFILE"]) / r"OneDrive - CONSULTORIA GLOBAL RPG S.C\Desktop\RPG\SCHUNK\parquets\raw" #Ruta de archivos crudos

proveedores = leer_parquet(folder, "PROVEEDORES", exact_match=False)
clientes = leer_parquet(folder, "CLIENTES", exact_match=False)
acreditable = leer_parquet(folder, "ACREDITABLE", exact_match=False)
cobrado = leer_parquet(folder, "COBRADO", exact_match=False)
bancos = leer_parquet(folder, "BANCOS", exact_match=False)
cfdi_emitidos = leer_parquet(folder, "EMITIDOS", exact_match=False)
cfdi_recibidos = leer_parquet(folder, "RECIBIDOS", exact_match=False)
#______________________________________________
#LIMPIAR ARCHIVO
clientes_proveedores_list = [
                "Description Offsetting Item",
                 "Assignment",
                 "Document Number",
                 "Document Type", 
                 "Tax Code",
                 "Withholding Tax Amt", 
                 "Clearing Document", 
                 "Text"]

cobrado_acreditable_list = [
                    "Debit/Credit Ind.",
                    "Offsetting Acct Type",
                    "Document Number", 
                    "Document Type",
                    "Assignment",
                    "Description Offsetting Item",
                    "Document Date",
                    "Amount in Local Currency",
                    "Amount in Doc. Curr.",
                    "Document Currency",
                    "Eff. Exchange Rate"] 

bancos_list = [
            "Account",
            "Document Number",
            "Document Date", 
            "Amount in Doc. Curr."] 

cfdi_list = [
        "UUID",
        "Serie",
        "Folio"]    

proveedores = manejar_columnas(proveedores, clientes_proveedores_list, mode="keep")
clientes = manejar_columnas(clientes, clientes_proveedores_list, mode="keep")
cobrado = manejar_columnas(cobrado, cobrado_acreditable_list, mode="keep")
acreditable = manejar_columnas(acreditable, cobrado_acreditable_list, mode="keep") 
bancos = manejar_columnas(bancos, bancos_list, mode="keep")
cfdi_emitidos = manejar_columnas(cfdi_emitidos, cfdi_list, mode="keep")
cfdi_recibidos = manejar_columnas(cfdi_recibidos, cfdi_list, mode="keep")

proveedores_rename = {
                "Description Offsetting Item": "Nombre Proveedor",
                 "Assignment": "Folio Interno",
                 "Document Number":"Merge_Key",
                 "Document Type":"Tipo Documento Aux",
                 "Tax Code":"filtro1", 
                 "Withholding Tax Amt":"filtro2", 
                 "Clearing Document" : "Merge_Key_Bank",
                 "Text": "Observaciones"} 

clientes_rename = {
                "Nombre Proveedor": "Nombre Cliente"} 

df_vendor = renombrar_columnas(proveedores, proveedores_rename)
#pasa dos veces por el proceso para menos lineas
df_customer = renombrar_columnas(clientes, proveedores_rename)
df_customer = renombrar_columnas(df_customer, clientes_rename)


cobrado_acreditable_rename = {
                    "Debit/Credit Ind.": "Filtro1",
                    "Offsetting Acct Type": "Filtro2",
                    "Document Number":"Poliza / No documento/ Compensacion/ Referencia",
                    "Document Type":"Tipo Documento",
                    "Assignment":"Merge_Key_Aux",
                    "Document Date":"Fecha de emisión",
                    "Amount in Local Currency":"Importe MXN",
                    "Amount in Doc. Curr.":"Importe", 
                    "Document Currency" :"Moneda",
                    "Eff. Exchange Rate" :"TC Reporte"} 

df_cob = renombrar_columnas(cobrado, cobrado_acreditable_rename)
df_acr = renombrar_columnas(acreditable, cobrado_acreditable_rename)

bancos_rename = {
            "Account":"Banco",
            "Document Number":"Merge_Key",
            "Document Date":"Fecha Banco", 
            "Amount in Doc. Curr.":"Importe Banco"}

df_bank = renombrar_columnas(bancos, bancos_rename)

#Limpiar NaN
df_cob = df_cob.replace([r'^\s*$', r'(?i)^nan$', r'(?i)^null$', r'(?i)^none$'],pd.NA,regex=True)
df_acr = df_acr.replace([r'^\s*$', r'(?i)^nan$', r'(?i)^null$', r'(?i)^none$'],pd.NA,regex=True)
df_bank = df_bank.replace([r'^\s*$', r'(?i)^nan$', r'(?i)^null$', r'(?i)^none$'],pd.NA,regex=True)
df_vendor = df_vendor.replace([r'^\s*$', r'(?i)^nan$', r'(?i)^null$', r'(?i)^none$'],pd.NA,regex=True)
df_customer = df_customer.replace([r'^\s*$', r'(?i)^nan$', r'(?i)^null$', r'(?i)^none$'],pd.NA,regex=True)

df_cob["TC Reporte"] = df_cob["TC Reporte"].fillna(0)
df_acr["TC Reporte"] = df_acr["TC Reporte"].fillna(0)

df_cob["Merge_Key_Aux"] = df_cob["Merge_Key_Aux"].apply(quitar_notacion)
df_acr["Merge_Key_Aux"] = df_acr["Merge_Key_Aux"].apply(quitar_notacion)
df_cob["Merge_Key_Aux"] = df_cob["Merge_Key_Aux"].astype("string").str[:-7]
df_acr["Merge_Key_Aux"] = df_acr["Merge_Key_Aux"].astype("string").str[:-7]
df_vendor["Merge_Key"] = df_vendor["Merge_Key"].apply(quitar_notacion)
df_customer["Merge_Key"] = df_customer["Merge_Key"].apply(quitar_notacion)

df_cob["TC Reporte"] = df_cob["TC Reporte"].astype(float).map(lambda x: f"{x:.4f}")
df_acr["TC Reporte"] = df_acr["TC Reporte"].astype(float).map(lambda x: f"{x:.4f}")

df_bank["Fecha Banco"] = pd.to_datetime(df_bank["Fecha Banco"]).dt.strftime("%d/%m/%Y")

guardar_dataframe(df_vendor, path, "clean", "proveedores", formato='parquet')
guardar_dataframe(df_customer, path, "clean", "clientes", formato='parquet')
guardar_dataframe(df_cob, path, "clean", "cobranza", formato='parquet')
guardar_dataframe(df_acr, path, "clean", "acreditable", formato='parquet')
guardar_dataframe(df_bank, path, "clean", "bancos", formato='parquet')
guardar_dataframe(cfdi_emitidos, path, "clean", "emitidos", formato='parquet')
guardar_dataframe(cfdi_recibidos, path, "clean", "recibidos", formato='parquet')

#guardar_dataframe(df_customer, path, "clean", "clientes", formato='csv')
#guardar_dataframe(df_cob, path, "clean", "cobranza", formato='csv')