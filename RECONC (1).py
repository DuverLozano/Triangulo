#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 20 09:19:46 2024

@author: duverlozano
"""
import unicodedata
import datetime as dt
import time
from datetime import datetime, timedelta
import pytz
# Librerias de procesamiento de datos
import pandas as pd
import numpy as np
import calendar
# Librerias de solicitudes a FTPs
import ftplib
import paramiko
import pycurl
from urllib.parse import quote_plus
import logging
import os
import csv
import glob
import base64
import json
import io
from io import StringIO
from io import BytesIO
# Librerias para cargar información a BigQueryimport pandas_gbq
import pandas_gbq
from pandas_gbq import to_gbq
from pandas_gbq import read_gbq
from google.cloud import bigquery
# Librerias de visualización
import streamlit as st
import altair as alt
from google.oauth2 import service_account
from google.cloud import bigquery
# Librerias de XM
from pydataxm import *
import locale
from collections import defaultdict
import re


# Caluculo del Valor de la garantias financieras en las traszaciones del MEM
# Función de lectura de datos del GRIP
# Descargado del repositorio

# Enable logging
logging.basicConfig(level=logging.DEBUG)

# Define Colombia timezone
colombia_timezone = pytz.timezone('America/Bogota')
utc_timezone = pytz.utc

# FTP connection details
host = 'xmftps.xm.com.co'
port = 210
username = '1214716257'
password = 'NuevaXMTrian001*'


# DATOS GRIP
try:
    logging.debug("Starting execution")

    # Connect to the FTP server with SSL/TLS
    with ftplib.FTP_TLS() as ftp:
        ftp.connect(host, port)
        ftp.login(username, password)
        ftp.prot_p()  # Switch to secure data connection (PROT P)

        # Get today's date in Colombia timezone
        today_datetime = pd.Timestamp.now(colombia_timezone)

        # Initialize an empty DataFrame to store all processed data
        final_df = pd.DataFrame()

        # Lista de variables guardadas en la columna TIPO
        required_tipos = ['GREA', 'GINA','GITI']

        # Process current month and previous three months
        for i in range(2):
            # Calculate year and month for the current iteration
            year_month = (today_datetime - timedelta(days=i * 30)
                          ).strftime('%Y-%m')

            # Construct directory path
            directory = f'/INFORMACION_XM/PUBLICOK/SIC/COMERCIA/{year_month}'

            try:
                ftp.cwd(directory)

                # Get list of files in the directory
                files = ftp.nlst()

                # Filtrar los archivos grip y agruparlos por prefijo
                grip_files = [file for file in files if file.startswith(
                    "grip") and file.endswith((".tx2", ".tx3", ".tx4", ".txf", ".txr"))]
                grouped_files = defaultdict(list)
                for file in grip_files:
                    # Agrupar por nombre sin extensión
                    prefix = file.rsplit('.', 1)[0]
                    grouped_files[prefix].append(file)

                # Prioridades de extensiones en orden
                priority_order = [".txf", ".txr", ".tx2", ".tx1"]

                # Seleccionar el archivo a procesar según prioridad
                selected_files = []
                for prefix, file_list in grouped_files.items():
                    if any(file.endswith(".tx3") for file in file_list):
                        selected_files.append(
                            next(file for file in file_list if file.endswith(".tx3")))
                    elif any(file.endswith(".txf") for file in file_list):
                        selected_files.append(
                            next(file for file in file_list if file.endswith(".txf")))
                    elif any(file.endswith(".txr") for file in file_list):
                        selected_files.append(
                            next(file for file in file_list if file.endswith(".txr")))
                    elif any(file.endswith(".tx2") for file in file_list):
                        selected_files.append(
                            next(file for file in file_list if file.endswith(".tx2")))

                # Procesar archivos seleccionados
                for file in selected_files:
                    last_modified_str = ftp.voidcmd(f"MDTM {file}")[4:].strip()
                    last_modified_date = datetime.strptime(
                        last_modified_str, "%Y%m%d%H%M%S")

                    # Check if the file was modified in the last three months including the current month
                    if (today_datetime - timedelta(days=i * 30)).strftime('%Y-%m') <= year_month:
                        file_data = BytesIO()
                        ftp.retrbinary(f"RETR {file}", file_data.write)
                        file_data.seek(0)

                        # Attempt to decode file content using latin-1 directly
                        content = file_data.read().decode('latin-1')

                        # Parse CSV-like structure into DataFrame
                        csv_reader = pd.read_csv(
                            BytesIO(content.encode()), delimiter=';')
                        df = pd.DataFrame(csv_reader)

                        # Filter rows where PLANTA is the target plant and TIPO is in required_tipos
                        df = df[(df['TIPO'].isin(required_tipos))]
                        df = df[(df['PLANTA'] == '3IQA')]

                        # Add Filename and Last Modified Timestamp to DataFrame
                        df['PubDate'] = last_modified_date - timedelta(hours=5)

                        # Extract year and month from directory
                        year, month = year_month.split('-')

                        # Extract day from filename
                        # Assuming day is the last two characters before extension
                        day = file[-6:-4]

                        # Create datetime object yyyy-mm-dd
                        df['Date'] = datetime(int(year), int(
                            month), int(day)).strftime('%Y-%m-%d')

                        # Extract Name and Name_Type from Filename
                        filename_parts = file.split('.')
                        df['Name'] = file
                        df['Name_Type'] = filename_parts[-1].lower()

                        # List of hour columns
                        hour_columns = [
                            col for col in df.columns if col.startswith('HORA')]

                        # Melt the DataFrame to long format
                        df_long = pd.melt(df, id_vars=['PLANTA', 'TIPO', 'Name', 'Name_Type', 'PubDate', 'Date'],
                                          value_vars=hour_columns, var_name='Hour', value_name='Value')

                        # Convert 'Hour' column to integers starting from 1
                        df_long['Hour'] = df_long['Hour'].str.extract(
                            '(\d+)').astype(int)

                        # Rename columns
                        df_long.rename(columns={
                            'PLANTA': 'Plant',
                            'TIPO': 'Type',
                            'Value': 'Value'
                        }, inplace=True)

                        # Append df_long to final_df
                        final_df = pd.concat(
                            [final_df, df_long], ignore_index=True)

                        logging.debug(f"Processed file {file} successfully")

            except ftplib.error_perm as e:
                logging.error(f"FTP error: {e}")
                continue

        # Print the final DataFrame
        if not final_df.empty:
            print(final_df)
        else:
            logging.debug(f"No files found for plant {
                          target_plant} with required TIPO types.")

except Exception as e:
    logging.error(f"Unexpected error: {e}")

# Descargo el TRSD para el precio de bolsa promedio
try:
    logging.debug("Starting execution")

    # Connect to the FTP server with SSL/TLS
    with ftplib.FTP_TLS() as ftp:
        ftp.connect(host, port)
        ftp.login(username, password)
        ftp.prot_p()  # Switch to secure data connection (PROT P)

        # Get today's date in Colombia timezone
        today_datetime = pd.Timestamp.now(colombia_timezone)

        # Initialize an empty DataFrame to store all processed data
        final_df_TRSD = pd.DataFrame()

        # Process current month and previous two months
        for i in range(2):
            # Calculate year and month for the current iteration
            year_month = (today_datetime - timedelta(days=i*30)
                          ).strftime('%Y-%m')

            # Construct directory path
            directory = f'/INFORMACION_XM/PUBLICOK/SIC/COMERCIA/{year_month}'

            try:
                ftp.cwd(directory)

            # Obtener lista de archivos en el directorio
                files = ftp.nlst()

            # Filtrar archivos que comienzan con "trsd" y tienen las extensiones especificadas
                trsd_files = [file for file in files if file.startswith(
                    "trsd") and file.endswith((".tx2", ".tx3", ".tx4", ".txf", ".txr"))]

                # Agrupar los archivos por prefijo (sin extensión)
                grouped_files = defaultdict(list)
                for file in trsd_files:
                    prefix = file.rsplit('.', 1)[0]
                    grouped_files[prefix].append(file)

                selected_files = []
                for prefix, file_list in grouped_files.items():
                    if any(file.endswith(".tx3") for file in file_list):
                        selected_files.append(
                            next(file for file in file_list if file.endswith(".tx3")))
                    elif any(file.endswith(".txf") for file in file_list):
                        selected_files.append(
                            next(file for file in file_list if file.endswith(".txf")))
                    elif any(file.endswith(".txr") for file in file_list):
                        selected_files.append(
                            next(file for file in file_list if file.endswith(".txr")))
                    elif any(file.endswith(".tx2") for file in file_list):
                        selected_files.append(
                            next(file for file in file_list if file.endswith(".tx2")))

                # Procesar archivos seleccionados
                for file in selected_files:
                    # Obtener la última fecha de modificación del archivo
                    last_modified_str = ftp.voidcmd(f"MDTM {file}")[4:].strip()
                    last_modified_date = datetime.strptime(
                        last_modified_str, "%Y%m%d%H%M%S")

                    # Check if the file was modified in the last three months including current month
                    if (today_datetime - timedelta(days=i*30)).strftime('%Y-%m') <= year_month:
                        # Download file contents
                        file_data = BytesIO()
                        ftp.retrbinary(f"RETR {file}", file_data.write)
                        file_data.seek(0)  # Move pointer to start of file_data

                        # Attempt to decode file content using latin-1 directly
                        content = file_data.read().decode('latin-1')

                        # Parse CSV-like structure into DataFrame
                        csv_reader = pd.read_csv(
                            BytesIO(content.encode()), delimiter=';')
                        df = pd.DataFrame(csv_reader)

                        # Filter rows where CODIGO is "PBNA"
                        df = df[df['CODIGO'].isin(['MPON $/KWH','MPOT','PBNA'])]

                        # Add Filename and Last Modified Timestamp to DataFrame
                        df['PubDate'] = last_modified_date - timedelta(hours=5)

                        # Extract year and month from directory
                        year, month = year_month.split('-')

                        # Extract day from filename
                        # Assuming day is the last two characters before extension
                        day = file[-6:-4]

                        # Create datetime object yyyy-mm-dd
                        df['Date'] = datetime(int(year), int(
                            month), int(day)).strftime('%Y-%m-%d')

                        # Extract Name and Name_Type from Filename
                        filename_parts = file.split('.')
                        # Get all characters before the first '.'
                        df['Name'] = file
                        # Get characters after the last '.'
                        df['Name_Type'] = filename_parts[-1].lower()

                        # List of hour columns
                        hour_columns = [
                            col for col in df.columns if col.startswith('HORA')]

                        # Melt the DataFrame to long format
                        df_long = pd.melt(df, id_vars=['CODIGO', 'Name', 'Name_Type', 'PubDate', 'Date'],
                                          value_vars=hour_columns, var_name='Hour', value_name='Value')

                        # Convert 'Hour' column to integers starting from 1
                        df_long['Hour'] = df_long['Hour'].str.split(
                            ' ').str[1].astype(int) - 1

                        # Add 'ValueFmt' column
                        df_long['ValueFmt'] = '$'
                        # Rename columns
                        df_long.rename(columns={
                            'CODIGO': 'Code',
                            'Name': 'Name',
                            'Name_Type': 'Name_Type',
                            'PubDate': 'PubDate',
                            'Date': 'Date',
                            'Hour': 'Hour',
                            'Value': 'Value',
                            'ValueFmt': 'ValueFmt'
                        }, inplace=True)

                        # Append df_long to final_df
                        final_df_TRSD = pd.concat(
                            [final_df_TRSD, df_long], ignore_index=True)

                        logging.debug(f"Processed file {file} successfully")

            except ftplib.error_perm as e:
                logging.error(f"FTP error: {e}")
                continue

except Exception as e:
    logging.error(f"Unexpected error: {e}")

# OEF desviaciones
try:
    logging.basicConfig(level=logging.DEBUG)
    logging.debug("Starting execution")

    # Conexión al servidor FTP con SSL/TLS
    with ftplib.FTP_TLS() as ftp:
        ftp.connect(host, port)
        ftp.login(username, password)
        ftp.prot_p()  # Conexión segura (PROT P)

        # Fecha actual en Colombia
        today_datetime = pd.Timestamp.now(colombia_timezone)

        # DataFrame final para almacenar los datos procesados
        final_df_DOEF = pd.DataFrame()

        # Procesar
        for i in range(2):
            # Determinar el año y mes de la iteración actual
            target_date = today_datetime - timedelta(days=i * 30)
            year_month = target_date.strftime('%Y-%m')

            # Ruta del directorio
            directory = f'/INFORMACION_XM/USUARIOSK/GNSG/SIC/COMERCIA/{
                year_month}'

            try:
                ftp.cwd(directory)

                # Listar archivos en el directorio
                files = ftp.nlst()

                # Filtrar archivos que inicien con "oefagnh"
                oefagnh_files = [
                    file for file in files if file.startswith("oefagnh")]

                # Agrupar los archivos por prefijo (sin extensión)
                grouped_files = defaultdict(list)
                for file in oefagnh_files:
                    # Agrupar por nombre sin extensión
                    prefix = file.rsplit('.', 1)[0]
                    grouped_files[prefix].append(file)

                selected_files = []
                for prefix, file_list in grouped_files.items():
                    if any(file.endswith(".tx4") for file in file_list):
                        selected_files.append(
                            next(file for file in file_list if file.endswith(".tx4")))
                    elif any(file.endswith(".tx3") for file in file_list):
                        selected_files.append(
                            next(file for file in file_list if file.endswith(".tx3")))
                    elif any(file.endswith(".txf") for file in file_list):
                        selected_files.append(
                            next(file for file in file_list if file.endswith(".txf")))
                    elif any(file.endswith(".tx2") for file in file_list):
                        selected_files.append(
                            next(file for file in file_list if file.endswith(".tx2")))

                # Procesar archivos seleccionados
                for file in selected_files:
                    # Obtener la fecha de última modificación del archivo
                    last_modified_str = ftp.voidcmd(f"MDTM {file}")[4:].strip()
                    last_modified_date = datetime.strptime(
                        last_modified_str, "%Y%m%d%H%M%S")

                    # Descargar el contenido del archivo
                    file_data = BytesIO()
                    ftp.retrbinary(f"RETR {file}", file_data.write)
                    file_data.seek(0)  # Mover el puntero al inicio del archivo

                    # Decodificar el contenido usando latin-1
                    content = file_data.read().decode('latin-1')

                    # Parsear el contenido en un DataFrame
                    csv_reader = pd.read_csv(
                        BytesIO(content.encode()), delimiter=';')
                    df = pd.DataFrame(csv_reader)

                    # Añadir la fecha de última modificación al DataFrame
                    df['PubDate'] = last_modified_date - timedelta(hours=5)

                    # Extraer año y mes de la ruta del directorio
                    year, month = year_month.split('-')

                    # Extraer el día del nombre del archivo
                    # Suponiendo que el día está en los últimos dos caracteres antes de la extensión
                    day = file[-6:-4]

                    # Crear objeto datetime con formato yyyy-mm-dd
                    df['Date'] = datetime(int(year), int(
                        month), int(day)).strftime('%Y-%m-%d')

                    # Extraer Name y Name_Type del nombre del archivo
                    filename_parts = file.split('.')
                    df['Name'] = file  # Obtener todo antes del primer '.'
                    # Obtener los caracteres después del último '.'
                    df['Name_Type'] = filename_parts[-1].lower()

                    # Listar las columnas de horas
                    hour_columns = [
                        col for col in df.columns if col.startswith('HORA')]

                    # Convertir el DataFrame de formato ancho a largo (con horas en una sola columna)
                    df_long = pd.melt(df, id_vars=['CONCEPTO', 'DESCRIPCION', 'Name', 'Name_Type', 'PubDate', 'Date'],
                                      value_vars=hour_columns, var_name='Hour', value_name='Value')

                    # Convertir la columna 'Hour' a formato numérico (si es necesario)
                    df_long['Hour'] = df_long['Hour'].str.extract(
                        '(\d+)').astype(int)

                    # Añadir los datos procesados al DataFrame final
                    final_df_DOEF = pd.concat(
                        [final_df_DOEF, df_long], ignore_index=True)

                    logging.debug(f"Processed file {file} successfully")

            except ftplib.error_perm as e:
                logging.error(f"FTP error: {e}")
                continue

        # Imprimir el DataFrame final
        if not final_df_DOEF.empty:
            print(final_df_DOEF)
        else:
            logging.debug("No data found for selected files.")

except Exception as e:
    logging.error(f"Unexpected error: {e}")
# DAtos para las OEF
try:
    logging.debug("Starting execution")

    # Connect to the FTP server with SSL/TLS
    with ftplib.FTP_TLS() as ftp:
        ftp.connect(host, port)
        ftp.login(username, password)
        ftp.prot_p()  # Switch to secure data connection (PROT P)

        # Get today's date in Colombia timezone
        today_datetime = pd.Timestamp.now(colombia_timezone)

        # Initialize an empty DataFrame to store all processed data
        final_df_OEF = pd.DataFrame()

        # Process current month (no past months in this example)
        for i in range(2):
            # Calculate year and month for the current iteration
            year_month = (today_datetime - timedelta(days=i * 30)
                          ).strftime('%Y-%m')

            # Construct directory path
            directory = f'/INFORMACION_XM/USUARIOSK/GNSG/SIC/COMERCIA/{
                year_month}'

            try:
                ftp.cwd(directory)

                # Get list of files in the directory
                files = ftp.nlst()

                # Filter files that start with "oefsubasbd" and end with .tx2, .txf, or .tx3
                oefsubasbd_files = [
                    file for file in files if file.startswith("oefagnd") and file.endswith((".tx2", ".txf", ".tx3", ".tx4"))
                ]

                for file in oefsubasbd_files:
                    # Get last modified timestamp of the file
                    last_modified_str = ftp.voidcmd(f"MDTM {file}")[4:].strip()
                    last_modified_date = datetime.strptime(
                        last_modified_str, "%Y%m%d%H%M%S")

                    # Check if the file was modified in the current month
                    if (today_datetime - timedelta(days=i * 30)).strftime('%Y-%m') <= year_month:
                        # Download file contents
                        file_data = BytesIO()
                        ftp.retrbinary(f"RETR {file}", file_data.write)
                        file_data.seek(0)  # Move pointer to start of file_data

                        # Attempt to decode file content using latin-1 directly
                        content = file_data.read().decode('latin-1')

                        # Parse CSV-like structure into DataFrame
                        df = pd.read_csv(
                            BytesIO(content.encode()), delimiter=';')

                        # No additional column modifications (keep original structure)

                        # Add Filename and Last Modified Timestamp to DataFrame
                        df['PubDate'] = last_modified_date - timedelta(hours=5)

                        # Extract year and month from directory
                        year, month = year_month.split('-')

                        # Extract day from filename
                        # Assuming day is the last two characters before extension
                        day = file[-6:-4]

                        # Create datetime object yyyy-mm-dd
                        df['Date'] = datetime(int(year), int(
                            month), int(day)).strftime('%Y-%m-%d')

                        # Extract Name and Name_Type from Filename
                        filename_parts = file.split('.')
                        # Get all characters before the first '.'
                        df['Name'] = file
                        # Get characters after the last '.'
                        df['Name_Type'] = filename_parts[-1].lower()

                        # Append the DataFrame to the final DataFrame
                        final_df_OEF = pd.concat(
                            [final_df_OEF, df], ignore_index=True)

                        logging.debug(f"Processed file {file} successfully")

            except ftplib.error_perm as e:
                logging.error(f"FTP error: {e}")
                continue

except Exception as e:
    logging.error(f"Unexpected error: {e}")
# DATOS AFAC
try:
    logging.debug("Starting execution")

    # Connect to the FTP server with SSL/TLS
    with ftplib.FTP_TLS() as ftp:
        ftp.connect(host, port)
        ftp.login(username, password)
        ftp.prot_p()  # Switch to secure data connection (PROT P)

        # Get today's date in Colombia timezone
        today_datetime = pd.Timestamp.now(colombia_timezone)

        # Initialize an empty DataFrame to store all processed data
        final_df_afac = pd.DataFrame()

        # Process current month and previous three months
        for i in range(5):
            # Calculate year and month for the current iteration
            year_month = (today_datetime - timedelta(days=i * 30)
                          ).strftime('%Y-%m')

            # Construct directory path
            directory = f'/INFORMACION_XM/PUBLICOK/SIC/COMERCIA/{year_month}'

            try:
                ftp.cwd(directory)

                # Get list of files in the directory
                files = ftp.nlst()

                # Filtrar los archivos afac y agruparlos por prefijo
                afac_files = [
                    file for file in files if file.startswith("afac")]
                grouped_files = defaultdict(list)

                for file in afac_files:
                    # Agrupar por nombre sin extensión
                    prefix = file.rsplit('.', 1)[0]
                    grouped_files[prefix].append(file)
                    # Seleccionar el archivo a procesar con prioridad: tx3 > txf > txr > tx2
                    selected_files = []
                for prefix, file_list in grouped_files.items():
                    if any(file.endswith(".tx3") for file in file_list):
                        selected_files.append(
                            next(file for file in file_list if file.endswith(".tx3")))
                    elif any(file.endswith(".txf") for file in file_list):
                        selected_files.append(
                            next(file for file in file_list if file.endswith(".txf")))
                    elif any(file.endswith(".txr") for file in file_list):
                        selected_files.append(
                            next(file for file in file_list if file.endswith(".txr")))
                    elif any(file.endswith(".tx2") for file in file_list):
                        selected_files.append(
                            next(file for file in file_list if file.endswith(".tx2")))

                # Procesar archivos seleccionados
                for file in selected_files:
                    last_modified_str = ftp.voidcmd(f"MDTM {file}")[4:].strip()
                    last_modified_date = datetime.strptime(
                        last_modified_str, "%Y%m%d%H%M%S")

                    # Descargar el contenido del archivo
                    file_data = BytesIO()
                    ftp.retrbinary(f"RETR {file}", file_data.write)
                    file_data.seek(0)

                    # Intentar decodificar el contenido usando latin-1
                    content = file_data.read().decode('latin-1')

                    # Cargar el contenido en un DataFrame
                    csv_reader = pd.read_csv(
                        BytesIO(content.encode()), delimiter=';')
                    df = pd.DataFrame(csv_reader)

                    # Verificar si la columna "AGENTE" contiene "GNSG"
                    if "AGENTE" in df.columns and not df[df['AGENTE'] == "GNSG"].empty:
                        # Filtrar filas donde "AGENTE" sea "GNSG"
                        df = df[df['AGENTE'] == "GNSG"]

                        # Agregar metadatos al DataFrame
                        df['PubDate'] = last_modified_date - timedelta(hours=5)
                        year, month = year_month.split('-')
                        # Día extraído del nombre del archivo
                        day = file[-6:-4]
                        df['Date'] = datetime(int(year), int(
                            month), int(day)).strftime('%Y-%m-%d')
                        df['Name'] = file
                        df['Name_Type'] = file.split('.')[-1].lower()

                        # Limpiar nombres de columnas
                        df.columns = (
                            df.columns.str.strip()
                            .str.replace(' ', '')
                            .str.replace('[^a-zA-Z0-9_]', '', regex=True)
                            .str.replace('$', 'mon')
                        )

                        # Concatenar con el DataFrame final
                        final_df_afac = pd.concat(
                            [final_df_afac, df], ignore_index=True)

                        logging.debug(f"Processed file {file} successfully")

            except ftplib.error_perm as e:
                logging.error(f"FTP error: {e}")
                continue

        # Imprimir el DataFrame final
        if not final_df_afac.empty:
            print(final_df_afac)
        else:
            logging.debug("No files found with the required criteria.")

except Exception as e:
    logging.error(f"Unexpected error: {e}")
# Filtrar valores de MPO, MPOI y PBNA
df_MPO = final_df_TRSD[final_df_TRSD['Code'] == 'MPON $/KWH'].rename(columns={'Value': 'MPO'})  # Para plantas de generación variables
PBN = final_df_TRSD[final_df_TRSD['Code'] == 'PBNA'].rename(columns={'Value': 'PBNA'})
df_MPOI = final_df_TRSD[final_df_TRSD['Code'] == 'MPOT'].rename(columns={'Value': 'MPOI'})

df_MPO['Hour'] += 1
PBN['Hour'] += 1
df_MPOI['Hour'] += 1

PrecioEscasez = 948.845

# Filtrar GINA, GREA y GITI
grea_df = final_df[final_df['Type'] == 'GREA'].rename(columns={'Value': 'GREA_Value'})
gina_df = final_df[final_df['Type'] == 'GINA'].rename(columns={'Value': 'GINA_Value'})
giti_df = final_df[final_df['Type'] == 'GITI'].rename(columns={'Value': 'GITI_Value'})

# Combinar GREA y GINA
combined_df = pd.merge(grea_df, gina_df, on=["Plant", "Date", "Hour"], suffixes=('_GREA', '_GINA'))

# Combinar GITI si está disponible
combined_df = pd.merge(combined_df, giti_df[['Plant', 'Date', 'Hour', 'GITI_Value']], 
                       on=["Plant", "Date", "Hour"], how="left")

# Agregar valores de MPO, MPOI y PBNA
combined_df = pd.merge(combined_df, df_MPO[['Date', 'Hour', 'MPO']], on=['Date', 'Hour'], how='left')
combined_df = pd.merge(combined_df, df_MPOI[['Date', 'Hour', 'MPOI']], on=['Date', 'Hour'], how='left')
combined_df = pd.merge(combined_df, PBN[['Date', 'Hour', 'PBNA']], on=['Date', 'Hour'], how='left')

# Convertir columnas relevantes a tipo numérico
columns_to_convert = ['GREA_Value', 'GINA_Value', 'GITI_Value', 'MPO', 'MPOI', 'PBNA']
for col in columns_to_convert:
    if col in combined_df.columns:  # Verificar si la columna existe
        combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')

# Eliminar columnas irrelevantes
columns_to_drop = [
    'Type_GREA', 'Name_GREA', 'Name_Type_GREA', 'PubDate_GREA',
    'Type_GINA', 'Name_GINA', 'Name_Type_GINA', 'PubDate_GINA',
    'Type_GITI', 'Name_GITI', 'Name_Type_GITI', 'PubDate_GITI'
]
combined_df = combined_df.drop(columns=columns_to_drop, errors='ignore')

# Reemplazar NaN en GITI_Value con 0
combined_df['GITI_Value'] = combined_df['GITI_Value'].fillna(0)

# Filtrar los valores relacionados con 'DDOEF' en final_df_OEF
doef_df = final_df_OEF[final_df_OEF['CONCEPTO'] == 'DDOEF'].copy()

# Sumar los valores de 'VALOR' para cada fecha
doef_df = doef_df.groupby(['Date'])['VALOR'].sum().reset_index()
doef_df.rename(columns={'VALOR': 'DOEF'}, inplace=True)

# Combinar con combined_df usando la columna 'Date'
combined_df = pd.merge(combined_df, doef_df, on='Date', how='left')

# Reemplazar valores NaN en la nueva columna DOEF con 0 (opcional)
combined_df['DOEF'] = combined_df['DOEF'].fillna(0)

# Filtrar valores de 'OEFHAGN' en final_df_DORF
obligacion_horaria_df = final_df_DOEF[final_df_DOEF['CONCEPTO'] == 'OEFHAGN'][['Date', 'Hour', 'Value']].copy()

# Renombrar la columna para mayor claridad
obligacion_horaria_df.rename(columns={'Value': 'ObligacionHoraria'}, inplace=True)

# Combinar con combined_df usando las columnas 'Date' y 'Hour'
combined_df = pd.merge(combined_df, obligacion_horaria_df, on=['Date', 'Hour'], how='left')

# Manejo de valores faltantes en ObligacionHoraria (opcional)
combined_df['ObligacionHoraria'] = combined_df['ObligacionHoraria'].fillna(0)


PEPA = 928.3426871
# Calcular PRN basado en nuevas condiciones

def calcular_prn_con_caso(df):
    """
    Calcula el PRN y etiqueta el caso correspondiente basado en las condiciones descritas.
    """
    def asignar_prn_y_caso(row):
        # Condición 1: Precio de Bolsa Nacional <= Precio de Escasez
        if row['PBNA'] <= PrecioEscasez:
            if row['GREA_Value'] < row['GINA_Value']:
                return ((row['GINA_Value'] - row['GREA_Value']) * row['MPO'] + row['GITI_Value'] * row['MPOI']) / (row['GINA_Value'] - row['GREA_Value']), 'Caso A - RecNeg'
            elif row['GREA_Value'] >= row['GINA_Value'] and row['GREA_Value'] < row['GINA_Value'] + row['GITI_Value']:
                return ((row['GINA_Value'] + row['GITI_Value'] - row['GREA_Value']) * row['MPOI']) / (row['GINA_Value'] - row['GREA_Value']), 'Caso B - RecNeg'
            elif row['GREA_Value'] >= row['GINA_Value'] + row['GITI_Value'] and row['GITI_Value'] > 0:
                return row['MPOI'], 'Caso C - RecNeg'
            elif     row['GREA_Value'] == 0 and row['GINA_Value'] ==0:
                return 0, 'Sin generacion'
            elif row['GREA_Value'] == row['GINA_Value']:
                return 0, 'Se igualo la generacion'
            
        # Condición 2: Precio de Bolsa Nacional > Precio de Escasez
        elif row['PBNA'] > PrecioEscasez:
            if row['GREA_Value'] > row['GINA_Value'] and row['GREA_Value'] < row['GINA_Value'] + row['GITI_Value']:
                return ((row['GINA_Value'] + row['GITI_Value'] - row['GREA_Value']) * row['MPOI']) / (row['GINA_Value'] - row['GREA_Value']), 'Caso D - RecNeg'
            elif row['GREA_Value'] > row['GINA_Value'] + row['GITI_Value'] and row['GITI_Value'] > 0:
                return row['MPOI'], 'Caso E - RecNeg'
            elif     row['GREA_Value'] == 0 and row['GINA_Value'] ==0:
                return 0, 'Sin generacion'
            elif row['GREA_Value'] == row['GINA_Value']:
                return 0, 'Se igualo la generacion'
            elif row['GREA_Value'] < row['GINA_Value']:
                # Subcondiciones de Caso f
                if row['DOEF'] <= 0:
                    return ((row['GINA_Value'] - row['GREA_Value']) * max(PEPA, row['MPO']) + row['GITI_Value'] * row['MPOI']) / (row['GINA_Value'] - row['GREA_Value']), 'Caso F1 - RecNeg'
                elif row['DOEF'] > 0 and row['GINA_Value'] > row['ObligacionHoraria']:
                    PP = row['GINA_Value'] * row['ObligacionHoraria'] / row['GINA_Value']  # estrictamente esta generación es distinta si hay contratos de respaldo
                    if row['GREA_Value'] > PP:
                        return ((row['GINA_Value'] - row['GREA_Value']) * row['MPO'] + row['GITI_Value'] * row['MPOI']) / (row['GINA_Value'] - row['GREA_Value']), 'Caso F2a - RecNeg'
                    if row['GREA_Value'] < PP and PP <= row['GINA_Value']:
                        return (((PP - row['GREA_Value']) * max(PEPA, row['MPO']) + (row['GINA_Value'] - PP) * row['MPO']) + row['GITI_Value'] * row['MPOI']) / (row['GINA_Value'] - row['GREA_Value']), 'Caso F2b - RecNeg'
                elif row['DOEF'] > 0 and row['GINA_Value'] <= row['ObligacionHoraria']:
                    return ((row['GINA_Value'] - row['GREA_Value']) * max(PEPA, row['MPO']) + row['GITI_Value'] * row['MPOI']) / (row['GINA_Value'] - row['GREA_Value']), 'Caso F3 - RecNeg'
                elif     row['GREA_Value'] == 0 and row['GINA_Value'] ==0:
                    return 0, 'Sin generacion'
                elif row['GREA_Value'] == row['GINA_Value']:
                    return 0, 'Se igualo la generacion'
                
        # Valor por defecto si ninguna condición se cumple
        return row['MPO'], 'RecPos'

    # Aplicar la función a cada fila del DataFrame
    df[['PRN', 'caso']] = df.apply(lambda row: pd.Series(asignar_prn_y_caso(row)), axis=1)
    return df

combined_df = calcular_prn_con_caso(combined_df)

# Agregar la nueva columna "GI - GR" restando 'GINA_Value' y 'GREA_Value'
combined_df['GI - GR'] = combined_df['GINA_Value'] - combined_df['GREA_Value']

# Agregar la nueva columna que multiplica "GI - GR" con "PRN"
combined_df['(GI - GR) * PRN'] = combined_df['GI - GR'] * combined_df['PRN']

# Filtrar las filas donde 'caso' es "RecPos" y validar que "GI - GR" no sea negativo o cero
invalid_rows = combined_df[(combined_df['caso'] == "RecPos") & (combined_df['GI - GR'] > 0)]

# Verificar si hay filas que no cumplen la condición
if not invalid_rows.empty:
    print("Alerta: Se encontraron filas donde 'caso' es 'RecPos' pero 'GI - GR' no es negativo o cero.")
    print("Filas afectadas:")
    print(invalid_rows[['Date', 'Hour', 'GI - GR', 'caso']])
else:
    print("Validación completada: Todas las filas cumplen la condición.")

resumen = combined_df.groupby('caso').agg(
    Total_Valor=('(GI - GR) * PRN', 'sum'),
    Promedio_Valor=('(GI - GR) * PRN', 'mean'),
    Filas=('(GI - GR) * PRN', 'count')
).reset_index()

# Mostrar el resumen
print("Resumen por tipo de caso:")
print(resumen)

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def analizar_casos(df):
    """
    Analiza los casos en el DataFrame, mostrando distribución por hora, día y totales.
    
    Parameters:
    df (pandas.DataFrame): DataFrame con las columnas 'caso', 'Date', 'Hour' y '(GI - GR) * PRN'
    """
    # 1. Análisis de frecuencia de casos
    frecuencia_casos = df['caso'].value_counts()
    print("\nFrecuencia de casos:")
    print(frecuencia_casos)
    
    # 2. Análisis por hora
    casos_por_hora = pd.crosstab(df['Hour'], df['caso'])
    print("\nDistribución de casos por hora:")
    print(casos_por_hora)
    
    # 3. Suma total por caso
    suma_por_caso = df.groupby('caso')['(GI - GR) * PRN'].sum()
    print("\nSuma total por caso:")
    print(suma_por_caso)
    
    # 4. Análisis por día
    df['Date'] = pd.to_datetime(df['Date'])
    casos_por_dia = pd.crosstab(df['Date'], df['caso'])
    print("\nDistribución de casos por día:")
    print(casos_por_dia)
    
    # Visualizaciones
    plt.figure(figsize=(15, 12))  # Aumentamos el tamaño vertical
    
    # Gráfico 1: Distribución total de casos
    plt.subplot(2, 2, 1)
    frecuencia_casos.plot(kind='bar')
    plt.title('Distribución Total de Casos')
    plt.xlabel('Tipo de Caso')
    plt.ylabel('Frecuencia')
    plt.xticks(rotation=45)
    
    # Gráfico 2: Heatmap de casos por hora
    plt.subplot(2, 2, 2)
    sns.heatmap(casos_por_hora, cmap='YlOrRd', annot=True, fmt='d')
    plt.title('Distribución de Casos por Hora')
    
    # Gráfico 3: Suma total por caso
    plt.subplot(2, 2, 3)
    suma_por_caso.plot(kind='bar')
    plt.title('Suma Total por Caso')
    plt.xlabel('Tipo de Caso')
    plt.ylabel('Suma Total')
    plt.xticks(rotation=45)
    
    # Gráfico 4: Distribución de casos por día
    plt.subplot(2, 2, 4)
    casos_por_dia.plot(kind='area', stacked=True)
    plt.title('Distribución de Casos por Día')
    plt.xlabel('Fecha')
    plt.ylabel('Cantidad de Casos')
    plt.xticks(rotation=45)
    plt.legend(title='Tipo de Caso', bbox_to_anchor=(1.05, 1), loc='upper left')
    
    plt.tight_layout()
    plt.show()
    
    return {
        'frecuencia_casos': frecuencia_casos,
        'casos_por_hora': casos_por_hora,
        'suma_por_caso': suma_por_caso,
        'casos_por_dia': casos_por_dia
    }

# Ejemplo de uso:
resultados = analizar_casos(combined_df)

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def analizar_casos(df):
    """
    Analiza los casos en el DataFrame, mostrando distribución por hora, día y totales.
    
    Parameters:
    df (pandas.DataFrame): DataFrame con las columnas 'caso', 'Date', 'Hour' y '(GI - GR) * PRN'
    """
    # 1. Análisis de frecuencia de casos
    frecuencia_casos = df['caso'].value_counts()
    print("\nFrecuencia de casos:")
    print(frecuencia_casos)
    
    # 2. Análisis por hora
    casos_por_hora = pd.crosstab(df['Hour'], df['caso'])
    print("\nDistribución de casos por hora:")
    print(casos_por_hora)
    
    # 3. Suma total por caso
    suma_por_caso = df.groupby('caso')['(GI - GR) * PRN'].sum()
    print("\nSuma total por caso:")
    print(suma_por_caso)
    
    # 4. Análisis por día
    df['Date'] = pd.to_datetime(df['Date'])
    casos_por_dia = pd.crosstab(df['Date'], df['caso'])
    print("\nDistribución de casos por día:")
    print(casos_por_dia)
    
    # Visualizaciones
    plt.figure(figsize=(15, 12))  # Aumentamos el tamaño vertical
    
    # Gráfico 1: Distribución total de casos
    plt.subplot(2, 2, 1)
    sns.barplot(x=frecuencia_casos.index, y=frecuencia_casos.values, palette='muted')
    plt.title('Distribución Total de Casos')
    plt.xlabel('Tipo de Caso')
    plt.ylabel('Frecuencia')
    plt.xticks(rotation=45)
    
    # Gráfico 2: Heatmap de casos por hora
    plt.subplot(2, 2, 2)
    sns.heatmap(casos_por_hora, cmap='Blues', annot=True, fmt='d')
    plt.title('Distribución de Casos por Hora')
    
    # Gráfico 3: Suma total por caso
    plt.subplot(2, 2, 3)
    sns.barplot(x=suma_por_caso.index, y=suma_por_caso.values, palette='viridis')
    plt.title('Suma Total por Caso')
    plt.xlabel('Tipo de Caso')
    plt.ylabel('Suma Total')
    plt.xticks(rotation=45)
    
    # Gráfico 4: Distribución de casos por día
    plt.subplot(2, 2, 4)
    casos_por_dia.plot(kind='area', stacked=True, colormap='Set2')
    plt.title('Distribución de Casos por Día')
    plt.xlabel('Fecha')
    plt.ylabel('Cantidad de Casos')
    plt.xticks(rotation=45)
    plt.legend(title='Tipo de Caso', bbox_to_anchor=(1.05, 1), loc='upper left')
    
    plt.tight_layout()
    plt.show()
    
    return {
        'frecuencia_casos': frecuencia_casos,
        'casos_por_hora': casos_por_hora,
        'suma_por_caso': suma_por_caso,
        'casos_por_dia': casos_por_dia
    }

# Ejemplo de uso:
resultados = analizar_casos(combined_df)

#Desviacion OEF

# Filtrar los valores
PBN = final_df_TRSD[final_df_TRSD['Code'] == 'PBNA'].rename(columns={'Value': 'PBNA'})
PBN['Hour'] += 1
gid_values = final_df_OEF[(final_df_OEF['CONCEPTO'] == 'GID') & (final_df_OEF['Name_Type'] == 'txf')][['Date', 'VALOR']]
oefda_values = final_df_OEF[(final_df_OEF['CONCEPTO'] == 'OEFDA') & (final_df_OEF['Name_Type'] == 'txf')][['Date', 'VALOR']]

# Renombrar columnas para hacer el merge
gid_values = gid_values.rename(columns={'VALOR': 'GID_VAL'})
oefda_values = oefda_values.rename(columns={'VALOR': 'OEFDA_VAL'})

# Unir los valores de GID y OEFDA por la fecha
merged_oef = pd.merge(gid_values, oefda_values, on='Date', how='inner')

# Agrupar por fecha y sumar valores en caso de duplicados
merged_oef = merged_oef.groupby('Date', as_index=False).sum()

# Calcular la diferencia GID - OEFDA y asignarla a DDOEF
merged_oef['DDOEF'] = merged_oef['GID_VAL'] - merged_oef['OEFDA_VAL']

giha_values = final_df_DOEF[(final_df_DOEF['CONCEPTO'] == 'GIHA') & (final_df_DOEF['Name_Type'] == 'txf')][['Date', 'Hour', 'Value']]

# Filtrar solo las filas donde DDOEF es positivo
filtered_oef = merged_oef[merged_oef['DDOEF'] > 0].copy()

# Renombrar la columna 'Value' de giha_values a 'GIHA' antes del merge
giha_values = giha_values.rename(columns={'Value': 'GIHA'})

# Unir filtered_oef con giha_values solo por 'Date'
df_desviacionpositiva = pd.merge(giha_values, filtered_oef, on='Date', how='inner')

# Calcular la columna OHEF
df_desviacionpositiva['OHEF'] = (df_desviacionpositiva['GIHA'] / df_desviacionpositiva['GID_VAL']) * df_desviacionpositiva['OEFDA_VAL']

# Seleccionar las columnas deseadas
df_desviacionpositiva = df_desviacionpositiva[['Date', 'Hour', 'GID_VAL', 'DDOEF', 'GIHA', 'OHEF']]

df_desviacionpositiva['Desviacion kWh'] = df_desviacionpositiva['GIHA'] - df_desviacionpositiva['OHEF']

df_desviacionpositiva = pd.merge(df_desviacionpositiva, PBN[['Date', 'Hour', 'PBNA']], on=['Date', 'Hour'], how='left')

PEA = 948.85
PEP = 928.3426871
PEPA = 928.3426871 # del agente hay que averiguarlo

df_desviacionpositiva['DesviacionOEF$'] = df_desviacionpositiva.apply(
    lambda row: (row['Desviacion kWh'] * (row['PBNA'] - PEP)) if row['PBNA'] > PEA else 0, axis=1
)

df_desviacionpositiva['AjusteOEF$'] = df_desviacionpositiva.apply(
    lambda row: (row['OHEF'] * (PEP - PEPA)) if row['PBNA'] > PEA else 0, axis=1
)
# Mostrar el resultado
print(df_desviacionpositiva)

# Obtener la suma de la columna 'DesviacionOEF$'
suma_desviacion_oef = df_desviacionpositiva['DesviacionOEF$'].sum()

# Mostrar la suma
print(f"La suma de 'DesviacionOEF$' es: {suma_desviacion_oef}")


final_df = final_df.pivot_table(index=['Date', 'Hour'],columns='Type', values='Value', aggfunc='sum').reset_index()

final_df[(final_df.GINA==final_df.GREA)]['Hour'].value_counts()







