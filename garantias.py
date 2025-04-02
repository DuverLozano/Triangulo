#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:44:50 2024

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
        required_tipos = ['GREA', 'GINA']

        # Process current month and previous three months
        for i in range(1):
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
        else:            logging.debug(f"Processed file {file} successfully")
                

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
        for i in range(8):
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
                    if any(file.endswith(".tx4") for file in file_list):
                        selected_files.append(
                            next(file for file in file_list if file.endswith(".tx4")))
                    elif any(file.endswith(".tx3") for file in file_list):
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
                        df = df[df['CODIGO'].isin(['PBNA', 'CBNA', 'DESV'])]

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

# TGRL para el PPP
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
        final_df_t = pd.DataFrame()

        # Lista de variables guardadas en la columna TIPO
        required_tipos = ['VADES', 'CBNA', 'VRCL', 'VCDS', 'VVDS']

        # Process current month and previous two months
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

                # Filter files that start with "grip"
                tgrl_files = [file for file in files if file.startswith(
                    "tgrl") and file.endswith((".tx2", ".tx3", ".tx4", ".txf", ".txr"))]

                grouped_files = defaultdict(list)
                for file in tgrl_files:
                    # Agrupar por nombre sin extensión
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
                    # Get last modified timestamp of the file
                    last_modified_str = ftp.voidcmd(f"MDTM {file}")[4:].strip()
                    last_modified_date = datetime.strptime(
                        last_modified_str, "%Y%m%d%H%M%S")

                    # Check if the file was modified in the last three months including the current month
                    if (today_datetime - timedelta(days=i * 30)).strftime('%Y-%m') <= year_month:
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

                        # Filter rows where PLANTA is the target plant and TIPO is in required_tipos
                        df = df[(df['CODIGO'].isin(required_tipos))]

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
                        df_long = pd.melt(df, id_vars=['CODIGO', 'AGENTE', 'CONTENIDO', 'Name', 'Name_Type', 'PubDate', 'Date'],
                                          value_vars=hour_columns, var_name='Hour', value_name='Value')

                        # Convert 'Hour' column to integers starting from 1
                        df_long['Hour'] = df_long['Hour'].str.split(
                            ' ').str[1].astype(int) - 1

                        # Rename columns
                        df_long.rename(columns={
                            'Name': 'Name',
                            'Name_Type': 'Name_Type',
                            'PubDate': 'PubDate',
                            'Date': 'Date',
                            'Hour': 'Hour',
                            'Value': 'Value',
                            'ValueFmt': 'ValueFmt'
                        }, inplace=True)

                        # Append df_long to final_df
                        final_df_t = pd.concat(
                            [final_df_t, df_long], ignore_index=True)

                        logging.debug(f"Processed file {file} successfully")

            except ftplib.error_perm as e:
                logging.error(f"FTP error: {e}")
                continue

        # Print the final DataFrame
        if not final_df_t.empty:
            print(final_df_t)
        else:
            logging.debug(
                f"No files found for plant with required TIPO types.")

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
            directory = f'/INFORMACION_XM/PUBLICOK/SIC/COMERCIA/{year_month}'

            try:
                ftp.cwd(directory)

                # Get list of files in the directory
                files = ftp.nlst()

                # Filter files that start with "oefsubasbd" and end with .tx2, .txf, or .tx3
                oefsubasbd_files = [
                    file for file in files if file.startswith("oefsubasbd") and file.endswith((".tx2", ".txf", ".tx3", ".tx4"))
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

# TDIA para el CEE
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
        final_df_tdia = pd.DataFrame()

        # Process current month and previous two months
        for i in range(3):
            # Calculate year and month for the current iteration
            year_month = (today_datetime - timedelta(days=i*30)
                          ).strftime('%Y-%m')

            # Construct directory path
            directory = f'/INFORMACION_XM/PUBLICOK/SIC/COMERCIA/{year_month}'

            try:
                ftp.cwd(directory)

                # Get list of files in the directory
                files = ftp.nlst()

                # Filter files that start with "trsd"
                trsd_files = [
                    file for file in files if file.startswith("tdia_sis")]
                for file in trsd_files:
                    # Get last modified timestamp of the file
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
                        df = df[(df['CODIGO'] == 'CERD') | (
                            df['CODIGO'] == 'IPP') | (df['CODIGO'] == 'OCVH')]

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

                        # Add 'ValueFmt' column
                        df['ValueFmt'] = '$'

                        # Rename columns
                        df.rename(columns={
                            'CODIGO': 'Code',
                            'Name': 'Name',
                            'Name_Type': 'Name_Type',
                            'PubDate': 'PubDate',
                            'Date': 'Date',
                            'Hour': 'Hour',
                            'VALOR': 'Value',
                            'ValueFmt': 'ValueFmt'
                        }, inplace=True)

                        # Append df_long to final_df
                        final_df_tdia = pd.concat(
                            [final_df_tdia, df], ignore_index=True)

                        logging.debug(f"Processed file {file} successfully")

            except ftplib.error_perm as e:
                logging.error(f"FTP error: {e}")
                continue

except Exception as e:
    logging.error(f"Unexpected error: {e}")
# Servicio CND ASIC y FAZNI

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
        final_df_tserv = pd.DataFrame()

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

                # Filtrar archivos que coincidan con el patrón tservXX.txf
                tserv_files = [file for file in files if re.match(
                    r'^tserv\d{2}\.txf$', file)]

                # Procesar archivos seleccionados
                for file in tserv_files:
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

                    # Agregar metadatos al DataFrame
                    df['PubDate'] = last_modified_date - timedelta(hours=5)
                    year, month = year_month.split('-')
                    day = file[-6:-4]  # Día extraído del nombre del archivo
                    df['Date'] = datetime(int(year), int(
                        month), int(day)).strftime('%Y-%m-%d')
                    df['Name'] = file
                    df['Name_Type'] = file.split('.')[-1].lower()

                    # Limpiar nombres de columnas
                    df.columns = (
                        df.columns.str.strip()
                        .str.replace(' ', '')
                        .str.replace('[^a-zA-Z0-9_]', '', regex=True)
                    )

                    # Concatenar con el DataFrame final
                    final_df_tserv = pd.concat(
                        [final_df_tserv, df], ignore_index=True)

                    logging.debug(f"Processed file {file} successfully")

            except ftplib.error_perm as e:
                logging.error(f"FTP error: {e}")
                continue

except Exception as e:
    logging.error(f"Unexpected error: {e}")
# AENC
try:
    logging.debug("Starting execution")

    with ftplib.FTP_TLS() as ftp:
        ftp.connect(host, port)
        ftp.login(username, password)
        ftp.prot_p()

        today_datetime = pd.Timestamp.now(colombia_timezone)
        final_df_AENC = pd.DataFrame()

        for i in range(4):
            year_month = (today_datetime - timedelta(days=i * 30)
                          ).strftime('%Y-%m')
            directory = f'/INFORMACION_XM/USUARIOSK/GNSG/SIC/COMERCIA/{
                year_month}'

            try:

                ftp.cwd(directory)

                files = ftp.nlst()

                aenc_files = [
                    file for file in files if file.startswith("aenc")]
                if not aenc_files:
                    print(f"No 'aenc' files found in {directory}")
                    continue

                grouped_files = defaultdict(list)
                for file in aenc_files:
                    prefix = file.rsplit('.', 1)[0]
                    grouped_files[prefix].append(file)

                selected_files = []
                for prefix, file_list in grouped_files.items():
                    if any(file.endswith(".Tx4") for file in file_list):
                        selected_files.append(
                            next(file for file in file_list if file.endswith(".Tx4")))
                    elif any(file.endswith(".Tx3") for file in file_list):
                        selected_files.append(
                            next(file for file in file_list if file.endswith(".Tx3")))
                    elif any(file.endswith(".TxF") for file in file_list):
                        selected_files.append(
                            next(file for file in file_list if file.endswith(".TxF")))
                    elif any(file.endswith(".TxR") for file in file_list):
                        selected_files.append(
                            next(file for file in file_list if file.endswith(".TxR")))
                    elif any(file.endswith(".Tx2") for file in file_list):
                        selected_files.append(
                            next(file for file in file_list if file.endswith(".Tx2")))

                for file in selected_files:
                    last_modified_str = ftp.voidcmd(f"MDTM {file}")[4:].strip()
                    last_modified_date = datetime.strptime(
                        last_modified_str, "%Y%m%d%H%M%S")

                    file_data = BytesIO()
                    ftp.retrbinary(f"RETR {file}", file_data.write)
                    file_data.seek(0)

                    content = file_data.read().decode('utf-8')
                    try:
                        csv_reader = pd.read_csv(
                            BytesIO(content.encode()), delimiter=';')
                        df = pd.DataFrame(csv_reader)
                    except Exception as e:
                        logging.error(f"Error reading file {file}: {e}")
                        continue

                    if len(file) >= 6 and file[-6:-4].isdigit():
                        day = file[-6:-4]
                    else:
                        logging.warning(
                            f"Could not extract day from file name: {file}")
                        continue

                    df['Last Modified'] = last_modified_date - \
                        timedelta(hours=5)
                    year, month = year_month.split('-')
                    df['Date'] = datetime(int(year), int(
                        month), int(day)).strftime('%Y-%m-%d')

                    filename_parts = file.split('.')
                    df['Name'] = file
                    df['Name_Type'] = filename_parts[-1].lower()

                    hour_columns = [
                        col for col in df.columns if col.startswith('HORA')]
                    df_long = pd.melt(df, id_vars=['CODIGO SIC', 'Name', 'Name_Type', 'Last Modified', 'Date'],
                                      value_vars=hour_columns, var_name='Hour', value_name='Value')
                    df_long['Hour'] = df_long['Hour'].str.extract(
                        r'(\d+)').astype(int)
                    df_long.rename(
                        columns={'CODIGO SIC': 'CodeSIC', 'Last Modified': 'PubDate'}, inplace=True)
                    df_long['ValueFmt'] = 'kWh'
                    df_long['Agent'] = 'GNSG'

                    final_df_AENC = pd.concat(
                        [final_df_AENC, df_long], ignore_index=True)

                    logging.debug(f"Processed file {file} successfully")

            except ftplib.error_perm as e:
                logging.error(f"FTP error: {e}")
                continue

except Exception as e:
    logging.error(f"Unexpected error: {e}")

# DSPCTTOS
try:
    logging.debug("Starting execution")

    # Connect to the FTP server with SSL/TLS
    with ftplib.FTP_TLS() as ftp:
        ftp.connect(host, port)
        ftp.login(username, password)
        ftp.prot_p()  # Switch to secure data connection (PROT P)

        # Get current date in Colombia timezone
        today_datetime = pd.Timestamp.now(colombia_timezone)

        # Initialize an empty DataFrame to store all processed data
        final_df_Dpcttos = pd.DataFrame()

        # Process current month and previous two months
        for i in range(5):
            # Calculate year and month for the current iteration
            year_month = (today_datetime - timedelta(days=i*30)
                          ).strftime('%Y-%m')

            # Construct directory path
            directory = f'/INFORMACION_XM/USUARIOSK/GNSG/SIC/COMERCIA/{
                year_month}'

            try:
                ftp.cwd(directory)

                # Get list of files in the directory
                files = ftp.nlst()

                # Filter files that start with "BalCttos"
                dspcttos_files = [
                    file for file in files if file.startswith("dspcttos")]

                # Agrupar los archivos por prefijo (sin extensión)
                grouped_files = defaultdict(list)
                for file in dspcttos_files:
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
                    elif any(file.endswith(".txr") for file in file_list):
                        selected_files.append(
                            next(file for file in file_list if file.endswith(".txr")))
                    elif any(file.endswith(".tx2") for file in file_list):
                        selected_files.append(
                            next(file for file in file_list if file.endswith(".tx2")))

                # Procesar archivos seleccionados
                for file in selected_files:
                    # Get last modified timestamp of the file
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

                        # Convertir de wide a long
                        df_long = pd.melt(df, id_vars=['CONTRATO', 'VENDEDOR', 'COMPRADOR', 'TIPO', 'TIPOMERC', 'TIPO ASIGNA', 'Name', 'Name_Type', 'PubDate', 'Date'],
                                          var_name='Hour', value_name='Value')

                        # Crear columna para indicar si es DESP_ o TRF_
                        df_long['Type'] = df_long['Hour'].str.extract(
                            r'(DESP|TRF)')

                        # Si quieres que la columna 'HORA' sea solo el número de hora
                        df_long['Hour'] = df_long['Hour'].str.extract(
                            r'(\d{2})').astype(int)-1

                        # Rename columns
                        df_long.rename(columns={
                            'CONTRATO': 'CodContract',
                            'VENDEDOR': 'Agent',
                            'COMPRADOR': 'Buyer'
                        }, inplace=True)

                        # Eliminar columnas no necesarias
                        df_long.drop(
                            columns=['TIPO', 'TIPOMERC', 'TIPO ASIGNA'], inplace=True)

                        # Convertir en formato wide pivotando por 'Type'
                        df_wide = df_long.pivot_table(index=['CodContract', 'Agent', 'Buyer', 'Name', 'Name_Type', 'PubDate', 'Date', 'Hour'],
                                                      columns='Type', values='Value', fill_value=0).reset_index()

                        # Eliminar el nombre de la columna 'Type' en las columnas
                        df_wide.columns.name = None

                        # Agregar prefijo "Value_" a las nuevas columnas
                        df_wide = df_wide.rename(columns=lambda x: f"Value_{
                                                 x}" if x in ['DESP', 'TRF'] else x)

                        # Append df_long to final_df
                        final_df_Dpcttos = pd.concat(
                            [final_df_Dpcttos, df_wide], ignore_index=True)

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
        for i in range(3):
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

# GLIQ para las liquidaciones diarias en TX@

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
        final_df_Gliq = pd.DataFrame()

        # Process current month and previous two months
        for i in range(2):
            # Calculate year and month for the current iteration
            year_month = (today_datetime - timedelta(days=i*30)).strftime('%Y-%m')

            # Construct directory path
            directory = f'/INFORMACION_XM/PUBLICOK/SIC/COMERCIA/{year_month}'

            try:
                ftp.cwd(directory)

                # Get list of files in the directory
                files = ftp.nlst()

                # Filter files that start with "gliq"
                gliq_files = [file for file in files if file.startswith("gliq")]

                for file in gliq_files:
                    # Get last modified timestamp of the file
                    last_modified_str = ftp.voidcmd(f"MDTM {file}")[4:].strip()
                    last_modified_date = datetime.strptime(last_modified_str, "%Y%m%d%H%M%S")

                    # Check if the file was modified in the last three months including current month
                    if (today_datetime - timedelta(days=i*30)).strftime('%Y-%m') <= year_month:
                        # Download file contents
                        file_data = BytesIO()
                        ftp.retrbinary(f"RETR {file}", file_data.write)
                        file_data.seek(0)  # Move pointer to start of file_data

                        # Attempt to decode file content using latin-1 directly
                        content = file_data.read().decode('latin-1')

                        # Parse CSV-like structure into DataFrame
                        csv_reader = pd.read_csv(BytesIO(content.encode()), delimiter=';')
                        df = pd.DataFrame(csv_reader)

                        # Filter rows where AGENTE is "GNSG"
                        df = df[df['AGENTE'] == 'GNSG']

                        # Add Filename and Last Modified Timestamp to DataFrame
                        df['PubDate'] = last_modified_date - timedelta(hours=5)

                        # Extract year and month from directory
                        year, month = year_month.split('-')

                        # Extract day from filename
                        day = file[-6:-4]  # Assuming day is the last two characters before extension

                        # Create datetime object yyyy-mm-dd
                        df['Date'] = datetime(int(year), int(month), int(day)).strftime('%Y-%m-%d')

                        # Extract Name and Name_Type from Filename
                        filename_parts = file.split('.')
                        df['Name'] = file  # Get all characters before the first '.'
                        df['Name_Type'] = filename_parts[-1].lower()  # Get characters after the last '.'

                        # Paso 1: Eliminar espacios al inicio y al final de los nombres de columna
                        df.columns = df.columns.str.strip()
                        
                        # Paso 2: Eliminar espacios dentro de los nombres de columna
                        df.columns = df.columns.str.replace(' ', '')
                        
                        # Paso 3: Eliminar caracteres no válidos para BigQuery (solo se permiten letras, números y '_')
                        df.columns = df.columns.str.replace(r'[^a-zA-Z0-9_]', '', regex=True)
                        
                        # Paso 4: Reemplazar '$' con 'mon' (por ejemplo, para columnas monetarias)
                        df.columns = df.columns.str.replace(r'\$', 'mon', regex=True)
                        
                        # Paso 5: Normalizar y eliminar tildes
                        df.columns = df.columns.map(lambda x: unicodedata.normalize('NFKD', x).encode('ascii', 'ignore').decode('utf-8'))
                        
                        # Paso 6: Truncar nombres de columnas a un máximo de 300 caracteres (límite de BigQuery)
                        df.columns = df.columns.str[:300]

                        # Append df to final_df
                        final_df_Gliq = pd.concat([final_df_Gliq, df], ignore_index=True)


                        logging.debug(f"Processed file {file} successfully")

            except ftplib.error_perm as e:
                logging.error(f"FTP error: {e}")
                continue

except Exception as e:
    logging.error(f"Unexpected error: {e}")
# DAtos para las OEF RRID
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
        final_df_OEFrrid = pd.DataFrame()

        # Process current month (no past months in this example)
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

                # Filter files that start with "oefsubasbd" and end with .tx2, .txf, or .tx3
                oefsubasbd_files = [
                    file for file in files if file.startswith("oefsbmd") and file.endswith((".tx2", ".txf", ".tx3", ".tx4"))
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
                        final_df_OEFrrid = pd.concat(
                            [final_df_OEFrrid, df], ignore_index=True)

                        logging.debug(f"Processed file {file} successfully")

            except ftplib.error_perm as e:
                logging.error(f"FTP error: {e}")
                continue

except Exception as e:
    logging.error(f"Unexpected error: {e}")

# proyeccion de GARANTIAS para cualquier numero de dias

# Para poder hacer la proyeccion es importante conocer los siguientes datos
print("Es necesario conocer cuantos dias tiene el mes a proyectar y como la proyeccion debe ser con el mismo tipo de dia es importante ver que tipo de dia termina el mes y ver el ultimo dia disponible con el mismo tipo de dia")
dias_mes = int(input("Ingrese el número de días del mes que va a proyectar: "))

# Solicitar la fecha del último día que tiene información
fecha_ultimo_dia_str = input(
    "Ingrese la fecha del último día que tiene información (formato: YYYY-MM-DD) de contratos ")
fecha_ultimo_dia_cal_str = input(
    "Ingrese la fecha del día del calculo (formato: YYYY-MM-DD)")
try:
    fecha_ultimo_dia = datetime.strptime(fecha_ultimo_dia_str, "%Y-%m-%d")
    fecha_ultimo_dia_cal = datetime.strptime(
        fecha_ultimo_dia_cal_str, "%Y-%m-%d")

except ValueError:
    print("Formato de fecha inválido. Por favor use el formato YYYY-MM-DD.")
    exit()


def calcular_rango(mes_facturado):
    año_actual = datetime.now().year

    # Ajustar el primer mes y su año
    primer_mes = (mes_facturado - 2) if (mes_facturado -
                                         2) > 0 else (mes_facturado - 2 + 12)
    primer_año = año_actual-1 if mes_facturado > 3 else año_actual - 1

    # El mes facturado es el tercer mes, con el año actual
    tercer_mes = mes_facturado
    tercer_año = año_actual-1

    # Calcular las fechas inicial y final
    fecha_inicialMF = datetime(primer_año, primer_mes, 1).strftime('%Y-%m-%d')
    ultimo_dia = calendar.monthrange(tercer_año, tercer_mes)[1]
    fecha_finalMF = datetime(tercer_año, tercer_mes,
                             ultimo_dia).strftime('%Y-%m-%d')

    return fecha_inicialMF, fecha_finalMF


# Solicitar al usuario el último mes facturado
try:
    mes_facturado = int(
        input("Introduce el número del último mes facturado (1-12): "))
    if 1 <= mes_facturado <= 12:
        fecha_inicialMF, fecha_finalMF = calcular_rango(mes_facturado)
        print(f"Fecha inicial: {
              fecha_inicialMF}, Fecha final: {fecha_finalMF}")
    else:
        print("Por favor, introduce un número de mes válido (1-12).")
except ValueError:
    print("Por favor, introduce un número válido.")

# Primer concepto EXPOSICION DE ENERGIA EN BOLSA
# Que corresponde a Ventas y compras en contratos, generacion ideal y demanda
# CALCULO GENERACION IDEAL PROMEDIO DE LOS ULTIMOS 3 MESES FACTURADOS
final_df['Date'] = pd.to_datetime(final_df['Date'])
df_GenIdeal = final_df[
    (final_df['Type'] == 'GINA') &
    (final_df['Date'] >= fecha_inicialMF) &
    (final_df['Date'] <= fecha_finalMF)
]
df_GenIdeal['Value'] = pd.to_numeric(df_GenIdeal['Value'], errors='coerce')
# Calcular GI
GI = (df_GenIdeal['Value'].sum() / len(df_GenIdeal))*24
# corresponde a Promedio mensual o semanal, según el caso, de la Generación Ideal del Agente, en kWh, de los últimos tres meses facturados : ")
GENIDEAL = float(GI*dias_mes)

# CALCULO DEMANDA DEL ULTIMO MES

if 'final_df_AENC' in locals() or 'final_df_AENC' in globals():
    # Asegurarse de que la columna 'Date' sea del tipo datetime
    final_df_AENC['Date'] = pd.to_datetime(final_df_AENC['Date'])
    fecha_inicio_Cal = fecha_ultimo_dia_cal - timedelta(days=6)

    # Filtrar el DataFrame
    df_filtrado_Demand = final_df_AENC[(final_df_AENC['Date'] >= fecha_inicio_Cal) & (
        final_df_AENC['Date'] <= fecha_ultimo_dia_cal)]

# Calcular DEMANDA

df_filtrado_Demand['Value'] = pd.to_numeric(
    df_filtrado_Demand['Value'], errors='coerce')
df_filtrado_Frt52615 = df_filtrado_Demand[df_filtrado_Demand['CodeSIC'] == 'Frt52615']

DEMANDA = (df_filtrado_Frt52615['Value'].sum()/
           len(df_filtrado_Frt52615))*24*dias_mes

# PRECIO PROMEDIO PONDERADO
# Convertir 'Date' a tipo datetime si no lo está
final_df_TRSD['Date'] = pd.to_datetime(final_df_TRSD['Date'])

# Obtener la última fecha y calcular el rango de fechas para la última semana
ultima_fecha = fecha_ultimo_dia_cal  # final_df_TRSD['Date'].max()
fecha_inicio = ultima_fecha - pd.Timedelta(days=6)

# Filtrar las filas que están dentro del rango de la última semana
df_PPP_UltimaSemana = final_df_TRSD[(final_df_TRSD['Date'] >= fecha_inicio) & (
    final_df_TRSD['Date'] <= ultima_fecha)]

# Asegurarse de que la columna 'Value' sea numérica
df_PPP_UltimaSemana['Value'] = pd.to_numeric(
    df_PPP_UltimaSemana['Value'], errors='coerce')

# Filtrar los códigos PBNA y CBNA en el DataFrame de la última semana
filtered_df = df_PPP_UltimaSemana[df_PPP_UltimaSemana['Code'].isin([
                                                                   'PBNA', 'CBNA'])]

# Pivotar para tener una columna por cada Code y así facilitar el cálculo
pivot_df = filtered_df.pivot_table(
    index=['Date', 'Hour'], columns='Code', values='Value', aggfunc='sum').reset_index()

# Asegurarse de que haya valores para ambos códigos (PBNA y CBNA)
pivot_df = pivot_df.dropna(subset=['PBNA', 'CBNA'])

# Calcular el numerador: suma de la multiplicación (PBNA * CBNA)
pivot_df['Producto'] = pivot_df['PBNA'] * pivot_df['CBNA']
numerador = pivot_df['Producto'].sum()

# Calcular el denominador: suma de CBNA
denominador = pivot_df['CBNA'].sum()

# Calcular el Precio Promedio Bolsa
PrecioPromedioBolsa = numerador / denominador if denominador != 0 else 0

# Imprimir el resultado
print("Precio Promedio Bolsa:", PrecioPromedioBolsa)

PrecioEscasezP = 918.487

# Convertir la columna 'Date' a formato datetime si no está ya
if 'final_df_Dpcttos' in locals() or 'final_df_Dpcttos' in globals():
    final_df_Dpcttos['Date'] = pd.to_datetime(final_df_Dpcttos['Date'])
    # Ajustar las fechas de consulta
    fecha_ultimo_dia = fecha_ultimo_dia
    fecha_inicio_Cttos = fecha_ultimo_dia - timedelta(days=dias_mes-1)

    # Filtrar el DataFrame para las fechas actuales
    df_filtrado_Dpcttos = final_df_Dpcttos[
        (final_df_Dpcttos['Date'] >= fecha_inicio_Cttos) &
        (final_df_Dpcttos['Date'] <= fecha_ultimo_dia)
    ]

    # Convertir 'Value_DESP' a numérico y calcular VCONTR
    df_filtrado_Dpcttos['Value_DESP'] = pd.to_numeric(
        df_filtrado_Dpcttos['Value_DESP'], errors='coerce')
    VCONTR = df_filtrado_Dpcttos['Value_DESP'].sum()

CCONT = 0  # el agente en estudio no tienes contratos de compra en energia de lo contrario habria que identificarlos

# TENIENDO LOS VALORES SE PROCEDE A CALCULAR LA EXPOCISION EN BOLSA

VEB = float((VCONTR-CCONT-GENIDEAL+DEMANDA) *
            min(PrecioPromedioBolsa, PrecioEscasezP))

fecha_inicial_MF = datetime.strptime(fecha_inicialMF, '%Y-%m-%d')
fecha_final_MF = datetime.strptime(fecha_finalMF, '%Y-%m-%d')
Diasparapromediar = dias = (fecha_final_MF - fecha_inicial_MF).days + 1
meses_en_rango = []
fecha_actual = fecha_inicial_MF
while fecha_actual <= fecha_final_MF:
    meses_en_rango.append(fecha_actual.strftime('%m'))
    # Avanzar al siguiente mes
    siguiente_mes = fecha_actual.month + 1 if fecha_actual.month < 12 else 1
    siguiente_año = fecha_actual.year if siguiente_mes != 1 else fecha_actual.year + 1
    fecha_actual = datetime(siguiente_año, siguiente_mes, 1)

print(f"Meses en el rango: {meses_en_rango}")
AFACMF = final_df_afac['Name'].str[:6].isin(
    [f'afac{mes}' for mes in meses_en_rango])
final_df_AFACMF = final_df_afac[AFACMF]


REST = float(final_df_AFACMF['RESTRICCIONESALIVIADAS'].sum(
)) * dias_mes/Diasparapromediar  # GNSG no cuenta con cobro por restricciones

VDES = float(final_df_AFACMF['COMPRASENDESVIACION'].sum()
             ) * dias_mes/Diasparapromediar

RCAGC = float(final_df_AFACMF['RESPONSABILIDADCOMERCIALAGC'].sum(
)) * dias_mes/Diasparapromediar

VALORAPAGARPORSRPF = float(
    final_df_AFACMF['VALORAPAGARPORSRPF'].sum()) * dias_mes/Diasparapromediar
VALORARECIBIRPORSRPF = float(
    final_df_AFACMF['VALORARECIBIRPORSRPF'].sum()) * dias_mes/Diasparapromediar
VSRPF = VALORAPAGARPORSRPF-VALORARECIBIRPORSRPF

VREC = float(
    final_df_AFACMF['VENTASENRECONCILIACION'].sum()) * dias_mes/Diasparapromediar
CREC = float(
    final_df_AFACMF['COMPRASENRECONCILIACION'].sum()) * dias_mes/Diasparapromediar
REC = CREC-VREC


ultima_fecha_CEE = fecha_ultimo_dia_str
CEE = final_df_tdia[(final_df_tdia["Code"] == "CERD") &
                    (final_df_tdia["Date"] == ultima_fecha_CEE) &
                    (final_df_tdia["Name_Type"] == "tx2")]['Value']
CEE = 88.758#CEE.values[0]

# Calcular GREAL de los ultimos 3 meses FACTURADOS
df_GenReal = final_df[
    (final_df['Type'] == 'GREA') &
    (final_df['Date'] >= fecha_inicialMF) &
    (final_df['Date'] <= fecha_finalMF)
]
df_GenReal['Value'] = pd.to_numeric(df_GenReal['Value'], errors='coerce')
# Calcular GR
GR = (df_GenReal['Value'].sum() / len(df_GenReal))*24
GENREAL = float(GR*dias_mes)

# Cargo por confibilidad
VR = float(CEE*(GENREAL))

# Vigencias de OEF
OEFSUNORTE = final_df_OEF[final_df_OEF['SUBM']== '3IQA']
RRIDSUNORTE = final_df_OEFrrid[final_df_OEFrrid['SUBMERCADO']== '3IQA']

PrecioRemu = OEFSUNORTE[(OEFSUNORTE["Date"] == ultima_fecha_CEE)]['PCXCA'].values[0]
OEFANUAL = OEFSUNORTE[(OEFSUNORTE["Date"] == ultima_fecha_CEE)]['OEF AÑO [kWh-año]'].values[0]
TRM = RRIDSUNORTE[(RRIDSUNORTE["Date"] == ultima_fecha_CEE)]['TRM $/USD'].values[0]
VD = (OEFANUAL/365*PrecioRemu*TRM) * dias_mes

# Cargos CND SIC y FAZNI
SERVMF = final_df_tserv['Name'].str[:7].isin(
    [f'tserv{mes}' for mes in meses_en_rango])
final_df_tservMF = final_df_tserv[SERVMF]

CND_df = final_df_tservMF[
    (final_df_tservMF['CONCEPTO'] == 'CND') &
    (final_df_tservMF['AGENTE'] == 'GNSG')]
CND = CND_df['VALOR'].sum() * dias_mes / Diasparapromediar

SIC_df = final_df_tservMF[
    (final_df_tservMF['CONCEPTO'] == 'SIC') &
    (final_df_tservMF['AGENTE'] == 'GNSG')]

SIC = SIC_df['VALOR'].sum() * 1.19 * dias_mes / Diasparapromediar

FZN_df = final_df_tservMF[(final_df_tservMF['CONCEPTO'] == 'FZN') &
                          (final_df_tservMF['AGENTE'] == 'GNSG')]

FZN = FZN_df['VALOR'].sum() * dias_mes / Diasparapromediar

SERVICIOS = CND + SIC + FZN

GARANTIA = VEB + REST + VDES + RCAGC + VSRPF + REC + VR - VD + SERVICIOS


# Proceso Ajustes de semana liquidada

# Solicitar la fecha del último día que tiene información
fecha_ultimo_dia_str = input(
    "Ingrese la fecha del último día que tiene información (formato: YYYY-MM-DD) de contratos, esta fecha de corresponder al ultimo dia con tx2 para el dia de calculo de XM verificar calendario: ")
Dias_liq = int(input("Ingrese el numero de dia que va a liquidar esto es para cuando la semana se divide en dos meses, de lo contrario el numero de dias corresponde al de la semana que son 7: "))
try:
    fecha_ultimo_dia = datetime.strptime(fecha_ultimo_dia_str, "%Y-%m-%d")


except ValueError:
    print("Formato de fecha inválido. Por favor use el formato YYYY-MM-DD.")
    exit()

Dias_liq = Dias_liq - 1
fecha_inicio = fecha_ultimo_dia - timedelta(days=Dias_liq)

# Exposicion energia en bolsa
# Gen ideal
df_GenIdeal = final_df[final_df['Type'] == 'GINA']
df_GenIdeal['Value'] = pd.to_numeric(df_GenIdeal['Value'], errors='coerce')
# Convertir la columna 'Date' a formato datetime si no está ya
df_GenIdeal['Date'] = pd.to_datetime(df_GenIdeal['Date'])

df_filtrado_Genideal = df_GenIdeal[
    (df_GenIdeal['Date'] >= fecha_inicio) &
    (df_GenIdeal['Date'] <= fecha_ultimo_dia)
]
# Calcular GI
GI = (df_filtrado_Genideal['Value'].sum())

# DEMANDA
final_df_AENC['Date'] = pd.to_datetime(final_df_AENC['Date'])
df_filtrado_Demand = final_df_AENC[(final_df_AENC['Date'] >= fecha_inicio) & (
    final_df_AENC['Date'] <= fecha_ultimo_dia)]
df_filtrado_Frt52615 = df_filtrado_Demand[df_filtrado_Demand['CodeSIC'] == 'Frt52615']
df_filtrado_Frt52615['Value'] = pd.to_numeric(
    df_filtrado_Frt52615['Value'], errors='coerce')
DEMANDA = (df_filtrado_Frt52615['Value'].sum())

# Despacho de contratos
final_df_Dpcttos['Date'] = pd.to_datetime(final_df_Dpcttos['Date'])
df_filtrado_Dpcttos = final_df_Dpcttos[
    (final_df_Dpcttos['Date'] >= fecha_inicio) &
    (final_df_Dpcttos['Date'] <= fecha_ultimo_dia)]

df_filtrado_Dpcttos['Value_DESP'] = pd.to_numeric(
    df_filtrado_Dpcttos['Value_DESP'], errors='coerce')
VCONTR = df_filtrado_Dpcttos['Value_DESP'].sum()


# PRECIO PROMEDIO PONDERADO
# Convertir 'Date' a tipo datetime si no lo está
final_df_TRSD['Date'] = pd.to_datetime(final_df_TRSD['Date'])
df_PPP_UltimaSemana = final_df_TRSD[(final_df_TRSD['Date'] >= fecha_inicio) & (
    final_df_TRSD['Date'] <= fecha_ultimo_dia)]

# Asegurarse de que la columna 'Value' sea numérica
df_PPP_UltimaSemana['Value'] = pd.to_numeric(
    df_PPP_UltimaSemana['Value'], errors='coerce')

# Filtrar los códigos PBNA y CBNA en el DataFrame de la última semana
filtered_df = df_PPP_UltimaSemana[df_PPP_UltimaSemana['Code'].isin([
                                                                   'PBNA', 'CBNA'])]

# Pivotar para tener una columna por cada Code y así facilitar el cálculo
pivot_df = filtered_df.pivot_table(
    index=['Date', 'Hour'], columns='Code', values='Value', aggfunc='sum').reset_index()

# Asegurarse de que haya valores para ambos códigos (PBNA y CBNA)
pivot_df = pivot_df.dropna(subset=['PBNA', 'CBNA'])

# Calcular el numerador: suma de la multiplicación (PBNA * CBNA)
pivot_df['Producto'] = pivot_df['PBNA'] * pivot_df['CBNA']
numerador = pivot_df['Producto'].sum()

# Calcular el denominador: suma de CBNA
denominador = pivot_df['CBNA'].sum()

# Calcular el Precio Promedio Bolsa
PrecioPromedioBolsa = numerador / denominador if denominador != 0 else 0

# Imprimir el resultado
print("Precio Promedio Bolsa:", PrecioPromedioBolsa)

PrecioEscasezP = 918.487  # PRecio de escasez ultimo mes facturado

VEB = float((VCONTR-GI+DEMANDA) * min(PrecioPromedioBolsa, PrecioEscasezP))

# RESPONSABILIDAD COMERCIAL
final_df_t['Date'] = pd.to_datetime(final_df_t['Date'])

# Define el orden de prioridad para 'Name_Type'
version_order = ['tx4', 'tx3', 'txf', 'txr', 'tx2']

# Asigna un valor numérico a 'Name_Type' basado en su prioridad
final_df_t['Version_Priority'] = final_df_t['Name_Type'].apply(
    lambda x: version_order.index(x))

# Ordena el DataFrame por 'Date', 'Hour', y 'Version_Priority'
final_df_sorted = final_df_t.sort_values(
    by=['Date', 'Hour', 'Version_Priority'])

# Elimina duplicados, quedándote con la versión más reciente (de menor prioridad numérica)
final_df_tgrl = final_df_sorted.drop_duplicates(
    subset=['Date', 'Hour'], keep='first')

# Elimina la columna auxiliar de prioridad
final_df_tgrl = final_df_tgrl.drop(columns=['Version_Priority'])

# Filtra el DataFrame por el código 'VRCL' y el rango de fechas
df_filtrado_VR = final_df_t[(final_df_t['CODIGO'] == 'VRCL') & (final_df_t['AGENTE'] == 'GNSG') &
                            (final_df_t['Date'] >= fecha_inicio) &
                            (final_df_t['Date'] <= fecha_ultimo_dia)]

# Suma los valores de la columna 'Value'
VRC = (df_filtrado_VR['Value'].sum())

# cargo por confiabilidad
# Gen real
df_GenReal = final_df[final_df['Type'] == 'GREA']
df_GenReal['Value'] = pd.to_numeric(df_GenReal['Value'], errors='coerce')
# Convertir la columna 'Date' a formato datetime si no está ya
df_GenReal['Date'] = pd.to_datetime(df_GenReal['Date'])

df_filtrado_GenReal = df_GenReal[
    (df_GenReal['Date'] >= fecha_inicio) &
    (df_GenReal['Date'] <= fecha_ultimo_dia)
]
GR = (df_filtrado_GenReal['Value'].sum())

# CEE
ultima_fecha_CEE = fecha_ultimo_dia_str
CEE = final_df_tdia[(final_df_tdia["Code"] == "CERD") &
                    (final_df_tdia["Date"] == ultima_fecha_CEE) &
                    (final_df_tdia["Name_Type"] == "tx2")]['Value']
CEE = 87.746 #CEE.values[0]

RRIDSUNORTE['Date'] = pd.to_datetime(RRIDSUNORTE['Date'])

VD_df = RRIDSUNORTE[
                  (RRIDSUNORTE['Date'] >= fecha_inicio) &
                  (RRIDSUNORTE['Date'] <= fecha_ultimo_dia)]
VD = (VD_df['RRID A RECIBIR $'].sum())

VR = float(CEE*(GR))

CargoC = VR - VD

# SErvicios CND, SIC y FAZni


def total_days_previous_three_months(input_date_str):
    # Convertir la fecha de entrada a un objeto datetime
    input_date = datetime.strptime(input_date_str, '%Y-%m-%d')

    # Inicializar el contador de días
    total_days = 0

    # Iterar sobre los últimos tres meses
    for i in range(1, 4):
        # Calcular el mes y el año anterior
        prev_month = (input_date.month - i - 1) % 12 + 1
        prev_year = input_date.year if input_date.month - i > 0 else input_date.year - 1

        # Obtener los días del mes
        days_in_month = calendar.monthrange(prev_year, prev_month)[1]
        total_days += days_in_month

    return total_days


Diasparapromediar = total_days_previous_three_months(fecha_ultimo_dia_str)
Dias_liq = Dias_liq + 1

CND_df = final_df_tserv[
    (final_df_tserv['CONCEPTO'] == 'CND') &
    (final_df_tserv['AGENTE'] == 'GNSG')]
CND = CND_df['VALOR'].sum()*(Dias_liq/Diasparapromediar)

SIC_df = final_df_tserv[
    (final_df_tserv['CONCEPTO'] == 'SIC') &
    (final_df_tserv['AGENTE'] == 'GNSG')]

SIC = SIC_df['VALOR'].sum() * 1.19 * (Dias_liq/Diasparapromediar)

FZN_df = final_df_tserv[(final_df_tserv['CONCEPTO'] == 'FZN') &
                        (final_df_tserv['AGENTE'] == 'GNSG')]

FZN = FZN_df['VALOR'].sum() * (Dias_liq/Diasparapromediar)

SERVICIOS = CND + SIC + FZN

# DEsviaciones de OEF
df_DOEF = final_df_DOEF[final_df_DOEF['CONCEPTO'] == 'VFDPOEFH']
df_DOEF['Value'] = pd.to_numeric(df_DOEF['Value'], errors='coerce')
# Convertir la columna 'Date' a formato datetime si no está ya
df_DOEF['Date'] = pd.to_datetime(df_DOEF['Date'])

df_filtrado_DOEF = df_DOEF[
    (df_DOEF['Date'] >= fecha_inicio) &
    (df_DOEF['Date'] <= fecha_ultimo_dia)
]
VDOEF = (df_filtrado_DOEF['Value'].sum())

df_VCDG = final_df_DOEF[final_df_DOEF['CONCEPTO'] == 'VCDGCB']
df_VCDG['Value'] = pd.to_numeric(df_VCDG['Value'], errors='coerce')
# Convertir la columna 'Date' a formato datetime si no está ya
df_VCDG['Date'] = pd.to_datetime(df_VCDG['Date'])

df_filtrado_VCDG = df_VCDG[
    (df_VCDG['Date'] >= fecha_inicio) &
    (df_VCDG['Date'] <= fecha_ultimo_dia)
]
VCDG = (df_filtrado_VCDG['Value'].sum())

final_df_t['Date'] = pd.to_datetime(final_df_t['Date'])

DESV = final_df_t[(final_df_t['CODIGO'] == 'VADES') & (final_df_t['AGENTE'] == 'GNSG') &
                  (final_df_t['Date'] >= fecha_inicio) &
                  (final_df_t['Date'] <= fecha_ultimo_dia)]

VDESV = (DESV['Value'].sum())

final_df_Gliq['Date'] = pd.to_datetime(final_df_Gliq['Date'])

GLIQ = final_df_Gliq[ (final_df_Gliq['Name_Type'] == 'tx2') &
                    (final_df_Gliq['Date'] >= fecha_inicio) &
                    (final_df_Gliq['Date'] <= fecha_ultimo_dia)]
ComRec = (GLIQ['COMPRASRECONCILIACION'].sum())
VenRec = (GLIQ['VENTASRECONCILIACION'].sum())
Reconci= ComRec - VenRec
ResComer = (GLIQ['RESPONSABILIDADCOMERCIAL'].sum())

GARANTIASEMANALIQ = VEB + VRC + SERVICIOS + CargoC - VDOEF + VCDG + VDESV + Reconci
