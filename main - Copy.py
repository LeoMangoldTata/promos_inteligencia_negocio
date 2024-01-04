from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
import pandas as pd
import numpy as np
import snowflake.connector
import json
import os
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from querys import *
import re

# ----------------------------------------------------------------------
# ----------------------------------------------------------------------

SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
DELETE_SCOPES = ['https://www.googleapis.com/auth/drive.file']

# Ya sea que el codigo se ejecute desde mi pc o desde la VM, el folder ID sera siempre el mismo
# ya que siempre guardaremos los spreadsheets en la misma carpeta

if os.getcwd() == 'C:\\Users\\leonardo.mangold\\PycharmProjects\\promos_inteligencia_negocio':
    FOLDER_ID = '11yV-IyMUXCjQRvXBiDNYIEg_pDc6HglL'
else:
    FOLDER_ID = '1js40bDjRG7XcxeDUGoWYy4wBbsDPjwMu'

# ----------------------------------------------------------------------
# ----------------------------------------------------------------------

def get_credentials():
    credentials = None

    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time.
    if os.getcwd() == 'C:\\Users\\leonardo.mangold\\PycharmProjects\\promos_inteligencia_negocio':
        if os.path.exists("token.json"):
            credentials = Credentials.from_authorized_user_file("token.json", SCOPES)
    else:
        if os.path.exists("token_vm.json"):
            credentials = Credentials.from_authorized_user_file("token_vm.json", SCOPES)

    # If there are no (valid) credentials available, let the user log in.

    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            if os.getcwd() == 'C:\\Users\\leonardo.mangold\\PycharmProjects\\promos_inteligencia_negocio':
                flow = InstalledAppFlow.from_client_secrets_file("api_credentials.json", SCOPES)
            else:
                flow = InstalledAppFlow.from_client_secrets_file("api_credentials_vm.json", SCOPES)
            credentials = flow.run_local_server(port=0)
    # Save the credentials for the next run
        if os.getcwd() == 'C:\\Users\\leonardo.mangold\\PycharmProjects\\promos_inteligencia_negocio':
            with open("token.json", "w") as token:
                token.write(credentials.to_json())
        else:
            with open("token_vm.json", "w") as token:
                token.write(credentials.to_json())
    return credentials

# ----------------------------------------------------------------------
# ----------------------------------------------------------------------

# Conexion Snowflake desde compu personal

if os.getcwd() == 'C:\\Users\\leonardo.mangold\\PycharmProjects\\promos_inteligencia_negocio':

	while True:
	    try:

	        f = open('credentials.json')
	        data_pass = json.load(f)

	        pass_ = input("INGRESAR PASSCODE:")

	        ctx = snowflake.connector.connect(
	            user = data_pass['snow']['USER'],
	            password = data_pass['snow']['PASS'],
	            account = data_pass['snow']['ACCOUNT'],
	            passcode = pass_,
	            database = 'SANDBOX_PLUS',
	            schema = 'DWH'
	        )

	        cursor = ctx.cursor()

	        print('Connected')

	        break

	    except:
	        print('Incorrect Password - provide again')

	    print('Correct Password - connected to SNOWFLAKE')


# Conexion VM a Snowflake

else:

    ctx = snowflake.connector.connect(
        user="PLUS_VM1",
        password="Plus22!",
        account="XZ23267-dp32414",
        database="SANDBOX_PLUS",
        schema="DWH"
        )

    cursor = ctx.cursor()

# ----------------------------------------------------------------------
# ----------------------------------------------------------------------

# DataFrames

# Fechas indicadas por Mauri para tomar los precios

url_fechas = 'https://docs.google.com/spreadsheets/d/1JnayuiljvaOebik4gKqNgxSLD6RO2AS8euG7apQykW8/export?format=csv'
fechas = pd.read_csv(url_fechas)

# Precios Oferta
df_precios_oferta = pd.DataFrame()
for i in range(len(fechas)):
    desde = fechas.iloc[i]['DESDE']
    hasta = fechas.iloc[i]['HASTA']
    oferta = fechas.iloc[i]['OFERTA']

    cursor.execute(precios_oferta.format(desde_snow = desde, hasta_snow = hasta))
    df_aux = cursor.fetch_pandas_all()
    df_aux['PRECIO DESDE'] = desde
    df_aux['PRECIO HASTA'] = hasta
    df_aux['NOMBRE_PROMO'] = oferta
    df_precios_oferta = pd.concat([df_precios_oferta, df_aux])

df_precios_oferta = df_precios_oferta[['TIPO_OFERTA_ID', 'TIPO_OFERTA_DESC', 'INICIO', 'FIN', 'NOMBRE_PROMO', 'PRECIO DESDE', 'PRECIO HASTA', 'ORIN', 'ESTADISTICO', 'PROM_PVP_OFERTA']]

df_precios_oferta['FIN'] = pd.to_datetime(df_precios_oferta['FIN'])

# Create a new 'Rank' column based on the maximum date for each element of 'X'
df_precios_oferta['R'] = df_precios_oferta.groupby('ORIN')['FIN'].rank(ascending=False, method='max').astype(int)
df_precios_oferta = df_precios_oferta[df_precios_oferta['R'] == 1]
df_precios_oferta.drop_duplicates(subset = 'ORIN', keep = 'first', inplace = True)
df_precios_oferta.drop(['R'], axis = 1, inplace = True)

df_precios_oferta.sort_values(by = ['PRECIO HASTA', 'NOMBRE_PROMO', 'FIN'], ascending = [False, True, False], inplace = True)

# Precios Stock Mediano dia de ayer
cursor.execute(precios_stock_mediano)
df_precios_stock_mediano = cursor.fetch_pandas_all()

# OPT
cursor.execute(opt)
df_opt = cursor.fetch_pandas_all()

# Locales Activos Ayer
cursor.execute(locales_activos_ayer)
df_locales_activos_ayer = cursor.fetch_pandas_all()

# Days on Hand - Articulos
cursor.execute(days_on_hand_articulo)
df_days_on_hand_articulo = cursor.fetch_pandas_all()

df_days_on_hand_articulo = df_days_on_hand_articulo[df_days_on_hand_articulo['UNIDADES'] != 0]
df_days_on_hand_articulo['DAYS ON HAND'] = df_days_on_hand_articulo['UNIDADES'] / df_days_on_hand_articulo['UNIDADES_VENDIDAS']
df_days_on_hand_articulo['DAYS ON HAND'][(df_days_on_hand_articulo['UNIDADES'] == 0) & (df_days_on_hand_articulo['UNIDADES_VENDIDAS'] == 0)] = 0
df_days_on_hand_articulo['DAYS ON HAND'][df_days_on_hand_articulo['DAYS ON HAND'] == np.inf] = 999999
df_days_on_hand_articulo['DAYS ON HAND'][df_days_on_hand_articulo['DAYS ON HAND'] == -np.inf] = -999999
df_days_on_hand_articulo['DAYS ON HAND'][df_days_on_hand_articulo['DAYS ON HAND'] > 999999] = 999999
df_days_on_hand_articulo['DAYS ON HAND'][df_days_on_hand_articulo['DAYS ON HAND'] < -999999] = -999999
df_days_on_hand_articulo['DAYS ON HAND'].fillna(999999, inplace=True)
df_days_on_hand_articulo['DAYS ON HAND'] = round(df_days_on_hand_articulo['DAYS ON HAND'], 0).astype(int)
df_days_on_hand_articulo['UNIDADES'] = round(df_days_on_hand_articulo['UNIDADES'], 0).astype(int)
df_days_on_hand_articulo['UNIDADES_VENDIDAS'] = round(df_days_on_hand_articulo['UNIDADES_VENDIDAS'], 0).astype(int)
df_days_on_hand_articulo.rename({'UNIDADES_VENDIDAS':'UNIDADES VENDIDAS'}, axis = 1, inplace = True)
df_days_on_hand_articulo = df_days_on_hand_articulo[['SUBCLASE', 'ORIN', 'UNIDADES', 'UNIDADES VENDIDAS', 'DAYS ON HAND']]

# Days on Hand - Subclases
cursor.execute(days_on_hand_subclase)
df_days_on_hand_subclase = cursor.fetch_pandas_all()

df_days_on_hand_subclase = df_days_on_hand_subclase[df_days_on_hand_subclase['UNIDADES'] != 0]
df_days_on_hand_subclase['DAYS ON HAND'] = df_days_on_hand_subclase['UNIDADES'] / df_days_on_hand_subclase['UNIDADES_VENDIDAS']
df_days_on_hand_subclase['DAYS ON HAND'][(df_days_on_hand_subclase['UNIDADES'] == 0) & (df_days_on_hand_subclase['UNIDADES_VENDIDAS'] == 0)] = 0
df_days_on_hand_subclase['DAYS ON HAND'][df_days_on_hand_subclase['DAYS ON HAND'] == np.inf] = 999999
df_days_on_hand_subclase['DAYS ON HAND'][df_days_on_hand_subclase['DAYS ON HAND'] == -np.inf] = -999999
df_days_on_hand_subclase['DAYS ON HAND'][df_days_on_hand_subclase['DAYS ON HAND'] > 999999] = 999999
df_days_on_hand_subclase['DAYS ON HAND'][df_days_on_hand_subclase['DAYS ON HAND'] < -999999] = -999999
df_days_on_hand_subclase['DAYS ON HAND'].fillna(999999, inplace=True)
df_days_on_hand_subclase['DAYS ON HAND'] = round(df_days_on_hand_subclase['DAYS ON HAND'], 0).astype(int)
df_days_on_hand_subclase['UNIDADES'] = round(df_days_on_hand_subclase['UNIDADES'], 0).astype(int)
df_days_on_hand_subclase['UNIDADES_VENDIDAS'] = round(df_days_on_hand_subclase['UNIDADES_VENDIDAS'], 0).astype(int)
df_days_on_hand_subclase.rename({'UNIDADES_VENDIDAS':'UNIDADES VENDIDAS'}, axis = 1, inplace = True)
df_days_on_hand_subclase = df_days_on_hand_subclase[['SUBCLASE', 'UNIDADES', 'UNIDADES VENDIDAS', 'DAYS ON HAND']]

# ----------------------------------------------------------------------
# ----------------------------------------------------------------------

# Functions

def save_dataframe_as_sheet(service, df, spreadsheet_id, sheet_name):

    '''
    Funcion 1. esta funcion es auxiliar, para usar dentro de la funcion 2
    '''

    # Convert date columns to strings
    df = df.applymap(lambda x: x.strftime('%Y-%m-%d') if hasattr(x, 'strftime') else x)

    # Replace NaN and Infinity values with None
    df = df.replace({np.nan: None, np.inf: None, -np.inf: None})

    values = df.values.tolist()
    values.insert(0, df.columns.tolist())  # Include headers
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=sheet_name,
        body={'values': values},
        valueInputOption='RAW'
    ).execute()


def create_spreadsheet():

    '''
    Funcion 2. toma la funcion 1 para generar una spreadsheet por cada dataframe
    '''

    # Create a new spreadsheet
    spreadsheet = service.spreadsheets().create(body={
        'properties': {'title': f"Promos"},
        'sheets': [{'properties': {'title': 'Precios Oferta'}},
                   {'properties': {'title': 'Precios Stock Mediano'}},
                   {'properties': {'title': 'OPT'}},
                   {'properties': {'title': 'Locales Activos Ayer'}},
                   {'properties': {'title': 'Days on Hand - Articulos'}},
                   {'properties': {'title': 'Days on Hand - Subclases'}}
                   ],
    }).execute()

    # Get the spreadsheet ID
    spreadsheet_id = spreadsheet['spreadsheetId']

    # Move the spreadsheet to the specified folder
    drive_service = build('drive', 'v3', credentials=credentials)
    drive_service.files().update(fileId=spreadsheet_id, addParents=FOLDER_ID).execute()


    # Save each DataFrame in a separate sheet
    save_dataframe_as_sheet(service, df_precios_oferta, spreadsheet_id, 'Precios Oferta')
    save_dataframe_as_sheet(service, df_precios_stock_mediano, spreadsheet_id, 'Precios Stock Mediano')
    save_dataframe_as_sheet(service, df_opt, spreadsheet_id, 'OPT')
    save_dataframe_as_sheet(service, df_locales_activos_ayer, spreadsheet_id, 'Locales Activos Ayer')
    save_dataframe_as_sheet(service, df_days_on_hand_articulo, spreadsheet_id, 'Days on Hand - Articulos')
    save_dataframe_as_sheet(service, df_days_on_hand_subclase, spreadsheet_id, 'Days on Hand - Subclases')

    print(f'Spreadsheet created and moved to the folder: https://docs.google.com/spreadsheets/d/{spreadsheet_id}')


def insert_dataframe_into_sheet(dataframe, spreadsheet_id, credentials, sheet_name):

    '''
    Funcion 3. dentro de un spreadsheet ya existente, crea sheets y pega dataframes
    '''

    # Authenticate with Google Sheets API using service account credentials
    gc = gspread.authorize(credentials)

    # Open the spreadsheet by its ID
    spreadsheet = gc.open_by_key(spreadsheet_id)

    # Get a unique sheet name (you can modify this logic based on your requirements)
    sheet_name = sheet_name

    # Create a new worksheet and set its title
    new_worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1", cols="1")

    # Set the header row and data from the DataFrame to the new sheet
    set_with_dataframe(new_worksheet, dataframe)

def delete_spreadsheet(spreadsheet_id, credentials):

    '''
    Funcion 4. elimina la spreadsheet
    '''

    client = gspread.authorize(credentials)

    # Open the spreadsheet by its ID
    spreadsheet = client.open_by_key(spreadsheet_id)
    client.del_spreadsheet(spreadsheet.id)

def delete_all_sheets(spreadsheet_id, credentials):

    '''
    Funcion 5. borra todas las sheets excepto la primera
    (No es posible eliminar todas las sheets de una spreadsheet)
    Mantiene la spreadsheet viva
    '''

    client = gspread.authorize(credentials)

    # Open the spreadsheet by its ID
    spreadsheet = client.open_by_key(spreadsheet_id)

    # Get all sheet titles
    sheet_titles = [worksheet.title for worksheet in spreadsheet.worksheets()]

    # Delete each sheet individually, except the last one
    # No permite eliminar todas las sheets, asi que dejo la primera como dummy
    for sheet_title in sheet_titles[1:]:
        spreadsheet.del_worksheet(spreadsheet.worksheet(sheet_title))

def delete_first_sheet(spreadsheet_id, credentials):

    '''
    Funcion 6. borra la primera sheet
    (No es posible eliminar todas las sheets de una spreadsheet)
    '''

    client = gspread.authorize(credentials)

    # Open the spreadsheet by its ID
    spreadsheet = client.open_by_key(spreadsheet_id)

    # Get all sheet titles
    sheet_titles = [worksheet.title for worksheet in spreadsheet.worksheets()]

    # Delete each sheet individually, except the last one
    # No permite eliminar todas las sheets, asi que dejo la primera como dummy
    for sheet_title in sheet_titles[:1]:
        spreadsheet.del_worksheet(spreadsheet.worksheet(sheet_title))

if __name__ == "__main__":

    credentials = get_credentials()
    spreadsheet_id = '1w5oGifgavq1GodX3z9t9UF8ALeOl6NzMWiu_4xPG8FE'
    # Debido a que siempre mantenemos la misma spreadsheet, su id no cambia

    # Create a Google Sheets API service --> esto se usa en caso de usar la funcion 2
    service = build('sheets', 'v4', credentials=credentials)

    # Create sheets and insert the DataFrames

    dataframes_dict = {
        'df_precios_oferta': 'Precios Oferta',
        'df_precios_stock_mediano': 'Precios Stock Mediano',
        'df_opt': 'OPT',
        'df_locales_activos_ayer': 'Locales Activos Ayer',
        'df_days_on_hand_articulo': 'Days on Hand - Articulos',
        'df_days_on_hand_subclase': 'Days on Hand - Subclases'
    }

    for df_name, sheet_name in dataframes_dict.items():

        # Elimino la primera sheet
        delete_first_sheet(spreadsheet_id, credentials)

        df = globals()[df_name]  # Retrieve the dataframe using its name
        print(df_name, sheet_name)

        # Inserto la DataFrame en una nueva sheet, que ocupa la ultima posicion
        insert_dataframe_into_sheet(df, spreadsheet_id, credentials, sheet_name)