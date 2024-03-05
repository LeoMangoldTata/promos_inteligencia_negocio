from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth import exceptions
import gspread
from gspread_dataframe import set_with_dataframe
import numpy as np
import os

# ----------------------------------------------------------------------
# ----------------------------------------------------------------------

#SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
#DELETE_SCOPES = ['https://www.googleapis.com/auth/drive.file']

# ----------------------------------------------------------------------
# ----------------------------------------------------------------------

def get_credentials():
    credentials = None

    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time.
    if os.getcwd() == 'C:\\Users\\leonardo.mangold\\PycharmProjects\\promos_inteligencia_negocio':
        if os.path.exists("token.json"):
            credentials = Credentials.from_authorized_user_file("token.json", SCOPES)
    elif os.getcwd() == r'C:\Users\arturo.botata12\Documents\GitHub\promos_inteligencia_negocio':
        if os.path.exists("token_vm.json"):
            credentials = Credentials.from_authorized_user_file("token_vm.json", SCOPES)
    else:
        raise ValueError("Unknown working directory. Unable to determine the correct client secrets file.")

    # If there are no (valid) credentials available, let the user log in.

    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            if os.getcwd() == 'C:\\Users\\leonardo.mangold\\PycharmProjects\\promos_inteligencia_negocio':
                flow = InstalledAppFlow.from_client_secrets_file("api_credentials.json", SCOPES)
            elif os.getcwd() == r'C:\Users\arturo.botata12\Documents\GitHub\promos_inteligencia_negocio':
                flow = InstalledAppFlow.from_client_secrets_file("api_credentials_vm.json", SCOPES)
            else:
                raise ValueError("Unknown working directory. Unable to determine the correct client secrets file.")
            credentials = flow.run_local_server(port=0)
    # Save the credentials for the next run
        if os.getcwd() == 'C:\\Users\\leonardo.mangold\\PycharmProjects\\promos_inteligencia_negocio':
            with open("token.json", "w") as token:
                token.write(credentials.to_json())
        else:
            with open("token_vm.json", "w") as token:
                token.write(credentials.to_json())
    return credentials

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

    # Elimino la primer sheet
    for sheet_title in sheet_titles[:1]:
        spreadsheet.del_worksheet(spreadsheet.worksheet(sheet_title))

def move_sheet_to_last_position(spreadsheet_id, credentials):
    try:

        client = gspread.authorize(credentials)

        # Open the spreadsheet by its ID
        spreadsheet = client.open_by_key(spreadsheet_id)

        # Get the worksheet object by its title
        target_sheet_name = 'Precios oferta 2'

        # Get a list of all worksheets excluding the target sheet
        other_sheets = [sheet for sheet in spreadsheet.worksheets() if sheet.title != target_sheet_name]

        # Find the target sheet object
        target_sheet = spreadsheet.worksheet(target_sheet_name)

        # Reorder the sheets by adding the target sheet at the end
        new_sheet_order = other_sheets + [target_sheet]

        # Batch update to move the sheets
        spreadsheet.batch_update({
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sheet._properties['sheetId'],
                            "index": i
                        },
                        "fields": "index"
                    }
                } for i, sheet in enumerate(new_sheet_order)
            ]
        })

        print(f"The sheet '{target_sheet_name}' has been moved to the last position.")

    except exceptions.GoogleAuthError as e:
        print(f"Authentication error: {e}")
    except Exception as e:
        print(f"Error: {e}")










































































