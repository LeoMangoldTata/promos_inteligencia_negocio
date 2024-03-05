import pandas as pd
import snowflake.connector
import json
from querys import *
from functions import *
from datetime import datetime
pd.options.mode.chained_assignment = None

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

    # 1. Precios Oferta
cursor.execute(precios_oferta)
df_precios_oferta = cursor.fetch_pandas_all()

df_precios_oferta['FIN'] = df_precios_oferta['FIN'].astype(str)
df_precios_oferta['INICIO'] = df_precios_oferta['INICIO'].astype(str)
df_precios_oferta['FECHA_ACTUALIZACION'] = datetime.today().strftime('%Y-%m-%d')

    # 2. Precios Stock Mediano dia de ayer
cursor.execute(precios_stock_mediano)
df_precios_stock_mediano = cursor.fetch_pandas_all()
df_precios_stock_mediano['FECHA_ACTUALIZACION'] = datetime.today().strftime('%Y-%m-%d')

    # 3. OPT
cursor.execute(opt)
df_opt = cursor.fetch_pandas_all()
df_opt['FECHA_ACTUALIZACION'] = datetime.today().strftime('%Y-%m-%d')

    # 4. Locales Activos Ayer

cursor.execute(locales_activos_ayer)
df_locales_activos_ayer = cursor.fetch_pandas_all()
df_locales_activos_ayer['FECHA_ACTUALIZACION'] = datetime.today().strftime('%Y-%m-%d')

    # 5. Days on Hand

    # Parte A - Articulos
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

df_days_on_hand_articulo.rename({'DAYS ON HAND':'DAYS ON HAND ARTICULO'}, axis = 1, inplace = True)

    # Parte B - Subclases
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

df_days_on_hand_subclase.rename({'DAYS ON HAND':'DAYS ON HAND SUBCLASE'}, axis = 1, inplace = True)

    # Parte C - Consolido en Articulos

df_days_on_hand_articulo = df_days_on_hand_articulo.merge(df_days_on_hand_subclase[['SUBCLASE', 'DAYS ON HAND SUBCLASE']], on = 'SUBCLASE', how = 'left')
df_days_on_hand_articulo['FECHA_ACTUALIZACION'] = datetime.today().strftime('%Y-%m-%d')

# ----------------------------------------------------------------------
# ----------------------------------------------------------------------


if __name__ == "__main__":

    credentials = get_credentials()

    # Unidad Inteligencia de Negocio
    url = 'https://docs.google.com/spreadsheets/d/1hqkPO6ych3MT3oJVFUkk9nBEvyhIcARPaVF86uFMPJ0/edit#gid=0'

    spreadsheet_id = url.split('/')[-2]

    # Create a Google Sheets API service --> esto se usa en caso de usar la funcion 2
    service = build('sheets', 'v4', credentials=credentials)

    # Create sheets and insert the DataFrames

    dataframes_dict = {
        'df_precios_oferta': 'Precios Oferta',
        'df_precios_stock_mediano': 'Precios Stock Mediano',
        'df_opt': 'OPT',
        'df_locales_activos_ayer': 'Locales Activos Ayer',
        'df_days_on_hand_articulo': 'Days on Hand - Articulos'
    }

    # Mueve la sheet 'Precios oferta 2' a la ultima posicion
    move_sheet_to_last_position(spreadsheet_id, credentials)

    for df_name, sheet_name in dataframes_dict.items():

        # Elimino la primera sheet
        delete_first_sheet(spreadsheet_id, credentials)

        df = globals()[df_name]  # Retrieve the dataframe using its name
        print(df_name, sheet_name)

        # Inserto la DataFrame en una nueva sheet, que ocupa la ultima posicion
        insert_dataframe_into_sheet(df.head(), spreadsheet_id, credentials, sheet_name)


    print(f"Lineas df_precios_oferta --> {df_precios_oferta.shape[0]}")
    print(f"Lineas df_precios_stock_mediano --> {df_precios_stock_mediano.shape[0]}")
