import pandas as pd
import snowflake.connector
import json
from querys import *
from functions import *

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

# Fechas indicadas por Juli para tomar los precios

url_fechas = 'https://docs.google.com/spreadsheets/d/1JnayuiljvaOebik4gKqNgxSLD6RO2AS8euG7apQykW8/export?format=csv'
fechas = pd.read_csv(url_fechas)

    # 1. Precios Oferta
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
df_precios_oferta['FIN'] = df_precios_oferta['FIN'].astype(str)
df_precios_oferta['INICIO'] = df_precios_oferta['INICIO'].astype(str)

    # 2. Precios Stock Mediano dia de ayer
cursor.execute(precios_stock_mediano)
df_precios_stock_mediano = cursor.fetch_pandas_all()

    # 3. OPT
cursor.execute(opt)
df_opt = cursor.fetch_pandas_all()

    # 4. Locales Activos Ayer

url_locales = 'https://docs.google.com/spreadsheets/d/1WmPELtkFBGWACl1o_Y1njDbAoxSVR3iI-Gx7hx9yrno/export?format=csv'
locales = pd.read_csv(url_locales)
cercania = locales[locales['FORMATO'] == 'CERCANIA']

cursor.execute(locales_activos_ayer.format(cercania_snow = tuple(cercania['LOCAL'])))
df_locales_activos_ayer = cursor.fetch_pandas_all()

    # 5. Days on Hand - Articulos
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

    # 6. Days on Hand - Subclases
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