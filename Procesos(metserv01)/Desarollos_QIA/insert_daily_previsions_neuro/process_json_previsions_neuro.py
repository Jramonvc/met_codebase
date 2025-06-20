#Imports
import requests
import json
from datetime import datetime,timedelta
import pymssql
import os
import time
import calendar

# Global Variables
server_bd_met = 'met-esp-prod.database.windows.net'  
database_bd_met = 'Risk_MGMT_Spain'  
username_bd_met = 'sqluser'  
password_bd_met = 'cwD9KVms4Qdv4mLy'  
user_api_neuro = "met_api"
pass_api_neuro = "UqKfOCOWIq7C9"
base_path_neuro = "http://neuro360.es/api/"
uprs = ['METEC01','METER01','METENSB','METENFL','METENGC','METENHI','METENLG','METENPA','METENTF','METENCE','METENML']
params = {'user': user_api_neuro, 'password': pass_api_neuro}
cups_metec01=['ES0031406288715001LQ', 'ES0027700032086001PB', 'ES0027700032085001BT','ES0021000009935376BV','ES0022000008438131DQ','ES0026000000886409KM','ES0031405090939001TF','ES0027700208406001BX']

## ----------------- HTTP FUNCTIONS -----------------
#Get
def make_get_request(base_path, endpoint=None, params=None):
    full_url = add_to_base_path(base_path, endpoint)
    response = None
    try:
        response = requests.get(full_url, params=params)
        response.raise_for_status()  
        return response.json()
    except requests.exceptions.HTTPError as errh:
        print(f"Error HTTP: {errh}")
    except requests.exceptions.ConnectionError as errc:
        print(f"Error de conexión: {errc}")
    except requests.exceptions.Timeout as errt:
        print(f"Tiempo de espera agotado: {errt}")
    except requests.exceptions.RequestException as err:
        print(f"Error: {err}")
    return response

#Post
def make_post_request(base_path, endpoint=None, params=None):
    full_url = add_to_base_path(base_path, endpoint)
    response = None
    try:
        response = requests.post(full_url, params=params)
        response.raise_for_status()  
        return response.json()
    except requests.exceptions.HTTPError as errh:
        print(f"Error HTTP: {errh}")
    except requests.exceptions.ConnectionError as errc:
        print(f"Error de conexión: {errc}")
    except requests.exceptions.Timeout as errt:
        print(f"Tiempo de espera agotado: {errt}")
    except requests.exceptions.RequestException as err:
        print(f"Error: {err}")
    return response
## ----------------- END HTTP FUNCTIONS -----------------


## ----------------- BDD FUNCTIONS -----------------
#Create the connection
def create_connection(server, user, password, database):
    try:
        connection = pymssql.connect(
            server=server,
            user=user,
            password=password,
            database=database
        )
        print('\n01 - CONEXION CON LA BBDD DE MET CREADA CORRECTAMENTE')
        return connection
    except pymssql.Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None
    
#Execute querys with params (optional)
def execute_query(connection, query, params=None):
    try:
        with connection.cursor(as_dict=True) as cursor:
            cursor.execute(query, params)
            result = cursor.fetchall()
        return result
    except pymssql.Error as e:
        print(f"Error al ejecutar la consulta: {e}")
        return None
        
def delete_query(connection, query, params=None):
    try:
        with connection.cursor(as_dict=True) as cursor:
            cursor.execute(query, params)
            connection.commit()  # Confirmar la eliminación
        print("Consulta ejecutada con éxito.")
    except pymssql.Error as e:
        print(f"Error al ejecutar la consulta: {e}")
        return None


#Execute query
def insert_query(connection, query):
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            connection.commit()
    except pymssql.Error as e:
        print(f"Error al ejecutar la consulta: {e}")


#Close Connection
def close_connection(connection):
    try:
        print('\n01 - CONEXION CERRADA CON LA BBDD DE MET CORRECTAMENTE')
        connection.close()
    except pymssql.Error as e:
        print(f"Error al cerrar la conexión: {e}")
## ----------------- END BDD FUNCTIONS -----------------


## ----------------- HELPER FUNCTIONS -----------------
# Get previsiones sin formateo de los datos, guardando la respuesta cruda de la API
def get_previsiones_raw(cups, up):
    print('\n03 - COMIENZO DE PROCESO DE OBTENCION DE PREVISIONES POR CUPS')
    if up == 1:
        token = get_token_neuro()
    else:
        token = get_token_neuro_meter()
    
    
    # Medir el tiempo de inicio
    tiempo_inicio = datetime.now()

    # Crear la carpeta temp_consumos_neuro si no existe
    carpeta = 'temp_previsions_neuro'
    if not os.path.exists(carpeta):
        os.makedirs(carpeta)

    # Obtener la fecha actual (hoy para fechaInicio)
    fecha_actual = datetime.now()
    fecha_inicio = fecha_actual.strftime('%Y-%m-%d')

    # Calcular el último día del mes siguiente para fechaFin
    mes_siguiente = fecha_actual.month + 1 if fecha_actual.month < 12 else 1
    año_siguiente = fecha_actual.year if fecha_actual.month < 12 else fecha_actual.year + 1
    ultimo_dia_mes_siguiente = calendar.monthrange(año_siguiente, mes_siguiente)[1]
    fecha_fin = datetime(año_siguiente, mes_siguiente, ultimo_dia_mes_siguiente).strftime('%Y-%m-%d')
    
    for cup in cups:
        cons_params = {
            'type': 'getPrevisionCups', 
            'token': token, 
            'cups': cup, 
            'fechaInicio': fecha_inicio, 
            'fechaFin': fecha_fin
        }

        # Hacer la petición a la API y obtener la respuesta
        resp = make_get_request(base_path_neuro, 'apiNeuro', cons_params)
        print('\n \t INICIO DE PROCESO PARA EL CUP: {} ENTRE LAS FECHAS: {} Y {}'.format(cup, fecha_inicio, fecha_fin))

        # Guardar la respuesta cruda en un archivo JSON
        file_name = os.path.join(carpeta, f"{cup}_raw_{fecha_inicio}.json")
        with open(file_name, 'w') as json_file:
            json.dump(resp, json_file, indent=4)
        print(f"Respuesta cruda de la API guardada en '{file_name}'.")

    # Medir el tiempo de finalización
    tiempo_fin = datetime.now()
    tiempo_total = (tiempo_fin - tiempo_inicio).total_seconds() / 60  # Tiempo total en minutos
    print(f"El proceso ha finalizado en {tiempo_total:.2f} minutos.")

def get_previsiones_uda_raw(upr):
    print('\n03 - COMIENZO DE PROCESO DE OBTENCION DE CONSUMOS ENTRANTES/SALIENTES')
    token = get_token_neuro()
        
    # Medir el tiempo de inicio
    tiempo_inicio = datetime.now()

    # Crear la carpeta temp_consumos_neuro si no existe
    carpeta = 'temp_previsions_neuro'
    if not os.path.exists(carpeta):
        os.makedirs(carpeta)

    # Obtener la fecha actual (hoy para fechaInicio)
    fecha_actual = datetime.now()
    fecha_inicio = fecha_actual.strftime('%Y-%m-%d')
    
    # Calcular el último día del mes siguiente para fechaFin
    mes_siguiente = fecha_actual.month + 1 if fecha_actual.month < 12 else 1
    año_siguiente = fecha_actual.year if fecha_actual.month < 12 else fecha_actual.year + 1
    ultimo_dia_mes_siguiente = calendar.monthrange(año_siguiente, mes_siguiente)[1]
    fecha_fin = datetime(año_siguiente, mes_siguiente, ultimo_dia_mes_siguiente).strftime('%Y-%m-%d')
    
    for cup in cups_metec01:
        cons_params = {
            'type': 'getPrevisionCups', 
            'token': token, 
            'cups': cup, 
            'upr': upr,
            'fechaInicio': fecha_inicio, 
            'fechaFin': fecha_fin
        }

        # Hacer la petición a la API y obtener la respuesta
        resp = make_get_request(base_path_neuro, 'apiNeuro', cons_params)
        print('\n \t INICIO DE PROCESO PARA EL CUP: {} ENTRE LAS FECHAS: {} Y {}'.format(cup, fecha_inicio, fecha_fin))

        # Guardar la respuesta cruda en un archivo JSON
        file_name = os.path.join(carpeta, f"{cup}_raw_{fecha_inicio}.json")
        with open(file_name, 'w') as json_file:
            json.dump(resp, json_file, indent=4)
        print(f"Respuesta cruda de la API guardada en '{file_name}'.")

    # Medir el tiempo de finalización
    tiempo_fin = datetime.now()
    tiempo_total = (tiempo_fin - tiempo_inicio).total_seconds() / 60  # Tiempo total en minutos
    print(f"El proceso ha finalizado en {tiempo_total:.2f} minutos.")
    
#Get all the cups
def get_cups(connection):
    query_cups=f"""select distinct CNT_CUP_CODIGO 
  from metdb.MET_CONTRATOS 
  where CNT_POTP1 is not null 
    and (CNT_CNS_CODIGO NOT IN ('A','B') or 
	(CNT_CNS_CODIGO = 'B' And CNT_FECVEN >= DATEADD (DAY, -15, CONVERT(DATE, GETDATE()))))"""
    
    resp_cups=execute_query(connection, query_cups) 
    cups = [item['CNT_CUP_CODIGO'] for item in resp_cups]
    print('00 - CUPS obtenidos de la BDD de MET de manera exitosa')

    return cups
 
#Add to base path
def add_to_base_path(base_path,  addition=None):
    if addition:
        return base_path + addition
    return base_path

 
#Get neuro token
def get_token_neuro():
    try:
        response_token = make_post_request('https://prod-14.westeurope.logic.azure.com:443/workflows/cb25c778440e41dab90274536fa0cc22/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=T9oGNiJLn72bfp9nC3SYvjMtpC5hz_4gwdQEEnpjncY')
        if response_token: token= response_token['token'] 
        print('\n01 - TOKEN DE NEURO OBTENIDO CORRECTAMENTE')
        return token   
    except Exception as e:
       print('Error obteniendo token de Neuro {}'.format(e))
       
#Get neuro token
def get_token_neuro_meter():
    try:
        response_token = make_post_request('https://prod-142.westeurope.logic.azure.com:443/workflows/3f1fd6d49d9e4ec79cc0407634a5f55f/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=GY7_24Y-9fBEbR3dpucvHk3tbQ-Bdygcff2H4Xp7hgo')
        if response_token: token= response_token['token'] 
        print('\n01 - TOKEN DE NEURO OBTENIDO CORRECTAMENTE')
        return token   
    except Exception as e:
       print('Error obteniendo token de Neuro {}'.format(e))
       
     
if __name__ == '__main__':
    cups_meter= ['ES0027700032085001BT']
    conn=create_connection(server_bd_met, username_bd_met, password_bd_met, database_bd_met) 
    cups = get_cups(conn)
    get_previsiones_raw(cups, 1)  
    get_previsiones_raw(cups_meter, 2)
    get_previsiones_uda_raw('METER01')



