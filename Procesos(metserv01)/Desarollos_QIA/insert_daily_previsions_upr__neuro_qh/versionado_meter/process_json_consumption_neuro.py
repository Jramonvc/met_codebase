#Imports
import requests
import json
from datetime import datetime,timedelta
import pymssql
import os
import time

# Global Variables
server_bd_met = 'met-esp-prod.database.windows.net'  
database_bd_met = 'Risk_MGMT_Spain'  
username_bd_met = 'sqluser'  
password_bd_met = 'cwD9KVms4Qdv4mLy'  
user_api_neuro = "met_api"
pass_api_neuro = "UqKfOCOWIq7C9"
base_path_neuro = "http://neuro360.es/api/"
uprs_meter = ['METER01']

params = {'user': user_api_neuro, 'password': pass_api_neuro}

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
        print('\nCONEXION CON LA BBDD DE MET CREADA CORRECTAMENTE')
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
        print('\nCONEXION CERRADA CON LA BBDD DE MET CORRECTAMENTE')
        connection.close()
    except pymssql.Error as e:
        print(f"Error al cerrar la conexión: {e}")
## ----------------- END BDD FUNCTIONS -----------------


## ----------------- HELPER FUNCTIONS -----------------
# Get consumos salida sin formateo de los datos, guardando la respuesta cruda de la API
def get_consumos_salida_raw(uprs, up):
    print('\nCOMIENZO DE PROCESO DE OBTENCION DE CONSUMOS CUARTO HORARIO')
    if up == 1: token = get_token_neuro()
    else: token = get_token_neuro_meter()
    
    # Medir el tiempo de inicio
    tiempo_inicio = datetime.now()

    # Crear la carpeta temp_consumos_neuro si no existe
    carpeta = 'temp_consumos_neuro'
    if not os.path.exists(carpeta):
        os.makedirs(carpeta)

    fecha_actual = datetime.now()
    fecha_inicio = fecha_actual.strftime('%Y-%m-%d')
    fecha_fin = (fecha_actual + timedelta(days=1)).strftime('%Y-%m-%d')

    for upr in uprs:
        
        cons_params = {
            'type': 'getPrevisionUprMercadoQH', 
            'token': token, 
            'uprMercado': upr, 
            'tipoPrevision': 'energia',
            'fechaInicio': fecha_inicio,
            'fechaFin': fecha_fin
        }

        # Hacer la petición a la API y obtener la respuesta
        resp = make_get_request(base_path_neuro, 'apiNeuroV2', cons_params)
        print('\tINICIO DE PROCESO PARA EL UPR: {} ENTRE LAS FECHAS: {} Y {}'.format(upr, fecha_inicio, fecha_fin))

        # Guardar la respuesta cruda en un archivo JSON
        file_name = os.path.join(carpeta, f"{upr}_raw_{fecha_inicio}-{fecha_fin}.json")
        with open(file_name, 'w') as json_file:
            json.dump(resp, json_file, indent=4)
        print(f"\tRespuesta cruda de la API guardada en '{file_name}'.")

    # Medir el tiempo de finalización
    tiempo_fin = datetime.now()
    tiempo_total = (tiempo_fin - tiempo_inicio).total_seconds() / 60  # Tiempo total en minutos
    print(f"El proceso ha finalizado en {tiempo_total:.2f} minutos.")
 
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
        print('\nTOKEN DE NEURO OBTENIDO CORRECTAMENTE')
        return token   
    except Exception as e:
       print('Error obteniendo token de Neuro {}'.format(e))
       
#Get neuro token
def get_token_neuro_meter():
    try:
        response_token = make_post_request('https://prod-142.westeurope.logic.azure.com:443/workflows/3f1fd6d49d9e4ec79cc0407634a5f55f/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=GY7_24Y-9fBEbR3dpucvHk3tbQ-Bdygcff2H4Xp7hgo')
        if response_token: token= response_token['token'] 
        print('\nTOKEN DE NEURO OBTENIDO CORRECTAMENTE')
        return token   
    except Exception as e:
       print('Error obteniendo token de Neuro {}'.format(e))
       
     
if __name__ == '__main__':
    get_consumos_salida_raw(uprs_meter, 2)  



