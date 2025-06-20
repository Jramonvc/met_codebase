#Imports
import logging
import os
import requests
import json
from datetime import datetime,timedelta
import pymssql

# Definición de variables globales
server_bd_met = "met-esp-prod.database.windows.net"
database_bd_met = "Risk_MGMT_Spain"
username_bd_met = "sqluser"
password_bd_met = "cwD9KVms4Qdv4mLy"
user_api_neuro = "met_api"
pass_api_neuro = "UqKfOCOWIq7C9"
base_path_neuro = "http://neuro360.es/api/"

# Obtener la fecha actual y la fecha del día anterior
fact = datetime.now()
factf = fact.strftime('%Y-%m-%d')
fant = fact - timedelta(days=1)
fantf = fant.strftime('%Y-%m-%d')

#Aux Vars
uprs = ['METEC01','METER01','METENSB','METENFL','METENGC','METENHI','METENLG','METENPA','METENTF','METENCE','METENML']

## MAIN - 'neuro_api_connection'
def main():
    try:
        conn=create_connection(server_bd_met, username_bd_met, password_bd_met, database_bd_met) #Conexion con bdd met
        delete_previous_data(conn, "METDB.MET_AGREGUPR", "AGU_FECHA")
        token_neuro = get_token_neuro() #Obtencion token neuro
        get_agregada_upr(uprs, conn, token_neuro) #Insert Agregada - MET_AGREGUPR
    except Exception as e:
        print(f"Error in main function: {e}")
    finally:
        close_connection(conn)

## ----------------- API FUNCTIONS -----------------
#Get neuro token
def get_token_neuro():
    try:
        response_token = make_post_request('https://prod-14.westeurope.logic.azure.com:443/workflows/cb25c778440e41dab90274536fa0cc22/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=T9oGNiJLn72bfp9nC3SYvjMtpC5hz_4gwdQEEnpjncY')
        if response_token: token= response_token['token'] 
        print('\n01 - TOKEN DE NEURO OBTENIDO CORRECTAMENTE')
        return token   
    except Exception as e:
       print('Error obteniendo token de Neuro {}'.format(e))
    
#Get previsiones
def get_previsiones(uprs,conn,token):
    print('\n02 - COMIENZO DE PROCESO DE OBTENCION E INSERCION DE PREVISIONES')
    insert_queries = []
    api_responses  = []  

    try:
        for upr in uprs: #Llamada recursiva a la API
            prevision_upr_params = {'type': 'getPrevisionUpr', 'token': token, 'upr' : upr, 'fechaInicio' : fantf, 'fechaFin' : fantf}
            resp=make_get_request(base_path_neuro,'apiNeuro',prevision_upr_params)
            if resp:resp['upr'] = upr
            api_responses.append(resp)
            
        valid_responses = [response for response in api_responses if response['resultado'] == 200 and response['datos']] #Limpieza de Datos
        for response in valid_responses:
            upr = response['upr'] 
            for date, hours in response['datos'].items():
                for hour, prevision in hours.items():
                    ppw_fecha = datetime.strptime(date, '%Y-%m-%d').date()
                    ppw_hora = int(hour)
                    ppw_prevision = float(prevision)
                    ppw_feccomu = datetime.now().date()
                    query = f"""
                    INSERT INTO METDB.MET_PREVPOWER (PPW_FECCOMU, PPW_UP, PPW_FECHA, PPW_HORA, PPW_PREVISION)
                    VALUES ('{ppw_feccomu}', '{upr}', '{ppw_fecha}', {ppw_hora}, {ppw_prevision})
                    """
                    insert_queries.append(query)
        print('\t{} previsiones obtenidas'.format(len(insert_queries)))
        for data in insert_queries:
            insert_query(conn, data)
       
    except Exception as e:
       print('Error en el tratamiento de las previsiones {}'.format(e))
    
#Get consumos salida
def get_consumos_salida( cups,conn, token):
    print('\n03 - COMIENZO DE PROCESO DE OBTENCION DE CONSUMOS ENTRANTES/SALIENTES')
    for cup in cups:
        cons_params = {'type': 'getLecturasSalida', 'token': token, 'cups': cup, 'fechaInicio': factf, 'fechaFin': factf}
        resp = make_get_request(base_path_neuro, 'apiNeuro', cons_params)

        if 'datos' not in resp or not resp['datos']:continue
        cup_codigo = list(resp['datos'].keys())[0]
        if not resp['datos'][cup_codigo]:continue

        fecha_api = list(resp['datos'][cup_codigo].keys())[0]
        procedencia_entrante = resp['datos'][cup_codigo][fecha_api]['Entrante'].get('Procedencia', 'Sin datos')
        procedencia_saliente = resp['datos'][cup_codigo][fecha_api]['Saliente'].get('Procedencia', 'Sin datos')

        lecturas_entrantes = resp['datos'][cup_codigo][fecha_api]['Entrante'].get('Lecturas', {})
        lecturas_salientes = resp['datos'][cup_codigo][fecha_api]['Saliente'].get('Lecturas', {})

        # Procesar lecturas entrantes
        if not isinstance(lecturas_entrantes, dict) or not lecturas_entrantes:
            print(f"No hay lecturas entrantes válidas para cup: {cup}")
        else:
            for hora, lectura in lecturas_entrantes.items():
                if lectura != 0:
                    csp_tipo = 'E'
                    csp_hora = int(hora)
                    csp_lecturas = float(lectura)
                    query = f"""
                        INSERT INTO METDB.MET_CONSUPOWER (CSP_CUP_CODIGO, CSP_FECHA, CSP_PROCED, CSP_TIPO, CSP_HORA, CSP_LECTURAS)
                        VALUES ('{cup_codigo}', '{fecha_api}', '{procedencia_entrante}', '{csp_tipo}', {csp_hora}, {csp_lecturas})
                    """
                    insert_query(conn, query)

        # Procesar lecturas salientes
        if not isinstance(lecturas_salientes, dict) or not lecturas_salientes:
            print(f"No hay lecturas salientes válidas para cup: {cup}")
        else:
            for hora, lectura in lecturas_salientes.items():
                if lectura != 0:
                    csp_tipo = 'S'
                    csp_hora = int(hora)
                    csp_lecturas = float(lectura)
                    query = f"""
                        INSERT INTO METDB.MET_CONSUPOWER (CSP_CUP_CODIGO, CSP_FECHA, CSP_PROCED, CSP_TIPO, CSP_HORA, CSP_LECTURAS)
                        VALUES ('{cup_codigo}', '{fecha_api}', '{procedencia_saliente}', '{csp_tipo}', {csp_hora}, {csp_lecturas})
                    """
                    insert_query(conn, query)
                    
def get_agregada_upr(uprs, conn, token):
    print('\n03 - COMIENZO DE PROCESO DE OBTENCION E INSERCION DE AGREGADA')
    insert_queries = []
    api_responses  = []  
    try:
        for upr in uprs: #Llamada recursiva a la API
            agregada_upr_params = {'type': 'getAgregadaUpr', 'token': token, 'upr' : upr, 'fechaInicio' : fantf, 'fechaFin' : fantf}
            resp=make_get_request(base_path_neuro,'apiNeuro',agregada_upr_params)
            if resp and resp['resultado'] == 200 and resp['datos']:
                    resp['upr'] = upr 
                    api_responses.append(resp)
                
                    
            for response in api_responses:
                AGU_UP = response['upr']
                for date, hours in response['datos']['agregada'].items():
                    for hour, prevision in hours.items():
                        AGU_FECHA = datetime.strptime(date, '%Y-%m-%d').date()
                        AGU_HORA = int(hour)
                        AGU_LECTURA = float(prevision)
                    
                        query = f"""
                            INSERT INTO METDB.MET_AGREGUPR (AGU_FECHA, AGU_HORA, AGU_LECTURA, AGU_UP)
                            VALUES ('{AGU_FECHA}', '{AGU_HORA}', {AGU_LECTURA}, '{AGU_UP}')
                        """
                        insert_queries.append(query)
            
            print('\t{} previsiones obtenidas'.format(len(insert_queries)))
            for data in insert_queries:
                insert_query(conn, data)  
    except Exception as err:
        print("Error obteniendo agregada: {}".format(err))

## ----------------- END API FUNCTIONS -----------------



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

#Delete previous data
def delete_previous_data(connection, table_name, column_name):
    try:
        factf = datetime.now().strftime('%Y-%m-%d')
        fantf = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

        query = f"""
        DELETE FROM {table_name}
        WHERE {column_name} IN ('{factf}', '{fantf}')
        """
        
        with connection.cursor() as cursor:
            cursor.execute(query)
            connection.commit()
        
        print(f"Datos eliminados en {table_name} para las fechas: {factf}, {fantf}")
    
    except pymssql.Error as e:
        print(f"Error al eliminar los datos de {table_name}: {e}")



## ----------------- HELPER FUNCTIONS -----------------
#Add to base path
def add_to_base_path(base_path,  addition=None):
    if addition:
        return base_path + addition
    return base_path


## ----------------- END HELPER FUNCTIONS -----------------
if __name__ == '__main__':
    main()