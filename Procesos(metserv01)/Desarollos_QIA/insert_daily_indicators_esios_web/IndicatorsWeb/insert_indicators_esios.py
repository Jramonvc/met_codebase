#Imports
import logging
import os
import requests
import json
from datetime import datetime,timedelta
import pymssql
from datetime import datetime, timedelta, timezone
import pytz
import esios
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError


# Definición de variables globales
server_bd_met = "met-esp-prod.database.windows.net"
database_bd_met = "Risk_MGMT_Spain"
username_bd_met = "sqluser"
password_bd_met = "cwD9KVms4Qdv4mLy"
base_path_esios = "http://api.esios.ree.es/"
token_esios = "28c83fff5780b353ba90dd62b4ec8db6b39eb9b7f7d08730ddfd36a478d95704"
header = None

#Fechas
now = datetime.now()
actual_date = now.strftime('%Y-%m-%d')
two_days_ago = now - timedelta(days=2)
one_day_plus = now + timedelta(days=1)
dayp1 = two_days_ago.strftime('%Y-%m-%d')
dayp2 = one_day_plus.strftime('%Y-%m-%d')

# Aux Vars
tname = 'MET_DATESIOS'
params_geoid = {'peninsula' : '8741','alemania' : '8826','francia' : '2'}
params_indicat = {'precio_mercado' : '600','rrtt_precio' : '708','rrtt_vol' : '704','desvios_subir' : '686','desvios_bajar' : '687',}
params_magnit = {'energia' : '13','precio' : '23'}


def main():
    os.environ['ESIOS_API_KEY'] = token_esios
    try:
        #Obtencion de variables y conexion con BBDD
        conn=create_connection(server_bd_met, username_bd_met, password_bd_met, database_bd_met) 
        engine=crear_conexion_bd()
        # Proceso de borrado de indicadores
        delete_previous_indicators(conn)

        # Ejecución de funciones principales
        get_pr_fr_al_values(engine)
        get_rrtt_price(engine)
        get_rrtt_vol(engine)
        get_desvios_bajar(engine)
        get_desvios_subir(engine)
         
        bs3_prec_bajar(engine)
        bs3_prec_subir(engine)
        bs3_subir(engine)
        bs3_bajar(engine)
        get_prevs_indicators(engine)

        #Respuesta Main
        response_message = "Indicadores eliminados y funciones ejecutadas correctamente" 


    except Exception as e:
        print(f"Error in main function: {e}")
    finally:
        close_connection(conn)
        
def bs3_prec_bajar(conn):
    print("Comienzo del proceso de obtención de Precio reserva de regulación secundaria a bajar")

    try:
        # Cliente ESIOS
        client = esios.ESIOSClient()
        endpoint = client.endpoint(name='indicators')
        indicator_rrt_pr = endpoint.select(id=634)

        today = datetime.now().strftime('%Y-%m-%d')
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        
        print(f"Obteniendo datos desde {today} hasta {tomorrow}")

        # Obtener datos históricos
        df_rrtt_pr = indicator_rrt_pr.historical(start=today, end=tomorrow, time_trunc="fifteen_minutes")
        
        if df_rrtt_pr.empty:
            print("No hay datos disponibles para el indicador 634 en el rango de fechas indicado.")
            return None

        # Convertir el índice del DataFrame a una columna de fecha sin zona horaria
        df_rrtt_pr.index.name = 'timestamp'  # Asigna el nombre antes de resetear el índice
        df_rrtt_pr = df_rrtt_pr.reset_index()  # Convierte el índice en una columna
        df_rrtt_pr['timestamp'] = pd.to_datetime(df_rrtt_pr['timestamp']).dt.tz_localize(None)

        # Agregar columnas necesarias para la inserción en la base de datos
        df_rrtt_pr['DES_IND_ID'] = 634
        df_rrtt_pr['DES_MAG_ID'] = 23
        df_rrtt_pr['DES_FECPUBLIC'] = now.strftime('%Y-%m-%d %H:%M:%S')
        df_rrtt_pr['DES_FECHA'] = df_rrtt_pr['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_rrtt_pr['DES_FECUTC'] = df_rrtt_pr['DES_FECHA']  # Suponiendo que está en UTC
        df_rrtt_pr['DES_FECTZ'] = df_rrtt_pr['DES_FECHA']  # Mantener mismo valor si no hay conversión

        # Seleccionar solo las columnas relevantes
        df_final = df_rrtt_pr[['DES_IND_ID', 'DES_MAG_ID', 'geo_id', 'DES_FECPUBLIC', 'DES_FECHA', 'DES_FECUTC', 'DES_FECTZ', '634']]
        df_final = df_final.rename(columns={'geo_id': 'DES_GEO_ID', '634': 'DES_VALOR'})  # Renombrar columnas

        # Insertar en la base de datos
        insert_dataframe_to_db(df_final, "MET_DATESIOS", conn)

    except Exception as e:
        print(f"Error en bs3_prec_bajar: {e}")
      
def bs3_prec_subir(conn):
    print("Comienzo del proceso de obtención de Precio reserva de regulación secundaria a subir")
    try:
        # Cliente ESIOS
        client = esios.ESIOSClient()
        endpoint = client.endpoint(name='indicators')
        indicator_rrt_pr = endpoint.select(id=2130)

        today = datetime.now().strftime('%Y-%m-%d')
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

        print(f"Obteniendo datos desde {today} hasta {tomorrow}")

        # Obtener datos históricos
        df_rrtt_pr = indicator_rrt_pr.historical(start=today, end=tomorrow, time_trunc="fifteen_minutes")
        if df_rrtt_pr.empty:
            print("No hay datos disponibles para el indicador 2130 en el rango de fechas indicado.")
            return None

        # Convertir el índice del DataFrame a una columna de fecha sin zona horaria
        df_rrtt_pr.index.name = 'timestamp'  # Asigna el nombre antes de resetear el índice
        df_rrtt_pr = df_rrtt_pr.reset_index()  # Convierte el índice en una columna
        df_rrtt_pr['timestamp'] = pd.to_datetime(df_rrtt_pr['timestamp']).dt.tz_localize(None)

        # Agregar columnas necesarias para la inserción en la base de datos
        df_rrtt_pr['DES_IND_ID'] = 2130
        df_rrtt_pr['DES_MAG_ID'] = 23
        df_rrtt_pr['DES_FECPUBLIC'] = now.strftime('%Y-%m-%d %H:%M:%S')
        df_rrtt_pr['DES_FECHA'] = df_rrtt_pr['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_rrtt_pr['DES_FECUTC'] = df_rrtt_pr['DES_FECHA']  # Suponiendo que está en UTC
        df_rrtt_pr['DES_FECTZ'] = df_rrtt_pr['DES_FECHA']  # Mantener mismo valor si no hay conversión

        # Seleccionar solo las columnas relevantes
        df_final = df_rrtt_pr[['DES_IND_ID', 'DES_MAG_ID', 'geo_id', 'DES_FECPUBLIC', 'DES_FECHA', 'DES_FECUTC', 'DES_FECTZ', '2130']]
        df_final = df_final.rename(columns={'geo_id': 'DES_GEO_ID', '2130': 'DES_VALOR'})  # Renombrar columnas

        # Insertar en la base de datos
        insert_dataframe_to_db(df_final, "MET_DATESIOS", conn)

    except Exception as e:
        print(f"Error en bs3_prec_subir: {e}")

def bs3_bajar(conn):
    print("Comienzo del proceso de obtencion de  Asignación reserva de regulación secundaria a bajar")
    try:
        # Cliente ESIOS
        client = esios.ESIOSClient()
        endpoint = client.endpoint(name='indicators')
        indicator_rrt_pr = endpoint.select(id=633)

        today = datetime.now().strftime('%Y-%m-%d')
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

        print(f"Obteniendo datos desde {today} hasta {tomorrow}")

        # Obtener datos históricos
        df_rrtt_pr = indicator_rrt_pr.historical(start=today, end=tomorrow, time_trunc="fifteen_minutes")

        if df_rrtt_pr.empty:
            print("No hay datos disponibles para el indicador 633 en el rango de fechas indicado.")
            return None

        # Convertir el índice del DataFrame a una columna de fecha sin zona horaria
        df_rrtt_pr.index.name = 'timestamp'  # Asigna el nombre antes de resetear el índice
        df_rrtt_pr = df_rrtt_pr.reset_index()  # Convierte el índice en una columna
        df_rrtt_pr['timestamp'] = pd.to_datetime(df_rrtt_pr['timestamp']).dt.tz_localize(None)

        # Agregar columnas necesarias para la inserción en la base de datos
        df_rrtt_pr['DES_IND_ID'] = 633
        df_rrtt_pr['DES_MAG_ID'] = 23
        df_rrtt_pr['DES_FECPUBLIC'] = now.strftime('%Y-%m-%d %H:%M:%S')
        df_rrtt_pr['DES_FECHA'] = df_rrtt_pr['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_rrtt_pr['DES_FECUTC'] = df_rrtt_pr['DES_FECHA']  # Suponiendo que está en UTC
        df_rrtt_pr['DES_FECTZ'] = df_rrtt_pr['DES_FECHA']  # Mantener mismo valor si no hay conversión

        # Seleccionar solo las columnas relevantes
        df_final = df_rrtt_pr[['DES_IND_ID', 'DES_MAG_ID', 'geo_id', 'DES_FECPUBLIC', 'DES_FECHA', 'DES_FECUTC', 'DES_FECTZ', '633']]
        df_final = df_final.rename(columns={'geo_id': 'DES_GEO_ID', '633': 'DES_VALOR'})  # Renombrar columnas

        # Insertar en la base de datos
        insert_dataframe_to_db(df_final, "MET_DATESIOS", conn)

    except Exception as e:
        print(f"Error en bs3_bajar: {e}")

def bs3_subir(conn):
    print("Comienzo del proceso de obtencion de  Asignación reserva de regulación secundaria a subir")
    try:
        # Cliente ESIOS
        client = esios.ESIOSClient()
        endpoint = client.endpoint(name='indicators')
        indicator_rrt_pr = endpoint.select(id=632)

        today = datetime.now().strftime('%Y-%m-%d')
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

        print(f"Obteniendo datos desde {today} hasta {tomorrow}")

        # Obtener datos históricos
        df_rrtt_pr = indicator_rrt_pr.historical(start=today, end=tomorrow, time_trunc="fifteen_minutes")
        if df_rrtt_pr.empty:
            print("No hay datos disponibles para el indicador 632 en el rango de fechas indicado.")
            return None

        # Convertir el índice del DataFrame a una columna de fecha sin zona horaria
        df_rrtt_pr.index.name = 'timestamp'  # Asigna el nombre antes de resetear el índice
        df_rrtt_pr = df_rrtt_pr.reset_index()  # Convierte el índice en una columna
        df_rrtt_pr['timestamp'] = pd.to_datetime(df_rrtt_pr['timestamp']).dt.tz_localize(None)

        # Agregar columnas necesarias para la inserción en la base de datos
        df_rrtt_pr['DES_IND_ID'] = 632
        df_rrtt_pr['DES_MAG_ID'] = 23
        df_rrtt_pr['DES_FECPUBLIC'] = now.strftime('%Y-%m-%d %H:%M:%S')
        df_rrtt_pr['DES_FECHA'] = df_rrtt_pr['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_rrtt_pr['DES_FECUTC'] = df_rrtt_pr['DES_FECHA']  # Suponiendo que está en UTC
        df_rrtt_pr['DES_FECTZ'] = df_rrtt_pr['DES_FECHA']  # Mantener mismo valor si no hay conversión

        # Seleccionar solo las columnas relevantes
        df_final = df_rrtt_pr[['DES_IND_ID', 'DES_MAG_ID', 'geo_id', 'DES_FECPUBLIC', 'DES_FECHA', 'DES_FECUTC', 'DES_FECTZ', '632']]
        df_final = df_final.rename(columns={'geo_id': 'DES_GEO_ID', '632': 'DES_VALOR'})  # Renombrar columnas

        # Insertar en la base de datos
        insert_dataframe_to_db(df_final, "MET_DATESIOS", conn)

    except Exception as e:
        print(f"Error en bs3_bajar: {e}")


def get_prevs_indicators(conn):
    print('Comienzo del proceso de obtención de previsiones')
    indicators_prevs_id = ['460', '541', '542', '543', '4', '32', '28']
    now = datetime.now()
    day_after_week = now + timedelta(days=7)
    dayp1 = now.strftime('%Y-%m-%d')
    dayp2 = day_after_week.strftime('%Y-%m-%d')
    client = esios.ESIOSClient()
    endpoint = client.endpoint(name='indicators')
    
    for indicator_prev in indicators_prevs_id:
        indicator = endpoint.select(id=indicator_prev)
        df_indicator_prev = indicator.historical(start=dayp1, end=dayp2, time_trunc="fifteen_minutes")
        if (len(df_indicator_prev) < 5): print(f"No se han encontrado datos para la prevision {indicator_prev}")
        filtered_df = df_indicator_prev
        # Convertir el índice del DataFrame a una columna de fecha sin zona horaria
        filtered_df.index.name = 'timestamp'  # Asigna el nombre antes de resetear el índice
        filtered_df = filtered_df.reset_index()  # Convierte el índice en una columna
        filtered_df['timestamp'] = pd.to_datetime(filtered_df['timestamp']).dt.tz_localize(None)

        # Agregar columnas necesarias para la inserción en la base de datos
        filtered_df['DES_IND_ID'] = indicator_prev
        filtered_df['DES_MAG_ID'] = 23
        filtered_df['DES_FECPUBLIC'] = now.strftime('%Y-%m-%d %H:%M:%S')
        filtered_df['DES_FECHA'] = filtered_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        filtered_df['DES_FECUTC'] = filtered_df['DES_FECHA']  # Suponiendo que está en UTC
        filtered_df['DES_FECTZ'] = filtered_df['DES_FECHA']  # Mantener mismo valor si no hay conversión
        
        # Seleccionar solo las columnas relevantes
        df_final = filtered_df[['DES_IND_ID', 'DES_MAG_ID', 'geo_id', 'DES_FECPUBLIC', 'DES_FECHA', 'DES_FECUTC', 'DES_FECTZ', indicator_prev]]
        df_final = df_final.rename(columns={'geo_id': 'DES_GEO_ID', indicator_prev: 'DES_VALOR'})  # Renombrar columnas
        insert_dataframe_to_db(df_final, "MET_DATESIOS", conn)

        print(f"Datos insertados para el indicador {indicator_prev}")


def delete_previous_indicators(conn):
    print('Comienzo del proceso de borrado de datos de indicadores')
    try:
        now = datetime.now()
        two_pdays= now - timedelta(days=2)
        one_pdays = now - timedelta(days=1)
        daym1 = two_pdays.strftime('%Y-%m-%d')
        daym2 = one_pdays.strftime('%Y-%m-%d')
        today = datetime.now().strftime('%Y-%m-%d')
        one_month_ago = now - timedelta(days=30)
        yesterday = now - timedelta(days=1)
        # Formatear las fechas como cadenas
        start_date = one_month_ago.strftime('%Y-%m-%d')
        end_date = yesterday.strftime('%Y-%m-%d')
        print('Borrando datos de indicadores de {}'.format(dayp2))
        query_dayp1 = """DELETE FROM [METDB].[MET_DATESIOS] WHERE CAST(DES_FECHA AS DATE) = '{}'""".format(dayp2)
        execute_query(conn, query_dayp1)
        print('Borrando datos de indicadores de desvios de {} y {}'.format(daym1,daym2 ))
        query_desvios = """
            DELETE FROM [METDB].[MET_DATESIOS]
            WHERE (DES_IND_ID = '687' OR DES_IND_ID = '686') 
            AND CAST(DES_FECHA AS DATE) BETWEEN '{}' AND '{}'
        """.format(start_date, end_date)        
        execute_query(conn, query_desvios)
        print('Borrando datos de previsiones de {} + 7 Dias'.format(today))
        delete_query = f'''
        DELETE FROM METDB.MET_DATESIOS
        WHERE CAST(DES_FECHA AS DATE) >= '{today}'
        AND DES_FECHA >= '{today} 00:00:00'
        AND DES_IND_ID IN (460, 541, 542, 543, 4, 32, 28, 632, 633, 634, 2130)
        '''
        execute_query(conn, delete_query)
        tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
        print('Borrando datos de BS3 para D y D+1'.format(today))
        delete_query_d = f'''
        DELETE FROM METDB.MET_DATESIOS
        WHERE CAST(DES_FECHA AS DATE) IN ('{today}', '{tomorrow}')
        AND DES_IND_ID IN (632, 633, 634, 2130);
        '''
        execute_query(conn, delete_query_d)

   
    except Exception as e:
        print(f"Error borrando indicadores: {e}")
        return func.HttpResponse(f"Error borrando indicadores: {e}", status_code=500)


def get_desvios_bajar(conn):
    print("Comienzo del proceso de obtención de desvíos a bajar para la península")

    try:
        # Cliente ESIOS
        client = esios.ESIOSClient()
        endpoint = client.endpoint(name='indicators')
        indicator_rrt_pr = endpoint.select(id=687)

        # Fechas para obtener datos (último mes hasta ayer)
        now = datetime.now()
        one_month_ago = now - timedelta(days=30)
        yesterday = now - timedelta(days=1)
        start_date = one_month_ago.strftime('%Y-%m-%d')
        end_date = yesterday.strftime('%Y-%m-%d')

        print(f"Obteniendo datos desde {start_date} hasta {end_date}")

        # Obtener datos históricos
        df_rrtt_pr = indicator_rrt_pr.historical(start=start_date, end=end_date, time_trunc="fifteen_minutes")

        if df_rrtt_pr.empty:
            print("No hay datos disponibles para el indicador 687 en el rango de fechas indicado.")
            return None

        # Convertir el índice del DataFrame a una columna de fecha sin zona horaria
        df_rrtt_pr.index.name = 'timestamp'  # Asigna el nombre antes de resetear el índice
        df_rrtt_pr = df_rrtt_pr.reset_index()  # Convierte el índice en una columna
        df_rrtt_pr['timestamp'] = pd.to_datetime(df_rrtt_pr['timestamp']).dt.tz_localize(None)

        # Agregar columnas necesarias para la inserción en la base de datos
        df_rrtt_pr['DES_IND_ID'] = 687
        df_rrtt_pr['DES_MAG_ID'] = 23
        df_rrtt_pr['DES_FECPUBLIC'] = now.strftime('%Y-%m-%d %H:%M:%S')
        df_rrtt_pr['DES_FECHA'] = df_rrtt_pr['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_rrtt_pr['DES_FECUTC'] = df_rrtt_pr['DES_FECHA']  # Suponiendo que está en UTC
        df_rrtt_pr['DES_FECTZ'] = df_rrtt_pr['DES_FECHA']  # Mantener mismo valor si no hay conversión

        # Seleccionar solo las columnas relevantes
        df_final = df_rrtt_pr[['DES_IND_ID', 'DES_MAG_ID', 'geo_id', 'DES_FECPUBLIC', 'DES_FECHA', 'DES_FECUTC', 'DES_FECTZ', '687']]
        df_final = df_final.rename(columns={'geo_id': 'DES_GEO_ID', '687': 'DES_VALOR'})  # Renombrar columnas

        # Insertar en la base de datos
        insert_dataframe_to_db(df_final, "MET_DATESIOS", conn)

    except Exception as e:
        print(f"Error en get_desvios_bajar: {e}")


import pandas as pd
import logging
import pytz
from datetime import datetime, timedelta
from sqlalchemy.exc import IntegrityError
import esios

def get_desvios_subir(conn):
    print("Comienzo del proceso de obtención de desvíos a subir para la península")

    try:
        # Cliente ESIOS
        client = esios.ESIOSClient()
        endpoint = client.endpoint(name='indicators')
        indicator_rrt_pr = endpoint.select(id=686)

        # Fechas para obtener datos (último mes hasta ayer)
        now = datetime.now()
        one_month_ago = now - timedelta(days=30)
        yesterday = now - timedelta(days=1)
        start_date = one_month_ago.strftime('%Y-%m-%d')
        end_date = yesterday.strftime('%Y-%m-%d')

        print(f"Obteniendo datos desde {start_date} hasta {end_date}")

        # Obtener datos históricos
        df_rrtt_pr = indicator_rrt_pr.historical(start=start_date, end=end_date, time_trunc="fifteen_minutes")

        if df_rrtt_pr.empty:
            print("No hay datos disponibles para el indicador 686 en el rango de fechas indicado.")
            return None

        # Convertir el índice del DataFrame a una columna de fecha sin zona horaria
        df_rrtt_pr.index.name = 'timestamp'  # Asigna el nombre antes de resetear el índice
        df_rrtt_pr = df_rrtt_pr.reset_index()  # Convierte el índice en una columna
        df_rrtt_pr['timestamp'] = pd.to_datetime(df_rrtt_pr['timestamp']).dt.tz_localize(None)

        # Agregar columnas necesarias para la inserción en la base de datos
        df_rrtt_pr['DES_IND_ID'] = 686
        df_rrtt_pr['DES_MAG_ID'] = 23
        df_rrtt_pr['DES_FECPUBLIC'] = now.strftime('%Y-%m-%d %H:%M:%S')
        df_rrtt_pr['DES_FECHA'] = df_rrtt_pr['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_rrtt_pr['DES_FECUTC'] = df_rrtt_pr['DES_FECHA']  # Suponiendo que está en UTC
        df_rrtt_pr['DES_FECTZ'] = df_rrtt_pr['DES_FECHA']  # Mantener mismo valor si no hay conversión

        # Seleccionar solo las columnas relevantes
        df_final = df_rrtt_pr[['DES_IND_ID', 'DES_MAG_ID', 'geo_id', 'DES_FECPUBLIC', 'DES_FECHA', 'DES_FECUTC', 'DES_FECTZ', '686']]
        df_final = df_final.rename(columns={'geo_id': 'DES_GEO_ID', '686': 'DES_VALOR'})  # Renombrar columnas

        # Insertar en la base de datos
        insert_dataframe_to_db(df_final, "MET_DATESIOS", conn)

    except Exception as e:
        print(f"Error en get_desvios_subir: {e}")


def get_rrtt_price(conn):
    print("Comienzo del proceso de obtención de RRTT price para la península")

    try:
        # Cliente ESIOS
        client = esios.ESIOSClient()
        endpoint = client.endpoint(name='indicators')
        indicator_rrt_pr = endpoint.select(id=708)

        # Fechas para obtener datos
        now = datetime.now()
        next_day = now + timedelta(days=1)
        day_after_next = now + timedelta(days=2)
        dayp1 = next_day.strftime('%Y-%m-%d')
        dayp2 = day_after_next.strftime('%Y-%m-%d')

        # Obtener datos históricos
        df_rrtt_pr = indicator_rrt_pr.historical(start=dayp1, end=dayp2, time_trunc="fifteen_minutes")

        if df_rrtt_pr.empty:
            print("No hay datos disponibles para el indicador 708 en el rango de fechas indicado.")
            return None

        # Convertir el índice del DataFrame a una columna de fecha sin zona horaria
        df_rrtt_pr.index.name = 'timestamp'  # Asigna el nombre antes de resetear el índice
        df_rrtt_pr = df_rrtt_pr.reset_index()  # Convierte el índice en una columna
        df_rrtt_pr['timestamp'] = pd.to_datetime(df_rrtt_pr['timestamp']).dt.tz_localize(None)

        # Agregar columnas necesarias para la inserción en la base de datos
        df_rrtt_pr['DES_IND_ID'] = 708
        df_rrtt_pr['DES_MAG_ID'] = 23
        df_rrtt_pr['DES_FECPUBLIC'] = now.strftime('%Y-%m-%d %H:%M:%S')
        df_rrtt_pr['DES_FECHA'] = df_rrtt_pr['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_rrtt_pr['DES_FECUTC'] = df_rrtt_pr['DES_FECHA']  # Suponiendo que está en UTC
        df_rrtt_pr['DES_FECTZ'] = df_rrtt_pr['DES_FECHA']  # Mantener mismo valor si no hay conversión

        # Seleccionar solo las columnas relevantes
        df_final = df_rrtt_pr[['DES_IND_ID', 'DES_MAG_ID', 'geo_id', 'DES_FECPUBLIC', 'DES_FECHA', 'DES_FECUTC', 'DES_FECTZ', '708']]
        df_final = df_final.rename(columns={'geo_id': 'DES_GEO_ID', '708': 'DES_VALOR'})  # Renombrar columnas

        # Insertar en la base de datos
        insert_dataframe_to_db(df_final, "MET_DATESIOS", conn)

    except Exception as e:
        print(f"Error en get_rrtt_price: {e}")


def get_rrtt_vol(conn):
    print("Comienzo del proceso de obtención de RRTT vol para la península")

    try:
        # Cliente ESIOS
        client = esios.ESIOSClient()
        endpoint = client.endpoint(name='indicators')
        indicator_rrt_pr = endpoint.select(id=704)

        # Fechas para obtener datos
        now = datetime.now()
        next_day = now + timedelta(days=1)
        day_after_next = now + timedelta(days=2)
        dayp1 = next_day.strftime('%Y-%m-%d')
        dayp2 = day_after_next.strftime('%Y-%m-%d')

        # Obtener datos históricos
        df_rrtt_pr = indicator_rrt_pr.historical(start=dayp1, end=dayp2, time_trunc="fifteen_minutes")

        if df_rrtt_pr.empty:
            print("No hay datos disponibles para el indicador 704 en el rango de fechas indicado.")
            return None

        # Convertir el índice del DataFrame a una columna de fecha sin zona horaria
        df_rrtt_pr.index.name = 'timestamp'  # Asigna el nombre antes de resetear el índice
        df_rrtt_pr = df_rrtt_pr.reset_index()  # Convierte el índice en una columna
        df_rrtt_pr['timestamp'] = pd.to_datetime(df_rrtt_pr['timestamp']).dt.tz_localize(None)

        # Agregar columnas necesarias para la inserción en la base de datos
        df_rrtt_pr['DES_IND_ID'] = 704
        df_rrtt_pr['DES_MAG_ID'] = 23
        df_rrtt_pr['DES_FECPUBLIC'] = now.strftime('%Y-%m-%d %H:%M:%S')
        df_rrtt_pr['DES_FECHA'] = df_rrtt_pr['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_rrtt_pr['DES_FECUTC'] = df_rrtt_pr['DES_FECHA']  # Suponiendo que está en UTC
        df_rrtt_pr['DES_FECTZ'] = df_rrtt_pr['DES_FECHA']  # Mantener mismo valor si no hay conversión

        # Seleccionar solo las columnas relevantes
        df_final = df_rrtt_pr[['DES_IND_ID', 'DES_MAG_ID', 'geo_id', 'DES_FECPUBLIC', 'DES_FECHA', 'DES_FECUTC', 'DES_FECTZ', '704']]
        df_final = df_final.rename(columns={'geo_id': 'DES_GEO_ID', '704': 'DES_VALOR'})  # Renombrar columnas

        # Insertar en la base de datos
        insert_dataframe_to_db(df_final, "MET_DATESIOS", conn)

    except Exception as e:
        print(f"Error en get_rrtt_vol: {e}")

def get_pr_fr_al_values(conn):
    print("Comienzo del proceso de obtencion de indicadores de FRANCIA y ALEMANIA")
    try:
        # Cliente ESIOS
        client = esios.ESIOSClient()
        endpoint = client.endpoint(name='indicators')
        indicator_pr_merc = endpoint.select(id=600)
        
        # Fechas para obtener datos
        now = datetime.now()
        next_day = now + timedelta(days=1)
        day_after_next = now + timedelta(days=2)
        dayp1 = next_day.strftime('%Y-%m-%d')
        dayp2 = day_after_next.strftime('%Y-%m-%d')
        
        # Obtener datos históricos
        df_precio = indicator_pr_merc.historical(start=dayp1, end=dayp2, time_trunc="fifteen_minutes")
        # Filtrar el DataFrame para los geo_id especificados
        filtered_df = df_precio[df_precio['geo_id'].isin([2, 8826])]  
        # Convertir el índice del DataFrame a una columna de fecha sin zona horaria
        filtered_df.index.name = 'timestamp'  # Asigna el nombre antes de resetear el índice
        filtered_df = filtered_df.reset_index()  # Convierte el índice en una columna
        filtered_df['timestamp'] = pd.to_datetime(filtered_df['timestamp']).dt.tz_localize(None)

        # Agregar columnas necesarias para la inserción en la base de datos
        filtered_df['DES_IND_ID'] = 600
        filtered_df['DES_MAG_ID'] = 23
        filtered_df['DES_FECPUBLIC'] = now.strftime('%Y-%m-%d %H:%M:%S')
        filtered_df['DES_FECHA'] = filtered_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        filtered_df['DES_FECUTC'] = filtered_df['DES_FECHA']  # Suponiendo que está en UTC
        filtered_df['DES_FECTZ'] = filtered_df['DES_FECHA']  # Mantener mismo valor si no hay conversión
        
        # Seleccionar solo las columnas relevantes
        df_final = filtered_df[['DES_IND_ID', 'DES_MAG_ID', 'geo_id', 'DES_FECPUBLIC', 'DES_FECHA', 'DES_FECUTC', 'DES_FECTZ', '600']]
        df_final = df_final.rename(columns={'geo_id': 'DES_GEO_ID', '600': 'DES_VALOR'})  # Renombrar columnas
        
        
        insert_dataframe_to_db(df_final, "MET_DATESIOS", conn)

    except Exception as e:
        print('Error: {}'.format(e))

def insert_dataframe_to_db(df: pd.DataFrame, table_name: str, engine, schema: str = "METDB"):
    print(f"\nINSERTANDO REGISTROS EN {table_name}")
    print(df.head())
    try:
        if df.empty:
            print(f"Ninguna columna del DataFrame coincide con {schema}.{table_name}.")
            return
        df.to_sql(table_name, engine, schema=schema, if_exists='append', index=False)
        print(f"\t{len(df)} registros insertados correctamente en {schema}.{table_name}.")
    
    except IntegrityError as e:
        if '2627' in str(e.orig):  # Error de clave primaria duplicada
            print(f"Clave primaria duplicada en {schema}.{table_name}. Se omiten registros duplicados.")
        else:
            print(f"Error de integridad en {schema}.{table_name}: {e}")

    except Exception as e:
        print(f"Error insertando datos en {schema}.{table_name}: {e}")

## API Functions
def get_indicators_value(indicator, sdate, edate, geoid): 
    try:
        insert_queries = []
        insert_query_template = '''
            INSERT INTO [METDB].[MET_DATESIOS] (
                DES_IND_ID, DES_MAG_ID, DES_GEO_ID, DES_FECPUBLIC, DES_FECHA, DES_FECUTC, DES_FECTZ, DES_VALOR
            ) VALUES ({}, {}, {}, '{}', '{}', '{}', '{}', {})    '''
        ep = "indicators/{}?start_date={}&end_date={}&geo_ids[]={}".format(indicator, sdate, edate, geoid)
        #ep = "indicators/{}?start_date={}&end_date={}&geo_ids[]={}".format(indicator, '24-06-2024T00:00:00Z', '25-06-2024T00:00:00Z', geoid)
        print("Comienzo del proceso para el endpoint: {}".format(ep))
        resp_raw = make_get_request(base_path_esios,ep,header)
        values_updated_at = datetime.fromisoformat(resp_raw['indicator']['values_updated_at'])
        magnitud_id = resp_raw['indicator']['magnitud'][0]['id']
        for value in resp_raw['indicator']['values']:
            des_fecha = datetime.fromisoformat(value['datetime'])
            des_fecutc = datetime.fromisoformat(value['datetime_utc'].replace('Z', '+00:00'))
            des_fectz = datetime.fromisoformat(value['tz_time'].replace('Z', '+00:00'))
            des_valor = value['value']
            query = insert_query_template.format(
                indicator, 
                magnitud_id, 
                geoid, 
                values_updated_at.strftime('%Y-%m-%d %H:%M:%S'),
                des_fecha.strftime('%Y-%m-%d %H:%M:%S'),
                des_fecutc.strftime('%Y-%m-%d %H:%M:%S'),
                des_fectz.strftime('%Y-%m-%d %H:%M:%S'),
                des_valor                
            )
            print(query)
            #execute_query(conn, query)
            
            insert_queries.append(query)
        return insert_queries

    except pymssql.Error as e:
        print('Error: {}'.format(e))

## BDD Functions

#Create the connection
def create_connection(server, user, password, database):
    try:
        connection = pymssql.connect(
            server=server,
            user=user,
            password=password,
            database=database
        )
        print(f"Conexion a la bdd de MET exitosa: {connection}")
        return connection
    except pymssql.Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None
    
#Execute querys with params (optional)
def execute_query(connection, query, params=None):
    try:
        with connection.cursor() as cursor:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            connection.commit()  
        return True  
    except pymssql.Error as e:
        print(f"Error al ejecutar la consulta: {e}")
        connection.rollback() 
        return False 

#Insert data 
def execute_batch_insert(connection, batch_queries):
    try:
        with connection.cursor() as cursor:
            for query in batch_queries:
                cursor.execute(query)
            connection.commit()
            print(f"Data inserted in bdd: {len(batch_queries)}")
    except Exception as e:
        print(f"Error al ejecutar las consultas: {e}")
        connection.rollback()

#Close Connection
def close_connection(connection):
    try:
        connection.close()
    except pymssql.Error as e:
        print(f"Error al cerrar la conexión: {e}")

## HTTP Functions

#Get
def make_get_request(base_path, endpoint=None, headers=None):
    full_url = add_to_base_path(base_path, endpoint)
    response = None
    try:
        response = requests.get(full_url, headers=headers)
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

def add_to_base_path(base_path,  addition=None):
    if addition:
        return base_path + addition
    return base_path

def get_vars_from_environment():
    global server_bd_met, database_bd_met, username_bd_met, password_bd_met, base_path_esios, token_esios, header
    try:
        server_bd_met = os.environ['server_bd_met']
        database_bd_met = os.environ['database_bd_met']
        username_bd_met = os.environ['username_bd_met']
        password_bd_met = os.environ['password_bd_met']
        token_esios = os.environ['token_esios']
        base_path_esios = os.environ['base_path_esios']
        header={'x-api-key' : '%s' %(token_esios),'Authorization' : 'Token token=%s' %(token_esios)}
        os.environ['ESIOS_API_KEY'] = token_esios

        print('\n00 - Variables de entorno obtenidas correctamente')
    except KeyError as err:
        print(f'Error obtaining environment variable: {err}')
    except Exception as err:
        print(f'Unexpected error obtaining environment variables: {err}')

def crear_conexion_bd():
    # Configuración de la conexión a la base de datos
    try:
        conn_str = r"mssql+pyodbc://sqluser:cwD9KVms4Qdv4mLy@met-esp-prod.database.windows.net:1433/Risk_MGMT_Spain?driver=ODBC+Driver+17+for+SQL+Server"
        engine = create_engine(conn_str, fast_executemany=True)
        print("CONEXION EXITOSA CON LA BBDD DE MET")
        return engine
    except Exception as e:
        print(f"Error conectando a la base de datos: {e}")
        return
    

if __name__ == "__main__":
    main()