import os
import pandas as pd
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
import pyodbc
import time


carpeta_destino = os.path.join(os.getcwd(), "files")
carpeta_logs = os.path.join(os.getcwd(), "logs")

cabecera_completa = [
    "NOMFICHERO", "CUP22", "TIPMED", "FECHAHORA", "BANDERA", "MEDACTENT", "CALACTENT",
    "MEDACTSAL", "CALACTSAL", "MEDREAC1", "CALREAC1", "MEDREAC2", "CALREAC2",
    "MEDREAC3", "CALREAC3", "MEDREAC4", "CALREAC4", "MEDRES1", "CALRES1",
    "MEDRES2", "CALRES2", "METOBT", "INDFIR"
]

cabecera_p5 = [
    "NOMFICHERO", "CUP", "FECHAHORA", "BANDERA", "MEDACTENT", "MEDACTSAL"]
 
cabecera_f5 = [
    "NOMFICHERO", "CUP", "FECHAHORA", "BANDERA", "MEDACTENT", "MEDACTSAL",
    "MEDREACT1", "MEDREACT2", "MEDREACT3", "MEDREACT4", "METOBT", "INDFIRMA", "CODFACT"
]


def main():
    dfs = leer_csvs_en_dataframe()
    if dfs:
        engine=crear_conexion_bd()
        for nombre_archivo, df in dfs.items():
            nombre_upper = nombre_archivo.upper()
            print(f"\nInsertando fichero: {nombre_archivo}")
            if "P5D" in nombre_upper or "P5_" in nombre_upper: insert_dataframe_to_db(df, "TMP_CONSUPOWERP5D", engine)
            if "P1" in nombre_upper or "P1D" in nombre_upper: insert_dataframe_to_db(df, "TMP_CONSUPOWERP", engine)
            if "P2" in nombre_upper or "P2D" in nombre_upper: insert_dataframe_to_db(df, "TMP_CONSUPOWERP", engine)
            if "F5" in nombre_upper or "F5D" in nombre_upper: insert_dataframe_to_db(df, "TMP_CONSUPOWERF5D", engine)
        ejecutar_procedimiento()
        borrar_archivos_carpeta()
 
def generar_log_csv(df: pd.DataFrame, nombre_archivo: str):
    if 'FECHAHORA' not in df.columns:
        print(f"No se pudo generar log para {nombre_archivo}: no hay columna FECHAHORA.")
        return

    try:
        df['FECHA_DIA'] = pd.to_datetime(df['FECHAHORA'], errors='coerce').dt.date
        resumen = df.groupby('FECHA_DIA').size().reset_index(name='REGISTROS')
        
        fecha_actual = datetime.now().strftime('%d%m')
        nombre_log = f"insertados_{fecha_actual}.txt"
        ruta_log = os.path.join(carpeta_logs, nombre_log)
        
        with open(ruta_log, "a", encoding="utf-8") as f:
            f.write(f"\nArchivo: {nombre_archivo}\n")
            for _, fila in resumen.iterrows():
                f.write(f"  Día: {fila['FECHA_DIA']} - Registros: {fila['REGISTROS']}\n")
            f.write("-" * 40 + "\n")
        
        print(f"Log generado para {nombre_archivo}")
    except Exception as e:
        print(f"Error generando log para {nombre_archivo}: {e}")

def borrar_archivos_carpeta():
    try:
        for archivo in os.listdir(carpeta_destino):
            ruta = os.path.join(carpeta_destino, archivo)
            if os.path.isfile(ruta):
                os.remove(ruta)
        print("Todos los archivos han sido eliminados.")
    except Exception as e:
        print(f"Error al borrar archivos: {e}")

def crear_conexion_pyodbc():
    try:
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=met-esp-prod.database.windows.net;'
            'DATABASE=Risk_MGMT_Spain;'
            'UID=sqluser;'
            'PWD=cwD9KVms4Qdv4mLy'
        )
        print("Conexión establecida con pyodbc")
        return conn
    except Exception as e:
        print(f"Error conectando con pyodbc: {e}")
        return None
 
def ejecutar_procedimiento():

    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=met-esp-prod.database.windows.net;'
        'DATABASE=Risk_MGMT_Spain;'
        'UID=sqluser;PWD=cwD9KVms4Qdv4mLy'
    )

    cursor = conn.cursor()

    start = time.time()

    try:
        cursor.execute("WAITFOR DELAY '00:00:05'; EXEC METDB.PR_CARGA_CONSUMOS_POWER")
        while cursor.nextset():
            pass
        conn.commit()
        print("Ejecutado sin errores")
    except Exception as e:
        print(f"Error: {e}")

    end = time.time()
    print(f"Duración: {end - start:.2f} segundos")

    cursor.close()
    conn.close()
            
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
        
def insert_dataframe_to_db(df: pd.DataFrame, table_name: str, engine, schema: str = "METDB"):
    print(f"\nINSERTANDO REGISTROS EN {table_name}")
    print(df.head())
    try:
        if df.empty:
            print(f"Ninguna columna del DataFrame coincide con {schema}.{table_name}.")
            return
        if 'FECHAHORA' in df.columns:
            df['FECHAHORA'] = pd.to_datetime(df['FECHAHORA'], errors='coerce')  # acepta cualquier formato válido
            df['FECHAHORA'] = df['FECHAHORA'].dt.floor('min')  # quita los segundos
        df.to_sql(table_name, engine, schema=schema, if_exists='append', index=False)
        print(f"\t{len(df)} registros insertados correctamente en {schema}.{table_name}.")
        generar_log_csv(df, df['NOMFICHERO'].iloc[0] if 'NOMFICHERO' in df.columns else table_name)
    except IntegrityError as e:
        if '2627' in str(e.orig):  # Error de clave primaria duplicada
            print(f"Clave primaria duplicada en {schema}.{table_name}. Se omiten registros duplicados.")
        else:
            print(f"Error de integridad en {schema}.{table_name}: {e}")

    except Exception as e:
        print(f"Error insertando datos en {schema}.{table_name}: {e}")
        
def leer_csvs_en_dataframe():
    dataframes = {} 
    print(f"\nLeyendo archivos CSV en: {carpeta_destino}\n")
    for nombre_archivo in os.listdir(carpeta_destino):
        ruta_archivo = os.path.join(carpeta_destino, nombre_archivo)
        if os.path.isfile(ruta_archivo):
            try:
                df = pd.read_csv(ruta_archivo, sep=";", encoding="utf-8", engine="python", header=None)

                # Eliminar columna vacía si existe
                if df.shape[1] > 1:
                    ultima_col = df.columns[-1]
                    if df[ultima_col].isnull().all() or (df[ultima_col] == '').all():
                        df = df.iloc[:, :-1]

                nombre_upper = nombre_archivo.upper()

                # Si es tipo P1/P1D/P2/P2P
                if any(p in nombre_upper for p in ["P1", "P1D", "P2", "P2P"]):
                    df.insert(0, "NOMFICHERO", nombre_archivo)
                    if df.shape[1] == 22:
                        df.columns = cabecera_completa[:-1]
                        df["INDFIR"] = None
                        print(f"[CABECERA P1/P2 - 22 cols + NULL] {nombre_archivo}")
                    elif df.shape[1] == 23:
                        df.columns = cabecera_completa
                        print(f"[CABECERA P1/P2 - 23 cols] {nombre_archivo}")
                    else:
                        print(f"[P1/P2 columnas inesperadas] {nombre_archivo} ({df.shape[1]} columnas)")

                # Si es tipo P5 o P5D
                elif any(p in nombre_upper for p in ["P5", "P5D"]):
                    if df.shape[1] == 5:
                        df.insert(0, "NOMFICHERO", nombre_archivo)
                        df.columns = cabecera_p5
                        print(f"[CABECERA P5 - 5 cols + NOMFICHERO] {nombre_archivo}")
                    elif df.shape[1] == 6:
                        df.columns = cabecera_p5
                        print(f"[CABECERA P5 - 6 cols] {nombre_archivo}")
                    else:
                        print(f"[P5 columnas inesperadas] {nombre_archivo} ({df.shape[1]} columnas)")

                # Si es tipo F5 o F5D
                elif any(p in nombre_upper for p in ["F5", "F5D"]):
                    if df.shape[1] > 12:
                        df = df.iloc[:, :12]
                    df.insert(0, "NOMFICHERO", nombre_archivo)
                    df.columns = cabecera_f5
                    print(f"[CABECERA F5 AJUSTADA] {nombre_archivo}")

                # AÑADIMOS TODOS LOS ARCHIVOS VÁLIDOS AL DICCIONARIO
                dataframes[nombre_archivo] = df

            except Exception as e:
                print(f"Error al leer {nombre_archivo}: {e}")
        else:
            print(f"Ignorado (no es archivo): {nombre_archivo}")

    print(f"\nSe han leído {len(dataframes)} archivos válidos para procesar.")
    return dataframes


if __name__ == '__main__':
    main()