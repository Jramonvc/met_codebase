import os
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
import pyodbc
import time

carpeta = 'temp_consumos_neuro'
fecha_comunicacion = datetime.now().strftime('%Y-%m-%d')
columnas = ['PQH_FECCOMU', 'PQH_UP', 'PQH_FECHA', 'PQH_HORA', 'PQH_QHORARIO', 'PQH_PREVISION']

def main():
    engine = crear_conexion_bd()
    eliminar_registros_previos("MET_PREVPOWERQH", "PQH_FECHA", "METDB", engine)

    for archivo in os.listdir(carpeta):
        if archivo.endswith('.json'):
            ruta_archivo = os.path.join(carpeta, archivo)

            with open(ruta_archivo, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError:
                    print(f"Error leyendo el archivo JSON: {archivo}")
                    continue

            registros = []

            if isinstance(data.get("data"), dict) and data["data"]:
                for up, fechas in data["data"].items():
                    for fecha, registros_dia in fechas.items():
                        for _, valores in registros_dia.items():
                            registro = {
                                'PQH_FECCOMU': fecha_comunicacion,
                                'PQH_UP': up.strip(),
                                'PQH_FECHA': fecha,
                                'PQH_HORA': valores['hora'],
                                'PQH_QHORARIO': valores['qh'],
                                'PQH_PREVISION': valores['valor']
                            }
                            registros.append(registro)

                df = pd.DataFrame(registros, columns=columnas)
                print(f"Archivo {archivo}: {len(df)} registros cargados.")

                # Transformaciones y limpieza
                df['PQH_UP'] = df['PQH_UP'].astype(str).str.strip()
                df = df[df['PQH_UP'].str.len() <= 10]

                df['PQH_FECCOMU'] = pd.to_datetime(df['PQH_FECCOMU'], errors='coerce')
                df['PQH_FECHA'] = pd.to_datetime(df['PQH_FECHA'], errors='coerce')
                df['PQH_HORA'] = pd.to_numeric(df['PQH_HORA'], errors='coerce').astype('Int64')
                df['PQH_QHORARIO'] = pd.to_numeric(df['PQH_QHORARIO'], errors='coerce').astype('Int64')
                df['PQH_PREVISION'] = pd.to_numeric(df['PQH_PREVISION'], errors='coerce')

                
                insert_dataframe_to_db(df, "MET_PREVPOWERQH", engine)
            else:
                print(f"Archivo {archivo}: sin datos.")
    eliminar_archivos_en_carpeta()

def crear_conexion_bd():
    try:
        conn_str = r"mssql+pyodbc://sqluser:cwD9KVms4Qdv4mLy@met-esp-prod.database.windows.net:1433/Risk_MGMT_Spain?driver=ODBC+Driver+17+for+SQL+Server"
        engine = create_engine(conn_str, fast_executemany=True)
        print("CONEXIÓN EXITOSA CON LA BBDD DE MET")
        return engine
    except Exception as e:
        print(f"Error conectando a la base de datos: {e}")
        return

def insert_dataframe_to_db(df: pd.DataFrame, table_name: str, engine, schema: str = "METDB"):
    print(f"\nINSERTANDO REGISTROS EN {schema}.{table_name}")
    try:
        if df.empty:
            print(f"DataFrame vacío. No se insertan registros en {schema}.{table_name}.")
            return

        df.to_sql(table_name, engine, schema=schema, if_exists='append', index=False)
        print(f"{len(df)} registros insertados correctamente en {schema}.{table_name}.")

    except IntegrityError as e:
        if '2627' in str(e.orig):
            print(f"Clave primaria duplicada en {schema}.{table_name}. Se omiten registros duplicados.")
        else:
            print(f"Error de integridad en {schema}.{table_name}: {e}")
    except Exception as e:
        print(f"Error insertando datos en {schema}.{table_name}: {e}")

def eliminar_registros_previos(table_name, fecha_columna, schema, engine):
    try:
        fecha_hoy = datetime.now().strftime('%Y-%m-%d')

        print(f"\n[INFO] Eliminando registros de {table_name} desde {fecha_hoy} en adelante")

        delete_query = text(f"""
            DELETE FROM {schema}.{table_name}
            WHERE {fecha_columna} >= :fecha_hoy
            AND PQH_UP != 'METER01'
        """)

        with engine.begin() as connection:
            result = connection.execute(delete_query, {
                "fecha_hoy": fecha_hoy
            })
            print(f"\t{result.rowcount} registros eliminados de {schema}.{table_name}")

    except Exception as e:
        print(f"[ERROR] Error eliminando registros de {table_name}: {e}")

def eliminar_archivos_en_carpeta():
    carpeta_path = Path(carpeta)
    
    if not carpeta_path.exists() or not carpeta_path.is_dir():
        print(f"[ERROR] La carpeta '{carpeta}' no existe o no es válida.")
        return
    
    archivos = list(carpeta_path.iterdir())
    
    if not archivos:
        print(f"[INFO] No hay archivos para eliminar en '{carpeta}'.")
        return

    for archivo in archivos:
        if archivo.is_file():
            try:
                archivo.unlink()
                print(f"[INFO] Archivo eliminado: {archivo.name}")
            except Exception as e:
                print(f"[ERROR] No se pudo eliminar {archivo.name}: {e}")

if __name__ == '__main__':
    main()
