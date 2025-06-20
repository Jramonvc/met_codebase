import os
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
import pyodbc
import time
import requests

carpeta = 'temp_consumos_neuro'
fecha_comunicacion = datetime.now().strftime('%Y-%m-%d')
columnas = ['PQH_FECCOMU', 'PQH_UP', 'PQH_FECHA', 'PQH_HORA', 'PQH_QHORARIO', 'PQH_PREVISION']
url_power_automate = 'https://prod-13.westeurope.logic.azure.com:443/workflows/edf936bb270a461c95c6febd0e985a80/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=owzOYnmcYYQP4iMBwCmS69xOhXt7WI-AH273iQ0DaF4'

def main():
    engine = crear_conexion_bd()
    escribir_log(f"--------- COMIENZO DE PROCESO ---------")

    for archivo in os.listdir(carpeta):
        if archivo.endswith('.json'):
            ruta_archivo = os.path.join(carpeta, archivo)

            with open(ruta_archivo, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError:
                    print(f"[ERROR] Error leyendo el archivo JSON: {archivo}")
                    escribir_log(f"[ERROR] Error leyendo el archivo JSON: {archivo}")
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
                print(f"\n[INFO] Archivo {archivo}: {len(df)} registros cargados.")
                escribir_log(f"→ [INFO] Archivo {archivo}: {len(df)} registros cargados.")

                # Limpieza y tipado
                df['PQH_UP'] = df['PQH_UP'].astype(str).str.strip()
                df = df[df['PQH_UP'].str.len() <= 10]
                df['PQH_FECCOMU'] = pd.to_datetime(df['PQH_FECCOMU'], errors='coerce')
                df['PQH_FECHA'] = pd.to_datetime(df['PQH_FECHA'], errors='coerce')
                df['PQH_HORA'] = pd.to_numeric(df['PQH_HORA'], errors='coerce').astype('Int64')
                df['PQH_QHORARIO'] = pd.to_numeric(df['PQH_QHORARIO'], errors='coerce').astype('Int64')
                df['PQH_PREVISION'] = pd.to_numeric(df['PQH_PREVISION'], errors='coerce')

                # Comparación con la BBDD
                df_existente = obtener_registros_existentes(engine, df)
                ya_existen, cambiados, nuevos, tabla_nuevos, tabla_existentes = comparar_registros(df, df_existente)

                print(f"\n[RESUMEN ARCHIVO {archivo}]")
                escribir_log(f"→ [RESUMEN ARCHIVO {archivo}]")
                print(f"→ Total registros en archivo: {len(df)}")
                escribir_log(f"→ Total registros en archivo: {len(df)}")
                print(f"→ Ya existen y son idénticos: {len(ya_existen)}")
                escribir_log(f"→ Ya existen y son idénticos: {len(ya_existen)}")
                print(f"→ Existen pero han cambiado: {len(cambiados)}")
                escribir_log(f"→ Existen pero han cambiado: {len(cambiados)}")
                print(f"→ No existen aún (nuevos): {len(nuevos)}")
                escribir_log(f"→ No existen aún (nuevos): {len(nuevos)}")

                # Insertar registros nuevos sin envío de correo
                if not nuevos.empty:
                    print(f"\n[INFO] Insertando {len(nuevos)} registros nuevos.")
                    escribir_log(f"→ [INFO] Insertando {len(nuevos)} registros nuevos.")
                    df_nuevos_con_version = get_nueva_version_para_registros(nuevos, engine)
                    insert_dataframe_to_db(df_nuevos_con_version[[
                        'PQH_FECCOMU', 'PQH_UP', 'PQH_FECHA', 'PQH_HORA',
                        'PQH_QHORARIO', 'PQH_PREVISION', 'PQH_VERSION'
                    ]], "MET_PREVPOWERQH", engine)
                    print("[INFO] Nuevos registros insertados. No se envía correo.")
                    escribir_log("→ Nuevos registros insertados. No se envía correo.")

                # Insertar cambios en registros existentes y enviar correo
                if not cambiados.empty:
                    print(f"\n[INFO] Calculando versiones para {len(cambiados)} registros modificados...")
                    escribir_log(f"→ [INFO] Calculando versiones para {len(cambiados)} registros modificados...")
                    df_cambiados_con_version = get_nueva_version_para_registros(cambiados, engine)
                    insert_dataframe_to_db(df_cambiados_con_version[[
                        'PQH_FECCOMU', 'PQH_UP', 'PQH_FECHA', 'PQH_HORA',
                        'PQH_QHORARIO', 'PQH_PREVISION', 'PQH_VERSION'
                    ]], "MET_PREVPOWERQH", engine)

                    tabla_diferencias = generar_tabla_diferencias(tabla_nuevos, tabla_existentes)
                    print("\n[TABLA COMBINADA (PREV ANT / PREV NUEVA / DIF)]")
                    print(tabla_diferencias.to_string(index=False))
                    enviar_a_power_automate(tabla_diferencias, url_power_automate)
                    escribir_log("→ Se han enviado registros modificados por correo.")

            else:
                print(f"[INFO] Archivo {archivo}: sin datos válidos.")
                escribir_log(f"[INFO] Archivo {archivo}: sin datos válidos.")

    eliminar_archivos_en_carpeta()
    escribir_log(f"→ Archivos eliminados de la carpeta {carpeta}")
    escribir_log(f"--------- FIN DEL PROCESO ---------\n")



def enviar_a_power_automate(tabla_diferencias, url_webhook):

    if 'PQH_FECHA' in tabla_diferencias.columns: tabla_diferencias['PQH_FECHA'] = tabla_diferencias['PQH_FECHA'].dt.strftime('%Y-%m-%d')
    payload = {"tabla_comparativa": tabla_diferencias.to_dict(orient="records")}
    try:
        response = requests.post(url_webhook, json=payload)
        if response.status_code in [200, 202]:
            print("[INFO] Datos enviados correctamente a Power Automate.")
        else:
            print(f"[ERROR] Error al enviar datos: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"[ERROR] Excepción durante el envío: {e}")


def generar_tabla_diferencias(tabla_nuevos, tabla_existentes):
    claves = ['PQH_UP', 'PQH_FECHA', 'PQH_HORA']
    # Calcular promedio por hora para ambos conjuntos
    prom_nuevos = tabla_nuevos.groupby(claves)['PQH_PREVISION'].mean().reset_index()
    prom_existentes = tabla_existentes.groupby(claves)['PQH_PREVISION'].mean().reset_index()
    # Renombrar columnas
    prom_nuevos = prom_nuevos.rename(columns={'PQH_PREVISION': 'PREV NUEVA'})
    prom_existentes = prom_existentes.rename(columns={'PQH_PREVISION': 'PREV ANT'})
    # Unir por claves
    comparacion = pd.merge(prom_nuevos, prom_existentes, on=claves, how='inner')
    # Multiplicar por 4 y redondear
    comparacion['PREV NUEVA'] = (comparacion['PREV NUEVA'] * 4).round(1)
    comparacion['PREV ANT'] = (comparacion['PREV ANT'] * 4).round(1)
    comparacion['DIF'] = (comparacion['PREV NUEVA'] - comparacion['PREV ANT']).round(1)

    return comparacion[['PQH_UP', 'PQH_FECHA', 'PQH_HORA', 'PREV ANT', 'PREV NUEVA', 'DIF']]


def escribir_log(mensaje):
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"log_{datetime.now().strftime('%Y-%m-%d')}.txt"
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M')}] - {mensaje}\n")

def calcular_promedio_por_hora(df):
    return df.groupby(['PQH_UP', 'PQH_FECHA', 'PQH_HORA'])['PQH_PREVISION'].mean().reset_index()


def obtener_registros_existentes(engine, df_nuevo):
    fechas = df_nuevo['PQH_FECHA'].dropna().dt.strftime('%Y-%m-%d').unique().tolist()
    if not fechas:
        return pd.DataFrame(columns=['PQH_UP', 'PQH_FECHA', 'PQH_HORA', 'PQH_QHORARIO', 'PQH_PREVISION'])

    placeholders = ', '.join([f"'{f}'" for f in fechas])

    query = f"""
    WITH UltimasVersiones AS (
        SELECT PQH_UP, PQH_FECHA, PQH_HORA, PQH_QHORARIO,
               MAX(PQH_VERSION) AS MaxVersion
        FROM METDB.MET_PREVPOWERQH
        WHERE CONVERT(date, PQH_FECHA) IN ({placeholders})
        GROUP BY PQH_UP, PQH_FECHA, PQH_HORA, PQH_QHORARIO
    )
    SELECT p.PQH_UP, p.PQH_FECHA, p.PQH_HORA, p.PQH_QHORARIO, p.PQH_PREVISION
    FROM METDB.MET_PREVPOWERQH p
    INNER JOIN UltimasVersiones uv
        ON p.PQH_UP = uv.PQH_UP
        AND p.PQH_FECHA = uv.PQH_FECHA
        AND p.PQH_HORA = uv.PQH_HORA
        AND p.PQH_QHORARIO = uv.PQH_QHORARIO
        AND p.PQH_VERSION = uv.MaxVersion
    """

    with engine.begin() as connection:
        df_existente = pd.read_sql_query(text(query), connection)

    return df_existente


def get_nueva_version_para_registros(df: pd.DataFrame, engine, table_name='MET_PREVPOWERQH', schema='METDB'):
    claves = ['PQH_UP', 'PQH_FECHA', 'PQH_HORA', 'PQH_QHORARIO']
    df = df.copy()
    versiones = []

    with engine.begin() as connection:
        for _, row in df.iterrows():
            query = text(f"""
                SELECT MAX(PQH_VERSION) as max_version
                FROM {schema}.{table_name}
                WHERE PQH_UP = :up AND PQH_FECHA = :fecha
                AND PQH_HORA = :hora AND PQH_QHORARIO = :qh
            """)
            result = connection.execute(query, {
                "up": row["PQH_UP"],
                "fecha": row["PQH_FECHA"],
                "hora": int(row["PQH_HORA"]),
                "qh": int(row["PQH_QHORARIO"])
            }).fetchone()

            current_version = result.max_version if result.max_version is not None else 0
            versiones.append(current_version + 1)

    df["PQH_VERSION"] = versiones
    return df


def comparar_registros(df_nuevo, df_existente):
    claves = ['PQH_UP', 'PQH_FECHA', 'PQH_HORA', 'PQH_QHORARIO']

    # Asegurar tipos consistentes
    df_nuevo['PQH_FECHA'] = pd.to_datetime(df_nuevo['PQH_FECHA'])
    df_existente['PQH_FECHA'] = pd.to_datetime(df_existente['PQH_FECHA'])

    for col in ['PQH_HORA', 'PQH_QHORARIO']:
        df_nuevo[col] = pd.to_numeric(df_nuevo[col], errors='coerce').astype('Int64')
        df_existente[col] = pd.to_numeric(df_existente[col], errors='coerce').astype('Int64')

    # Merge para comparar valores
    df_merged = df_nuevo.merge(
        df_existente,
        on=claves,
        how='left',
        suffixes=('', '_existente')
    )

    ya_existen_iguales = df_merged[
        df_merged['PQH_PREVISION'] == df_merged['PQH_PREVISION_existente']
    ]

    existen_diferentes = df_merged[
        df_merged['PQH_PREVISION_existente'].notna() &
        (df_merged['PQH_PREVISION'] != df_merged['PQH_PREVISION_existente'])
    ].copy()

    no_existen = df_merged[df_merged['PQH_PREVISION_existente'].isna()]

    # Mostrar diferencias detalladas
    if not existen_diferentes.empty:
        print("\n[DIFERENCIAS DETECTADAS ENTRE BBDD Y NUEVO DATAFRAME]")
        for _, row in existen_diferentes.iterrows():
            print(f"→ {row['PQH_UP']} | Fecha: {row['PQH_FECHA'].date()} | Hora: {row['PQH_HORA']} | QH: {row['PQH_QHORARIO']}")
            print(f"   - Base de datos: {row['PQH_PREVISION_existente']}")
            print(f"   - Nuevo valor  : {row['PQH_PREVISION']}\n")

    # Crear tablas de comparación
    claves = ['PQH_UP', 'PQH_FECHA', 'PQH_HORA', 'PQH_QHORARIO']

    # Datos nuevos con cambios
    tabla_nuevos = df_nuevo.merge(
        existen_diferentes[claves],
        on=claves,
        how='inner'
    )[['PQH_FECCOMU', 'PQH_UP', 'PQH_FECHA', 'PQH_HORA',
        'PQH_QHORARIO', 'PQH_PREVISION']]

    # Datos antiguos de la BBDD con los mismos registros
    tabla_existentes = df_existente.merge(
        existen_diferentes[claves],
        on=claves,
        how='inner'
    )[['PQH_UP', 'PQH_FECHA', 'PQH_HORA',
        'PQH_QHORARIO', 'PQH_PREVISION']]

    # Añadir la misma fecha de comunicación a los datos antiguos
    tabla_existentes.insert(0, 'PQH_FECCOMU', fecha_comunicacion)

    return ya_existen_iguales, existen_diferentes, no_existen, tabla_nuevos, tabla_existentes



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
