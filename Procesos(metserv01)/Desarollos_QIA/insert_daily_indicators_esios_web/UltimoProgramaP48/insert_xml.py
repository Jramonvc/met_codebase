import xml.etree.ElementTree as ET
import pandas as pd
import os
import numpy as np
from sqlalchemy.sql import text
from sqlalchemy import create_engine
import pyodbc
from tqdm import tqdm 

# Ruta de la carpeta que contiene los archivos XML
ruta_carpeta = 'files_xml'
archivos_xml = [f for f in os.listdir(ruta_carpeta) if f.endswith('.xml')]

# Lista para almacenar todos los datos en un solo DataFrame final
data_total = []

print("")
print("\t0. - COMIENZO DE PROCESO DE FORMATEO DE XMLs")
# Procesar cada archivo XML
for archivo in archivos_xml:
    nombre_mercado = archivo.split('_')[0]
    columna_valor = f"{nombre_mercado} Valor"

    tree = ET.parse(os.path.join(ruta_carpeta, archivo))
    root = tree.getroot()

    namespace = ''
    if root.tag.startswith('{'):
        namespace = root.tag.split('}')[0] + '}'

    data = []

    for series_temporales in root.findall(f'.//{namespace}SeriesTemporales'):
        up = None
        if series_temporales.find(f'{namespace}UPEntrada') is not None:
            up = series_temporales.find(f'{namespace}UPEntrada').get('v')
        elif series_temporales.find(f'{namespace}UPSalida') is not None:
            up = series_temporales.find(f'{namespace}UPSalida').get('v') 

        if up is not None:
            periodo_elem = series_temporales.find(f'{namespace}Periodo')
            if periodo_elem is not None:
                intervalo_tiempo_elem = periodo_elem.find(f'{namespace}IntervaloTiempo')
                if intervalo_tiempo_elem is not None:
                    fecha_fin_completa = intervalo_tiempo_elem.get('v').split('/')[1]
                    fecha_fin = fecha_fin_completa.split('T')[0]

                    hora = 1
                    for i, intervalo in enumerate(periodo_elem.findall(f'{namespace}Intervalo'), start=1):
                        pos_elem = intervalo.find(f'{namespace}Pos')
                        ctd_elem = intervalo.find(f'{namespace}Ctd')
                        if pos_elem is not None and ctd_elem is not None:
                            intervalo_valor = ctd_elem.get('v').replace(',', '.')
                            data.append({
                                'Fecha': fecha_fin,
                                'UP': up,
                                'Hora': hora,
                                'Intervalo': pos_elem.get('v'),
                                columna_valor: intervalo_valor
                            })

                        if i % 4 == 0:
                            hora += 1

    df_temp = pd.DataFrame(data)
    data_total.append(df_temp)

# Combinar todos los DataFrames y crear el DataFrame final
df_final = pd.concat(data_total, ignore_index=True)
df_final = df_final.pivot_table(index=['Fecha', 'UP', 'Hora', 'Intervalo'], aggfunc='first').reset_index()

# Renombrar las columnas para que coincidan con la tabla de SQL Server
df_final = df_final.rename(columns={
    'Fecha': 'DUC_FECHA',
    'UP': 'DUC_UP',
    'Hora': 'DUC_HORA',
    'Intervalo': 'DUC_INTERV',
    'IDA1 Valor': 'DUC_VALIDA1',
    'IDA2 Valor': 'DUC_VALIDA2',
    'IDA3 Valor': 'DUC_VALIDA3',
    'MD Valor': 'DUC_VALMD',
    'PREIDA2 Valor': 'DUC_VALPREIDA2',
    'PREIDA3 Valor': 'DUC_VALPREIDA3',
    'CIERRE Valor': 'DUC_VALCIERR',
    'H17 Valor': 'DUC_VALH17',
    'H13 Valor': 'DUC_VALH13'
})

# Asegurar que todas las columnas esperadas están presentes
columnas_esperadas = [
    'DUC_FECHA', 'DUC_UP', 'DUC_HORA', 'DUC_INTERV',
    'DUC_VALCIERR', 'DUC_VALIDA1', 'DUC_VALIDA2', 'DUC_VALIDA3',
    'DUC_VALMD', 'DUC_VALPREIDA2', 'DUC_VALPREIDA3', 'DUC_VALH17', 'DUC_VALH13'
]

for columna in columnas_esperadas:
    if columna not in df_final.columns:
        df_final[columna] = np.nan

# Convertir columnas a tipo float y reemplazar NaN por None
for columna in columnas_esperadas[4:]:
    df_final[columna] = pd.to_numeric(df_final[columna], errors='coerce').astype(float)

df_final = df_final.where(pd.notnull(df_final), None)
df_final = df_final.replace({np.nan: None})
df_final = df_final[columnas_esperadas]

#Conexión a la base de datos usando pyodbc
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=met-esp-prod.database.windows.net;"
    "DATABASE=Risk_MGMT_Spain;"
    "UID=sqluser;"
    "PWD=cwD9KVms4Qdv4mLy;"
)
conn = pyodbc.connect(conn_str, autocommit=True)
cursor = conn.cursor()

try:
    print("\t1. - CONEXIÓN EXITOSA A LA BASE DE DATOS")

   #Crear la tabla temporal
    cursor.execute("""
        CREATE TABLE #TempDatosUPComb (
            DUC_FECHA DATE,
            DUC_UP NVARCHAR(255),
            DUC_HORA INT,
            DUC_INTERV INT,
            DUC_VALCIERR FLOAT,
            DUC_VALIDA1 FLOAT,
            DUC_VALIDA2 FLOAT,
            DUC_VALIDA3 FLOAT,
            DUC_VALMD FLOAT,
            DUC_VALPREIDA2 FLOAT,
            DUC_VALPREIDA3 FLOAT,
            DUC_VALH17 FLOAT,
            DUC_VALH13 FLOAT
        )
    """)
    print("\t2. - TABLA TEMPORAL CREADA")

    chunksize = 1000  
    total_chunks = len(df_final) // chunksize + (1 if len(df_final) % chunksize != 0 else 0)

    print("\t3. - INSERTANDO DATOS P48 EN LA TABLA TEMPORAL")
    print("")
    for i, chunk_start in enumerate(
        tqdm(range(0, len(df_final), chunksize), total=total_chunks, desc="Progreso: ", unit="chunk")
    ):
        df_chunk = df_final.iloc[chunk_start:chunk_start + chunksize]
        rows = [tuple(x) for x in df_chunk.itertuples(index=False, name=None)]

        insert_query = """
            INSERT INTO #TempDatosUPComb (
                DUC_FECHA, DUC_UP, DUC_HORA, DUC_INTERV,
                DUC_VALCIERR, DUC_VALIDA1, DUC_VALIDA2, DUC_VALIDA3,
                DUC_VALMD, DUC_VALPREIDA2, DUC_VALPREIDA3, DUC_VALH17, DUC_VALH13
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.fast_executemany = True
        cursor.executemany(insert_query, rows)
    print("")
    print("\t4. - INICIANDO PROCESO DE MERGE")

   #Ejecutar el MERGE desde la tabla temporal
    merge_query = """
        MERGE INTO METDB.MET_DATOSUPCOMB AS target
        USING #TempDatosUPComb AS source
        ON target.DUC_FECHA = source.DUC_FECHA AND
           target.DUC_UP = source.DUC_UP AND
           target.DUC_HORA = source.DUC_HORA AND
           target.DUC_INTERV = source.DUC_INTERV
        WHEN MATCHED THEN
            UPDATE SET
                DUC_VALCIERR = COALESCE(source.DUC_VALCIERR, target.DUC_VALCIERR),
                DUC_VALIDA1 = COALESCE(source.DUC_VALIDA1, target.DUC_VALIDA1),
                DUC_VALIDA2 = COALESCE(source.DUC_VALIDA2, target.DUC_VALIDA2),
                DUC_VALIDA3 = COALESCE(source.DUC_VALIDA3, target.DUC_VALIDA3),
                DUC_VALMD = COALESCE(source.DUC_VALMD, target.DUC_VALMD),
                DUC_VALPREIDA2 = COALESCE(source.DUC_VALPREIDA2, target.DUC_VALPREIDA2),
                DUC_VALPREIDA3 = COALESCE(source.DUC_VALPREIDA3, target.DUC_VALPREIDA3),
                DUC_VALH17 = COALESCE(source.DUC_VALH17, target.DUC_VALH17),
                DUC_VALH13 = COALESCE(source.DUC_VALH13, target.DUC_VALH13)
        WHEN NOT MATCHED THEN
            INSERT (DUC_FECHA, DUC_UP, DUC_HORA, DUC_INTERV,
                    DUC_VALCIERR, DUC_VALIDA1, DUC_VALIDA2, DUC_VALIDA3,
                    DUC_VALMD, DUC_VALPREIDA2, DUC_VALPREIDA3, DUC_VALH17, DUC_VALH13)
            VALUES (source.DUC_FECHA, source.DUC_UP, source.DUC_HORA, source.DUC_INTERV,
                    source.DUC_VALCIERR, source.DUC_VALIDA1, source.DUC_VALIDA2, source.DUC_VALIDA3,
                    source.DUC_VALMD, source.DUC_VALPREIDA2, source.DUC_VALPREIDA3, source.DUC_VALH17, source.DUC_VALH13);
    """
    cursor.execute(merge_query)
    print("\t5. - MERGE FINALIZADO CON EXITO.")

except Exception as e:
    print(f"Error procesando la base de datos: {e}")
finally:
    conn.close()

try:
    archivos_xml = [f for f in os.listdir(ruta_carpeta) if f.endswith('.xml')]
    for archivo in archivos_xml:
        archivo_path = os.path.join(ruta_carpeta, archivo)
        os.remove(archivo_path)
except Exception as e:
    print(f"Error al eliminar archivos XML: {e}") 
    
print("\n- PROCESO FINALIZADO")