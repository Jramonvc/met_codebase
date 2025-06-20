import os
import esios
import pandas as pd
from datetime import datetime, timedelta, timezone
import calendar
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
import re

ruta_base = r"C:\Procesos\Desarrollos_QIA\insert_daily_indicators_esios_web\Liquicomun\A1"
os.chdir(ruta_base)

#Aux Vars
esios_token = '28c83fff5780b353ba90dd62b4ec8db6b39eb9b7f7d08730ddfd36a478d95704'
os.environ['ESIOS_API_KEY'] = esios_token
client = esios.ESIOSClient()
endpoint = client.endpoint(name='archives')
fecha_hoy = datetime.today().strftime("%Y-%m-%d")
folder_path_a1 = None
files_aux = os.path.join("files_aux", f"{fecha_hoy}") 

def main():
    os.makedirs(files_aux, exist_ok=True)
    carpetas_descargadas = descarga_liquicomun()  # Descarga liquicomun
    
    if not carpetas_descargadas:
        print("No se descargaron carpetas para procesar.")
        write_log("No se descargaron carpetas para procesar.")
        raise

    for carpeta_path in carpetas_descargadas:
        global folder_path_a1
        folder_path_a1 = carpeta_path  # Redefinimos la ruta activa
        print(f"\n=== Procesando carpeta: {folder_path_a1} ===")

        # 1. Obtención de ficheros
        df_a1_prdvdatos = tratamientos_segmentos_prdvdatos()
        df_a1_compodem = tratamiento_compodem()
        df_a1_perd_h = tratamiento_perdidas_h()
        df_a1_perd_qh = tratamiento_perdidas_cuarto_qh()
        df_a1_period_h = tratamiento_periodos_h()
        df_a1_kestimado_h = tratamiento_kestimada_h()
        df_a1_kestimado_qh = tratamiento_kestimada_kh()
        df_a1_prdemcad_h = tratamientos_segmentos_cad()

        # 2. Normalización: si alguna función devuelve None, convertirlo en un DataFrame vacío
        dfs = {
            "df_a1_prdvdatos": df_a1_prdvdatos,
            "df_a1_compodem": df_a1_compodem,
            "df_a1_perd_h": df_a1_perd_h,
            "df_a1_perd_qh": df_a1_perd_qh,
            "df_a1_period_h": df_a1_period_h,
            "df_a1_kestimado_h": df_a1_kestimado_h,
            "df_a1_kestimado_qh": df_a1_kestimado_qh,
            "df_a1_prdemcad_h": df_a1_prdemcad_h,
        }
        for key in dfs:
            if dfs[key] is None:
                dfs[key] = pd.DataFrame()

        df_a1_prdvdatos = dfs["df_a1_prdvdatos"]
        df_a1_compodem = dfs["df_a1_compodem"]
        df_a1_perd_h = dfs["df_a1_perd_h"]
        df_a1_perd_qh = dfs["df_a1_perd_qh"]
        df_a1_period_h = dfs["df_a1_period_h"]
        df_a1_kestimado_h = dfs["df_a1_kestimado_h"]
        df_a1_kestimado_qh = dfs["df_a1_kestimado_qh"]
        df_a1_prdemcad_h = dfs["df_a1_prdemcad_h"]

        # 3. Combinar pérdidas con tarifas (solo si hay datos)
        if not df_a1_period_h.empty and not df_a1_perd_h.empty:
            df_perd_tarif_h = combinar_perdidas_tarifas_h(df_a1_period_h, df_a1_perd_h) 
        else:
            print("[WARNING] No se combinan pérdidas horarias por falta de datos.")
            df_perd_tarif_h = pd.DataFrame()

        if not df_a1_period_h.empty and not df_a1_perd_qh.empty:
            df_perd_tarif_qh = combinar_perdidas_tarifas_qh(df_a1_period_h, df_a1_perd_qh) 
        else:
            print("[WARNING] No se combinan pérdidas cuarto-horarias por falta de datos.")
            df_perd_tarif_qh = pd.DataFrame()

        # 4. DataFrames a insertar
        dataframes_to_insert = {
            "MET_A1A2PERDPETARQH": df_perd_tarif_qh,
            "MET_A1A2PERDPETARH": df_perd_tarif_h,
            "MET_A1A2KESTIMADAQH": df_a1_kestimado_qh,
            "MET_A1A2KESTIMADAH": df_a1_kestimado_h,
            "MET_A1A2PRDEMCAD": df_a1_prdemcad_h,
            "MET_A1A2COMPODEM": df_a1_compodem,
            "MET_A1A2PRDVDATOS": df_a1_prdvdatos
        }

        # 5. Conexión a base de datos
        engine = crear_conexion_bd()

        # 6. Eliminación de registros antiguos
        print("\nELIMINANDO REGISTROS POR RANGO DE FECHA")
        columnas_fecha = {
            "MET_A1A2PERDPETARQH": "APQ_FECHA",
            "MET_A1A2PERDPETARH": "APH_FECHA",
            "MET_A1A2KESTIMADAQH": "A1Q_FECHA",
            "MET_A1A2KESTIMADAH": "A1H_FECHA",
            "MET_A1A2PRDEMCAD": "A1D_FECHA",
            "MET_A1A2COMPODEM": "A1C_FECHA",
            "MET_A1A2PRDVDATOS": "APV_FECHA"
        }
        for table_name, df in dataframes_to_insert.items():
            if df.empty:
                print(f"[INFO] No se eliminan registros para {table_name} porque el DataFrame está vacío.")
                continue
            fecha_columna = columnas_fecha.get(table_name)
            if fecha_columna:
                eliminar_registros_por_rango(engine, df, table_name, fecha_columna)
            else:
                print(f"[WARNING] No se encontró columna fecha para {table_name}.")

        # 7. Inserción en base de datos
        for table_name, df in dataframes_to_insert.items():
            insert_dataframe_to_db(df, table_name, engine, schema="METDB")



def eliminar_registros_por_rango(engine, df, table_name, fecha_columna, schema="METDB"):
    if df.empty:
        print(f"[INFO] El DataFrame para {table_name} está vacío. No se elimina nada.")
        return
    
    try:
        fecha_min = df[fecha_columna].min()
        fecha_max = df[fecha_columna].max()

        if pd.isnull(fecha_min) or pd.isnull(fecha_max):
            print(f"[WARNING] No se encontraron fechas válidas en la columna {fecha_columna} del DataFrame.")
            return

        print(f"\n[INFO] Eliminando registros de {table_name} entre {fecha_min} y {fecha_max}")

        delete_query = text(f"""
            DELETE FROM {schema}.{table_name}
            WHERE {fecha_columna} BETWEEN :fecha_min AND :fecha_max
        """)

        with engine.begin() as connection:
            result = connection.execute(delete_query, {
                "fecha_min": fecha_min,
                "fecha_max": fecha_max
            })
            print(f"\t{result.rowcount} registros eliminados de {schema}.{table_name}")

    
    except Exception as e:
        print(f"[ERROR] Error eliminando registros de {table_name}: {e}")

def tratamientos_segmentos_prdvdatos():
    def parse_float(val):
        try:
            return float(val.strip().replace(",", ".")) if val.strip() else None
        except:
            return None

    try:
        write_log("LIMPIEZA Y TRATAMIENTO DE SEGMENTOS PRDVDATOS")
        print("\nLIMPIEZA Y TRATAMIENTO DE SEGMENTOS PRDVDATOS")

        files = [f for f in os.listdir(folder_path_a1) if f.startswith("A1_prdvdatos")]
        if not files:
            write_log("No se encontraron archivos A1_prdvdatos para procesar.")
            print("No se encontraron archivos A1_prdvdatos para procesar.")
            return None

        data_list = []

        for file_name in files:
            file_path = os.path.join(folder_path_a1, file_name)
            print(f"\tProcesando archivo: {file_name}")

            try:
                with open(file_path, "r", encoding="utf-8") as file:
                    lines = file.readlines()

                if len(lines) < 3:
                    continue

                for line in lines[2:]:
                    if line.strip() == "*" or not line.strip():
                        continue

                    parts = line.strip().split(";")

                    # Rellenar con valores vacíos si hay menos de 15-16 columnas
                    while len(parts) < 16:
                        parts.append("")

                    fecha_raw = parts[0].strip()
                    if not fecha_raw or fecha_raw == "*" or not re.match(r"\d{2}/\d{2}/\d{4}", fecha_raw):
                        continue

                    try:
                        row = [
                            datetime.strptime(fecha_raw, "%d/%m/%Y").date(),  # APV_FECHA
                            parse_float(parts[1]),                             # APV_HORA
                            parse_float(parts[2]),                             # APV_QHORARIO
                            parse_float(parts[3]),                             # APV_FRRSUB
                            parse_float(parts[4]),                             # APV_FRRBAJ
                            parse_float(parts[5]),                             # APV_RRSUB
                            parse_float(parts[6]),                             # APV_RRBAJ
                            parse_float(parts[7]),                             # APV_DESVSIST
                            parse_float(parts[8]),                             # APV_PBALSUB
                            parse_float(parts[9]),                             # APV_PBALBAJ
                            parse_float(parts[10]),                            # APV_PORFRR
                            parts[11].strip() if parts[11].strip() else None,  # APV_TIPPRECIO
                            parse_float(parts[12]),                            # APV_PRECDDESSUB
                            parse_float(parts[13]),                            # APV_PRECDDESBAJ
                            parse_float(parts[14]) if len(parts) > 14 else None  # APV_PRECUDES
                        ]
                        data_list.append(row)
                    except Exception as e:
                        write_log(f"Error procesando línea en {file_name}: {line.strip()} - {e}")
                        print(f"Error en línea:\n{line.strip()}")
                        print(f"Exception: {e}")
                        continue

            except Exception as e:
                write_log(f"Error general al procesar el archivo {file_name}: {e}")
                print(f"Error general al procesar el archivo {file_name}: {e}")

        if not data_list:
            write_log("No se generaron datos válidos para PRDVDATOS, no se creará el archivo Excel.")
            print("No se generaron datos válidos para PRDVDATOS, no se creará el archivo Excel.")
            return None

        df = pd.DataFrame(data_list, columns=[
            "APV_FECHA",
            "APV_HORA",
            "APV_QHORARIO",
            "APV_FRRSUB",
            "APV_FRRBAJ",
            "APV_RRSUB",
            "APV_RRBAJ",
            "APV_DESVSIST",
            "APV_PBALSUB",
            "APV_PBALBAJ",
            "APV_PORFRR",
            "APV_TIPPRECIO",
            "APV_PRECDDESSUB",
            "APV_PRECDDESBAJ",
            "APV_PRECUDES"
        ])

        # Eliminar filas con fecha '*', por seguridad extra
        df = df[df["APV_FECHA"] != "*"]
        df = df.dropna(how='all')  # Eliminar filas completamente vacías

        output_path = os.path.join(files_aux, f"a1_prdvdatos_h_{fecha_hoy}.xlsx")
        df.to_excel(output_path, index=False)

        print(f"Archivo Excel generado con éxito: {output_path}")
        return df

    except Exception as e:
        write_log(f"Error general en el procesamiento de a2_prdvdatos: {e}")
        print(f"Error general en el procesamiento de a2_prdvdatos: {e}")
        
def tratamiento_compodem():
    try:
        print("\nLIMPIEZA Y TRATAMIENTO DE ARCHIVOS A2_COMPODEM")
        files = [
            f for f in os.listdir(folder_path_a1)
            if f.startswith("A1_compodem") 
        ]

        data = []

        for file_name in files:
            file_path = os.path.join(folder_path_a1, file_name)
            print(f"\tProcesando archivo: {file_name}")
            
            # Extraer fecha del archivo
            fecha_archivo = file_name.split("_")[2].split(".")[0]  # Extraer YYYYMMDD
            fecha_archivo = datetime.strptime(fecha_archivo, "%Y%m%d")
            
            # Leer el archivo .data
            with open(file_path, "r") as file:
                for line in file:
                    if line.startswith("compodem;") or line.startswith("2025") or line.startswith("2024")  or line.startswith("*"):  # Saltar encabezado
                        continue
                    
                    elementos = [e.strip() for e in line.strip().split(";") if e.strip()]
                    if len(elementos) == 7:  
                        fecha, hora, componente, tipo, valor_principal, divisor, indice = elementos
                        fechaf = datetime.strptime(fecha, "%d/%m/%Y").strftime("%Y-%m-%d")
                        # Agregar los datos al listado
                        data.append({
                            "nombre_fichero": file_name,
                            "Fecha": fechaf,
                            "Hora": int(hora),
                            "Componente": componente,
                            "Tipo": tipo,
                            "Valor": float(valor_principal.replace(",", ".")),
                            "Divisor": float(divisor.replace(",", ".")),
                            "Índice": float(indice.replace(",", "."))
                        })

        # Crear un DataFrame con los datos procesados
        df = pd.DataFrame(data)
        
        # Guardar el DataFrame en un archivo Excel
        output_path = os.path.join(files_aux, f"a1_compodem_{fecha_hoy}.xlsx")
        df.to_excel(output_path, index=False)
        print(f"Archivo Excel creado: {output_path}")
        if "nombre_fichero" in df.columns:
            df.drop(columns=["nombre_fichero"], inplace=True)

        df.rename(columns={
            "Fecha": "A1C_FECHA",
            "Hora": "A1C_HORA",
            "Componente": "A1C_COMPONENTE",
            "Tipo": "A1C_TIPO",
            "Valor": "A1C_VALOR",
            "Divisor": "A1C_DIVISOR",
            "Índice": "A1C_INDICE",
        }, inplace=True)
        return df

    except Exception as e:
        write_log(f"Error tratando compodem:{e}")
        print(f"Error tratando compodem:{e}")
        
def eliminar_registros_previos(engine, schema="METDB"):
    now = datetime.now()
    current_year = now.year
    current_month = now.month
    print("\nELIMINANDO REGISTROS PREVIOS DE LAS TABLAS")
    
    columnas_fecha = {
        "MET_A1A2PERDPETARQH": "APQ_FECHA",
        "MET_A1A2PERDPETARH": "APH_FECHA",
        "MET_A1A2KESTIMADAQH": "A1Q_FECHA",
        "MET_A1A2KESTIMADAH": "A1H_FECHA",
        "MET_A1A2PRDEMCAD": "A1D_FECHA",
        "MET_A1A2COMPODEM": "A1C_FECHA",
        "MET_A1A2PRDVDATOS": "APV_FECHA"
    }
   
    try:
        with engine.begin() as connection:  
            for table, fecha_columna in columnas_fecha.items():
                delete_query = text(f"""
                    DELETE FROM {schema}.{table}
                    WHERE YEAR({fecha_columna}) = :year AND MONTH({fecha_columna}) = :month
                """)
                
                result = connection.execute(delete_query, {"year": current_year, "month": current_month})
                print(f"\tBorrados {result.rowcount} registros de la tabla {table} correctamente")
            write_log("REGISTROS ELIMINADOS CORRECTAMENTE")
    except Exception as e:
        print(f"Error eliminando datos en {schema}: {e}")
        write_log(f"Error eliminando datos en {schema}: {e}")


def insert_dataframe_to_db(df: pd.DataFrame, table_name: str, engine, schema: str = "METDB"):
    print(f"\nINSERTANDO REGISTROS EN {table_name}")
    try:
        if df.empty:
            print(f"Ninguna columna del DataFrame coincide con {schema}.{table_name}.")
            return
        df.to_sql(table_name, engine, schema=schema, if_exists='append', index=False)
        print(f"\t{len(df)} registros insertados correctamente en {schema}.{table_name}.")
    
    except IntegrityError as e:
        if '2627' in str(e.orig):  # Error de clave primaria duplicada
            print(f"Clave primaria duplicada en {schema}.{table_name}. Se omiten registros duplicados.")
            write_log(f"Clave primaria duplicada en {schema}.{table_name}. Se omiten registros duplicados.")
        else:
            print(f"Error de integridad en {schema}.{table_name}: {e}")
            write_log(f"Error de integridad en {schema}.{table_name}: {e}")

    
    except Exception as e:
        print(f"Error insertando datos en {schema}.{table_name}: {e}")
        write_log(f"Error insertando datos en {schema}.{table_name}: {e}")
        
def crear_conexion_bd():
    # Configuración de la conexión a la base de datos
    try:
        conn_str = r"mssql+pyodbc://sqluser:cwD9KVms4Qdv4mLy@met-esp-prod.database.windows.net:1433/Risk_MGMT_Spain?driver=ODBC+Driver+17+for+SQL+Server"
        engine = create_engine(conn_str, fast_executemany=True)
        write_log("CONEXION EXITOSA CON LA BBDD DE MET")
        print("\nCONEXION EXITOSA CON LA BBDD DE MET")
        return engine
    except Exception as e:
        print(f"Error conectando a la base de datos: {e}")
        write_log(f"Error conectando a la base de datos: {e}")
        return
        
def combinar_perdidas_tarifas_qh(df_a1_period_h, df_a1_perd_qh):
    try:
        write_log("COMBINANDO PERDIDAS CUARTO-HORARIAS CON TARIFAS")
        print("\nCOMBINANDO PERDIDAS CUARTO-HORARIAS CON TARIFAS")

        # Normalizar las fechas
        df_a1_perd_qh["Fecha"] = pd.to_datetime(df_a1_perd_qh["Fecha"], format="%d/%m/%Y")
        df_a1_period_h["Fecha"] = pd.to_datetime(df_a1_period_h["Fecha"], format="%Y-%m-%d")
        # Normalizar las zonas
        df_a1_perd_qh["Zona"] = df_a1_perd_qh["Zona"].str.strip().str.upper()
        df_a1_period_h["Zona"] = df_a1_period_h["Zona"].str.strip().str.upper()
        # Filtrar tarifas en df_a1_period_h según las reglas
        df_period_3P = df_a1_period_h[df_a1_period_h["tarifa"] == "3P"]
        df_period_6P = df_a1_period_h[df_a1_period_h["tarifa"] == "6P"]
        # Verificar que los DataFrames no estén vacíos antes de continuar
        if df_period_3P.empty:
            write_log("Advertencia: df_period_3P está vacío. Verifica las tarifas en df_a1_period_h.")
        if df_period_6P.empty:
            write_log("Advertencia: df_period_6P está vacío. Verifica las tarifas en df_a1_period_h.")
        # Crear una copia del DataFrame de pérdidas para no modificar el original
        df_combinado = df_a1_perd_qh.copy()
        # Agregar la columna 'Valor_periodo' inicializada en NaN
        df_combinado["Valor_periodo"] = None
        # Lista para almacenar registros sin coincidencia
        sin_coincidencia = []
        # Iterar sobre las filas de df_a1_perd_qh y asignar el valor correspondiente de df_a1_period_h
        for index, row in df_combinado.iterrows():
            try:
                # Obtener valores clave para la combinación
                tarifa_perd = row["tarifa"]
                zona = row["Zona"]
                fecha = row["Fecha"]
                hora = row["Hora"]
                # Seleccionar el DataFrame de periodos según la tarifa
                if tarifa_perd.startswith("2"):
                    periodo = df_period_3P
                elif tarifa_perd.startswith("3") or tarifa_perd.startswith("6"):
                    periodo = df_period_6P
                else:
                    continue

                # Filtrar el periodo que coincide con la zona, fecha y hora
                match = periodo[
                    (periodo["Zona"] == zona) & 
                    (periodo["Fecha"] == fecha) & 
                    (periodo["Hora"] == hora)
                ]

                # Si hay coincidencia, asignar el valor del periodo
                if not match.empty:
                    df_combinado.at[index, "Valor_periodo"] = match.iloc[0]["Valor"]
                else:
                    # Registrar los casos donde no hay coincidencia
                    sin_coincidencia.append({
                        "nombre_fichero_perd": row["nombre_fichero"],
                        "tarifa_perd": tarifa_perd,
                        "Zona": zona,
                        "Fecha": fecha,
                        "Hora": hora
                    })

            except Exception as e:
                write_log(f"Error procesando la fila {index}: {e}")

        # Mostrar los registros sin coincidencia
        if sin_coincidencia:
            write_log(f"Se encontraron {len(sin_coincidencia)} registros sin coincidencia.")
        else:
            write_log("Todos los registros tienen coincidencia.")
            print("\tTodos los registros tienen coincidencia.")
            
        if "nombre_fichero" in df_combinado.columns:
            df_combinado.drop(columns=["nombre_fichero"], inplace=True)
            
        df_combinado.rename(columns={
            "tarifa": "APQ_TARIFA",
            "Zona": "APQ_ZONA",
            "Fecha": "APQ_FECHA",
            "Hora": "APQ_HORA",
            "Cuarto_Horario_Global": "APQ_CHORARIO",
            "Valor": "APQ_PERDIDA",
            "Valor_periodo": "APQ_PERIODO"
        }, inplace=True)
        # Guardar el resultado en un archivo Excel
        output_path = os.path.join(files_aux, f"df_combinado_perd_period_qh_{fecha_hoy}.xlsx")
        df_combinado.to_excel(output_path, index=False)
        print(f"Archivo combinado guardado en: {output_path}")
        return df_combinado
    except Exception as e:
        write_log(f"Error general en la combinación de pérdidas cuarto-horarias con tarifas: {e}")
        print(f"Error general en la combinación: {e}")

def combinar_perdidas_tarifas_h(df_a1_period_h, df_a1_perd_h):
    try:
        write_log("COMBINANDO PERDIDAS HORARIAS CON TARIFAS")
        print("\nCOMBINANDO PERDIDAS HORARIAS CON TARIFAS")

        # Filtrar tarifas en df_a1_period_h según las reglas
        df_period_3P = df_a1_period_h[df_a1_period_h["tarifa"] == "3P"]
        df_period_6P = df_a1_period_h[df_a1_period_h["tarifa"] == "6P"]

        # Crear una copia del DataFrame de pérdidas para no modificar el original
        df_combinado = df_a1_perd_h.copy()

        # Agregar la columna 'Valor_periodo' inicializada en NaN
        df_combinado["Valor_periodo"] = None

        # Lista para almacenar registros sin coincidencia
        sin_coincidencia = []

        # Iterar sobre las filas de df_a1_perd_h y asignar el valor correspondiente de df_a1_period_h
        for index, row in df_combinado.iterrows():
            try:
                # Obtener valores clave para la combinación
                tarifa_perd = row["tarifa"]
                zona = row["Zona"]
                fecha = row["Fecha"]
                hora = row["Hora"]

                # Seleccionar el DataFrame de periodos según la tarifa
                if tarifa_perd.startswith("2"):
                    periodo = df_period_3P
                elif tarifa_perd.startswith("3") or tarifa_perd.startswith("6"):
                    periodo = df_period_6P
                else:
                    continue

                # Filtrar el periodo que coincide con la zona, fecha y hora
                match = periodo[
                    (periodo["Zona"] == zona) & 
                    (periodo["Fecha"] == fecha) & 
                    (periodo["Hora"] == hora)
                ]

                # Si hay coincidencia, asignar el valor del periodo
                if not match.empty:
                    df_combinado.at[index, "Valor_periodo"] = match.iloc[0]["Valor"]
                else:
                    # Registrar los casos donde no hay coincidencia
                    sin_coincidencia.append({
                        "nombre_fichero_perd": row["nombre_fichero"],
                        "tarifa_perd": tarifa_perd,
                        "Zona": zona,
                        "Fecha": fecha,
                        "Hora": hora
                    })

            except Exception as e:
                write_log(f"Error al procesar la fila {index}: {e}")

        # Mostrar los registros sin coincidencia
        if sin_coincidencia:
            write_log(f"Se encontraron {len(sin_coincidencia)} registros sin coincidencia.")
    
        else:
            write_log("Todos los registros tienen coincidencia.")
            print("\tTodos los registros tienen coincidencia.")

        if "nombre_fichero" in df_combinado.columns:
            df_combinado.drop(columns=["nombre_fichero"], inplace=True)
            
        df_combinado.rename(columns={
            "tarifa": "APH_TARIFA",
            "Zona": "APH_ZONA",
            "Fecha": "APH_FECHA",
            "Hora": "APH_HORA",
            "Valor": "APH_PERDIDA",
            "Valor_periodo": "APH_PERIODO",
        }, inplace=True)
        
        # Guardar el resultado en un archivo Excel
        output_path = os.path.join(files_aux, f"df_combinado_perd_period_h_{fecha_hoy}.xlsx")
        df_combinado.to_excel(output_path, index=False)
        print(f"Archivo combinado guardado en: {output_path}")
        return df_combinado

    except Exception as e:
        write_log(f"Error general en la combinación de pérdidas horarias con tarifas: {e}")
        print(f"Error general en la combinación: {e}")

def tratamientos_segmentos_cad():
    try:
        write_log("LIMPIEZA Y TRATAMIENTO DE SEGMENTOS CAD")
        print("\nLIMPIEZA Y TRATAMIENTO DE SEGMENTOS CAD")

        # Filtrar archivos que comiencen con "A1_prdemcad"
        files = [f for f in os.listdir(folder_path_a1) if f.startswith("A1_prdemcad")]
        if not files:
            write_log("No se encontraron archivos A1_PRDEMCAD para procesar.")
            print("No se encontraron archivos A1_PRDEMCAD para procesar.")

        # Lista para almacenar los datos combinados
        data_list = []

        # Procesar cada archivo encontrado
        for file_name in files:
            file_path = os.path.join(folder_path_a1, file_name)
            print(f"\tProcesando archivo: {file_name}")

            try:
                # Leer el archivo línea por línea
                with open(file_path, "r", encoding="utf-8") as file:
                    lines = file.readlines()

                # Verificar si el archivo tiene suficientes líneas
                if len(lines) < 3:
                    continue  # Salta este archivo y sigue con los demás

                # Extraer el año desde la segunda línea
                year = lines[1].split(";")[0].strip()
                # Extraer el mes desde el nombre del archivo
                month = file_name.split("_")[2][4:6]  # Obtiene los caracteres 5 y 6 (MM)
                # Procesar cada línea de datos
                for line in lines[2:]:  # Saltamos las dos primeras líneas
                    parts = line.strip().split(";")

                    if len(parts) > 1:
                        try:
                            # Extraer correctamente el día del mes
                            day = parts[0].split(" ")[-1]  # Obtiene solo el número del día
                            # Extraer valores horarios y limpiar celdas vacías
                            values = [v.strip() for v in parts[1:-2] if v.strip()]
                            year = int(year)
                            month = int(month)
                            day = int(day)
                            fecha_str = f"{year:04d}-{month:02d}-{day:02d}"
                            # Crear registros para cada hora
                            for hour, value in enumerate(values, start=1):
                                data_list.append([
                                    fecha_str,
                                    hour,  # Hora
                                    float(value),  # Convertir valor a número
                                ])
                        except Exception as e:
                            write_log(f"Error procesando línea en {file_name}: {line.strip()} - {e}")

            except Exception as e:
                write_log(f"Error general al procesar el archivo {file_name}: {e}")
                print(f"Error general al procesar el archivo {file_name}: {e}")

        # Verificar si se recopilaron datos antes de generar el Excel
        if not data_list:
            write_log("No se generaron datos válidos, no se creará el archivo Excel.")
            print("No se generaron datos válidos, no se creará el archivo Excel.")
        else:
            # Crear un DataFrame con los datos recopilados
            df = pd.DataFrame(data_list, columns=["fecha_str", "Hora", "Valor"])
            df.rename(columns={
            "fecha_str": "A1D_FECHA",
            "Hora": "A1D_HORA",
            "Valor": "A1D_COSTDEM",
            }, inplace=True)
            # Guardar en un archivo Excel consolidado
            output_path = os.path.join(files_aux, f"a1_prdemcad_h_{fecha_hoy}.xlsx")
            df.to_excel(output_path, index=False)

            print(f"Archivo Excel generado con éxito: {output_path}")
            return df
    except Exception as e:
        write_log(f"Error general en el procesamiento de A1_PRDEMCAD: {e}")
        print(f"Error general en el procesamiento: {e}")

def tratamiento_kestimada_kh():
    try:
        write_log("LIMPIEZA Y TRATAMIENTO DE ARCHIVOS DE K ESTIMADAS CUARTOHORARIO")
        print("\nLIMPIEZA Y TRATAMIENTO DE ARCHIVOS DE K ESTIMADAS CUARTOHORARIO")

        # Filtrar archivos que comiencen con "A1_Kestimqh"
        files = [f for f in os.listdir(folder_path_a1) if f.startswith("A1_Kestimqh")]
        if not files:
            write_log("No se encontraron archivos A1_Kestimqh para procesar en A1_liquicomun")
        data = []
        for file_name in files:
            file_path = os.path.join(folder_path_a1, file_name)
            print(f"\tProcesando archivo: {file_name}")
            try:
                # Extraer fechas del nombre del archivo
                fecha_inicio = file_name.split("_")[2]  # Fecha de inicio
                fecha_fin = file_name.split("_")[3].split(".")[0]  # Fecha de fin
                fecha_inicio = datetime.strptime(fecha_inicio, "%Y%m%d")
                fecha_fin = datetime.strptime(fecha_fin, "%Y%m%d")

                with open(file_path, "r") as file:
                    cuarto_horario_global = 0  # Contador para los cuartos horarios dentro de un día
                    for line in file:
                        if line.startswith(("Kestimqh;", "2025", "*")):  # Saltar encabezado
                            continue

                        # Normalizar la línea eliminando caracteres adicionales
                        line = line.strip().strip(";")  # Elimina espacios y separadores al final
                        valores = line.split(";")  # Dividir por el separador
                        # Validar la cantidad de valores
                        if len(valores) != 4:
                            continue  # Saltar líneas que no cumplan el formato esperado
                        try:
                            # Extraer valores
                            fecha, hora, cuarto_horario, valor = valores
                            # Calcular el cuarto horario global
                            cuarto_horario_global = (int(hora) - 1) * 4 + int(cuarto_horario)
                            data.append({
                                "nombre_fichero": file_name,
                                "Fecha": fecha,
                                "Hora": int(hora),
                                "Cuarto_Horario_Global": cuarto_horario_global,  # Cuarto horario único para el día
                                "Valor": float(valor)
                            })
                            # Reiniciar el contador si es un nuevo día
                            if cuarto_horario_global == 96:
                                cuarto_horario_global = 0
                        except ValueError as e:
                            write_log(f"Error procesando línea en {file_name}: {line} - {e}")
                            continue

            except Exception as e:
                write_log(f"Error al procesar el archivo {file_name}: {e}")
                print(f"Error al procesar {file_name}: {e}")

        # Crear un DataFrame con los datos procesados
        df_a1_kestimado_qh = pd.DataFrame(data)
        
        if "nombre_fichero" in df_a1_kestimado_qh.columns:
            df_a1_kestimado_qh.drop(columns=["nombre_fichero"], inplace=True)
            
        df_a1_kestimado_qh.rename(columns={
            "Fecha": "A1Q_FECHA",
            "Hora": "A1Q_HORA",
            "Cuarto_Horario_Global": "A1Q_CHORARIO",
            "Valor": "A1Q_VALOR",
        }, inplace=True)
        df_a1_kestimado_qh["A1Q_FECHA"] = pd.to_datetime(df_a1_kestimado_qh["A1Q_FECHA"], format="%d/%m/%Y")
        # Definir carpeta de salida y asegurarse de que existe
        output_path = os.path.join(files_aux, f"a1_kestimada_qh_{fecha_hoy}.xlsx")
        df_a1_kestimado_qh.to_excel(output_path, index=False)
        print(f"Archivo Excel creado: {output_path}")
        return df_a1_kestimado_qh

    except Exception as e:
        write_log(f"Error general en la limpieza y tratamiento de archivos A1_KESTIMQH: {e}")
        print(f"Error general en el procesamiento: {e}")


def tratamiento_kestimada_h():
    try:
        write_log("LIMPIEZA Y TRATAMIENTO DE ARCHIVOS DE K ESTIMADAS HORARIO")
        print("\nLIMPIEZA Y TRATAMIENTO DE ARCHIVOS DE K ESTIMADAS HORARIO")
        # Filtrar archivos que comiencen con "A1_Kestimado"
        files = [f for f in os.listdir(folder_path_a1) if f.startswith("A1_Kestimado")]
        if not files:
            write_log("No se encontraron archivos A1_KESTIMADO para procesar en A1_liquicomun")
        data = []
        for file_name in files:
            file_path = os.path.join(folder_path_a1, file_name)
            print(f"\tProcesando archivo: {file_name}")
            try:
                # Extraer fechas del nombre del archivo
                fecha_inicio = file_name.split("_")[2]  # Fecha de inicio
                fecha_fin = file_name.split("_")[3].split(".")[0]  # Fecha de fin
                fecha_inicio = datetime.strptime(fecha_inicio, "%Y%m%d")
                fecha_fin = datetime.strptime(fecha_fin, "%Y%m%d")
                # Leer el archivo .data
                with open(file_path, "r") as file:
                    for line in file:
                        if line.startswith(("Kestimado;", "2025", "*")):  # Saltar encabezado
                            continue
                        # Procesar línea
                        dia, *valores = line.split(";")
                        valores = [v for v in valores if v.strip()]  # Filtrar valores no vacíos
                        try:
                            # Calcular la fecha
                            dia_num = int(dia.split()[1])  # Obtener el número del día
                            fecha = fecha_inicio + timedelta(days=dia_num - 1)
                            # Crear un registro por cada hora
                            for hora, valor in enumerate(valores, start=1):
                                data.append({
                                    "nombre_fichero": file_name,
                                    "Fecha": fecha.strftime("%Y-%m-%d"),
                                    "Hora": hora,
                                    "Valor": float(valor)
                                })
                        except ValueError as e:
                            continue

            except Exception as e:
                write_log(f"Error al procesar el archivo {file_name}: {e}")
                print(f"Error al procesar {file_name}: {e}")

        # Crear un DataFrame con los datos procesados
        df_a1_kestimado_h = pd.DataFrame(data)
        if "nombre_fichero" in df_a1_kestimado_h.columns:
            df_a1_kestimado_h.drop(columns=["nombre_fichero"], inplace=True)
            
        df_a1_kestimado_h.rename(columns={
            "Fecha": "A1H_FECHA",
            "Hora": "A1H_HORA",
            "Valor": "A1H_VALOR",
        }, inplace=True)
        # Definir carpeta de salida y asegurarse de que existe
        output_path = os.path.join(files_aux, f"a1_kestimada_h_{fecha_hoy}.xlsx")
        df_a1_kestimado_h.to_excel(output_path, index=False)
        print(f"Archivo Excel creado: {output_path}")
        return df_a1_kestimado_h

    except Exception as e:
        write_log(f"Error general en la limpieza y tratamiento de archivos A1_KESTIMADO: {e}")
        print(f"Error general en el procesamiento: {e}")

def tratamiento_periodos_h():
    try:
        write_log("LIMPIEZA Y TRATAMIENTO DE ARCHIVOS DE PERIODOS HORARIOS (A1_PETAR / A1_SPETAR)")
        print("\nLIMPIEZA Y TRATAMIENTO DE ARCHIVOS DE PERIODOS HORARIOS (A1_PETAR / A1_SPETAR)")
        # Filtrar archivos que comiencen con "A1_petar" o "A1_Spetar"
        files = [
            f for f in os.listdir(folder_path_a1)
            if f.startswith("A1_petar") or f.startswith("A1_Spetar")
        ]
        if not files:
            write_log("No se encontraron archivos A1_PETAR / A1_SPETAR para procesar en A1_liquicomun")
        data = []
        for file_name in files:
            file_path = os.path.join(folder_path_a1, file_name)
            print(f"\tProcesando archivo: {file_name}")
            try:
                # Determinar si el archivo tiene zona explícita
                if file_name.startswith("A1_S"):
                    # Archivos con zona explícita
                    tarifa = file_name.split("_")[1].split("petar")[1]  # Extraer tipo periodo
                    zona = file_name.split("_")[2]  # Extraer la zona
                    fecha_inicio = file_name.split("_")[3]  # Fecha de inicio
                    fecha_fin = file_name.split("_")[4].split(".")[0]  # Fecha de fin
                else:
                    # Archivos sin zona explícita
                    tarifa = file_name.split("_")[1].split("petar")[1]  # Extraer tipo periodo
                    zona = "PENÍNSULA"  # Asignar zona predeterminada
                    fecha_inicio = file_name.split("_")[2]  # Fecha de inicio
                    fecha_fin = file_name.split("_")[3].split(".")[0]  # Fecha de fin
                fecha_inicio = datetime.strptime(fecha_inicio, "%Y%m%d")
                fecha_fin = datetime.strptime(fecha_fin, "%Y%m%d")
                # Leer el archivo .data
                with open(file_path, "r") as file:
                    for line in file:
                        if line.startswith(("petar", "Spetar", "2025", "*")):  # Saltar encabezado
                            continue
                        # Procesar línea
                        dia, *valores = line.split(";")
                        valores = [v for v in valores if v.strip()]  # Filtrar valores no vacíos
                        try:
                            # Calcular la fecha
                            dia_num = int(dia.split()[1])  # Obtener el número del día
                            fecha = fecha_inicio + timedelta(days=dia_num - 1)

                            # Crear un registro por cada hora
                            for hora, valor in enumerate(valores, start=1):
                                data.append({
                                    "nombre_fichero": file_name,
                                    "tarifa": tarifa,
                                    "Zona": zona,
                                    "Fecha": fecha.strftime("%Y-%m-%d"),
                                    "Hora": hora,
                                    "Valor": float(valor)
                                })
                        except ValueError as e:
                            continue

            except Exception as e:
                write_log(f"Error al procesar el archivo {file_name}: {e}")
                print(f"Error al procesar {file_name}: {e}")

        # Crear un DataFrame con los datos procesados
        df_a1_period_h = pd.DataFrame(data)

        # Guardar el DataFrame en un archivo Excel
        output_path = os.path.join(files_aux, f"a1_period_h_{fecha_hoy}.xlsx")
        df_a1_period_h.to_excel(output_path, index=False)

        print(f"Archivo Excel creado: {output_path}")
        return df_a1_period_h

    except Exception as e:
        write_log(f"Error general en la limpieza y tratamiento de archivos A1_PETAR / A1_SPETAR: {e}")
        print(f"Error general en el procesamiento: {e}")

def tratamiento_perdidas_cuarto_qh():
    try:
        write_log("LIMPIEZA Y TRATAMIENTO DE ARCHIVOS DE PERDIDAS CUARTO HORARIAS (A1_PERDQH)")
        print("\nLIMPIEZA Y TRATAMIENTO DE ARCHIVOS DE PERDIDAS CUARTO HORARIAS (A1_PERDQH)")

        # Filtrar archivos que comiencen con "A1_perdqh"
        files = [f for f in os.listdir(folder_path_a1) if f.startswith("A1_perdqh")]

        if not files:
            write_log("No se encontraron archivos A1_PERDQH para procesar en A1_liquicomun")

        data = []

        for file_name in files:
            file_path = os.path.join(folder_path_a1, file_name)
            print(f"\tProcesando archivo: {file_name}")

            try:
                # Extraer tarifa y fechas del nombre del archivo
                tarifa = file_name.split("_")[1].split("perdqh")[1]  
                fecha_inicio = file_name.split("_")[2]
                fecha_fin = file_name.split("_")[3].split(".")[0]
                fecha_inicio = datetime.strptime(fecha_inicio, "%Y%m%d")
                fecha_fin = datetime.strptime(fecha_fin, "%Y%m%d")
                zona = "PENÍNSULA"  # Actualmente solo hay QH para península

                with open(file_path, "r") as file:
                    cuarto_horario_global = 0  # Contador para los cuartos horarios dentro de un día

                    for line in file:
                        if line.startswith(("perdqh;", "2025", "*")):  # Saltar encabezado
                            continue
                        
                        # Normalizar la línea eliminando caracteres adicionales
                        line = line.strip().strip(";")  # Elimina espacios y separadores al final
                        valores = line.split(";")  # Dividir por el separador

                        # Validar la cantidad de valores
                        if len(valores) != 4:
                            continue  # Saltar líneas que no cumplan el formato esperado
                        
                        try:
                            # Extraer valores
                            fecha, hora, cuarto_horario, valor = valores

                            # Calcular el cuarto horario global
                            cuarto_horario_global = (int(hora) - 1) * 4 + int(cuarto_horario)

                            data.append({
                                "nombre_fichero": file_name,
                                "tarifa": tarifa,
                                "Zona": zona, 
                                "Fecha": fecha,
                                "Hora": int(hora),
                                "Cuarto_Horario_Global": cuarto_horario_global,  # Cuarto horario único para el día
                                "Valor": float(valor)
                            })

                            # Reiniciar el contador si es un nuevo día
                            if cuarto_horario_global == 96:
                                cuarto_horario_global = 0

                        except ValueError as e:
                            write_log(f"Error procesando línea en {file_name}: {line} - {e}")
                            continue

            except Exception as e:
                write_log(f"Error al procesar el archivo {file_name}: {e}")
                print(f"Error al procesar {file_name}: {e}")

        # Crear un DataFrame con los datos procesados
        df_a1_perd_qh = pd.DataFrame(data)

        # Guardar el DataFrame en un archivo Excel
        output_path = os.path.join(files_aux, f"a1_perd_qh_{fecha_hoy}.xlsx")
        df_a1_perd_qh.to_excel(output_path, index=False)

        print(f"Archivo Excel creado: {output_path}")
        return df_a1_perd_qh

    except Exception as e:
        write_log(f"Error general en la limpieza y tratamiento de archivos A1_PERDQH: {e}")
        print(f"Error general en el procesamiento: {e}")


def tratamiento_perdidas_h():
    try:
        write_log("LIMPIEZA Y TRATAMIENTO DE ARCHIVOS DE PERDIDAS HORARIAS (A1_PERD / A1_SPERD)")
        print("\nLIMPIEZA Y TRATAMIENTO DE ARCHIVOS DE PERDIDAS HORARIAS (A1_PERD / A1_SPERD)")

        # Filtrar archivos que comiencen con "A1_perd" 
        files = [
            f for f in os.listdir(folder_path_a1)
            if (f.startswith("A1_perd") or f.startswith("A1_Sperd")) 
            and not f.startswith("A1_perdqh") 
            and not f.startswith("A1_perddema")
        ]

        if not files:
            write_log("No se encontraron archivos para procesar en A1_liquicomun")

        data = []

        for file_name in files:
            file_path = os.path.join(folder_path_a1, file_name)
            print(f"\tProcesando archivo: {file_name}")

            try:
                # Determinar si el archivo tiene zona explícita
                if file_name.startswith("A1_S"):
                    # Archivos con zona explícita
                    tarifa = file_name.split("_")[1].split("perd")[1]  # Extraer tarifa
                    zona = file_name.split("_")[2]  # Extraer la zona
                    fecha_inicio = file_name.split("_")[3]  # Fecha de inicio
                    fecha_fin = file_name.split("_")[4].split(".")[0]  # Fecha de fin
                else:
                    # Archivos sin zona explícita
                    tarifa = file_name.split("_")[1].split("perd")[1]  # Extraer tarifa
                    zona = "PENÍNSULA"  # Asignar zona predeterminada
                    fecha_inicio = file_name.split("_")[2]  # Fecha de inicio
                    fecha_fin = file_name.split("_")[3].split(".")[0]  # Fecha de fin
                
                fecha_inicio = datetime.strptime(fecha_inicio, "%Y%m%d")
                fecha_fin = datetime.strptime(fecha_fin.split(".")[0], "%Y%m%d")

                # Leer el archivo .data
                with open(file_path, "r") as file:
                    for line in file:
                        if line.startswith(("perd;", "Sperd;", "2025", "*")):  # Saltar encabezado
                            continue
                        
                        # Procesar línea
                        dia, *valores = line.split(";")
                        valores = [v for v in valores if v.strip()]  # Filtrar valores no vacíos
                        
                        # Calcular la fecha
                        dia_num = int(dia.split()[1])  # Obtener el número del día
                        fecha = fecha_inicio + timedelta(days=dia_num - 1)
                        
                        # Crear un registro por cada hora
                        for hora, valor in enumerate(valores, start=1):
                            data.append({
                                "nombre_fichero": file_name,
                                "tarifa": tarifa,
                                "Zona": zona, 
                                "Fecha": fecha.strftime("%Y-%m-%d"),
                                "Hora": hora,
                                "Valor": float(valor)
                            })

            except Exception as e:
                write_log(f"Error al procesar el archivo {file_name}: {e}")
                print(f"Error al procesar {file_name}: {e}")

        # Crear un DataFrame con los datos procesados
        df_a1_perd_h = pd.DataFrame(data)
        # Guardar el DataFrame en un archivo Excel
        output_path = os.path.join(files_aux, f"a1_perd_h_{fecha_hoy}.xlsx")
        print(f"Archivo Excel creado: {output_path}")
        df_a1_perd_h.to_excel(output_path, index=False)

        return df_a1_perd_h

    except Exception as e:
        write_log(f"Error general en la limpieza y tratamiento de archivos A1_PERD / A1_SPERD: {e}")
        print(f"Error general en el procesamiento: {e}")


def descarga_liquicomun():
    try:
        print("DESCARGANDO A1_LIQUICOMUN")
        today = datetime.today()
        start = today.replace(day=1).strftime('%Y-%m-%d')
        last_day = calendar.monthrange(today.year, today.month)[1]
        end = today.replace(day=last_day).strftime('%Y-%m-%d')
        write_log(f"Inicio del proceso de descarga de A1_LIQUICOMUN para las fechas {start}/{end}")

        # Descarga
        archive = endpoint.select(id=2)
        archive.configure(start=start, end=end, data_type="publication")
        archive.download_and_extract('files')

        # Verificar contenido en files/A1_liquicomun
        base_folder = os.path.join("files", "A1_liquicomun")
        if not os.path.exists(base_folder):
            write_log("No se encontró la carpeta A1_liquicomun.")
            return []

        entries = os.listdir(base_folder)
        subfolders = [os.path.join(base_folder, name) for name in entries if os.path.isdir(os.path.join(base_folder, name))]
        files = [os.path.join(base_folder, name) for name in entries if os.path.isfile(os.path.join(base_folder, name))]

        if subfolders:
            write_log(f"Subcarpetas encontradas: {subfolders}")
            return subfolders
        elif files:
            write_log(f"Archivos encontrados directamente en: {base_folder}")
            return [base_folder]
        else:
            write_log("No se encontraron archivos ni subcarpetas en A1_liquicomun.")
            return []
            raise
    except Exception as e:
        write_log(f"Error en descarga_liquicomun: {str(e)}")
        return []
        raise


def eliminar_innecesarios():
    try:
        write_log("Eliminando ficheros adicionales en A1_liquicomun")
        print("\tEliminando ficheros adicionales")

        # Dejamos solo los archivos con los prefijos permitidos 
        os.makedirs(files_aux, exist_ok=True) 
        allowed_prefixes = ("A1_perd", "A1_perdqh", "A1_Sperd", "A1_Kestimqh", "A1_Kestimado", 
                            "A1_petar", "A1_Spetar", "A1_prdemcad", "A1_compodem")

        if os.path.exists(folder_path_a1) and os.path.isdir(folder_path_a1):
            for file_name in os.listdir(folder_path_a1):
                file_path = os.path.join(folder_path_a1, file_name)
                if os.path.isfile(file_path) and not file_name.startswith(allowed_prefixes):
                    os.remove(file_path)
        else:
            write_log(f"Error: No se encontró la carpeta {folder_path_a1}")

    except Exception as e:
        write_log(f"Error al eliminar archivos en {folder_path_a1}: {e}")
        print(f"Error al eliminar archivos: {e}")


def write_log(message, log_file="logs.txt"):
    try:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_path = os.path.join(log_dir, log_file)
        with open(log_path, "a", encoding="utf-8") as log:
            log.write(f"[{now}] {message}\n")
    except Exception as e:
        print(f"Error al escribir en el log: {e}")


if __name__ == "__main__":
    main()