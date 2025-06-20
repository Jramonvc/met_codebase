import json
import pandas as pd
from sqlalchemy import create_engine, text
import os
import time
from sqlalchemy.exc import IntegrityError
from datetime import datetime


# Función para procesar todos los archivos JSON de la carpeta y ejecutar las inserciones
def procesar_archivos_json(carpeta):
    # Configuración de la conexión a la base de datos
    try:
        conn_str = r"mssql+pyodbc://sqluser:cwD9KVms4Qdv4mLy@met-esp-prod.database.windows.net:1433/Risk_MGMT_Spain?driver=ODBC+Driver+17+for+SQL+Server"
        engine = create_engine(conn_str, fast_executemany=True)
        print("Conexión exitosa a la base de datos.")
    except Exception as e:
        print(f"Error conectando a la base de datos: {e}")
        return
        
    
    # Obtener los archivos JSON en la carpeta
    archivos_json = [f for f in os.listdir(carpeta) if f.endswith('.json')]

    if archivos_json:
        start_time = time.time()
        for archivo_json in archivos_json:
            ruta_archivo = os.path.join(carpeta, archivo_json)
            
            # Cargar y procesar el archivo JSON
            try:
                with open(ruta_archivo, 'r') as f:
                    datos_json = json.load(f)
                    if datos_json is None:
                        print(f"Advertencia: El archivo {archivo_json} está vacío o no contiene un formato JSON válido.")
                        continue
            except json.JSONDecodeError as e:
                print(f"Error decodificando el archivo JSON {archivo_json}: {e}")
                continue
            except Exception as e:
                print(f"Error cargando el archivo {archivo_json}: {e}")
                continue

            # Verificar si 'datos' existe y no está vacío
            if 'datos' in datos_json and isinstance(datos_json['datos'], dict):
                cups_data = []

                # Iterar sobre los datos del JSON
                for cup_codigo, fechas in datos_json['datos'].items():
                    # Verificar que 'fechas' es un diccionario
                    if isinstance(fechas, dict):
                        for fecha, tipos in fechas.items():
                            for tipo, detalles in tipos.items():
                                # Verificar si el valor 'Lecturas' existe y no está vacío
                                if isinstance(detalles, dict) and 'Lecturas' in detalles and detalles['Lecturas']:
                                    procedencia = detalles['Procedencia']
                                    lecturas = detalles['Lecturas']
                                    
                                    for hora, lectura in lecturas.items():
                                        proced = 'E' if tipo == 'Entrante' else 'S'
                                        if tipo == 'Entrante':
                                            hora = int(hora) / 100
                                        cups_data.append([cup_codigo, fecha, procedencia, proced, int(hora), lectura])
                                else:
                                    print(f"Sin datos para {tipo} el {fecha}")
                    else:
                        print(f"El formato de 'fechas' no es correcto para {cup_codigo}. Se esperaba un diccionario.")

                # Si hay datos para insertar
                if cups_data:
                    df_cups = pd.DataFrame(cups_data, columns=['PPC_CUP_CODIGO', 'PPC_FECHA', 'PPC_PROCED', 'PPC_TIPO', 'PPC_HORA', 'PPC_PREVISION'])

                    # Inserción masiva usando pandas
                    try:
                        df_cups.to_sql('MET_PREVPOWERCUP', engine, schema='METDB', if_exists='append', index=False)
                        print(f"Datos insertados correctamente desde {archivo_json}.")
                    except Exception as e:
                        if '2627' in str(e.orig):  # Error de clave primaria duplicada
                            print(f"Error de clave primaria duplicada en {archivo_json}. Continuando con el siguiente archivo.")
                        else:
                            print(f"Error de integridad en {archivo_json}: {e}")
                    except Exception as e:
                        print(f"Error insertando datos desde {archivo_json}: {e}")
                else:
                    print(f"No se generaron datos válidos para el archivo {archivo_json}.")
            else:
                print(f"El archivo {archivo_json} no contiene datos o está vacío.")
            
            # Borrar el archivo JSON después de procesarlo
            try:
                os.remove(ruta_archivo)
                print(f"Archivo {archivo_json} procesado y eliminado.")
            except Exception as e:
                print(f"Error al intentar eliminar el archivo {archivo_json}: {e}")
        
        # Tiempo total de procesamiento
        end_time = time.time()
        tiempo_total_segundos = end_time - start_time
        tiempo_total_minutos = tiempo_total_segundos / 60
        print(f"Tiempo total de procesamiento: {tiempo_total_minutos:.2f} minutos.")
    else:
        print("No hay archivos JSON en la carpeta.")
    

            
if __name__ == '__main__':
    carpeta_json = 'temp_previsions_neuro'
    procesar_archivos_json(carpeta_json)
