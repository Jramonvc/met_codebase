import json
import pandas as pd
from sqlalchemy import create_engine
import os
import time
from datetime import datetime
from sqlalchemy.exc import IntegrityError

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
            
            # Extraer PPW_UP del nombre del archivo
            ppw_up = archivo_json.split('_')[0]  # Nombre antes del primer guion bajo
            
            # Cargar y procesar el archivo JSON
            try:
                with open(ruta_archivo, 'r') as f:
                    datos_json = json.load(f)
                    if datos_json is None or 'datos' not in datos_json:
                        print(f"El archivo {archivo_json} no contiene datos válidos.")
                        continue
            except json.JSONDecodeError as e:
                print(f"Error decodificando el archivo JSON {archivo_json}: {e}")
                continue
            except Exception as e:
                print(f"Error cargando el archivo {archivo_json}: {e}")
                continue

            # Procesar datos
            registros = []
            fecha_actual = datetime.now().date()  # Solo la fecha actual en formato AAAA-MM-DD

            if isinstance(datos_json['datos'], dict):
                for fecha, horas in datos_json['datos'].items():
                    for hora, prevision in horas.items():
                        try:
                            registros.append({
                                'PPW_FECCOMU': fecha_actual,  # Fecha actual
                                'PPW_UP': ppw_up,            # Nombre del archivo antes del guion bajo
                                'PPW_FECHA': fecha,          # Fecha del JSON
                                'PPW_HORA': int(hora),       # Hora del JSON
                                'PPW_PREVISION': float(prevision)  # Previsión
                            })
                        except Exception as e:
                            print(f"Error procesando {fecha} {hora}: {e}")
            
            # Inserción masiva usando pandas
            if registros:
                df_registros = pd.DataFrame(registros)
                try:
                    df_registros.to_sql('MET_PREVPOWER', engine, schema='METDB', if_exists='append', index=False)
                    print(f"Datos insertados correctamente desde {archivo_json}.")
                except IntegrityError as e:
                    if '2627' in str(e.orig):  # Clave duplicada
                        print(f"Error de clave duplicada en {archivo_json}.")
                    else:
                        print(f"Error de integridad en {archivo_json}: {e}")
                except Exception as e:
                    print(f"Error insertando datos desde {archivo_json}: {e}")
            else:
                print(f"No hay datos para insertar desde el archivo {archivo_json}.")

            # Borrar el archivo JSON después de procesarlo
            try:
                os.remove(ruta_archivo)
                print(f"Archivo {archivo_json} procesado y eliminado.")
            except Exception as e:
                print(f"Error al intentar eliminar el archivo {archivo_json}: {e}")
        
        # Tiempo total de procesamiento
        end_time = time.time()
        print(f"Tiempo total de procesamiento: {(end_time - start_time) / 60:.2f} minutos.")
    else:
        print("No hay archivos JSON en la carpeta.")

if __name__ == '__main__':
    carpeta_json = 'temp_previsions_upr_neuro'
    procesar_archivos_json(carpeta_json)
