import json
import pandas as pd
from sqlalchemy import create_engine, text
import os
import time
from sqlalchemy.exc import IntegrityError
from datetime import datetime



 # Función para eliminar registros de D-3 y D-2
def eliminar_registros_antiguos(tamano_lote=10000):
    try:
        conn_str = r"mssql+pyodbc://sqluser:cwD9KVms4Qdv4mLy@met-esp-prod.database.windows.net:1433/Risk_MGMT_Spain?driver=ODBC+Driver+17+for+SQL+Server"
        engine = create_engine(conn_str, fast_executemany=True)
        print("Conexión exitosa a la base de datos.")
    except Exception as e:
        print(f"Error conectando a la base de datos: {e}")
    start_time = time.time()
    hoy = datetime.now().strftime('%Y-%m-%d')
    # Query para eliminar por lotes
    query_delete = text(f"DELETE TOP ({tamano_lote}) FROM METDB.MET_CONSUPOWER  WHERE CSP_FECHA BETWEEN CAST(DATEADD(DAY, -3, GETDATE()) AS DATE) AND CAST(DATEADD(DAY, -2, GETDATE()) AS DATE);")
    print('Lanzando: {}'.format(query_delete))
    
    with engine.connect() as connection:
        transaction = connection.begin()  # Iniciar transacción grande para evitar múltiples commits
        try:
            while True:
                # Ejecutar la eliminación por lotes
                result = connection.execute(query_delete)
                registros_eliminados = result.rowcount
                
                print(f"Registros eliminados en este lote: {registros_eliminados}")
                
                if registros_eliminados == 0:
                    print("No hay más registros para eliminar.")
                    break
                
            transaction.commit()  # Confirmar todos los cambios al final
        except Exception as e:
            transaction.rollback()  # Hacer rollback en caso de error
            print(f"Error eliminando registros por lotes: {e}")
            
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
        
    eliminar_registros_antiguos()
    
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
            except Exception as e:
                print(f"Error cargando el archivo {archivo_json}: {e}")
                continue

            # Verificar si 'datos' existe y no está vacío
            if 'datos' in datos_json and datos_json['datos']:
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
                                        cups_data.append([cup_codigo, fecha, procedencia, proced, int(hora), lectura])
                                else:
                                    print(f"Sin datos para {tipo} el {fecha}")
                    else:
                        print(f"El formato de 'fechas' no es correcto para {cup_codigo}. Se esperaba un diccionario.")

                # Si hay datos para insertar
                if cups_data:
                    df_cups = pd.DataFrame(cups_data, columns=['CSP_CUP_CODIGO', 'CSP_FECHA', 'CSP_PROCED', 'CSP_TIPO', 'CSP_HORA', 'CSP_LECTURAS'])

                    # Inserción masiva usando pandas
                    try:
                        df_cups.to_sql('MET_CONSUPOWER', engine, schema='METDB', if_exists='append', index=False)
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
    carpeta_json = 'temp_consumos_neuro'
    procesar_archivos_json(carpeta_json)
