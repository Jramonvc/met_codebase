import os
import esios
import pandas as pd
from datetime import datetime, timedelta, timezone
import calendar
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

ruta_base = r"C:\Procesos\Desarrollos_QIA\insert_daily_indicators_esios_web\Liquicomun"
os.chdir(ruta_base)

#Aux Vars
esios_token = '28c83fff5780b353ba90dd62b4ec8db6b39eb9b7f7d08730ddfd36a478d95704'
os.environ['ESIOS_API_KEY'] = esios_token
client = esios.ESIOSClient()
endpoint = client.endpoint(name='archives')
fecha_hoy = datetime.today().strftime("%Y-%m-%d")
folder_path_a1 = os.path.join("files", "A1_liquicomun")
files_aux = os.path.join("files_aux", f"{fecha_hoy}") 

def main():
    #Descarga de ficheros
    descarga_liquicomun() #Descarga liquicomun

def descarga_liquicomun():
    try:
        print("DESCARGANDO A1_LIQUICOMUN")
        # Cálculo de las fechas de inicio y fin del mes actual
        today = datetime.today()
        start = today.replace(day=1).strftime('%Y-%m-%d')
        last_day = calendar.monthrange(today.year, today.month)[1]
        end = today.replace(day=last_day).strftime('%Y-%m-%d')
        print(f"Inicio del proceso de descarga de A1_LIQUICOMUN para las fechas {start}/{end}")

        # Obtención y descarga de A1 liquicomun (ID 2)
        archive = endpoint.select(id=2)
        archive.configure(start=start, end=end, data_type="publication")
        archive.metadata
        archive.download_and_extract('A1/files')
        print("A1_LIQUICOMUN descargado correctamente.")

    except Exception as e:
        print(f"Error descargando 1_LIQUICOMUN para las fechas {start}/{end}: {e}")
        print(f"Error al descargar A1_LIQUICOMUN: {e}")
        
        # Intento con ID 3
        try:
            print("DESCARGANDO A2_LIQUICOMUN")
            
            # Obtención y descarga de A1 liquicomun (ID 3)
            archive = endpoint.select(id=3)
            archive.configure(start=start, end=end, data_type="publication")
            archive.metadata
            archive.download_and_extract('A2/files')
            print("A1_LIQUICOMUN descargado correctamente con ID 3.")
            
        except Exception as e3:
            print(f"Error descargando A1_LIQUICOMUN con ID 3 para las fechas {start}/{end}: {e3}")
            print(f"Error al descargar A1_LIQUICOMUN con ID 3: {e3}")
            
if __name__ == "__main__":
    main()