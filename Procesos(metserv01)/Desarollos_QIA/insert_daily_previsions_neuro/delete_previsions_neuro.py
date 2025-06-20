import json
import pandas as pd
from sqlalchemy import create_engine, text
import os
import time
from sqlalchemy.exc import IntegrityError
from datetime import datetime

    
 # Función para eliminar registros con fecha mayoro igual a hoy
def eliminar_registros_antiguos(engine, tamano_lote=10000):
    start_time = time.time()
    hoy = datetime.now().strftime('%Y-%m-%d')
    # Query para eliminar por lotes
    query_delete = text(f"DELETE TOP ({tamano_lote}) FROM METDB.MET_PREVPOWERCUP WHERE PPC_FECHA >= '{hoy}'")
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
            
if __name__ == '__main__':
    try:
        conn_str = r"mssql+pyodbc://sqluser:cwD9KVms4Qdv4mLy@met-esp-prod.database.windows.net:1433/Risk_MGMT_Spain?driver=ODBC+Driver+17+for+SQL+Server"
        engine = create_engine(conn_str, fast_executemany=True)
        print("Conexión exitosa a la base de datos.")
        eliminar_registros_antiguos(engine)
    except Exception as e:
        print(f"Error conectando a la base de datos: {e}")

