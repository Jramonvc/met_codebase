import pymssql
import requests
import json
from datetime import datetime
from decimal import Decimal

# Global Variables
server_bd_met = 'met-esp-prod.database.windows.net'
database_bd_met = 'Risk_MGMT_Spain'
username_bd_met = 'sqluser'
password_bd_met = 'cwD9KVms4Qdv4mLy'

# Serializador personalizado para JSON
def json_serial(obj):
    if isinstance(obj, Decimal):
        return float(obj)  # Convertir Decimal a float
    if isinstance(obj, datetime):
        return obj.isoformat()  # Convertir datetime a formato ISO
    raise TypeError(f"Tipo de dato no serializable: {type(obj)}")

# Función principal
def obtener_datos_bilaterales():
    # Crear conexión a la base de datos
    connection = create_connection(server_bd_met, username_bd_met, password_bd_met, database_bd_met)
    
    if connection:
        try:
            print("Conexión exitosa a la base de datos")
            
            # Consulta SQL dinámica
            query = """
			SELECT ROW_NUMBER() OVER (ORDER BY A.BLT_UNIDOFER, A.BLT_CONTBIL) AS Id,
                A.BLT_UNIDOFER AS "Codigo UP",
                A.BLT_CONTBIL AS "Cod. Cont. Bilateral",
                ROUND(COALESCE(A.BLT_ENERGTOT_D, 0), 2) AS "D",
                ROUND(COALESCE(A.BLT_ENERGTOT_D1, 0), 2) AS "D1",
                ROUND(COALESCE(A.BLT_ENERGTOT_D, 0) - COALESCE(A.BLT_ENERGTOT_D1, 0), 2) AS "Diferencia"
			  FROM (
			SELECT BLT_UNIDOFER, BLT_CONTBIL, SUM(CASE BLT_FECHA WHEN CAST(GETDATE() AS DATE) THEN BLT_ENERGTOT ELSE 0 END) AS BLT_ENERGTOT_D, 
			       SUM(CASE BLT_FECHA WHEN CAST(DATEADD(DAY, 1, GETDATE()) AS DATE) THEN BLT_ENERGTOT ELSE 0 END) AS BLT_ENERGTOT_D1 
                 FROM METDB.MET_BILATERAL
                 WHERE BLT_FECHA BETWEEN CAST(GETDATE() AS DATE) AND CAST(DATEADD(DAY, 1, GETDATE()) AS DATE)
				   AND BLT_CONTBIL IS NOT NULL
                 GROUP BY BLT_UNIDOFER, BLT_CONTBIL) A
			ORDER BY A.BLT_UNIDOFER, A.BLT_CONTBIL;
            """
            
            # Ejecutar la consulta y obtener resultados
            resultados = execute_query(connection, query)
            if not resultados:
                print("No se encontraron registros.")
                return
            
            # Formatear resultados en JSON utilizando el serializador personalizado
            json_data = json.dumps(resultados, indent=4, ensure_ascii=False, default=json_serial)
            print("Datos en formato JSON:")
            print(json_data)
            
            # Enviar datos al webhook de Power Automate
            enviar_datos_a_webhook(json_data)
        
        except Exception as e:
            print(f"Error durante el proceso: {e}")
        
        finally:
            # Cerrar conexión
            close_connection(connection)

# Función para enviar datos al webhook
def enviar_datos_a_webhook(json_data):
    url = "https://prod-202.westeurope.logic.azure.com:443/workflows/f6a7b08c9abb4073a16850ab371bda08/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=xCnzcJOp3QUUv48AjlwBuP3lQUREtpx_DkLXWvEQ_Lw"
    headers = {'Content-Type': 'application/json'}
    
    try:
        response = requests.post(url, headers=headers, data=json_data)
        if response.status_code == 200:
            print("Datos enviados exitosamente.")
            print("Respuesta de Power Automate:", response.json())
        else:
            print(f"Error al enviar los datos: {response.status_code}")
            print("Detalle del error:", response.text)
    except Exception as e:
        print(f"Error durante el envío al webhook: {e}")

## ----------------- BDD FUNCTIONS -----------------
# Crear la conexión
def create_connection(server, user, password, database):
    try:
        connection = pymssql.connect(
            server=server,
            user=user,
            password=password,
            database=database
        )
        print('\n01 - CONEXION CON LA BBDD DE MET CREADA CORRECTAMENTE')
        return connection
    except pymssql.Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None
        
# Ejecutar consultas
def execute_query(connection, query, params=None):
    try:
        with connection.cursor(as_dict=True) as cursor:
            cursor.execute(query, params)
            result = cursor.fetchall()
        return result
    except pymssql.Error as e:
        print(f"Error al ejecutar la consulta: {e}")
        return None

# Cerrar la conexión
def close_connection(connection):
    try:
        print('\n01 - CONEXION CERRADA CON LA BBDD DE MET CORRECTAMENTE')
        connection.close()
    except pymssql.Error as e:
        print(f"Error al cerrar la conexión: {e}")
## ----------------- END BDD FUNCTIONS -----------------

# Llamada a la función principal
obtener_datos_bilaterales()
