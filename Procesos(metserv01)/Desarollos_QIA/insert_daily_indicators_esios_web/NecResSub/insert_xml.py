import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import pytz
import os
import pymssql


# Global Vars
folder = 'files_xml'
utc = pytz.utc
peninsula = pytz.timezone('Europe/Madrid')
server_bd_met = 'met-esp-prod.database.windows.net'
database_bd_met = 'Risk_MGMT_Spain'
username_bd_met = 'sqluser'
password_bd_met = 'cwD9KVms4Qdv4mLy'


# Función para borrar los registros del día siguiente
def delete_tomorrow_entries(connection):
    delete_query = """
    DELETE FROM [METDB].[MET_NECRESSUB]
    WHERE [NSS_FECHA] = CONVERT(DATE, DATEADD(DAY, 1, GETDATE()));
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(delete_query)
            connection.commit()
            print("Registros del día siguiente eliminados correctamente.")
    except pymssql.Error as e:
        print(f"Error al ejecutar el DELETE: {e}")
        registrar_en_log(f"Error al ejecutar el DELETE: {e}")
        
# Función para obtener el archivo XML con el día siguiente en el nombre
def get_xml_filename(folder):
    # Obtener la fecha de mañana
    tomorrow = datetime.now() + timedelta(days=1)
    fecha_tomorrow = tomorrow.strftime("%d-%m-%Y")

    # Buscar el archivo XML que contiene la fecha en su nombre
    for file_name in os.listdir(folder):
        if fecha_tomorrow in file_name and file_name.endswith(".xml"):
            return os.path.join(folder, file_name)
    
    return None


# Función para registrar eventos en el archivo log.txt
def registrar_en_log(mensaje):
    log_file = "log.txt"
    # Obtener la fecha y hora actuales con milisegundos
    ahora = datetime.now().strftime("%d/%m/%Y %H:%M:%S,%f")[:-3]
    # Escribir en el log
    with open(log_file, "a") as log:
        log.write(f"[{ahora}] {mensaje}\n")


# Función para procesar el archivo XML y generar la consulta SQL
def process_xml(xml_file):
    tree = ET.parse(xml_file)
    root = tree.getroot()

    # Obtener los valores del XML
    identificacion_mensaje = root.find(".//{urn:sios.ree.es:necressub:1:0}IdentificacionMensaje").attrib['v']
    horizonte = root.find(".//{urn:sios.ree.es:necressub:1:0}Horizonte").attrib['v']

    # Extraer horizonte de tiempo
    horizonte_inicio_utc, horizonte_fin_utc = horizonte.split('/')
    horizonte_inicio_utc = utc.localize(datetime.strptime(horizonte_inicio_utc, '%Y-%m-%dT%H:%MZ'))
    horizonte_fin_utc = utc.localize(datetime.strptime(horizonte_fin_utc, '%Y-%m-%dT%H:%MZ'))

    # Convertir el horizonte de UTC a horario peninsular
    horizonte_inicio_peninsula = horizonte_inicio_utc.astimezone(peninsula)
    horizonte_fin_peninsula = horizonte_fin_utc.astimezone(peninsula)

    # Obtener solo la fecha
    fecha = horizonte_inicio_peninsula.strftime('%Y-%m-%d')

    # Formato de la consulta SQL
    sql_insert = "INSERT INTO METDB.MET_NECRESSUB (NSS_FECHA, NSS_HORA, NSS_INTERVALO, NSS_CANTPOT) VALUES\n"

    # Proceso de intervalos de tiempo
    series_temporales = root.findall(".//{urn:sios.ree.es:necressub:1:0}Intervalo")
    intervalos = []
    
    # Inicializamos la hora en 1
    hora = 1
    intervalo_global = 1

    for i, intervalo in enumerate(series_temporales, start=1):
        cantidad_potencia = intervalo.find(".//{urn:sios.ree.es:necressub:1:0}Ctd").attrib['v']

        # Agregar el SQL para este intervalo
        intervalos.append(f"('{fecha}', {hora}, {intervalo_global}, {cantidad_potencia})")

        # Cada 4 intervalos cambiamos de hora
        if intervalo_global % 4 == 0:
            hora += 1

        # Incrementar el número de intervalo global
        intervalo_global += 1

    # Unir todos los valores generados para el insert
    sql_insert += ",\n".join(intervalos) + ";"

    # Registrar finalización del procesamiento en el log
    registrar_en_log(f"Archivo {os.path.basename(xml_file)} procesado correctamente.")
    
    # Retornar el SQL generado
    return sql_insert


## ----------------- BDD FUNCTIONS -----------------
def create_connection(server, user, password, database):
    try:
        connection = pymssql.connect(
            server=server,
            user=user,
            password=password,
            database=database
        )
        print('\n01 - CONEXIÓN CON LA BBDD DE MET CREADA CORRECTAMENTE')
        return connection
    except pymssql.Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None


# Función para ejecutar las consultas SQL
def execute_sql_queries(connection, sql_query):
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql_query)
            connection.commit()
            print("Consulta ejecutada correctamente.")
    except pymssql.Error as e:
        print(f"Error al ejecutar la consulta: {e}")
        registrar_en_log(f"Error al ejecutar la consulta: {e}")


# Función para cerrar la conexión
def close_connection(connection):
    try:
        connection.close()
        print('\n01 - CONEXIÓN CERRADA CON LA BBDD DE MET CORRECTAMENTE')
    except pymssql.Error as e:
        print(f"Error al cerrar la conexión: {e}")


## ----------------- END BDD FUNCTIONS -----------------

if __name__ == '__main__':
    xml_file = get_xml_filename(folder)
    if xml_file:    
        conn = create_connection(server_bd_met, username_bd_met, password_bd_met, database_bd_met)
        if conn: 
            delete_tomorrow_entries(conn)
            consulta_sql = process_xml(xml_file)
            execute_sql_queries(conn, consulta_sql)
            close_connection(conn)
        else:
            registrar_en_log("Error al establecer la conexión con la base de datos.")
    else:
        registrar_en_log(f"No se encontró ningún archivo XML para la fecha de mañana en la carpeta {folder}.")
        print(f"No se encontró ningún archivo XML para la fecha de mañana en la carpeta {folder}.")
