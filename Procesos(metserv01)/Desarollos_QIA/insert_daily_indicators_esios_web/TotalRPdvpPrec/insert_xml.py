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
    DELETE FROM [METDB].[MET_TOTALRPDVPPREC]
    WHERE [TPR_FECHA] = CONVERT(DATE, DATEADD(DAY, 1, GETDATE()));
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

    ns = {"ns": "urn:sios.ree.es:totalrpdvpprec:1:0"}

    identificacion_mensaje = root.find("ns:IdentificacionMensaje", ns).attrib['v']

    # Obtener las series temporales
    series_temporales = root.findall("ns:SeriesTemporales", ns)

    # Inicializar SQL
    sql_insert = "INSERT INTO METDB.MET_TOTALRPDVPPREC (TPR_IDSERIE, TPR_TIPREDESP, TPR_TIPAGREG, TPR_UNIDMED, TPR_UNIDPREC, TPR_INTERVINI, TPR_INTERVFIN, TPR_FECHA, TPR_HORA, TPR_INTERV, TPR_CANTPOT, TPR_PRECIO) VALUES\n"

    registros = []
    for serie in series_temporales:
        identificacion_serie = serie.find("ns:IdentificacionSeriesTemporales", ns).attrib['v']
        tipo_redespacho = serie.find("ns:TipoRedespacho", ns).attrib['v']
        tipo_agregacion = serie.find("ns:TipoAgregacion", ns).attrib['v']
        unidad_medida = serie.find("ns:UnidadMedida", ns).attrib['v']
        unidad_precio = serie.find("ns:UnidadPrecio", ns).attrib['v']

        periodos = serie.findall("ns:Periodo", ns)

        for periodo in periodos:
            intervalo_tiempo = periodo.find("ns:IntervaloTiempo", ns).attrib['v']
            intervalo_inicio_utc, intervalo_fin_utc = intervalo_tiempo.split('/')
            intervalo_inicio_utc = utc.localize(datetime.strptime(intervalo_inicio_utc, '%Y-%m-%dT%H:%MZ'))
            intervalo_fin_utc = utc.localize(datetime.strptime(intervalo_fin_utc, '%Y-%m-%dT%H:%MZ'))

            # Convertir a horario peninsular
            intervalo_inicio_peninsula = intervalo_inicio_utc.astimezone(peninsula)
            intervalo_fin_peninsula = intervalo_fin_utc.astimezone(peninsula)

            # Formatear como DATETIME en formato 'YYYY-MM-DD HH:MM:SS'
            intervalo_inicio_str = intervalo_inicio_peninsula.strftime('%Y-%m-%d %H:%M:%S')
            intervalo_fin_str = intervalo_fin_peninsula.strftime('%Y-%m-%d %H:%M:%S')

            # Extraer solo la fecha como DATE en formato 'YYYY-MM-DD'
            fecha = intervalo_inicio_peninsula.strftime('%Y-%m-%d')

            intervalos = periodo.findall("ns:Intervalo", ns)
            intervalo_global = 1  # Contador de cuartos horarios

            for intervalo in intervalos:
                ctd_baj_elem = intervalo.find("ns:CtdBaj", ns)
                precio_baj_elem = intervalo.find("ns:PrecioBaj", ns)

                ctd_baj = ctd_baj_elem.attrib['v'] if ctd_baj_elem is not None else 'NULL'
                precio_baj = precio_baj_elem.attrib['v'] if precio_baj_elem is not None else 'NULL'

                # Calcular la hora del intervalo +1 (horario peninsular)
                instante_intervalo = intervalo_inicio_peninsula + timedelta(minutes=(intervalo_global - 1) * 15)
                hora = instante_intervalo.hour + 1

                registros.append(f"('{identificacion_serie}', '{tipo_redespacho}', '{tipo_agregacion}', "
                                 f"'{unidad_medida}', '{unidad_precio}', '{intervalo_inicio_str}', '{intervalo_fin_str}', "
                                 f"'{fecha}', {hora}, {intervalo_global}, {ctd_baj}, {precio_baj})")

                intervalo_global += 1

    sql_insert += ",\n".join(registros) + ";"

    registrar_en_log(f"Archivo {os.path.basename(xml_file)} procesado correctamente.")
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
