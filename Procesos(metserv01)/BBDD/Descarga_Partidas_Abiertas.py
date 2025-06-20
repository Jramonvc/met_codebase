import os
import paramiko
import pandas as pd
import pyodbc
import os
import glob
# Establecer la conexión SFTP
hostname = 'ftp.covline.es'
port = 22
username = 'met'
password = 'enqd7YaRU0kR_2HnRPKgZUpMkZsHGV'

# Carpeta donde se encuentran los archivos CSV
remote_directory = '/interface/'

# Directorio local donde se guardarán los archivos descargados
local_directory = 'C:\Procesos\BBDD\PartidasAbiertas'

# Crear una instancia de cliente SFTP
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

try:
    # Conectar al servidor SFTP
    client.connect(hostname, port, username, password)

    # Abrir una conexión SFTP
    sftp = client.open_sftp()

    # Cambiar al directorio remoto
    sftp.chdir(remote_directory)

    # Listar archivos en el directorio remoto
    files = sftp.listdir()

    # Descargar archivos CSV
    for file in files:
        if file.endswith('.csv'):
            remote_file = os.path.join(remote_directory, file)
            local_file = os.path.join(local_directory, file)
            sftp.get(remote_file, local_file)
            print(f"Descargado: {file}")

    # Cerrar conexión SFTP
    sftp.close()

finally:
    # Cerrar conexión SSH
    client.close()
# Configuración de la conexión a SQL Server
# Conexión a la base de datos SQL Server
conn = pyodbc.connect('DRIVER={SQL Server};'
                      'SERVER=82.194.94.135;'
                      'DATABASE=SIGEMET;'
                      'UID=sa;'
                      'PWD=Aq-1d.BlkY;')

# Crear un cursor
cursor = conn.cursor()
consulta = "DELETE SIGEMET.DBO.MET_PARTIDASAB"
cursor.execute(consulta)
# Directorio donde están los archivos CSV
directorio = "C:\\Procesos\\BBDD\\PartidasAbiertas\\"
archivos_csv = glob.glob(os.path.join(directorio, "DeudaPendiente*.csv"))  # Obtener todos los CSV
# Nombre de la tabla en SQL Server
table_name = 'SIGEMET.DBO.MET_PARTIDASAB'
for archivo in archivos_csv:
    # Cargar el CSV en un DataFrame de Pandas, especificando el delimitador "|"
    df = pd.read_csv(archivo, sep='|', encoding='utf-8')  # Usa encoding según sea necesario
    # Insertar datos fila por fila
    for index, row in df.iterrows():
        cursor.execute(f"""
                       INSERT INTO {table_name} (NIF, NUMFACT, IMPORTE, DIVISA, FECFACT, FECVEN, IDPLANPAGO) 
                       VALUES (?, ?, ?, ?, ?, ?, ?)
                       """, row['NIF'], row['Nro_Factura'],row['Importe'], row['Divisa'],row['Fecha_Factura'], row['Fecha_Vencimiento'], row['ID_PlanPago'] if pd.notna(row['ID_PlanPago']) else None)
                       # Confirmar los cambios
        conn.commit()
    # Borrar el archivo después de procesarlo    
    os.remove(archivo)
# Cerrar la conexión
cursor.close()
conn.close()

print("Carga de datos completada exitosamente.")