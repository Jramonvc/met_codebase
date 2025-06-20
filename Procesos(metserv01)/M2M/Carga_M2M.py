# -*- coding: utf-8 -*-
"""
Created on Fri Mar 15 10:57:24 2024

@author: ramon.vicioso
"""
from datetime import datetime, timedelta
import subprocess
import json
import pyodbc
import keyring
import os
# Obtener la fecha de hoy
hoy = datetime.now()

# Restar un día a la fecha de hoy para obtener la fecha de ayer
ayer = hoy - timedelta(days=1)

# Formatear la fecha de ayer como una cadena en el formato deseado (YYYY-MM-DD)
fecha_ayer = ayer.strftime("%Y-%m-%d")
curl1 = (
    """curl http://metwsva0009:5000/api/EUSales/exposure/current/"""
    + fecha_ayer
    + """/METEE/true -H "Accept: application/json" -H "Authorization: Bearer eyJhbGcsz6IO56s32Dhf665652d6a99344s3fdaw3HlOWeE5fd5Am8R1GaT-Al2lkdsajfk2Ihdlwo4rSldkwKwh4lik548wl2" | jq > M2M.json"""
)
curl2 = (
    """curl http://metwsva0009:5000/api/Credit/exposure/current/"""
    + fecha_ayer
    + """/21141 -H "Accept: application/json" -H "Authorization: Bearer eyJhbGcsz6IO56s32Dhf665652d6a99344s3fdaw3HlOWeE5fd5Am8R1GaT-Al2lkdsajfk2Ihdlwo4rSldkwKwh4lik548wl2" | jq > M2M_W.json"""
)
# Ejecuta el comando curl
try:
    subprocess.run(curl1, shell=True)  
    subprocess.run(curl2, shell=True)  
except subprocess.CalledProcessError as e:
    print("Error al ejecutar el comando curl:", e.output)
# Paso 1: Leer el archivo JSON
with open('M2M.json', 'r', encoding='utf-8', errors='ignore') as f:
    datos_json = json.load(f)
try:
    if os.path.exists('M2M_W.json'):
        # Verificar el tamaño del archivo para determinar si está vacío
        if os.path.getsize('M2M_W.json') > 0:
            with open('M2M_W.json', 'r', encoding='utf-8', errors='ignore') as f:
                datos_json_W = json.load(f)
            V_Error = 'N'
        else:
            print("El archivo 'M2M_W.json' está vacío.")
            V_Error = 'S'
    else:
        print("El archivo 'M2M_W.json' no existe.")
        V_Error = 'S'
except Exception as e:
    print("Se produjo un error:", e)
    V_Error = 'S'
# Paso 2: Conectar a la base de datos SQL Server
sql = keyring.get_credential("SQL", None)
username = sql.username
password = sql.password
server = 'met-esp-prod.database.windows.net'
database = 'Risk_MGMT_Spain'
#conexion = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
conexion = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password+';ssl=1')
# Paso 3: Crear un cursor
cursor = conexion.cursor()
# Paso 4: Insertar los datos del JSON en la tabla
for item in datos_json:
    cursor.execute('''
        INSERT INTO metdb.tmp_M2M (NIF, CREDITLIMIT) 
        VALUES (?, ?)
        ''',
        item['externalBU'],
        item['mtmExposure']
    )
if V_Error == 'N':    
    for item in datos_json_W:
         cursor.execute('''
        INSERT INTO metdb.tmp_M2M (NIF, CREDITLIMIT) 
        VALUES (?, ?)
        ''',
        item['externalBU'],
        item['mtmExposure']
    )
sql_query = """UPDATE C
				   SET C.CLI_M2M = CREDIT
				 FROM METDB.MET_CLIENTES C
				 INNER JOIN (
				select NIF, SUM(CREDITLIMIT) CREDIT
				 FROM METDB.TMP_M2M
				 GROUP BY NIF) B
				   ON CLI_NIF = NIF"""
cursor.execute(sql_query)
sql_query = """UPDATE C
				   SET C.CLI_M2M = CREDIT
				 FROM METDB.MET_CLIENTES C
				 INNER JOIN (
				select NIF, SUM(CREDITLIMIT) CREDIT
				 FROM METDB.TMP_M2M
				 GROUP BY NIF) B
				   ON CLI_RAZSOC = NIF"""
cursor.execute(sql_query)
sql_query = """DELETE METDB.TMP_M2M"""
cursor.execute(sql_query)
# Commit de los cambios y cerrar la conexión
conexion.commit()
conexion.close()    