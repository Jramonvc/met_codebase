import keyring
import pandas as pd
import pyodbc
import time
import shutil
import os

start_time = time.time()

#path to files + processed folder
base_folder = r'M:\MET EE\Operaciones\Repartos\Repartos Cisternas - BBDD'      #change on metserv01
processed_folder = os.path.join(base_folder, 'procesados')

sql = keyring.get_credential("sql", None)
server = 'met-esp-prod.database.windows.net'
db = 'Risk_MGMT_Spain'
sqlusername = sql.username
sqlpassword = sql.password

conn_str = (
     f'DRIVER={{ODBC Driver 17 for SQL Server}};'
    f'SERVER={server};'
    f'DATABASE={db};'
    f'UID={sqlusername};'
    f'PWD={sqlpassword};'
    f'Encrypt=yes;'
    f'TrustServerCertificate=no;'
    f'Connection Timeout=30;'
)

def upload_excel_to_db(file_path):
    df_all = pd.read_excel(file_path, sheet_name='Datos', engine='openpyxl', header=None)
    table_indicator = str(df_all.iloc[0, 1]).strip()
    print(f"Table indicator found: {table_indicator}")

    table_map = {
        'Consulta Repartos Diarios Provisionales': 'METDB.MET_REPARTOSDIARIOSPROV',
        'Consulta Repartos Diarios Finales Provisionales': 'METDB.MET_REPARTOSDIARIOSFINALPROV',
        'Consulta Repartos Diarios Finales Definitivos': 'METDB.MET_REPARTOSDIARIOSFINALDEF'
    }

    table_map_columns = {
        'METDB.MET_REPARTOSDIARIOSPROV': 'RDP_FEC, RDP_PEDIDO, RDP_INFRASTRUCT, RDP_PUNTO, RDP_DESTINO, RDP_PORCENT_DESTINO, RDP_USOGAS, RDP_CDM, RDP_PORCENT_REPARTO, RDP_REPARTO, RDP_CANTIDAD, RDP_ORIGEN, RDP_TITULAR, RDP_ALBARAN, RDP_HORA, RDP_TOT_CISTERNA, RDP_PCS',
        'METDB.MET_REPARTOSDIARIOSFINALPROV': 'RDFP_FEC, RDFP_PEDIDO, RDFP_INFRASTRUCT, RDFP_PUNTO, RDFP_DESTINO, RDFP_PORCENT_DESTINO, RDFP_USOGAS, RDFP_CDM, RDFP_PORCENT_REPARTO, RDFP_REPARTO, RDFP_CANTIDAD, RDFP_ORIGEN, RDFP_TITULAR, RDFP_ALBARAN, RDFP_HORA, RDFP_TOT_CISTERNA, RDFP_PCS',
        'METDB.MET_REPARTOSDIARIOSFINALDEF': 'RDFD_FEC, RDFD_PEDIDO, RDFD_INFRASTRUCT, RDFD_PUNTO, RDFD_DESTINO, RDFD_PORCENT_DESTINO, RDFD_USOGAS, RDFD_CDM, RDFD_PORCENT_REPARTO, RDFD_REPARTO, RDFD_CANTIDAD, RDFD_ORIGEN, RDFD_TITULAR, RDFD_ALBARAN, RDFD_HORA, RDFD_TOT_CISTERNA, RDFD_PCS'
    }

    if table_indicator not in table_map:
        raise ValueError(f"Unknown table type in B1: {table_indicator}")

    table_name = table_map[table_indicator]
    table_columns = table_map_columns[table_name]
    columns_list = [col.strip() for col in table_columns.split(',')]


    df = pd.read_excel(file_path, sheet_name='Datos', engine='openpyxl', header=2)
    df.columns = df.columns.str.strip()
    print(df.columns)


    pct_columns = ['Porcentaje Destino', 'Porcentaje Reparto Comercial']
    for col in pct_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace('%', '', regex=False).str.replace(',', '.').astype(float)


    if 'Fecha' in df.columns:
        df['Fecha'] = pd.to_datetime(df['Fecha'], dayfirst=True, errors='coerce').dt.date
    if 'Hora' in df.columns:
        df['Hora'] = pd.to_datetime(df['Hora'], format='%H:%M:%S', errors='coerce').dt.time

    placeholders = ', '.join(['?'] * len(columns_list))
    sql = f"INSERT INTO {table_name} ({table_columns}) VALUES ({placeholders})"

    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        for (_, row) in enumerate(df.iterrows()):
            params = row[1].tolist()
            cursor.execute(sql, params)

        conn.commit()

    print(f"Data uploaded successfully to {table_name}")


for filename in os.listdir(base_folder):
    if filename.lower().endswith('.xlsx'):
        file_path = os.path.join(base_folder, filename)
        upload_excel_to_db(file_path)

        dest_path = os.path.join(processed_folder, filename)
        shutil.move(file_path, dest_path)



end_time = time.time()
elapsed_time = end_time - start_time
print(f"Finished processing in {elapsed_time:.2f} seconds.")
