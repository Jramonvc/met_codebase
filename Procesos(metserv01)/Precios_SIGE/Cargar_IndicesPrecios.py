import pandas as pd
from sqlalchemy import create_engine
# Configurar las conexiones a las bases de datos
SIGE_url = 'mssql+pyodbc://sa:Aq-1d.BlkY@82.194.94.135/SIGEMET?driver=ODBC+Driver+17+for+SQL+Server'
Azure_url = 'mssql+pyodbc://sqluser:cwD9KVms4Qdv4mLy@met-esp-prod.database.windows.net/Risk_MGMT_Spain?driver=ODBC+Driver+17+for+SQL+Server'

#conn_str_src = ('DRIVER={SQL Server};SERVER=82.194.94.135;DATABASE=SIGEMET;UID=sa;PWD=Aq-1d.BlkY;')
# Reemplaza 'server2', 'database2', 'username2', 'password2' con los detalles de la segunda base de datos
#conn_str_dest = ('DRIVER={SQL Server};SERVER=met-esp-prod.database.windows.net;DATABASE=Risk_MGMT_Spain;UID=sqluser;PWD=cwD9KVms4Qdv4mLy')

# Conectar a ambas bases de datos
#conn_src = pyodbc.connect(conn_str_src)
#conn_dest = pyodbc.connect(conn_str_dest)

# Crear cursores para ambas conexiones
#cursor_src = conn_src.cursor()
#cursor_dest = conn_dest.cursor()

try:
    # Ejecutar una consulta para seleccionar los datos que deseas transferir
       #cursor_src.execute(
       sql_query = """   Select convert(date,fxg_fecha) fecha, fxg_idm_codigo INDICE, avg(fxg_price) Precio
                       From METDB.MET_FIXINGS
                       Where FXG_IDM_CODIGO IN ('OMIE','MIBGAS','TTFD','MIBGDA')
                        And CONVERT(DATE,FXG_FECHA) between  dateadd(month, -1, convert(date,getdate())) and convert(date,getdate())
                      Group by convert(date,fxg_fecha), fxg_idm_codigo
					  union all 
					  select CONVERT(DATE, DIA) FECHA, STF_IDM_CODIGO INDICE, STF_PRICE Precio
	                   from METDB.MET_SYNTHFIX, METDB.VW_DIASANYO
	                  where CONVERT(DATE,DIA) between  dateadd(month, -1, convert(date,getdate())) and convert(date,getdate())
					    AND STF_IDM_CODIGO = 'TTFM'
	                    AND FORMAT(STF_FECHA,'MM/yyyy') = FORMAT(DIA, 'MM/yyyy');"""
       
       engine_source = create_engine(Azure_url, fast_executemany=True)
       engine_destination = create_engine(SIGE_url, fast_executemany=True)
       df = pd.read_sql(sql_query, engine_source)
    # Escribir los datos en la tabla de la base de datos de destino
       #df.to_sql('SGEMET.DBO.MET_PRECIOS', engine_destination, schema='sigemet.dbo',if_exists='append', index=False)
       df.to_sql('MET_PRECIOS', engine_destination, schema='SIGEMET.DBO',if_exists='append', index=False) 
       print("Transferencia de datos exitosa.")

except Exception as e:
    print("Error durante la transferencia de datos:", e)




