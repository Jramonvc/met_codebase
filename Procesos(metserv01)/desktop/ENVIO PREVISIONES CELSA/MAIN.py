import pandas as pd
import os
import glob
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
import requests
import json
from datetime import date
from requests.auth import HTTPBasicAuth

client_id = "meten-integrator"
client_secret = "fD2FacbeLuQ9vVcBRDRorx5mt1GNDfmt"
token_url = "https://sso.trading.bluence.com/realms/trading/protocol/openid-connect/token"
api_base_url = "https://trading.bluence.com/api/integrator"

def main():
    directorio_actual = os.getcwd()

    archivos_excel = [
        f for f in glob.glob(os.path.join(directorio_actual, "*.xls*"))
        if not os.path.basename(f).startswith("~$")
    ]

    if len(archivos_excel) != 1:
        raise Exception(f"Se esperaba un único archivo Excel, pero se encontraron: {len(archivos_excel)}")

    ruta_excel = archivos_excel[0]
    df = pd.read_excel(ruta_excel)

    print("ARCHIVO EXCEL OBTENIDO CORRECTAMENTE: ")
    print(df.head())
    # Convertimos fecha y calculamos HORA y QHOR como en la base de datos
    df['Fecha'] = pd.to_datetime(df['Fecha']).dt.date
    df['PQH_HORA'] = ((df['QH'] - 1) // 4 + 1).astype(int)
    df['PQH_QHORARIO'] = ((df['QH'] - 1) % 4 + 1).astype(int)

    engine = crear_conexion_bd()

    print("\nPREVISIONES DE CELSA OBTENIDAS CORRECTAMENTE")
    df_prevs = obtener_prevs_meter(engine)

    # Convertir fechas a formato date para comparación
    df['Fecha'] = pd.to_datetime(df['Fecha']).dt.date
    df_prevs['PQH_FECHA'] = pd.to_datetime(df_prevs['PQH_FECHA']).dt.date
    df.rename(columns={'Fecha': 'PQH_FECHA'}, inplace=True)
    # Merge entre Excel y BBDD por fecha, hora y qhorario
    df_merged = pd.merge(
        df,
        df_prevs,
        on=['PQH_FECHA', 'PQH_HORA', 'PQH_QHORARIO'],
        how='inner'
    )

    # Calcular previsiones ponderadas (sin dividir % porque ya vienen como decimales)
    df_merged['Prevision_Total'] = df_merged['PQH_PREVISION']
    df_merged['Prevision_Bilateral'] = df_merged['Prevision_Total'] * df_merged['%Bilateral']
    df_merged['Prevision_OMIE'] = df_merged['Prevision_Total'] * df_merged['%OMIE']
    df_merged['Total_Prevision_Calculada'] = ((df_merged['Prevision_Bilateral'] + df_merged['Prevision_OMIE']) * 4).round(1)

    # Reordenar columnas
    columnas_resultado = [
        'PQH_FECHA', 'PQH_HORA', 'PQH_QHORARIO',
        'Prevision_Total', '%Bilateral', '%OMIE',
        'Prevision_Bilateral', 'Prevision_OMIE', 'Total_Prevision_Calculada'
    ]

    print("\nRESULTADO FINAL CON CÁLCULOS:")
    print(df_merged[columnas_resultado].to_string(index=False))
    token=get_token(client_id, client_secret)
    upload_forecasts(token,df_merged)

def crear_conexion_bd():
    try:
        conn_str = r"mssql+pyodbc://sqluser:cwD9KVms4Qdv4mLy@met-esp-prod.database.windows.net:1433/Risk_MGMT_Spain?driver=ODBC+Driver+17+for+SQL+Server"
        engine = create_engine(conn_str, fast_executemany=True)
        print("\nCONEXIÓN EXITOSA CON LA BBDD DE MET")
        return engine
    except Exception as e:
        print(f"Error conectando a la base de datos: {e}")
        return

def obtener_prevs_meter(engine):
    query = text("""
    WITH UltimasVersiones AS (
        SELECT PQH_UP, PQH_FECHA, PQH_HORA, PQH_QHORARIO,
               MAX(PQH_VERSION) AS MaxVersion
        FROM METDB.MET_PREVPOWERQH
        WHERE PQH_UP = 'METER01'
          AND PQH_FECHA >= CONVERT(date, GETDATE())
          AND PQH_FECHA < DATEADD(DAY, 7, CONVERT(date, GETDATE()))
        GROUP BY PQH_UP, PQH_FECHA, PQH_HORA, PQH_QHORARIO
    )
    SELECT p.*
    FROM METDB.MET_PREVPOWERQH p
    INNER JOIN UltimasVersiones uv
        ON p.PQH_UP = uv.PQH_UP
       AND p.PQH_FECHA = uv.PQH_FECHA
       AND p.PQH_HORA = uv.PQH_HORA
       AND p.PQH_QHORARIO = uv.PQH_QHORARIO
       AND p.PQH_VERSION = uv.MaxVersion
    """)
    with engine.begin() as conn:
        df_resultado = pd.read_sql_query(query, conn)
    return df_resultado
    
def upload_forecasts(token, df):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    url = f"{api_base_url}/forecast"

    # Calculamos el contrato (periodo de 1 a 96)
    df['contract'] = ((df['PQH_HORA'] - 1) * 4 + df['PQH_QHORARIO']).astype(int)

    forecast_payload = []

    for trading_day in df['PQH_FECHA'].unique():
        df_dia = df[df['PQH_FECHA'] == trading_day]

        for _, row in df_dia.iterrows():
            payload = {
                "unit": "METER01",
                "tradingDay": str(trading_day),
                "contract": int(row['contract']),
                "measureType": "MW",
                "periodSeconds": 900,
                "zoneId": "Europe/Madrid",
                "values": [
                    {"x": 0, "y": 0},
                    {"x": 50, "y": float(row['Total_Prevision_Calculada'])},
                    {"x": 100, "y": 0}
                ]
            }
            forecast_payload.append(payload)

    # Enviar datos
    response = requests.put(url, headers=headers, json=forecast_payload)

    if response.status_code == 204:
        print("Forecast enviado correctamente.")
    else:
        print("Error al enviar forecast:", response.status_code, response.text)
        
def get_token(client_id, client_secret):
    response = requests.post(
        token_url,
        data={"grant_type": "client_credentials"},
        auth=HTTPBasicAuth(client_id, client_secret),
        headers={"Content-Type": "application/x-www-form-urlencoded"}
    )
    if response.status_code == 200:
        return response.json()["access_token"]
    else:
        print("❌ Error obteniendo token:", response.status_code, response.text)
        return None


if __name__ == '__main__':
    main()
