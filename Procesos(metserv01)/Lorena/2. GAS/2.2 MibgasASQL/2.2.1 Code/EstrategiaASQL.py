# -*- coding: utf-8 -*-
"""
Created on Tue Jan 16 17:08:48 2024

@author: lorena.sanz
"""
# %% IMPORTS
import seaborn as sns
import time
from logging import handlers
import logging
from datetime import date
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.stattools import adfuller
import matplotlib.pyplot as plt
from sqlalchemy.dialects.mysql import insert
import pyodbc
from datetime import datetime, timedelta
from urllib.request import urlopen
import json
import keyring
import datetime
import pandas as pd
import numpy as np
import requests
from sqlalchemy import create_engine
pd.set_option('display.max_columns', None)
# from curvas_230_234 import curvas

now = datetime.datetime.now().strftime('%d/%m/%Y %H:%M')
# wd = r'\MET EE' #lor
wd = '' #meterisk

path_resultados = r"M:"+wd+r"\Portfolio Management\Lorena\2. GAS\2.2 MibgasASQL\2.2.1 Code\Resultados.txt"
path_mibgas = r"M:"+wd+r"\Wholesale\MIBGAS.xlsm"

# wdir = r"M:"+wd+r"\Portfolio Management\Lorena\1. POWER\1.1 SourcingCalculations (ENDUR)\1.1.1 Code"

fRes = open(path_resultados, "a+")
withTable = 'append'
fRes.write("\n\nLog "+now+":\n")

# %% CREDENTIALS
sql = keyring.get_credential("sql", None)
# sql = keyring.get_credential("SQL", None)  # lor
sqluser = sql.username
sqlpassword = sql.password

sql_url = f'mssql+pyodbc://{sqluser}:{sqlpassword}@met-esp-prod.database.windows.net/Risk_MGMT_Spain?driver=ODBC Driver 17 for SQL Server'
table = "METDB.TMP_CURVFORW"
engine = create_engine(sql_url, fast_executemany=True)

# %% EXCEL
try:
    key = {'PAPERTECH': 1, 'MIBGAS DA REF': 2, 'ARBITRAJE AVB': 3, 'ARBITRAJE TVB': 4,
           'PESSA 2024': 5, 'COMPRA DIRECTA': 6, 'TVB CISTERNAS VSS': 7, np.nan: 8,
           'TRANSFER PRICE': 9, 'ANALISIS_DA': 10, 'MIBGAS LPI': 11, 'AUCTION DA': 12,
           'MIBGAS API':13}

    xcl = pd.read_excel(path_mibgas, sheet_name='Transacciones', header=4)
    xcl = xcl[[c for c in xcl if c != "AUXILIAR"]]
    xcl['Fecha de sesión'] = pd.to_datetime(xcl['Fecha de sesión'])
    
    try:
        xcl[' Día gas inicial'] = pd.to_datetime(xcl[' Día gas inicial'])
        xcl[' Día gas final'] = pd.to_datetime(xcl[' Día gas final'])
    # xcl = xcl[xcl['Fecha de sesión'] >= dateFrom]
    except:
        pass

    xcl[' Día gas inicial'] = xcl[' Día gas inicial'].dt.date
    xcl[' Día gas final'] = xcl[' Día gas final'].dt.date
    xcl['Fecha de sesión'] = xcl['Fecha de sesión'].dt.date

    xcl['Strategy'] = xcl['Strategy'].str.upper()
    xcl['Strategy Code'] = xcl['Strategy'].map(lambda x: key[x])
    xcl['Comments'].fillna('', inplace=True)

    xcl = xcl[[i for i in xcl.columns if all(
        j not in i for j in ['Unnamed', 'Aux', 'Agente', 'Strategy'])]+['Strategy Code']]

    # AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AU
    # xcl.fillna('NAN', inplace=True)
    # if 'NAN' in xcl[[i for i in xcl.columns if all(j not in i for j in ['alta', 'producto', 'Código de transacción', 'Código de oferta', 'Segmento', 'Importe', 'Comments', 'Strategy'])]].values:
    vcols = [i for i in xcl.columns if all(j not in i for j in ['alta', 'producto', 'Código de transacción', 'Código de oferta', 'Segmento', 'Importe', 'Comments', 'Strategy'])]
    # if any(len(j)!= 0.0 for i in vcols for j in xcl[i][xcl[i].isnull()]):
    if any(len(xcl[i][xcl[i].isnull()])!= 0.0 for i in vcols):
        # body = 'Quedan valores por rellenar en el excel'
        # msg.attach(MIMEText(body, 'plain'))
        # with smtplib.SMTP(smtp_server, smtp_port) as server:
        #     server.starttls()
        #     server.login(email_address, email_password)
        #     server.send_message(msg)
        # logger.exception()
        t = "\nQuedan valores por rellenar en el excel.\nEl archivo no se ha ejecutado correctamente, rellene los datos que faltan e intente otra vez."
        fRes.write(t)
        fRes.close()
        raise Exception("Quedan valores por rellenar en el excel")
    # xcl.dropna(axis=0, how = 'any',inplace=True)
    # xcl = xcl['Código de transacción'].replace(
    #     'NAN', 0)
    xcl['Código de transacción'] = xcl['Código de transacción'].fillna(0).astype(int)
    # AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AU

    cols_mo = ['TST_CDTRANS', 'TST_ORIGEN', 'TST_ETG_ID']  # 'ETG_DENOM',
    cols_met = ['ME5_cdcartera', 'ME5_FECINI', 'ME5_FECFIN', 'ME5_FECSESION', 'ME5_FECALTA', 'ME5_CDPROD', 'ME5_CDINSTAL', 'ME5_CDZONA', 'ME5_CDTRANS',
                'ME5_CDOFERTA', 'ME5_TCASAC', 'ME5_INTIPOFE', 'ME5_CANTIDAD', 'ME5_PRECIO', 'ME5_IMPORTE', 'ME5_SEGMENTO', 'ME5_ORIGEN', 'ME5_OBSERV', 'ME5_ETG_ID']
    # float_cols = ['ME5_CANTIDAD','ME5_PRECIO','ME5_IMPORTE']
    # cols_met_aux = ['ME5_cdcartera','ME5_FECINI','ME5_FECFIN','ME5_FECSESION','ME5_FECALTA','ME5_CDPROD','ME5_CDINSTAL','ME5_CDZONA','ME5_CDTRANS','ME5_CDOFERTA','ME5_TCASAC','ME5_INTIPOFE','ME5_SEGMENTO','ME5_ORIGEN','ME5_ETG_ID']
    # cols_met_aux = ['ME5_CDTRANS', 'ME5_ETG_ID']

    strat_mo = xcl[xcl['Origen'] == 'MO'][[
        'Código de transacción', 'Origen', 'Strategy Code']].copy()
    strat_mo.columns = cols_mo
    strat_mo = strat_mo[cols_mo].copy()  # [1:]

    # AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AU
    # strat_mo = strat_mo.drop_duplicates(['TST_CDTRANS'])
    if len(set(strat_mo['TST_CDTRANS'][strat_mo['TST_CDTRANS'].duplicated(
            keep=False)])) != 0:
        now = pd.Timestamp.now(tz='CET').strftime('%d/%m/%Y %H:%M')
        t = "\nHay Trades MO Repetidas: " + str(set(strat_mo['TST_CDTRANS'][strat_mo['TST_CDTRANS'].duplicated(
            keep=False)])) + ".\nEl archivo no se ha ejecutado correctamente, arregle los conflictos e intente otra vez."
        # logger.exception(t)
        fRes.write(t)
        fRes.close()
        raise Exception(t)
    # AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AU
    strat_met = xcl[xcl['Origen'] == 'MET'].copy()
    strat_met.columns = cols_met
    strat_met.ME5_FECALTA = pd.to_datetime(
        strat_met.ME5_FECALTA, dayfirst=True)
    # strat_met[float_cols] = strat_met[float_cols].astype(float)

    if len(strat_met[['ME5_FECINI','ME5_CDTRANS', 'ME5_ETG_ID']][strat_met[['ME5_CDTRANS', 'ME5_ETG_ID']].duplicated(keep=False)]) != 0:
        now = pd.Timestamp.now(tz='CET').strftime('%d/%m/%Y %H:%M')
        t = "\nHay Trades MET Con Código y Estrategia Repetidas: " + str(set(strat_met['ME5_CDTRANS'][strat_met[['ME5_CDTRANS', 'ME5_ETG_ID']].duplicated(
            keep=False)])) + ".\nEl archivo no se ha ejecutado correctamente, arregle los conflictos e intente otra vez."
        # logger.exception(t)
        fRes.write(t)
        fRes.close()
        raise Exception(t)
    # AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AU

    # %%

    metOld = pd.read_sql('SELECT * FROM [METDB].[MET_MET3145]', engine)
    # metOld[float_cols] = metOld[float_cols].astype(float)

    moOld = pd.read_sql('SELECT * FROM [METDB].[MET_TRANSESTRAT]', engine)

    # %% EXCEPTIONS

    checkMo = pd.merge(strat_mo.copy(), moOld.copy(), how='outer', on=[
                       'TST_CDTRANS', 'TST_ETG_ID'], indicator=True)
    checkMet = pd.merge(strat_met.copy(), metOld.copy(),
                        how='outer', on=cols_met, indicator=True)

    if len(checkMo[checkMo._merge == 'both']['TST_CDTRANS']) != len(moOld['TST_CDTRANS']):
        codesr1 = checkMo[checkMo._merge ==
                          'right_only']['TST_CDTRANS'].tolist()
        for num1 in codesr1:
            auxOld1 = moOld[moOld['TST_CDTRANS'] == num1].copy()
            auxNew1 = strat_mo[strat_mo['TST_CDTRANS'] == num1].copy()
            if len(auxNew1) == 0:
                t1 = f"\nEl código MO {num1} ha sido borrado del excel. Revisen la base de datos."
                fRes.write(t1)
            else:
                for l1 in auxOld1.index:
                    for p1 in auxNew1.index:
                        diff1 = [i for i in cols_mo if auxOld1.loc[l1]
                                 [i] != auxNew1.loc[p1][i]]
                        if len(diff1) == 0:
                            continue
                        else:
                            t11 = f"\n\nEl código MO {num1} ha sido alterado, el valor de {diff1} ha cambiado de\n{auxOld1.loc[l1][diff1]}\na\n{auxNew1.loc[p1][diff1]}.\nEstos valores no se actualizarán hasta que se verifique si el cambio es correcto."
                            fRes.write(t11)
                            checkMo = checkMo[checkMo['TST_CDTRANS']
                                              != num1].copy()
        # fRes.close()

    if len(checkMet[checkMet._merge == 'both']['ME5_CDTRANS']) != len(metOld['ME5_CDTRANS']):
        codesr2 = list(
            set(checkMet[checkMet._merge == 'right_only']['ME5_CDTRANS']))
        # %%
        for num2 in codesr2:
            auxOld2 = metOld[metOld['ME5_CDTRANS'] == num2]
            auxNew2 = strat_met[strat_met['ME5_CDTRANS'] == num2]
            if len(auxNew2) == 0:
                t2 = f"\nEl código MET {num2} ha sido borrado del excel. Revisen la base de datos."
                fRes.write(t2)
            else:
                for l2 in auxOld2.index:
                    for p2 in auxNew2.index:
                        diff2 = [i for i in cols_met if auxOld2.loc[l2]
                                 [i] != auxNew2.loc[p2][i]]
                        # print(diff2)
                        if len(diff2) == 0:
                            continue
                        else:
                            t22 = f"\n\nEl código MET {num2} ha sido alterado, el valor ha cambiado de:\n{auxOld2.loc[l2][diff2]}\na\n{auxNew2.loc[p2][diff2]}.\nEstos valores no se actualizarán hasta que se verifique si el cambio es correcto."
                            fRes.write(t22)
                            checkMet = checkMet[checkMet['ME5_CDTRANS'] != num2].copy(
                            )
        # fRes.close()
        # raise Exception(t)

    # %%

    strat_mo = strat_mo.set_index(
        'TST_CDTRANS').loc[checkMo[checkMo._merge == 'left_only']['TST_CDTRANS']].reset_index()
    strat_met = strat_met.set_index(
        'ME5_CDTRANS').loc[checkMet[checkMet._merge == 'left_only']['ME5_CDTRANS']].reset_index()

    if len(strat_mo) == len(strat_met) == 0:
        fRes.write(
            '\n\nNo se han encontrado trades nuevas para subir a SQL.\nNO BORRAR EL ARCHIVO\n\n\n\n')
        fRes.close()
    elif len(strat_mo) != len(strat_met) == 0:
        fRes.write('\n\nNo se han encontrado trades MET nuevas para subir a SQL.')
        print('Uploading TRANSESTRAT values to SQL')
        strat_mo.to_sql('MET_TRANSESTRAT', engine, schema='METDB',
                        if_exists=withTable, index=False)
        fRes.write('\n\nLos datos MO con código de transacción no mencionado en este log han sido subidos correctamente.\nSi se han mencionado conflictos dirijanse al archivo BorrarConflictos.txt y sigan las instrucciones.\nNO BORRAR EL ARCHIVO\n\n\n\n')
        fRes.close()
    elif len(strat_met) != len(strat_mo) == 0:
        fRes.write('\n\nNo se han encontrado trades MO nuevas para subir a SQL.')
        print('Uploading MET3145 values to SQL')
        strat_met.to_sql('MET_MET3145', engine, schema='METDB',
                         if_exists=withTable, index=False)
        fRes.write('\n\nLos datos MET con código de transacción no mencionado en este log han sido subidos correctamente.\nSi se han mencionado conflictos dirijanse al archivo BorrarConflictos.txt y sigan las instrucciones.\nNO BORRAR EL ARCHIVO\n\n\n\n')
        fRes.close()
    else:
        print('Uploading TRANSESTRAT values to SQL')
        strat_mo.to_sql('MET_TRANSESTRAT', engine, schema='METDB',
                        if_exists=withTable, index=False)
        print('Uploading MET3145 values to SQL')
        strat_met.to_sql('MET_MET3145', engine, schema='METDB',
                         if_exists=withTable, index=False)
        fRes.write('\n\nLos datos con código de transacción no mencionado en este log han sido subidos correctamente.\nSi se han mencionado conflictos dirijanse al archivo BorrarConflictos.txt y sigan las instrucciones.\nNO BORRAR EL ARCHIVO\n\n\n\n')
        fRes.close()

except:
    fRes.write(
        '\n\nHa ocurrido algún error: EL ARCHIVO NO SE HA EJECUTADO CORRECTAMENTE.\n\n\n\n')
    fRes.close()

# %% REJECTED CODE

# strat_met.ME5_FECINI = strat_met.ME5_FECINI.dt.strftime('%Y-%m-%d')
# strat_met.ME5_FECFIN = strat_met.ME5_FECFIN.dt.strftime('%Y-%m-%d')
# strat_met.ME5_FECSESION = strat_met.ME5_FECSESION.dt.strftime('%Y-%m-%d')

# q = []
# for j in cols[1:]:
#     if cols[1:].index(j) == 0:
#         q.append(f"WHERE [{j}] = '{record[j]}'")
#     else:
#         q.append(f" AND [{j}] = '{record[j]}'")
# q = ''.join(q)#

# choose NA strategy: remove or bring to attention
# #AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AU
# strat_mo.dropna(axis=0, how = 'any',inplace=True)
# strat_mo.fillna('NAN',inplace=True)
# #AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AUX AU

# strat_mo = strat[strat['ME5_ORIGEN']=='MO'].copy()
# strat_met = strat[strat['ME5_ORIGEN']=='MET'].copy()

# strat['Strategy'] = strat['Strategy'].str.upper()
# strat['Strategy'] =  strat['Strategy'].map(lambda x: key[x])


# for (i,record) in strat_met.iterrows():
#     q = []
#     for j in cols_met_aux:
#         if cols_met_aux.index(j) == 0:
#             q.append(f"WHERE [{j}] = '{record[j]}'")
#         else:
#             q.append(f" AND [{j}] = '{record[j]}'")
#     q = ''.join(q)

#     deleteQuery = '''DELETE FROM [METDB].[MET_MET3145] '''+q
#     print('DELETING MET3145 values from db. DO NOT INTERUPT THIS PROCESS. {i}/{len(strat_met}')
#     with engine.begin() as conn:
#         conn.execute(deleteQuery)

# for (i,record) in strat_mo.iterrows():

#     q = []
#     for j in cols_mo: #[1:]
#         if cols_mo.index(j) == 0: #[1:]
#             q.append(f"WHERE [{j}] = '{record[j]}'")
#         else:
#             q.append(f" AND [{j}] = '{record[j]}'")
#     q = ''.join(q)

#     deleteQuery = '''DELETE FROM [METDB].[MET_TRANSESTRAT] '''+q
#     print('DELETING TRANSESTRAT values from db. DO NOT INTERUPT THIS PROCESS. {i}/{len(strat_mo}')
#     with engine.begin() as conn:
#         conn.execute(deleteQuery)


# credential = keyring.get_credential("system", None)
# username = credential.username
# password = credential.password

# smtp_handler = logging.handlers.SMTPHandler(mailhost = ('smtp-mail.outlook.com',587),
#                                             fromaddr = 'lorena.sanz@met.com',
#                                             toaddrs = ['matteo.liberati@met.com','luismiguel.munoz@met.com'],
#                                             subject = 'Error con el Fichero de Estrategias',
#                                             credentials = (username,password),
#                                             secure = ())


# logger = logging.getLogger()
# logger.addHandler(smtp_handler)

# #%%

# import smtplib
# from email.mime.text import MIMEText
# from email.mime.multipart import MIMEMultipart

# # Define email settings
# smtp_server = 'smtp-mail.outlook.com'
# smtp_port = 587
# email_address = username
# email_password = password

# # Create email message
# subject = 'Error al Subir el Fichero de Estrategias'

# msg = MIMEMultipart()
# msg['From'] = email_address
# msg['To'] = ['matteo.liberati@met.com','luismiguel.munoz@met.com']
# msg['Subject'] = subject


# dateFrom = date.today() - pd.DateOffset(days=1)

# codesr = list(zip(checkMo[checkMo._merge == 'right_only']['TST_CDTRANS'].tolist(),checkMo[checkMo._merge == 'right_only']['TST_ETG_ID'].tolist()))
# codesl = checkMo[checkMo._merge == 'left_only']['TST_CDTRANS'].tolist()
# t = "Estos códigos MO han sido borrados o alterados, sus valores no corresponden a los de la base de datos: "+str(codes)
# fCam.write(t)
# fCam.close()
# raise Exception(t)
