# -*- coding: utf-8 -*-
"""
Created on Thu Nov 14 11:49:08 2024

@author: lorena.sanz
"""
import os

# wd = r"\MET EE" # lor 
wd = r"" # meterisk

wdir = r"M:"+wd+r"\Portfolio Management\Lorena\1. POWER\1.1 SourcingCalculations (ENDUR)\1.1.1 Code"
os.chdir(wdir)

path_perfiles = r"M:"+wd+r"\Portfolio Management\Lorena\1. POWER\1.2 WorkingData\1.2.3 Output\1.2.3.2 Final\2024\PerfilesPower.xlsx"
path_neuro = r"M:"+wd+r"\Portfolio Management\Lorena\1. POWER\1.2 WorkingData\1.2.2 Datasets"
neuro_pt2 = r"1.2.2.1 DemandaNeuro/*"
output_path = r"M:"+wd+r"\Portfolio Management\Lorena\1. POWER\1.1 SourcingCalculations (ENDUR)\1.1.4 Output\1.1.4.1 PosiciónPower"

import matplotlib.pyplot as plt
import numpy as np
import keyring
from datetime import date
import pandas as pd
from sqlalchemy import create_engine, text
pd.set_option('display.max_columns', None)
import glob
import time

from PowerToEndur import postDeals, checkStatus
from json import loads, dumps
from PowerProfiledData_bbddED import getVolumes, profiledVol, neuroLoss
from GetPowerCartera import getCartera, getHourlyCartera
from dateutil.relativedelta import relativedelta
from datetime import datetime, date
from GetPowerData import *
import numpy as np
import pandas as pd

# %% USER INPUT

tipo = 'prod' # prod | dev
v  = 'v2' # v1 | v2
txt = 'GTF_CODIGO'

dfColumns = ['CUP_CODIGO','TAR_CODIGO','GTF_CODIGO','CONSESTIMADO','FECALT', 'FECBAJ', 'FECVEN']

primas = ['Flexibility','Profile','Apuntamiento',
              'Validity', 'SEIE','Ajuste KYOS'] #, 'Financiero''SSAA','Imbalance',
primasCols = ['flex_premium','profile_premium',
              'aim_premium','validity_premium','seie_premium','kyos_adj_premium'] #,'financial_cost''ssaa_premium','imbalance_premium',
primasDict = dict(zip(primas,primasCols))
preciosCols = ["KYOS Precio Base PML"]

directory = {'PENINSULA': 'PEN', 'CANARIAS': 'CAN',
             'BALEARES': 'BAL', 'CEUTA': 'CEU', 'MELILLA': 'MEL', 'NA': 'NA'}

cargo = {"request_type": "PowerVolumeAndPrice",
          "source_system": "CRM_ES",
          "email": "lorena.sanz@met.com"
          }
pImportant = ['Tarifa Neuro','ID 2024','Peaje','Perfil','Zona','KYOS Precio Base PML']

celsaCups = ['ES0031406288715001LQ','ES0027700032086001PB','ES0021000009935376BV','ES0027700032085001BT','ES0022000008438131DQ','ES0026000000886409KM','ES0031405090939001TF','ES0027700208406001BX','ES0021000000067980JN','ES0021000000049113KM']

td = date.today()
# start_m1 = pd.to_datetime(td).floor('D').replace(day=1)
# start = pd.to_datetime(td).floor('D').replace(day=1) + pd.Timedelta(days=pd.to_datetime(td).daysinmonth)
start = pd.to_datetime('01/01/2025') 
# start = pd.to_datetime('1/1/2024')
# start = pd.to_datetime(date.today()).floor('D').replace(day=1) + pd.Timedelta(days=pd.to_datetime(date.today()).daysinmonth)
start_m2 = start + pd.Timedelta(days=start.daysinmonth)

lstart = start.tz_localize('CET')

premium = False
upload = True
neuroM1 = False
celsa = False
loss = True

primasDeals = {'profile_premium':14476852,'aim_premium':14476852,'seie_premium':14477302,
               'validity_premium':14477310,'kyos_adj_premium':14477310,'flex_premium':14477316}

# %% AUTHENTICATION
def auth():
    sql = keyring.get_credential("SQL", None)
    sqluser = sql.username
    sqlpassword = sql.password
    
    sql_url = (f'mssql+pyodbc://{sqluser}:{sqlpassword}@met-esp-' +
               'prod.database.windows.net/Risk_MGMT_Spain?driver' +
               '=ODBC Driver 17 for SQL Server')
    
    eng = create_engine(sql_url, fast_executemany=True)
    return eng

#%% DATA QUERY
startstr = start.strftime('%Y-%m-%d')

queryCli = """SELECT * 
FROM [METDB].[MET_CONTRATOS] C1, METDB.MET_CONTTARIF P1
WHERE CNT_CODIGO = CTT_CNT_CODIGO 
AND CNT_POTP1 IS NOT NULL 
AND (CNT_CNS_CODIGO NOT IN ('A','B','T') OR (CNT_CNS_CODIGO = 'T' 
                                             and not exists (SELECT 'X' FROM METDB.MET_CONTRATOS C2 
                                                             WHERE C2.CNT_CUP_CODIGO = C1.CNT_CUP_CODIGO 
                                                             AND C2.CNT_CNS_CODIGO = 'C')))
"""

# queryCli = "SELECT * FROM [METDB].[MET_CONTRATOS] WHERE CNT_POTP1 IS NOT NULL and CNT_CNS_CODIGO NOT IN ('A','B')"
# queryCli = "select * from metdb.met_contratos as cont left join metdb.met_contsitua as situa on cont.cnt_cns_codigo = situa.cns_codigo where cnt_potp1 is not null"
# queryCli = "SELECT * FROM [METDB].[MET_CONTRATOS] WHERE CNT_POTP1 IS NOT NULL"
queryPre = "SELECT * FROM [METDB].[MET_POWPRIC]"#" where PWP_FECFIN >= '"+startstr+"' or PWP_FECFIN is null"
queryPerf = "SELECT * FROM [METDB].[MET_POWPERFIL]"
queryZone = "SELECT * FROM [METDB].[MET_ZONAS]"
queryAct = " SELECT * FROM [METDB].[MET_CONTSITUA]"
queryClicks = "SELECT * FROM [METDB].[MET_CLICKS] WHERE CLK_FECFIN >= '"+startstr+"'"
querynewContracts = "SELECT CNT_CUP_CODIGO, CNT_FECALT, CNT_FECBAJ, CNT_FECVEN, CNT_TAR_CODIGO, CNT_CNS_CODIGO, PWP_TIPPRIC FROM METDB.MET_CONTRATOS LEFT JOIN METDB.MET_POWPRIC ON CNT_GTF_CODIGO = PWP_GTF_CODIGO WHERE CNT_POTP1 IS NOT NULL AND CNT_FECVEN >= '"+startstr+"'"
 
engine = auth()
with engine.begin() as conn:
    clientes = pd.read_sql(queryCli, conn)
    precios = pd.read_sql(queryPre, conn)
    perfil = pd.read_sql(queryPerf, conn)
    zone = pd.read_sql(queryZone, conn)
    activity = pd.read_sql(queryAct, conn)
    clicks = pd.read_sql(queryClicks,conn)
    newContracts = pd.read_sql(querynewContracts,conn)
    
clientes.columns = [c[4:] for c in clientes.columns]
precios.columns = [c[4:] for c in precios.columns]
clicks.columns = [c[4:] for c in clicks.columns]
# perfil.columns = [c[4:] for c in perfil.columns]

clientes = clientes.loc[:,~clientes.columns.duplicated()].copy(deep=True)
contratos = clientes.merge(precios, how='outer',on=txt,suffixes=('','_2'),indicator=True).copy(deep=True)


ct = contratos[contratos._merge == 'both'][[c for c in contratos.columns if c != '_merge']].copy(deep=True)
ct = ct.dropna(subset='CUP_CODIGO')
# ct['CAUDAL'] = [p/1000 for p in ct['CAUDAL'] if p != 'ES0027700032085001BT']

ct['CAUDAL'] = ct['CAUDAL'].replace(0,np.nan)
# ct.update((ct['CONSESTIMADO'].rename('CAUDAL')),overwrite=False)
ct.update((ct['CONSCONTR'].rename('CAUDAL')),overwrite=False)



ct['consumo'] = (ct['CONSCONTR'] * ct['PORCENTAJE'])/1000 #(ct['CAUDAL'] * ct['PORCENTAJE'])/1000



ct = ct.merge(perfil, how='left', on = 'PPF_CODIGO',indicator=True)
if len(ct[ct._merge != 'both']) > 0:
    raise Exception()
ct = ct[ct._merge == 'both'][[c for c in ct.columns if c != '_merge']]
    
ct = ct.merge(zone, how='left', on = 'ZNS_ID',indicator=True)
if len(ct[ct._merge != 'both']) > 0:
    raise Exception()
ct = ct[ct._merge == 'both'][[c for c in ct.columns if c != '_merge']]

ct.ZNS_DENOM = ct.ZNS_DENOM.fillna("NA")

#%%

def getVols(row,losses,auxlist):
    # get times
    tmln = pd.date_range(row.Inicio, row['Fin Calc'], freq='h',tz='CET',inclusive='left')
    # get profile
    if row.Perfil == 'ESTANDAR REE':
        if '6' in row.Tarifa:
            profile = profiles[row.Zona]['6.XTD'].loc[tmln]
        elif '3' in row.Tarifa:
            profile = profiles[row.Zona]['3.0TD'].loc[tmln]
        else:
            profile = profiles[row.Zona]['2.0TD'].loc[tmln]
    else:
        profile = profiles[row.Zona][row.Perfil].loc[tmln]
    
    # raise losses
    if losses == True:
        l = profiles[row.Zona][row.Tarifa + ' w/k'].loc[tmln] + 1
        profile = profile * l
    
    # put it all together
    consumo = pd.DataFrame(row['Volumen Anual MWh'] * profile,columns=['cons'])
    consumo['month'] = consumo.index.month
    consumo['year'] = consumo.index.year
    
    consumoPrime = (consumo.groupby(['year','month'])['cons'].sum()).reset_index()
    consumoPrime.index = consumoPrime.apply(lambda x: pd.to_datetime(str(int(x.year))+'-'+str(int(x.month))+'-01'), axis=1)
    
    consumoPrime = consumoPrime[['cons']]
    
    for col in ['CUPS','Tarifa','Precio','Formula','Zona','Flexibility','Profile','Apuntamiento','Validity','SEIE','Ajuste KYOS','Perfil']:
        consumoPrime[col] = row[col]
    
    return auxlist.append(consumoPrime)

#%% DATA DATING

# clicks = clicks[clicks.FECINI >= start]

clicks.FECINI = pd.to_datetime(clicks.FECINI).dt.tz_localize('CET')
clicks.FECFIN = pd.to_datetime(clicks.FECFIN).dt.tz_localize('CET')

# clicks.FECFIN = clicks.FECFIN - pd.DateOffset(days=1)

"""FIX DATES"""
ct = ct[(ct.FECALT.notnull())&(ct.FECVEN.astype(str) != '5000-01-01')&(ct.FECVEN.notnull())].copy(deep=True)

ct.FECALT = pd.to_datetime(ct.FECALT).dt.tz_localize('CET')
ct.FECVEN = pd.to_datetime(ct.FECVEN).dt.tz_localize('CET')

ct.FECVEN = ct.FECVEN + pd.DateOffset(days=1)
# ct.at[5894,'FECVEN'] = pd.to_datetime('2029/12/31').tz_localize('CET')


ct.at[[i for i in ct[ct.CUP_CODIGO=='ES0021000003296974NJ'].index][0],'FECVEN'] = pd.to_datetime('2029/12/31').tz_localize('CET')


ct['Fin Calc'] = (ct['FECVEN']+pd.DateOffset(days=1)).dt.floor('D')

ct['sInicio'] = ct['FECALT'].dt.strftime('%Y-%m-%d')
ct['sFin'] = ct['FECVEN'].dt.strftime('%Y-%m-%d')

ctDated = ct[ct.FECVEN >= start.tz_localize('CET')][['CUP_CODIGO','consumo','FECALT','FECVEN','TAR_CODIGO','PRECMOL','sInicio','sFin','TIPPRIC','ZNS_DENOM','FLEXIB','PROFILE','APUNTAMIENTO','VALIDITY','SOBRECOST','AJUSTKYOS','Fin Calc','PPF_DENOM']].copy(deep=True)
ctDated.columns = ['CUPS','Volumen Anual MWh','Inicio','Fin','Tarifa','Precio','sInicio','sFin','Formula','Zona','Flexibility','Profile','Apuntamiento','Validity','SEIE','Ajuste KYOS','Fin Calc','Perfil']
lastdate = max(ctDated.Fin) + pd.DateOffset(5)

# ctDated.Zona = ctDated.apply(lambda x: x.Zona[:3], axis=1)

#%% CLICKS PT1

clicksM = clicks.merge(clientes[['CUP_CODIGO','TAR_CODIGO']],how = 'outer',on = 'CUP_CODIGO',indicator = True).copy(deep=True)
clicksBoth = clicksM[clicksM._merge == 'both'].copy(deep=True)

click = clicksBoth[['CUP_CODIGO','TAR_CODIGO','FECINI','FECFIN','VOLUMBL','PRECSOURCE']].copy(deep=True)
click.columns = ['CUPS','Tarifa','Inicio','Fin','Volumen Click MW BL','Precio']

click['Fin Calc'] = (click['Fin']+pd.DateOffset(days=1)).dt.floor('D').copy(deep=True)

#%% NEURO M+0

if neuroM1:
    neuro_fc = getNeuroForecast(path_perfiles,path_neuro,neuro_pt2)
    naux = []
    for i in neuro_fc.keys():
        naux.append(pd.Series(neuro_fc[i]['Total'], name=i))
    neuro = pd.concat(naux, axis=1)
    neuro = neuro[neuro.index >= lstart]
    neuro.index = neuro.index.tz_localize(None)
    print('Dividing up neuro forecast by LOC')
    neuro['CAN'] = neuro['NGC']+neuro['NFL']+neuro['NTF']
    neuro['PEN'] = neuro['C01']+neuro['R01']
    neuro['BAL'] = neuro['NSB']
    neuro = neuro[['PEN', 'CAN', 'BAL']].sum()
    neuro['NA'] = neuro['PEN']+neuro['CAN']+neuro['BAL']

#%% PROFILES

"""MUST UPDATE"""

profiles = {}
for loc in set(ct['ZNS_DENOM']):
    if loc == 'NA':
        profiles[loc] = getProfiles('PEN',path_perfiles)
    else:
        profiles[loc] = getProfiles(directory[loc],path_perfiles)
    profiles[loc] = profiles[loc][profiles[loc].index <=
                                  lastdate].copy(deep=True)
    
#%% DATAFRAMES

index = ctDated[ctDated.Formula == 'I'].copy(deep=True)
allFixed = ctDated[ctDated.Formula == 'F'].copy(deep=True)
justFixed = allFixed[allFixed.Perfil != 'AUTOCONSUMO20'].copy(deep=True)
solarBattery = allFixed[allFixed.Perfil == 'AUTOCONSUMO20'].copy(deep=True)



index['Volume MWh'] = ""
allFixed['Volume MWh'] = ""
solarBattery['Volume MWh'] = ""
justFixed['Volume MWh'] = ""
click['Volume MWh'] = ""

index['Deal'] = 6857467
solarBattery['Deal'] = 13970247
justFixed['Deal'] = 10451651
click['Deal'] = 7555996

#%% 
dates = {}
dates['Start'] = start
dates['End'] = pd.to_datetime('1/1/'+str(max([max(df['Fin']) for df in [index,click,justFixed,solarBattery] if len(df) != 0]).year+1))#,clicks

for df in [t for t in [index,justFixed,solarBattery,click] if len(t)]:#
    dates[[i for i in set(df.Deal)][0]] = max(df['Fin'])
dates[6857460] = dates[6857467]

print('Resetting cartera...')
# cartera = getCartera(start,primasCols,primasDeals)
cartera = getCartera(dates,primasCols,primasDeals)
# hourlyCartera = getHourlyCartera(start_m1)

index.drop_duplicates(inplace=True)
solarBattery.drop_duplicates(inplace=True)
justFixed.drop_duplicates(inplace=True)
# clicks.drop_duplicates(inplace=True)

# index = index.drop('Perfil',axis=1)
# justFixed = justFixed.drop('Perfil',axis=1)
# clicks = clicks.drop('Perfil',axis=1)

# %% CALCULATIONS
print('Performing index volume calculations: This may take a while...')
if len(index) != 0:
    index['Volume MWh'] = index['Volume MWh'].update(index.apply(
lambda x: profiledVol(x, 'Index', profiles[x.Zona].copy(deep=True), primas), axis=1))

if len(solarBattery) != 0:    
    print('Performing solar volume calculations: This may take a while...')
    solarBattery['Volume MWh'] = solarBattery['Volume MWh'].update(solarBattery.apply(
    lambda x: profiledVol(x, 'Solar', profiles[x.Zona].copy(deep=True), primas), axis=1))
    
if len(justFixed) != 0:
    print('Performing fixed volume calculations: This may take a while...')
    justFixed['Volume MWh'] = justFixed['Volume MWh'].update(justFixed.apply(
    lambda x: profiledVol(x, 'justFixed', profiles[x.Zona].copy(deep=True), primas), axis=1))
volumes = getVolumes().copy()  # volumes come in hourly format

#%% CLICKS PT2
print('Performing calculations for deal number 7555996...')
# clicks_df = pd.concat(clicks.apply(lambda x: pd.DataFrame('',index=pd.date_range(x.Inicio,x['Fin Calc'],freq='h',tz='CET'),columns=['volume']),axis=1))

if len(click) != 0:
    cartera[7555996] = cartera[7555996][cartera[7555996]['start_date'].dt.tz_localize('CET') >= min(click['Inicio'])].copy(deep=True)
    
    click['Precio'] = click['Precio'].fillna(0).copy(deep=True)
    
    store = []
    for ind in click.index:
        clickAux = pd.DataFrame(pd.date_range(click.loc[ind]['Inicio'],click.loc[ind]['Fin Calc'],freq='MS',inclusive='left'),columns=['start_date'])#,tz='CET'
        clickAux['end_date'] = pd.date_range(click.loc[ind]['Inicio'],click.loc[ind]['Fin Calc'],freq='MS',inclusive='right')#,tz='CET'
        clickAux['volume'] = click.loc[ind]['Volumen Click MW BL']#.copy(deep=True)
        clickAux['price'] = click.loc[ind]['Precio']#.copy(deep=True)
        store.append(clickAux)
    
    
    clicks_df = pd.concat(store,ignore_index=True)
    clicks_df['start_date'] = clicks_df['start_date'].dt.tz_localize(None)
    clicks_df['end_date'] = clicks_df['end_date'].dt.tz_localize(None)
    # clicks_df['dif'] = clicks_df.apply(lambda x: len(pd.date_range(x['start_date'],x['end_date'],freq='h',tz='CET',inclusive='left')), axis=1)
    clicks_df['vp'] = clicks_df['volume']*clicks_df['price']
    clicks2 = clicks_df.groupby('start_date')[['volume','price','vp']].sum()
    clicks2['price'] = clicks2['vp']/clicks2['volume']
    # clicks_df['volume'] = clicks_df['volume'] * clicks_df['dif']
    
    cartera[7555996] = cartera[7555996].set_index('start_date').copy(deep=True)
    cartera[7555996].update(clicks2[['volume','price']].copy())
    
    clicks_m = cartera[7555996].copy()

# %% SOLAR
if len(solarBattery) != 0:
    print('Performing calculations for deal number 13970247...')
    solar_df = pd.concat(volumes['Solar']['Volumes'], ignore_index=True)
    solar_df = pd.DataFrame(solar_df.groupby(['zone', 'date'])[
                            ['vol', 'vol price']+primas].sum()).reset_index().copy(deep=True)
    
    solarM1 = solar_df[solar_df['date'].dt.tz_localize(None) < start].copy(deep=True)
    solarM2 = solar_df[(solar_df['date'].dt.tz_localize(None) >= start) & (
        solar_df['date'].dt.tz_localize(None) < start_m2)].copy(deep=True)
    solarM3 = solar_df[solar_df['date'].dt.tz_localize(None) >= start].copy(deep=True)
    
    # solarM1.columns = ['date','vol','vol price']
    solarM1['vol price'] = 57
    for  p in primas:
        solarM1[p] = solarM1[p]/solarM1['vol']
    solarM1['fin'] = solarM1['date']+pd.Timedelta(hours=1)
    solarM1.sort_values(by='date', ascending=True, inplace=True)
    
    sauxie = solarM3.groupby([solarM3['date'].dt.year, solarM3['date'].dt.month])[
        ['vol', 'vol price']+primas].sum()
    sauxie.index = sauxie.index.map(lambda x: pd.to_datetime(
        str(x[1])+'/1/'+str(x[0])))  # .tz_localize('CET')
    solarM3 = sauxie.reset_index().copy()
    solarM3.columns = ['date', 'vol', 'vol price']+primas
    solarM3['vol price'] = solarM3['vol price']/solarM3['vol']
    for p in primas:
        solarM3[p] = solarM3[p]/solarM3['vol']
    solarM3['fin'] = solarM3['date'] + \
        pd.to_timedelta(solarM3['date'].dt.daysinmonth, 'D')
    solarM3.sort_values(by='date', ascending=True, inplace=True)
    solarM3['vol'] = solarM3['vol'] / solarM3.apply(lambda x: len(
        pd.date_range(x.date, x.fin, freq='h', tz='CET'))-1, axis=1)

# %% JUST FIXED
if len(justFixed) != 0:
    print('Performing calculations for deal number 10451651...')
    fixed_df = pd.concat(volumes['justFixed']['Volumes'], ignore_index=True)
    fixed_df = pd.DataFrame(fixed_df.groupby(['zone', 'date'])[
                            ['vol', 'vol price']+primas].sum()).reset_index().copy(deep=True)
    
    fixedM1 = fixed_df[fixed_df['date'].dt.tz_localize(None) < start].copy(deep=True)
    fixedM2 = fixed_df[(fixed_df['date'].dt.tz_localize(None) >= start) & (
        fixed_df['date'].dt.tz_localize(None) < start_m2)].copy(deep=True)
    fixedM3 = fixed_df[fixed_df['date'].dt.tz_localize(None) >= start].copy(deep=True)
    
    # fixedM1.columns = ['zone','date','vol','vol price']
    fixedM1['vol price'] = fixedM1['vol price']/fixedM1['vol']
    for p in primas:
        fixedM1[p] = fixedM1[p]/fixedM1['vol']
    fixedM1['fin'] = fixedM1['date']+pd.Timedelta(hours=1)
    fixedM1.sort_values(by='date', ascending=True, inplace=True)
    
    fauxie = fixedM3.groupby([fixedM3['date'].dt.year, fixedM3['date'].dt.month])[
        ['vol', 'vol price']+primas].sum()
    fauxie.index = fauxie.index.map(lambda x: pd.to_datetime(
        str(x[1])+'/1/'+str(x[0])))  # .tz_localize('CET')
    fixedM3 = fauxie.reset_index().copy()
    fixedM3.columns = ['date', 'vol', 'vol price']+primas
    fixedM3['vol price'] = fixedM3['vol price']/fixedM3['vol']
    for p in primas:
        fixedM3[p] = fixedM3[p]/fixedM3['vol']
    fixedM3['fin'] = fixedM3['date'] + \
        pd.to_timedelta(fixedM3['date'].dt.daysinmonth, 'D')
    fixedM3.sort_values(by='date', ascending=True, inplace=True)
    fixedM3['vol'] = fixedM3['vol'] / fixedM3.apply(lambda x: len(
        pd.date_range(x.date, x.fin, freq='h', tz='CET'))-1, axis=1)

# fixedM3['vol price'] = fixedM3['vol'] * fixedM3['vol price']
# fixedM3 = fixedM3.groupby(['date','fin'])[['vol','vol price']].sum().reset_index().copy()

# %% INDEX
if len(index) != 0:
    print('Performing calculations for deal number 6857467...')
    index_df = pd.DataFrame(pd.concat(volumes['Index']['Volumes'], ignore_index=True).groupby([pd.concat(volumes['Index']['Volumes'], ignore_index=True)['zone'], pd.concat(
        volumes['Index']['Volumes'], ignore_index=True)['tar'], pd.concat(volumes['Index']['Volumes'], ignore_index=True)['date']])[['vol']+primas].sum()).reset_index().copy(deep=True)
    
    indexM1 = index_df[index_df['date'].dt.tz_localize(None) < start].copy(deep=True)
    indexM2 = index_df[(index_df['date'].dt.tz_localize(None) >= start) & (
        index_df['date'].dt.tz_localize(None) < start_m2)].copy(deep=True)
    indexM3 = index_df[index_df['date'].dt.tz_localize(None) >= start].copy(deep=True)
    
    for p in primas:
        indexM1[p] = indexM1[p]/indexM1['vol']
    indexM1['fin'] = indexM1['date']+pd.Timedelta(hours=1)
    indexM1.sort_values(by='date', ascending=True, inplace=True)
    
    iauxie = indexM3.groupby([indexM3['date'].dt.year, indexM3['date'].dt.month])[[
        'vol']+primas].sum()
    iauxie.index = iauxie.index.map(
        lambda x: pd.to_datetime(str(x[1])+'/1/'+str(x[0])))
    indexM3 = iauxie.reset_index().copy()
    indexM3.columns = ['date', 'vol']+primas
    for p in primas:
        indexM3[p] = indexM3[p]/indexM3['vol']
    indexM3['fin'] = (
        indexM3['date']+pd.to_timedelta(indexM3['date'].dt.daysinmonth, 'D'))
    indexM3 = indexM3[indexM3['date'] >= start].copy(deep=True)
    indexM3.sort_values(by='date', ascending=True, inplace=True)
    indexM3['vol'] = indexM3['vol'] / indexM3.apply(lambda x: len(
        pd.date_range(x.date, x.fin, freq='h', tz='CET'))-1, axis=1)

# %% M2 CALCULATIONS

if neuroM1:
    print('Calculating and assigning M+1 neuro volumes based on LOC...')
    label = {'Index': indexM2.copy(deep=True), 'Solar': solarM2.copy(deep=True), 'Fixed': fixedM2.copy(deep=True)}  # ,'Click':clicks.copy()
    volCartera = 0
    final = {}
    
    for loct in profiles.keys():
        final[loct] = dict()
        for trade in [c for c in label.keys() if loct in set(label[c]['zone'])]:
            ddf = pd.DataFrame([start], columns=['date'])  # columns=['date']
            # final[loct][trade] = label[trade][label[trade]['zone'] == loct]['vol'].sum()
            ddf['vol'] = label[trade][label[trade]['zone'] == loct]['vol'].sum()
            # volCartera += final[loct][trade]
            volCartera += ddf['vol']
    
            # if trade != 'Index':
            #     ddf['vol price'] = label[trade][label[trade]['zone'] == loct]['vol price'].sum()#/ddf['vol']
            # ddf['vol price'] = ddf['vol price']
            ddf['fin'] = (
                ddf['date']+pd.to_timedelta(ddf['date'].dt.daysinmonth, 'D'))
            ddf['vol'] = ddf['vol'] / \
                ddf.apply(lambda x: len(pd.date_range(
                    x.date, x.fin, freq='h', tz='CET'))-1, axis=1)
            final[loct][trade] = ddf
    
    m2 = final.copy()
    
    for loc in m2.keys():
        for trade in m2[loc].keys():
            m2[loc][trade] = (m2[loc][trade]['vol']/volCartera) * \
                neuro.loc[directory[loc]]
    
    if sum([m2[loc]['Solar'] for loc in m2.keys() if 'Solar' in m2[loc].keys()]).values != 0:
        solarM3['vol'].update(sum([m2[loc]['Solar']
                              for loc in m2.keys() if 'Solar' in m2[loc].keys()]))
    if sum([m2[loc]['Fixed']for loc in m2.keys() if 'Fixed' in m2[loc].keys()]).values != 0:
        fixedM3['vol'].update(sum([m2[loc]['Fixed']
                              for loc in m2.keys() if 'Fixed' in m2[loc].keys()]))
    if sum([m2[loc]['Index']for loc in m2.keys() if 'Index' in m2[loc].keys()]).values != 0:
        indexM3['vol'].update(sum([m2[loc]['Index']
                              for loc in m2.keys() if 'Index' in m2[loc].keys()]))

# %% CONCAT

# solarMM = pd.concat([solarM2,solarM3])
# fixedMM = pd.concat([fixedM2,fixedM3])
# indexMM = pd.concat([indexM2,indexM3])

# %% CARTERA

cartera = getCartera(dates,primasCols,primasDeals)
print('Adding some finishing touches...')

# for x,y in cartera[6857467][['start_date','end_date']].iterrows():
#     for u,v in y:
#         print(u,v,'beep')
    

#%% 6857467
if len(index) != 0:
    cartera[6857467] = cartera[6857467].set_index('start_date').loc[min(indexM3.reset_index().date):].copy(deep=True) #no end
    cartera[6857467]['volume'].update(indexM3.copy(deep=True).reset_index().set_index('date')['vol'])
    
    for p in primas:
        cartera[6857467][primasDict[p]].update(indexM3.copy(deep=True).reset_index().set_index('date')[p])
        cartera[6857467][primasDict[p]] = cartera[6857467][primasDict[p]].astype(float).copy(deep=True)
    
    cartera[6857467]['volume'] = cartera[6857467]['volume'].astype(float).copy(deep=True)
    cartera[6857467].dropna(subset='volume', inplace=True)
    cartera[6857467] = cartera[6857467][(cartera[6857467]['volume'] != 0)].copy(deep=True)
    
    cartera[6857467] = cartera[6857467].reset_index().copy(deep=True)
    cartera[6857467]['price'] = 0.0
    try:
        cartera[6857467]['start_date'] = cartera[6857467]['start_date'].dt.strftime('%d/%m/%Y %H:%M')
        cartera[6857467]['end_date'] = cartera[6857467]['end_date'].dt.strftime('%d/%m/%Y %H:%M')
    except:
        pass

#%% 10451651
if len(justFixed) != 0:
    cartera[10451651] = cartera[10451651].set_index('start_date').loc[min(fixedM3.reset_index().date):].copy(deep=True)#max(fixedM3.reset_index().date)
    cartera[10451651]['volume'].update(fixedM3.reset_index().set_index('date')['vol'])
    cartera[10451651]['price'].update(fixedM3.reset_index().set_index('date')['vol price'])
    
    for p in primas:
        cartera[10451651][primasDict[p]].update(fixedM3.reset_index().set_index('date')[p])
        cartera[10451651][primasDict[p]] = cartera[10451651][primasDict[p]].astype(float).copy(deep=True)
    
    cartera[10451651]['volume'] = cartera[10451651]['volume'].astype(float).copy(deep=True)
    cartera[10451651]['price'] = cartera[10451651]['price'].astype(float).copy(deep=True)
    cartera[10451651].dropna(subset='price', inplace=True)
    cartera[10451651] = cartera[10451651][(cartera[10451651]['price'] != 0) & (cartera[10451651]['volume'] != 0)].copy(deep=True)
    
    cartera[10451651] = cartera[10451651].reset_index().copy(deep=True)
    try:
        cartera[10451651]['start_date'] = cartera[10451651]['start_date'].dt.strftime('%d/%m/%Y %H:%M').copy(deep=True)
        cartera[10451651]['end_date'] = cartera[10451651]['end_date'].dt.strftime('%d/%m/%Y %H:%M').copy(deep=True)
    except:
        pass

#%% 13970247
if len(solarBattery) != 0:
    cartera[13970247] = cartera[13970247].set_index('start_date').loc[min(solarM3.reset_index().date):].copy(deep=True)#max(solarM3.reset_index().date)
    cartera[13970247]['volume'].update(solarM3.reset_index().set_index('date')['vol'])
    cartera[13970247]['price'].update(solarM3.reset_index().set_index('date')['vol price'])
    
    for p in primas:
        cartera[13970247][primasDict[p]].update(solarM3.reset_index().set_index('date')[p])
        cartera[13970247][primasDict[p]] = cartera[13970247][primasDict[p]].astype(float).copy(deep=True)
    
    cartera[13970247]['volume'] = cartera[13970247]['volume'].astype(float).copy(deep=True)
    cartera[13970247]['price'] = cartera[13970247]['price'].astype(float).copy(deep=True)
    cartera[13970247].dropna(subset='price', inplace=True)
    cartera[13970247] = cartera[13970247][(cartera[13970247]['price'] != 0) & (cartera[13970247]['volume'] != 0)].copy(deep=True)
    
    cartera[13970247] = cartera[13970247].reset_index().copy(deep=True)
    try:
        cartera[13970247]['start_date'] = cartera[13970247]['start_date'].dt.strftime('%d/%m/%Y %H:%M').copy(deep=True)
        cartera[13970247]['end_date'] = cartera[13970247]['end_date'].dt.strftime('%d/%m/%Y %H:%M').copy(deep=True)
    except:
        pass

#%% 6857460
try:
    cartera[6857460] = cartera[6857467].copy()
    cartera[6857460]['volume'] = 0
    
    cartera[6857460]['volume'] = pd.concat([cartera[num]['volume'].fillna(0) for num in cartera.keys() if num not in list(primasDeals.values())+[7555996,6857460]], axis=1).sum(axis=1)
    cartera[6857460].deal_num = 6857460
    cartera[6857460]['price'] = 0.0
except:
    pass

#%% 7555996

if len(click) != 0:
    cartera[7555996] = cartera[7555996].set_index('start_date').loc[min(clicks_m.reset_index().start_date):].copy(deep=True)#max(clicks_m.reset_index().start_date)
    cartera[7555996]['volume'].update(clicks_m.reset_index().set_index('start_date')['volume'])
    cartera[7555996]['price'].update(clicks_m.reset_index().set_index('start_date')['price'])
    
    for p in primasDict.values():
        cartera[7555996][p] = cartera[7555996][p].update(clicks_m.reset_index().set_index('start_date')[p])
        cartera[7555996][p] = cartera[13970247][p].astype(float).copy(deep=True)
    
    cartera[7555996]['volume'] = cartera[7555996]['volume'].astype(float).copy(deep=True)
    cartera[7555996]['price'] = cartera[7555996]['price'].astype(float).copy(deep=True)
    cartera[7555996].dropna(subset='price', inplace=True)
    cartera[7555996] = cartera[7555996][(cartera[7555996]['price'] != 0) & (cartera[7555996]['volume'] != 0)].copy(deep=True)
    
    cartera[7555996] = cartera[7555996].reset_index().copy(deep=True)
    try:
        cartera[7555996]['start_date'] = cartera[7555996]['start_date'].dt.strftime('%d/%m/%Y %H:%M').copy(deep=True)
        cartera[7555996]['end_date'] = cartera[7555996]['end_date'].dt.strftime('%d/%m/%Y %H:%M').copy(deep=True)
    except:
        pass

#%% PRIMAS

for x in set(primasDeals.values()):
    cartera[x]['start_date'] = cartera[x]['start_date'].dt.strftime(
        '%d/%m/%Y %H:%M')
    cartera[x]['end_date'] = cartera[x]['end_date'].dt.strftime(
        '%d/%m/%Y %H:%M')


    
if len(index) == 0:
    cartera.pop(6857467)
else:
    if len(cartera[6857467]['volume'].dropna()) == 0:
        cartera.pop(6857467)

if len(justFixed) == 0:
    cartera.pop(10451651)
else:
    if len(cartera[10451651]['volume'].dropna()) == 0:
        cartera.pop(10451651)
    
if len(solarBattery) == 0:
    cartera.pop(13970247)
else:
    if len(cartera[13970247]['volume'].dropna()) == 0:
        cartera.pop(13970247)

if len(click) == 0:
    cartera.pop(7555996)
else:
    if len(cartera[7555996]['volume'].dropna()) == 0:
        cartera.pop(7555996)
    
# cartera = cartera2.copy()
# for n in cartera.keys():
#     cartera[n] = cartera2[n].copy(deep=True)
    
# %% DELTA

delta_0_dict = dict()

for file in glob.glob(output_path+"\*"):
    try:
        delta_0_dict[pd.to_datetime(file[-15:-5],dayfirst=True)]=file
    except:
        pass

delta_0_dt = max([i for i in delta_0_dict.keys() if i != pd.to_datetime(td)])
# delta_1 = pd.concat(cartera.values())

difference = []
deltaCols = ['volume','flex_premium', 'profile_premium','aim_premium', 'validity_premium', 'seie_premium', 'kyos_adj_premium','price']
for deal_num in [k for k in cartera.keys() if k not in primasDeals.values()]:
    try:
        delta_0 = pd.read_excel(delta_0_dict[delta_0_dt],sheet_name=str(deal_num)).set_index("start_date").copy(deep=True)[deltaCols]
        delta_1 = cartera[deal_num].set_index("start_date").copy(deep=True)[deltaCols]
        
        #delta_0 will have dates delta_1 won't
        delta_0 = delta_0.reindex(delta_1.index)
        d = delta_1-delta_0
        d['deal_num'] = deal_num
        difference.append(d)
    except:
        pass

try:    
    delta_diff = pd.concat(difference)
except:
    delta_diff = pd.DataFrame([0])
    
#%% OUTPUT

now = td.strftime('%d/%m/%Y %H:%M')
f = '%d/%m/%Y %H:%M'

cartera2 = cartera.copy()
for xx in cartera2.keys():
    cartera2[xx] = cartera2[xx].copy(deep=True)
    
os.chdir(output_path)
with pd.ExcelWriter('Posición '+now[:10].replace('/','.')+'.xlsx') as writer:
    for xn in [k for k in cartera2.keys() if k not in primasDeals.values()]:
        # cartera[n][pd.to_datetime(cartera[n].end_date, format=f).dt.date <= dates[n].date()].to_excel(writer, sheet_name=str(n))
        
        cartera2[xn].round(3).to_excel(writer, sheet_name=str(xn))
    newContracts.to_excel(writer, sheet_name="Contracts")
    delta_diff.round(3).to_excel(writer, sheet_name="Differences wrt " + delta_0_dt.strftime('%d.%m.%Y'))
    
#%%
for p in primasDeals.keys():
    try:
        cartera[primasDeals[p]].set_index('start_date',inplace=True)
    except:
        continue
    
    # for n in [k for k in cartera.keys() if k not in primasDeals.values()]:
    for n in [k for k in cartera.keys() if k not in list(primasDeals.values())+[6857460,7555996]]:
        
        if len(cartera[n][cartera[n][p]!=0]) == 0:
            cartera[n] = cartera[n][[i for i in cartera[n].columns if i != p]]
            continue
        else:
            try:
                cartera[n].set_index('start_date',inplace=True)
                # cartera[primasDeals[p]].set_index('start_date',inplace=True)
            except:
                pass
            for dt in cartera[n].index:
                mh = len(pd.date_range(pd.to_datetime(dt,format=f),pd.to_datetime(cartera[n].copy().loc[dt]['end_date'],format=f),freq='h',tz='CET',inclusive='left'))
                cartera[primasDeals[p]].at[dt,'price'] = (cartera[n].copy().fillna(0).loc[dt]['volume'].astype(float)*cartera[n].copy().fillna(0).loc[dt][p].astype(float)*mh)+cartera[primasDeals[p]].loc[dt]['price']
                cartera[primasDeals[p]].at[dt,'volume'] = (cartera[n].copy().fillna(0).loc[dt]['volume'].astype(float)*mh)+cartera[primasDeals[p]].loc[dt]['volume']
                cartera[primasDeals[p]].at[dt,'hours'] = mh
            cartera[n] = cartera[n][[i for i in cartera[n].columns if i != p]]
            cartera[n].reset_index(inplace=True)
    cartera[primasDeals[p]].reset_index(inplace=True)

for k in set(primasDeals.values()):
    if len(cartera[k][cartera[k]['price']!=0]) == 0:
        del cartera[k]
    else:
        cartera[k] = cartera[k][cartera[k]['price']!=0]
        cartera[k]['price'] = cartera[k]['price']/cartera[k]['volume']
        cartera[k]['volume'] = cartera[k]['volume']/cartera[k]['hours']
        cartera[k] = cartera[k][[i for i in cartera[k].columns if 'hours' not in i]]

"""
cartera = cartera2.copy()
for n in cartera.keys():
    cartera[n] = cartera2[n].copy(deep=True)
"""
# cartera[14476852] = cartera[14476852].reset_index()


#%% ENDUR

for n in [k for k in cartera.keys() if k not in primasDeals.values()]:
    cartera[n] = cartera[n][pd.to_datetime(cartera[n].end_date, format=f).dt.date <= dates[n].date()]

if upload:
    os.chdir(wdir)
    print('Creating json file and uploading to endur...')
    writer = open('jsonLoadResults.txt', 'w')
    
    if v == 'v1':
        for j in [num for num in cartera.keys() if len(cartera[num]) != 0]:
            juason = cartera[j].to_dict(orient='records')
            f_dict = dumps(juason, indent=1)
            r = postDeals(f_dict, "PowerVolumeAndPrice", tipo, v)
            writer.write(f"Data for trade number {j} received response {r}")
            # s = checkStatus('"'+r[r.index('ID: ')+4:]+'"',tipo)
            # s = checkStatus(r[r.index('ID: ')+4:],tipo)
            print(r)
        writer.close()
    if v == 'v2':
        for j in [num for num in cartera.keys() if len(cartera[num]) != 0]:
            juason = cartera[j].to_dict(orient='records')
            cargo["requests"] = juason
            f_dict = dumps(cargo, indent=1)
            r = postDeals(f_dict, "PowerVolumeAndPrice", tipo, v)
            writer.write(f"Data for trade number {j} received response {r}")
            print(j)
            # s = checkStatus('"'+r[r.index('ID: ')+4:]+'"',tipo)
            # s = checkStatus(r[r.index('ID: ')+4:],tipo)
            print(r)
        writer.close()

#%%
# CUPSconsumosMensuales = []

# ctDated.apply(lambda x: getVols(x,loss,CUPSconsumosMensuales), axis=1)

# final = pd.concat(CUPSconsumosMensuales)

# with pd.ExcelWriter(r"M:"+wd+r"\Portfolio Management\Lorena\1. POWER\1.1 SourcingCalculations (ENDUR)\1.1.4 Output\1.1.4.2 ConsumoPorCUPS\ConsumoPorCUPS"+(pd.to_datetime(date.today()).floor('D').strftime('%Y.%m.%d'))+'.xlsx') as writer:
#     final.to_excel(writer, sheet_name='Consumos')
#     click.to_excel(writer, sheet_name='Clicks')

