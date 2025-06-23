# -*- coding: utf-8 -*-
"""
Created on Thu Jun  6 11:20:35 2024

@author: lorena.sanz
"""
import matplotlib.pyplot as plt
from pmdarima import auto_arima
import numpy as np
import keyring
from datetime import date
import pandas as pd
from sqlalchemy import create_engine, text
pd.set_option('display.max_columns', None)
import os
import glob
import time
t0 = time.time()

# %% CONTROL

# dr = '\MET EE' #lor
dr = '' #meterisk

outputPath = r"M:"+dr+r"\Portfolio Management\Lorena\2. GAS\2.3 Forecasts\2.3.3 Output\2.3.3.2 VSS"

yearStart = 2024
yearEnd = 2025

primas = ['STRASTORAGE','FLEXIMB','MARGCOM','MERMAS']
precioCliente = ['COEFICIENTE','SPREAD']
precioMeti = ['COEFMOL','MOL']

pricing = [i for t in [primas,precioCliente,precioMeti] for i in t]

translator = {'COEFICIENTE':'c_pricing','SPREAD':'b_pricing',
              'COEFMOL':'c_mol','MOL':'b_mol'}

tarifasQuery = r"SELECT GTF_DENOM AS Tarifa_Domesticos, GTF_DENOMCOM As Nombre_SPECS FROM METDB.MET_GRTARIF"

specsQuery = r"SELECT SPC_ID, SPC_DENOM, SPC_SPREAD, SPC_COEFICIENTE, SPC_MOL, SPC_COEFMOL, SPC_CREDFINAN, SPC_CREDRISK, SPC_STRASTORAGE, SPC_FLEXIMB, SPC_MARGCOM, SPC_FNEE, SPC_MUNTAS, SPC_MERMAS , SPC_COSTLOG, SPC_TF, SPC_TFREG FROM METDB.MET_SPECS"

peajeAnt = {'RL01':{1:3.1,2:3.1},'RLPS1':{1:3.1,2:3.1},'RL02':{1:3.2,2:3.2},'RLPS2':{1:3.2,2:3.2},'RL03':{1:3.2,2:3.2},'RLPS3':{1:3.2,2:3.2},'RL04':{1:3.3,2:3.4},'RLPS4':{1:3.4,2:3.4}}

auxCode = {'35':1, '38':1}

avg31 = 2748.791667
avg32 = 8979.1875
avg33 = 68083.6875

tCoefUse = 'T2'
zIDUse = 'ID2'

# %% DATES

dateStartStr = '01/01/'+str(yearStart) #for sp needs to be str
dateToday = pd.to_datetime(date.today()).floor('D')
date1Y = dateToday.replace(year=dateToday.year + 1)
dateStart = pd.to_datetime(dateStartStr)
dateEnd = pd.to_datetime('12/31/'+str(yearEnd))

# %% AUTHENTICATION

sql = keyring.get_credential("SQL", None)
sqluser = sql.username
sqlpassword = sql.password

sql_url = (f'mssql+pyodbc://{sqluser}:{sqlpassword}@met-esp-' +
           'prod.database.windows.net/Risk_MGMT_Spain?driver' +
           '=ODBC Driver 17 for SQL Server')

engine = create_engine(sql_url, fast_executemany=True)

# %% QUERYS

qVssSige = text(f"EXEC [sp_VSS_SIGE] @Fechini = '{dateStartStr}'")

with engine.begin() as conn:
    vssSige = pd.DataFrame(conn.execute(qVssSige))
    tarifas = pd.read_sql(tarifasQuery, conn)
    specs = pd.read_sql(specsQuery, conn)

tarifas.columns = [i.replace('GTF_','') for i in tarifas.columns]
tarifas = tarifas.drop_duplicates()
tarifas.set_index('Tarifa_Domesticos',inplace=True)

tarifasD = tarifas.to_dict()[tarifas.columns[0]]

specs.columns = [i.replace('SPC_','') for i in specs.columns]
specs = specs[[i for i in specs.columns if 'ID' not in i]]
specs = specs.drop_duplicates()
specs.set_index('DENOM',inplace=True)
specs['TFMARG'] = (specs['TF']-specs['TFREG'])#*12/365

# specs['Margen en Tf [EUR/día]'] = (specs.TF-specs.TFREG)/30

vssSige = vssSige.fillna(np.nan).replace([np.nan], [None])

# %% SQL TABLES

# tempCoeff = pd.read_sql('SELECT [COF_FECHA],[COF_PRV_CODIGO],[COF_'+tCoefUse+'], [COF_ORIGEN] FROM [METDB].[MET_COEFTEMP]', engine)
tempCoeff = pd.read_sql("SELECT [COF_FECHA],[COF_PRV_CODIGO],[COF_"+tCoefUse+"] FROM [METDB].[MET_COEFTEMP] WHERE COF_ORIGEN = 'l1'", engine)
tempCoeff.columns = [i[4:] for i in tempCoeff.columns]

tempCoeff = tempCoeff.drop_duplicates()

for i in tempCoeff.groupby(['FECHA','PRV_CODIGO'],as_index=False):
    if len(i[1]) > 1:
        tempCoeff.drop(i[1][i[1].ORIGEN == 'PR'].index,inplace=True)

# tempCoeff = tempCoeff.drop(columns='ORIGEN')
tempCoeff['FECHA'] = pd.to_datetime(tempCoeff['FECHA'])
tempCoeff = tempCoeff[tempCoeff['FECHA'] >= dateStart]

profilesEnagas = pd.read_sql('SELECT [PFE_TAR_CODIGO],[PFE_PRV_CODIGO],[PFE_ZNS_'+zIDUse+'],[PFE_MONTH],[PFE_PERFIL] FROM [METDB].[MET_PERFENAGAS]', engine)
profilesEnagas.columns = [i[4:] for i in profilesEnagas.columns]

profilesEnagas.TAR_CODIGO = profilesEnagas.TAR_CODIGO.astype(float)

provDict = dict(set(zip(profilesEnagas.PRV_CODIGO,profilesEnagas['ZNS_'+zIDUse])))

perfDict = pd.pivot_table(profilesEnagas,values='PERFIL',index=['TAR_CODIGO','PRV_CODIGO'],columns=['MONTH']).to_dict('index')

tempDict = pd.pivot_table(tempCoeff,values=tCoefUse,index=['FECHA'],columns=['PRV_CODIGO']).to_dict('index')


# %% CATEGORIZATION

vssSige['GN/GNL'] = vssSige['TARIFA'].mask(vssSige['TARIFA'].str.contains('RLPS'),other='GNL').where(vssSige['TARIFA'].str.contains('RLPS'),other='GN')

vssSige['VSS'] = np.where((vssSige['TARIFA'].str.contains('RLPS'))|(vssSige['TARIFA'].str.fullmatch('RL01|RL02|RL03'))|((vssSige['TARIFA'].str.contains('RL04'))&(vssSige['TIPO'] == 'Tipo 2')),1,0)

vssSige['VSS not in azure'] = np.where((vssSige['VSS'] == 1)&(vssSige['AZURE'] == 'N'),1,0)

vssSige['Fecha Inicio'] = np.where((vssSige['FECBAJ'].notnull()) & (vssSige['FECBAJ']<vssSige['FECINI_TARIFA']), None, vssSige['FECALTA'].where(vssSige['FECALTA']>vssSige['FECINI_TARIFA'],other=vssSige['FECINI_TARIFA']))
vssSige['Fecha Fin'] = vssSige['FechaFinal'].where(~(vssSige['FechaFinal'].isnull()), other = date1Y)

vssSige['Check Fecha'] = np.where(vssSige['FECALTA'] > vssSige['FechaFinal'],'Check',np.where(vssSige['FECALTA'] == vssSige['FechaFinal'],'One-day Contract',None)) #'Check's are bad
vssSigeFilter = vssSige[(vssSige.VSS == 1)&(vssSige['Check Fecha'].isnull())].copy()

def PeajeAntFunc(row):
    if row.TARIFA in peajeAnt.keys():
        if (row.PRESION != None) and (int(row.PRESION) in peajeAnt[row.TARIFA].keys()):
            return peajeAnt[row.TARIFA][int(row.PRESION)]
        else:
            if row.TARIFA != 'RL04':
                    return peajeAnt[row.TARIFA][1]
            else:
                raise Exception(f'Al CUPS {row.CUPS} le falta la Presión')
    else:
        return None

vssSigeFilter['PeajeAnt'] = vssSigeFilter.apply(PeajeAntFunc, axis=1)
# vssSigeFilter['Existe Tarifa'] = vssSigeFilter.apply(lambda x: tarifas.loc[x['GRUPO_TARIFA']], axis=1)
vssSigeFilter['Existe Tarifa'] = vssSigeFilter['GRUPO_TARIFA'].map(tarifasD) ##################################################################################################################################################################

vssSigeFilter['Index'] = np.where(((vssSigeFilter['GRUPO_TARIFA'].notnull())&(vssSigeFilter['AZURE'] == 'N')),np.where((vssSigeFilter['TERMVAR'].notnull()), 'PF',vssSigeFilter['INDICE'].where((vssSigeFilter['INDICE'].notnull()),'ERROR')),"None")

vssSigeFilter = vssSigeFilter[vssSigeFilter['Index'] != 'ERROR']

def MolFunc(row):
    if row['Existe Tarifa'] in specs.index:
        return specs.loc[row['Existe Tarifa']]
    else:
        return None

vssSigeFilter[specs.columns] = vssSigeFilter.apply(MolFunc, axis=1)

vssSigeFilter.index = vssSigeFilter.CUPS + '_' + vssSigeFilter.FechaFinal.astype(str) + '_' + vssSigeFilter.CLIENTE+ vssSigeFilter.GRUPO_TARIFA# + ' ' + vssSigeFilter.CONTRATO.astype(str) + ' '

vssSige.index = vssSige.CUPS + '_' + vssSige.FechaFinal.astype(str) + '_' + vssSige.CLIENTE+ vssSige.GRUPO_TARIFA# + ' ' + vssSigeFilter.CONTRATO.astype(str) + ' '

for ind in pricing:
    if ind in ['COEFICIENTE','COEFMOL']:
        vssSigeFilter[ind] = vssSigeFilter[ind].fillna(1.0)
    else:
        vssSigeFilter[ind] = vssSigeFilter[ind].fillna(0.0)

# %% FORECAST

forecast = vssSigeFilter[['CUPS','COD_PROV']+list(vssSigeFilter.columns[list(vssSigeFilter.columns).index('GN/GNL'):])+['SPREAD']].copy()#.set_index('CUPS')

forecast['Fecha Inicio'] = pd.to_datetime(forecast['Fecha Inicio'])
forecast['Fecha Fin'] = pd.to_datetime(forecast['Fecha Fin'])
forecast['Fecha Inicio'] = forecast['Fecha Inicio'].where(forecast['Fecha Inicio'] > dateStart, dateStart)
forecast = forecast.drop_duplicates()

tmln = pd.date_range(dateStart,max(forecast['Fecha Fin']),freq='d',inclusive = 'both')
temptmln = [t for t in tempDict.keys() if t in tmln]
mtmln = [t for t in tmln if t.day == 1]

auxCols = ['PeajeAnt','COD_PROV','Fecha Inicio','Fecha Fin']

def volumeAssigner(row,t):
    if row['Fecha Inicio'] <= t and row['Fecha Fin'] >= t:
        return perfDict[(row.PeajeAnt,row.COD_PROV)][t.month]/t.days_in_month
    else:
        return 0.0

t1 = time.time()
vol = dict([(t,forecast[auxCols].apply(lambda x: volumeAssigner(x,t),axis=1)) for t in tmln])

for t in vol.keys():
    vol[t] = vol[t].to_dict()
volume = pd.DataFrame.from_dict(vol)

volume[temptmln] = volume[temptmln].apply(lambda x: x*forecast['COD_PROV'].map(lambda y: tempDict[x.name][y]))

outputDaily = pd.concat([forecast,volume], axis=1)

volumeM = outputDaily[tmln].T
volumeM.index = pd.to_datetime(volumeM.index)
volumeM = volumeM.groupby(pd.Grouper(freq='MS')).sum().T
volumeM.columns = mtmln

outputMonthly = pd.concat([forecast,volumeM],axis=1)

# margen = {}

#     margen[m] = maux

#%%

pricingDict = {}

for tup in outputMonthly.groupby(['GN/GNL','Index']):
    pricingDict[tup[0]] = {'vol':tup[1][mtmln].sum()}
    hold = {}
    for m in mtmln:
        maux = 0
        end = m.replace(day = m.days_in_month)
        for cups in tup[1].index:
            if tup[1].loc[cups]['Fecha Inicio'] <= m and tup[1].loc[cups]['Fecha Fin'] >= m:
                  maux += (len(pd.date_range(max(m,tup[1].loc[cups]['Fecha Inicio']),min(end,tup[1].loc[cups]['Fecha Fin']), freq='d',inclusive='both'))/m.days_in_month)*tup[1].loc[cups].fillna(0)['TFMARG']
            else:
                continue
        hold[m] = maux
    pricingDict[tup[0]]['TFMARG'] = pd.Series(pd.DataFrame.from_dict(hold,orient='index')[0])
    for p in pricing:
        pricingDict[tup[0]][p] = tup[1][mtmln].mul(tup[1][p],axis='index').sum()/pricingDict[tup[0]]['vol']
    pricingDict[tup[0]] = pd.DataFrame.from_dict(pricingDict[tup[0]])
    pricingDict[tup[0]]['TFMARG'] = pricingDict[tup[0]]['TFMARG']/pricingDict[tup[0]]['vol']

t3 = time.time()
print('Tiempo total: ',(t3-t0)/60, ' minutos. Tiempo del forecast: ',(t3-t1)/60,' minutos.')

outputDaily = outputDaily.set_index('CUPS')
outputMonthly.index = outputMonthly.index.map(lambda x: x[:x.index('_')])

errors = vssSige.loc[[i for i in vssSige.index if i not in vssSigeFilter.index]]
errors = errors[errors['VSS'] == 1]
errors = errors[[c for c in errors.columns if len(errors[c][errors[c].notnull()]) > 0]+[i for i in ['INDICE'] if len(errors[i][errors[i].notnull()]) == 0]]

now = dateToday.strftime('%Y.%m.%d')

for d in pricingDict.keys():
    c = [translator[col] if col in translator.keys() else col for col in pricingDict[d].columns]
    pricingDict[d].columns = c

os.chdir(outputPath)
with pd.ExcelWriter(r'VSSForecast_'+now+r'.xlsx') as writer:
    pricingDict[('GN','PF')].to_excel(writer,sheet_name= 'GN_PF')
    pricingDict[('GN','MIBGAS')].to_excel(writer,sheet_name= 'GN_MIBGAS')
    pricingDict[('GNL','PF')].to_excel(writer,sheet_name= 'GNL_PF')
    pricingDict[('GNL','MIBGAS')].to_excel(writer,sheet_name= 'GNL_MIBGAS')
    errors.to_excel(writer,sheet_name= 'Errores')
