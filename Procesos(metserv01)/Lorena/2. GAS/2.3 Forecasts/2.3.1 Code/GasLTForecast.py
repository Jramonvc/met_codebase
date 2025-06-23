# -*- coding: utf-8 -*-
"""
Created on Mon Apr  8 09:32:51 2024

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
import glob
import os
# from curvas_230_234 import curvas

# dr = '\MET EE'
dr = '' #metesrisk

# %% USER INPUTS

wdir = "M:"+dr+r"\Portfolio Management\Lorena\2. GAS\2.3 Forecasts"

metriskStartDate = pd.to_datetime('01/01/2022')
metriskEndDate = pd.to_datetime('12/31/2027')

metriskStartDate = '01/01/2022'
metriskEndDate = '31/12/2027'

metriskOffset = 12
deltaDevOffset = -3
deltaDevLTOffset = 9
sparkSpreadOffset = -3

dateToday = pd.to_datetime(date.today()).floor('D')
currentMonthStart = dateToday.replace(day=1)

stForecast ="M:"+dr+"\Portfolio Management\Demanda diaria.xlsm"
stSheet = 'Demanda mensual'

tmax = pd.to_datetime('12/1/2024')
tFrom = pd.to_datetime('1/1/2024')

forecastType = 'Both' # TimeSeries | Excel | Both
excelStart = pd.to_datetime('1/1/1900')
adj = 0.95

errors = {}

# lastLT = {}
# for i in glob.glob("M:"+dr+"\Portfolio Management\GAS toEndur python_scripts\LTForecast_*"):
#     lastLT[pd.to_datetime(i[i.index('_2')+1:i.index('.x')])] = i
# # lastLT = lastLT[max(lastLT.keys())]
# # lastLT = pd.read_excel(lastLT)
# # lastLT = lastLT[[i for i in lastLT.columns[1:]]].set_index('CUPS')
# # lastLT = lastLT.set_index('CUPS') "M:\Portfolio Management\GAS toEndur python_scripts\Changes From 2024.05.13 To 2024.05.14.xlsx"
# "M:\MET EE\Portfolio Management\Demanda diaria.xlsm"

errorMissing = 'La previsión de este CUPS no se ha podido realizar. Compruebe si faltan valores en la base de datos'
errorNull = 'La previsión ST de este CUPS no se ha podido realizar. Tiene todos los valores históricos nulos'
errorMulti = 'No se puede realizar un forecast de ST multivariable para este CUPS por falta de volúmenes contractuales (variable externa)'
errorTS = 'No se puede realizar un forecast de ST multivariable para este CUPS porque ha ocurrido algún error'
errorCogen = 'No se puede realizar un forecast de excel para este CUPS porque es una cogeneradora'

# if dateToday in lastLT.keys():
#     del lastLT[dateToday]

# oldForecast = pd.read_excel(lastLT[max(lastLT.keys())]) #,sheet_name='EXCEL FORECAST'
# oldForecast = oldForecast[[i for i in oldForecast.columns if 'Unnamed' not in str(i)]]
# oldForecast = oldForecast.set_index('CUPS')

# %% DATES

metriskOffsetDate = currentMonthStart - pd.DateOffset(months=metriskOffset)
deltaDevOffsetDate = currentMonthStart + pd.DateOffset(months=deltaDevOffset)
deltaDevLTOffsetDate = currentMonthStart + \
    pd.DateOffset(months=deltaDevLTOffset)
sparkSpreadOffsetDate = currentMonthStart + \
    pd.DateOffset(months=sparkSpreadOffset)

# %% AUTHENTICATION

sql = keyring.get_credential("SQL", None)
sqluser = sql.username
sqlpassword = sql.password

sql_url = (f'mssql+pyodbc://{sqluser}:{sqlpassword}@met-esp-' +
           'prod.database.windows.net/Risk_MGMT_Spain?driver' +
           '=ODBC Driver 17 for SQL Server')

engine = create_engine(sql_url, fast_executemany=True)

abc = {0: "", 1: "A", 2: "B", 3: "C", 4: "D", 5: "E", 6: "F", 7: "G", 8: "H",
       9: "I", 10: "J", 11: "K", 12: "L", 13: "M", 14: "N", 15: "O", 16: "P",
       17: "Q", 18: "R", 19: "S", 20: "T", 21: "U", 22: "V", 23: "W", 24: "X",
       25: "Y", 26: "Z"}

# %% QUERYS

qInpBal = text(f"EXEC [sp_Input_Balance] @Fechini = '{metriskStartDate}'," +
               f" @Fechfin = '{metriskEndDate}'")
qRepCon = text(f"EXEC [sp_Report_Contracts] @Fechini = '{metriskStartDate}'," +
               f" @Fechfin = '{metriskEndDate}'")
qVolHis = text(f"EXEC [sp_Volume_History] @month_num = {metriskOffset}," +
               f" @V_Estado = 'T', @V_Tipo = 'T'")
qDaiNom = text(f"EXEC [sp_Daily_Nomination] @Fechini = '{metriskStartDate}'," +
               f" @Fechfin = '{metriskEndDate}'")

with engine.begin() as conn:
    inputBalance = pd.DataFrame(conn.execute(qInpBal))
    reportContracts = pd.DataFrame(conn.execute(qRepCon))
    volumeHistory = pd.DataFrame(conn.execute(qVolHis))
    dailyNomination = pd.DataFrame(conn.execute(qDaiNom))

dailyNomination = dailyNomination[dailyNomination['CUPS'].notna()]

cImpBal = ['CUPS', 'RAZSOC', 'DONE_DATE', 'START_DATE', 'END_DATE', 'FECBAJ',
           'INDICE', 'YEAR', '01', '02', '03', '04', '05', '06', '07', '08',
           '09', '10', '11', '12']

cRepCon = ['FECDONE','CUPS', 'PEAJE', 'TM', 'COGENERACION', 'MERMA']

# # inputBalance.drop_duplicates(subset=cImpBal,inplace=True) ##
reportContracts.drop_duplicates(subset=cRepCon,inplace=True)

macroDF = inputBalance[cImpBal].merge(
    reportContracts[cRepCon],
    how='inner',
    left_on=['CUPS','DONE_DATE'],
    right_on = ['CUPS','FECDONE']
)

for i in macroDF.CUPS:
    errors[i] = []

for i in [j for j in macroDF.CUPS if j not in inputBalance.CUPS]:
    errors[i].append(errorMissing)

macroDF['END_DATE'].update(macroDF['FECBAJ'][macroDF['FECBAJ'].notna()])

""""""
# # macroDF.drop_duplicates(inplace=True) ##
macroDF
""""""

vLookUpStartDate = pd.read_excel(stForecast, usecols='D', skiprows=6,
                                 nrows=1, sheet_name=stSheet, header=None).iloc[0].to_list()[0]

#%% CONTRACTUAL QUANTITY

au = {1:'01', 2:'02', 3:'03', 4:'04', 5:'05', 6:'06', 7:'07', 8:'08', 9:'09',
    10:'10', 11:'11', 12:'12'}

contractVol = {}
for i in macroDF.groupby('CUPS'):
#     # i1 = i[1]
#     # i1.update(i1[(i1.YEAR.astype(int) >= min(i1.START_DATE).year)&(i1.YEAR.astype(int) < dateToday.year)][[v for (k,v) in au.items() if k >= min(i1.START_DATE).month]].fillna(0.0))
#     # i1.update(i1[i1.YEAR.astype(int) == dateToday.year][[v for (k,v) in au.items() if k < dateToday.month]].fillna(float(0.0)))
#     # i1.update(i1[au.values()].astype(float))
    contractVol[i[0]] = i[1]

contractVolDF = dict()

for cups in contractVol.keys():

    cvAux = contractVol[cups].copy()
    cvAux = cvAux.loc[((cvAux.replace(0.0,np.nan)).dropna(how='all',subset=au.values(),axis=0, inplace = False)).index]
    # # cvAux.drop_duplicates(inplace=True) ##

    if len(cvAux[['INDICE','YEAR']].drop_duplicates()) != len(cvAux[['INDICE','YEAR']]):
        dupes = cvAux[cvAux[['INDICE','YEAR']].duplicated()][['INDICE','YEAR']].drop_duplicates()
        for ind in dupes.index:
            dvol = (cvAux[(cvAux.INDICE == dupes.loc[ind].INDICE)&(cvAux.YEAR == dupes.loc[ind].YEAR)][au.values()].sum()).astype(float)
            nind = (cvAux[(cvAux.INDICE == dupes.loc[ind].INDICE)&(cvAux.YEAR == dupes.loc[ind].YEAR)].index).to_list()
            if len(nind) > 1:
                cvAux.drop([j for j in nind[1:]],inplace=True)
                nind = nind[0]
            for a in au.values():
                cvAux.at[nind,a] = float(dvol.loc[a])

    cvAux = cvAux.loc[((cvAux.replace(0.0,np.nan)).dropna(how='all',subset=au.values(),axis=0)).index]
    if len(set(cvAux.INDICE)) == 1:
        y = set([int(i) for i in cvAux.YEAR])
        cv = pd.DataFrame(pd.concat([j for i,j in (cvAux[au.values()].T).items()],
                                    ignore_index=True))
        cv.columns = [cups]
        cv.index = (pd.concat([pd.Series(pd.date_range('1/1/'+str(q),'12/1/'+str(q),freq='MS',inclusive='both')) for q in y])).sort_values(ascending=True)
        cv = cv.loc[[i for i in cv.index if ((cv.replace({np.nan: None})).loc[i][cups]) != None or (((cv.loc[i:][cups]).notnull()).any() and ((cv.loc[:i][cups]).notnull()).any())]] #
        contractVolDF[cups] = cv

    elif len(cvAux) == 0:
        continue

    else:
        aux = []
        for ind in set(cvAux.INDICE):
            y = [int(i) for i in cvAux[cvAux.INDICE == ind].YEAR]
            cv = pd.DataFrame(pd.concat([j for i,j in (cvAux[cvAux.INDICE == ind][au.values()].T).items()],
                                        ignore_index=True))
            cv.columns = [ind]
            cv.index = (pd.concat([pd.Series(pd.date_range('1/1/'+str(q),'12/1/'+str(q),freq='MS',inclusive='both')) for q in y])).sort_values(ascending=True)
            aux.append(cv)
        cv = pd.concat(aux,axis=1)
        cv = pd.DataFrame((cv.replace({np.nan: None})).apply(lambda x: sum([float(x[i]) for i in cv.columns if x[i] != None]), axis=1)) #(cv.fillna(0.0))
        cv.columns = [cups]
        cv = cv.loc[[i for i in cv.index if ((cv.replace({np.nan: None})).loc[i][cups]) != None  or (((cv.loc[i:][cups]).notnull()).any() and ((cv.loc[:i][cups]).notnull()).any())]] #
        contractVolDF[cups] = cv

cq = pd.concat([(i).astype(float) for i in contractVolDF.values()],axis=1).T

# # def cqFixer(row):
# #     dtRng = []
# #     for m in row.columns:
# #         if m in [q for j in contractVol[row.name].apply(lambda x: pd.date_range(x.START_DATE,x.END_DATE, freq='MS', inclusive='both'), axis=1) for q in j]:


#%% REAL MEASURE CALCULATIONS

contInfo = pd.DataFrame(contractVol.keys(),columns=['CUPS'])
contInfo['Fin'] = contInfo.apply(lambda x: pd.to_datetime(max(macroDF[macroDF['CUPS'] == x['CUPS']]['END_DATE'])),axis=1)
contInfo['Start'] = contInfo.apply(lambda x: pd.to_datetime(min(macroDF[macroDF['CUPS'] == x['CUPS']]['START_DATE'])),axis=1)

contInfo.set_index('CUPS',inplace=True)

difDatesStart = len(
    pd.date_range(
        vLookUpStartDate,
        metriskOffsetDate,
        freq='MS',
        inclusive='both')
)+2

difDatesEnd = len(
    pd.date_range(
        vLookUpStartDate,
        currentMonthStart,
        freq='MS',
        inclusive='both')
)+2

cc = ("C," +
      abc[int(difDatesStart/26)] +
      abc[difDatesStart-(int(difDatesStart/26)*26)+1] +
      ':' +
      abc[int(difDatesEnd/26)] +
      abc[difDatesEnd-(int(difDatesEnd/26)*26)+1]
      )

#Table 2
forecastST = pd.read_excel(stForecast, usecols=cc, header=6, sheet_name=stSheet)
forecastST = forecastST[forecastST["CUPS"].str.contains('Domesticos') == False]

#Table 1
l = forecastST.columns.to_list().index(pd.to_datetime(volumeHistory.columns[-1]))
volumeHistory.columns = ['CUPS', 'CLIENTE']+forecastST.columns[1:l+1].to_list()
volumeHistory.drop_duplicates(
    subset=[c for c in volumeHistory.columns if c != 'CLIENTE'],
    inplace=True)

cups = [c for c in forecastST.CUPS if c in list(volumeHistory.CUPS)]

#Table 3
realMeasure = forecastST.set_index('CUPS').copy()
realMeasure.update(
    volumeHistory.set_index('CUPS').loc[cups][forecastST.columns[1:l+1]])
realMeasure = realMeasure/1000

#Table 4 RHS
deltaPerMonth = pd.DataFrame(np.nan,columns=pd.date_range(deltaDevOffsetDate,currentMonthStart,freq='MS',inclusive='both'),index=macroDF['CUPS'].drop_duplicates())
deltaPerMonth['START_DATE'] = pd.to_datetime(deltaPerMonth.apply(lambda x: min(macroDF[macroDF.CUPS == x.name]['START_DATE']), axis=1))
deltaPerMonth['END_DATE'] = pd.to_datetime(deltaPerMonth.apply(lambda x: max(macroDF[macroDF.CUPS == x.name]['END_DATE']), axis=1))

dpmCols = [i for i in deltaPerMonth.columns if 'DATE' not in str(i)]

for month in dpmCols:
    negInd = [i for i in cq[cq[month]==0].index if i in realMeasure[realMeasure[month]==0].index]
    cqInd = [i for i in cq.index if i not in negInd]
    rmInd = [i for i in realMeasure.index if i not in negInd]
    # # deltaPerMonth.loc[[i for i in cq[cq[month]==0].index if i in realMeasure[realMeasure[month]==0].index]][month] = 1#
    for row in negInd:
        deltaPerMonth.at[row,month] = 1
    
    cqVol = cq.loc[cqInd][month][(cq[month]!=0)&(cq[month].notna())]
    rmVol = realMeasure.loc[rmInd][month][(realMeasure[month].notna())] #(realMeasure[month]!=0)&
    auxind = [i for i in cqVol.index if i in rmVol.index]
    v = (rmVol[auxind] - cqVol[auxind])/cqVol[auxind]
    for i in [j for j in v.index if v.loc[j] <= 10.0]:
        deltaPerMonth.at[i,month] = v.loc[i]
    for q in [j for j in v.index if v.loc[j] > 10.0]:
        deltaPerMonth.at[q,month] = 10.0
    for w in [i for i in rmVol.index if i not in cqVol.index]:
        deltaPerMonth.at[w,month] = 0.0
    # # deltaPerMonth.update(v[v<=10])
    # # deltaPerMonth.loc[v[v>10].index][month] = 10.0

    for i in deltaPerMonth[(deltaPerMonth.START_DATE > month)|(deltaPerMonth.END_DATE < month)].index:
        deltaPerMonth.at[i,month] = np.nan


deltaPerMonth = deltaPerMonth.loc[contInfo[contInfo['Fin'] >= min(dpmCols)].index][dpmCols]
dpmCols = deltaPerMonth.columns
# newCupsIndex = [i for i in deltaPerMonth.index if i not in list(oldForecast.index)]

#%% TIME SERIES FORECAST

if forecastType == 'TimeSeries' or forecastType == 'Both':

    historical = dict()
    timeSeriesForecast = dict()
    failed = dict()
    auxDF = pd.DataFrame(np.nan, columns=realMeasure.columns, index=[0])
    for cups in macroDF['CUPS'].drop_duplicates():
    
        edate = min(max(macroDF[macroDF['CUPS'] == cups]['END_DATE']), min(
            [x for x in macroDF[macroDF['CUPS'] == cups]['FECBAJ']if x is not None]+[(dateToday + pd.DateOffset(years=100)).date()]))
    
        if edate < metriskOffsetDate.date():
            continue
    
        else:
            deefe = auxDF.copy()
            deefe.index = [cups]
            deefe.update(realMeasure.replace(0, np.nan))
    
            if len(deefe.columns[deefe.isna().any()]) != 0:
                for dt in deefe.columns[deefe.isna().any()]:
                    try:
                        vol = (contractVol[cups][contractVol[cups]
                                                 ['YEAR'] == str(dt.year)][dt.strftime('%m')].astype(float)).to_list()[0]
                        if vol == 0:
                            continue
                        else:
                            deefe.at[cups, dt] = vol
                    except:
                        continue
    
            if len(deefe) == 0:
                failed[cups] = deefe
                errors[cups].append(errorNull)
                continue
    
            deefe = pd.Series(deefe.loc[cups])
            historical[cups] = deefe
            deefe = (deefe.T).interpolate(method='linear',
                                      limit_direction='forward', axis=0, limit_area='inside')
            ext = (contractVolDF[cups].copy()).astype(float)
            ext = ext[ext.index >= min(deefe.index)]
            ext.replace(0.0, np.nan,inplace=True)
            ext = ext.interpolate(method='linear',
                                      limit_direction='forward', axis=0, limit_area='inside')
            ext = ext.dropna()
            """n3: logged data, ext in model -> same rersults as n1, BUT no white noise"""
            if len(deefe) < 2 and max(ext.index)>max(deefe.index):
                deefe = pd.concat([deefe,pd.Series(ext[cups].loc[ext.index>max(deefe.index)])],axis=0)
    
                if len(deefe) < 2:
                    failed[cups] = deefe
                    errors[cups].append[errorMulti]
                    continue
    
            elif len(deefe) < 2 and max(ext.index)<=max(deefe.index):
                failed[cups] = deefe
                errors[cups].append[errorMulti]
                continue
    
            try:
                n = len(pd.date_range(max(deefe.index),tmax,freq='MS',inclusive='both'))
                model = auto_arima(np.log(deefe),X=ext.loc[[j for j in ext.index if j in deefe.index]], trace=True, error_action="ignore", suppress_warnings=True,method='bfgs') #,m=12,stationary=True
                model.fit(np.log(deefe), X=ext.loc[[j for j in ext.index if j in deefe.index]])
                f = model.predict(n_periods=len(ext[ext.index>max(deefe.index)]), X=ext[ext.index>max(deefe.index)])
                f = pd.Series(np.exp(f.to_frame(cups))[cups])
                f.index = pd.date_range(max(deefe.index),max(deefe.index)+pd.DateOffset(months=n),freq='MS',inclusive='left')
                deefe = pd.concat([deefe,f],axis=0)
                if max(deefe.index) < tmax:
                    n = len(pd.date_range(max(deefe.index),tmax,freq='MS',inclusive='both'))
                    model = auto_arima(np.log(deefe), trace=True, error_action="ignore", suppress_warnings=True,method='bfgs') #,m=12,stationary=True
                    model.fit(np.log(deefe))
                    f = model.predict(n_periods= n)
                    f.index = pd.date_range(max(deefe.index),max(deefe.index)+pd.DateOffset(months=n),freq='MS',inclusive='left')
                    f = np.exp(f)
                    deefe = pd.concat([deefe,f],axis=0)
                if len(contractVolDF[cups][contractVolDF[cups][cups]==0.0]) != 0:
                    deefe = deefe[cups].update(contractVolDF[cups][contractVolDF[cups][cups]==0.0])
                timeSeriesForecast[cups] = deefe
            except:
                errors[cups].append(errorTS)
                try:
                    n = len(pd.date_range(max(deefe.index),tmax,freq='MS',inclusive='both'))
                    model = auto_arima(np.log(deefe), trace=True, error_action="ignore", suppress_warnings=True,method='bfgs') #,m=12,stationary=True,information_criterion='oob'
                    model.fit(np.log(deefe))
                    f = model.predict(n_periods= n)
                    f = pd.Series(np.exp(f.to_frame(cups))[cups])
                    f.index = pd.date_range(max(deefe.index),max(deefe.index)+pd.DateOffset(months=n),freq='MS',inclusive='left')
                    deefe = pd.concat([deefe,f],axis=0)
                    if len(contractVolDF[cups][contractVolDF[cups][cups]==0.0]) != 0:
                        deefe = deefe[cups].update(contractVolDF[cups][contractVolDF[cups][cups]==0.0])
                    timeSeriesForecast[cups] = deefe
                except:
                    failed[cups] = deefe
                    errors[cups].append('No se puede realizar ningún forecast de ST para este CUPS')
    timeSeriesForecast = pd.concat([i for i in timeSeriesForecast.values()], axis=1).T
    timeSeriesForecast = timeSeriesForecast[[i for i in timeSeriesForecast.columns if i >= metriskOffsetDate]]

#%% OTHER FORECAST

if forecastType == 'Excel' or forecastType == 'Both':
#     # deltaPerMonth['Q3'] = deltaPerMonth.apply(lambda x: x.quantile(0.75), axis=1)
#     # deltaPerMonth['Q1'] = deltaPerMonth.apply(lambda x: x.quantile(0.25), axis=1)
    deltaPerMonth['Q3'] = deltaPerMonth[dpmCols].apply(lambda x: np.nanpercentile(x,75), axis=1)
    deltaPerMonth['Q1'] = deltaPerMonth[dpmCols].apply(lambda x: np.nanpercentile(x,25), axis=1)
    deltaPerMonth['IQR'] = deltaPerMonth.apply(lambda x: x.Q3 - x.Q1, axis=1)
    deltaPerMonth['LB'] = deltaPerMonth.apply(lambda x: x.Q1 - (1.5 * x.IQR), axis=1)
    deltaPerMonth['UB'] = deltaPerMonth.apply(lambda x: x.Q3 + (1.5 * x.IQR), axis=1)

    dpm = []
    for month in dpmCols:
        dpm.append(deltaPerMonth[['LB','UB']+[month]].apply(lambda x: x[month] if x[month]<=x['UB'] and x[month]>=x['LB'] else np.nan, axis=1))
    deltaPerMonth = pd.concat(dpm,axis=1,ignore_index=False)
    deltaPerMonth.columns = dpmCols

    ln = {}
    for i,j in enumerate(dpmCols):
        ln[j] = np.log(2**((i)+1))
    lns = sum(ln.values())

#%%
    caux = macroDF[['CUPS','START_DATE','END_DATE','YEAR','PEAJE','TM','COGENERACION','MERMA']].copy().drop_duplicates()
    caux[['YEAR','MERMA']] = caux[['YEAR','MERMA']].astype(float)
    caux['START_DATE'] = pd.to_datetime(caux['START_DATE'])
    caux['END_DATE'] = pd.to_datetime(caux['END_DATE'])
    caux.set_index('CUPS',inplace=True)

    for i in caux.index:
    #     #if len(caux.loc[i]) > 1:
        try:
            my = max(caux.loc[i]['YEAR'])
            val = caux[(caux['YEAR'] == my)&(caux.index == i)]
            if len(val) > 1:
                dt = max([pd.to_datetime(t) for t in val.END_DATE])
                val = val[val.END_DATE == dt]
            caux.drop(i,inplace=True)
            caux = pd.concat([caux,val],axis=0)
        except:
            continue

    forecast = pd.DataFrame((deltaPerMonth.fillna(0).apply(lambda x: x*(ln[x.name]/lns))).apply(lambda x: sum(x.fillna(0)), axis=1),columns=['Delta'])

    forecast = forecast.merge(caux, how='inner',left_on=forecast.index,right_on=caux.index)
    forecast.columns = ['CUPS']+[i for i in forecast.columns[1:]]
    forecast.set_index('CUPS')

    a = np.log((currentMonthStart-excelStart).days/(deltaDevLTOffsetDate-excelStart).days)
    b = np.log((deltaDevLTOffsetDate-excelStart).days)

    def bFunc(row):
        if row.Delta == 0:
            return np.nan
        else:
            return np.exp(-row.Delta*b/row.Delta)

    forecast['a'] = forecast.apply(lambda x: x.Delta / a, axis=1)
    forecast['b'] = forecast.apply(bFunc, axis=1)
    forecast['adj'] = adj

    forecast['START_DATE'] = pd.to_datetime(forecast.apply(lambda x: min(macroDF[macroDF.CUPS == x.CUPS]['START_DATE']), axis=1))
    forecast['MERMA'] = (forecast['MERMA']/100)+1

    mList = [pd.to_datetime(m) for m in pd.date_range(currentMonthStart,deltaDevLTOffsetDate,freq='MS',inclusive='both')]
    # # forecast[mList] = ''

    def forecaster(row,month):
        if month > row.END_DATE: #row.Delta == 0 or
            fc = 0
        else:
            if month <= dateToday:
                # if row.CUPS in newCupsIndex:
                #     fc = 0
                # else:
                try: ###
                    fc = realMeasure.loc[row.CUPS][month] ###
                except:
                    fc = 0
                merma = 1
            else:
                merma = row.MERMA
                if row.COGENERACION == 'N':
                    fc = 1
                    if row.Delta != 0 and m <= deltaDevLTOffsetDate:
                        fc = 1+(row.a*np.log(row.b*(month-excelStart).days))#+
                    try:
                        fc = fc*float(contractVolDF[row.CUPS].loc[month].iloc[0])
                    except:
                        pass
                else:
                    errors[row.CUPS].append(errorCogen)
                    return(np.nan)
                    pass
                fc = fc*row.adj
    
            fc =fc*merma
            if mList.index(month)+1 < len(mList):
                dtM1 = mList[mList.index(month)+1]
            else:
                dtM1 = month.replace(day=month.days_in_month)
            if row['START_DATE'].replace(day=1) == month:
               fc = fc*((dtM1-row['START_DATE']).days/int(month.days_in_month))
            else:
                if row['END_DATE'].replace(day=1) == month:
                    fc = fc*(((row['END_DATE']-month).days+1)/int(month.days_in_month))
        return fc

    for m in mList:
        forecast[m] = forecast.apply(lambda x: forecaster(x,m), axis=1)

#%% DIFFERENCES

# # comCols = [c for c in forecast.columns if c in oldForecast.columns and type(c) != str]+['Delta','a']
# # comInd = [i for i in forecast.index if i in oldForecast.index]

try:
    forecast = forecast.set_index('CUPS')
except:
    pass

forecastCopy = forecast.copy()

forecastCopy = forecastCopy.merge(cq[[i for i in cq.columns if i not in forecastCopy.columns]],how='left',left_index=True,right_index=True)

for i in errors.keys():
    if len(errors[i]) == 0:
        del errors[i]

# # newCups = [i for i in forecast.index if i not in list(oldForecast.index)]
# # newCups = forecast.loc[newCupsIndex]
# # changes = 100*(oldForecast.loc[comInd][comCols].fillna(0.0) - forecast.loc[comInd][comCols].fillna(0.0))/oldForecast.loc[comInd][comCols].fillna(0.0)
# # changes = forecast.loc[changes.index]
# # changes = []
# # for i in [j for j in forecast.index if j not in newCups]:
# #     if any(forecast[comCols].loc[i] != oldForecast[comCols].loc[i]):
# #         changes.append(forecast.loc[i][forecast[comCols].loc[i] != oldForecast[comCols].loc[i]])
# # changes = [i.T for i in changes]
# # changes = pd.concat(changes, axis=1).T[forecast.columns]
# # changes = forecast[forecast != oldForecast]
# # changes = []
# # for i,row in forecast.iterrows():
# #     if row != oldForecast[oldForecast.CUPS == row.CUPS]:
# #         changes.append(row)
# # changes['CUPS'] = forecast['CUPS']
# # changes = changes.dropna(how='all',axis=1)

# # with pd.ExcelWriter('Changes From '+max(lastLT.keys()).strftime('%Y.%m.%d')+' To '+dateToday.strftime('%Y.%m.%d')+'.xlsx') as writer:
# #     newCups.to_excel(writer,sheet_name='NEW CUPS')
# #     changes.to_excel(writer, sheet_name='CHANGES (%)')
# #     forecast.to_excel(writer,sheet_name=dateToday.strftime('%Y.%m.%d'))
# #     oldForecast.loc[changes.index].to_excel(writer,sheet_name=max(lastLT.keys()).strftime('%Y.%m.%d'))

#%%

os.chdir(wdir)

with pd.ExcelWriter(r'2.3.3 Output\2.3.3.1 LT\LTForecast_'+dateToday.strftime('%Y.%m.%d')+'.xlsx') as writer:
    if forecastType == 'Both':
        forecastCopy.to_excel(writer,sheet_name='EXCEL FORECAST')
        timeSeriesForecast.to_excel(writer,sheet_name='TIME SERIES FORECAST')
    elif forecastType == 'TimeSeries':
        timeSeriesForecast.to_excel(writer,sheet_name='TIME SERIES FORECAST')
    elif forecastType == 'Excel':
        forecastCopy.to_excel(writer,sheet_name='EXCEL FORECAST')
    pd.DataFrame.from_dict(errors, orient='index').to_excel(writer,sheet_name='ERRORS')

# # with pd.ExcelWriter('LTForecast.xlsx', engine='xlsxwriter') as writer:
# #     sheet = writer.book.add_worksheet()
# #     for i,j in enumerate(forecast.keys()):

# #         if i == 0:
# #             sheet.write(0,(i*2)+1,j)
# #             (forecast[j].loc[tFrom:].T).to_excel(writer,startrow = 1, startcol = i*2,header=False)
# #         else:
# #             try:
# #                 sheet.write(0,i+1,j)
# #                 (forecast[j].loc[tFrom:].T).to_excel(writer,startrow = 1, startcol = i+1,header=False,index=False)
# #             except:
# #                 continue

#%%

# if len(deefe) != len(deefe[[j for j in deefe.index if j in ext.index]]) or len(deefe)-len(deefe[[j for j in deefe.index if j in ext.index]]) >= 2:
    
                #     deefe.update(f)
                #     deefe = pd.concat([deefe,f],axis=0)
                #     model = auto_arima(np.log(deefe), trace=True, error_action="ignore", suppress_warnings=True,method='bfgs') #,m=12,stationary=True
                #     model.fit(np.log(deefe))
                #     f = model.predict(n_periods= n)
                #     f = np.exp(f)
                #     f.index = pd.date_range(max(deefe.index),max(deefe.index)+pd.DateOffset(months=n),freq='MS',inclusive='left')
                #     deefe = pd.concat([deefe,f],axis=0)
                #     deefe = deefe[cups].update(contractVolDF[cups][contractVolDF[cups][cups]==0.0])

# vLookUp = pd.read_excel(stForecast, usecols=cc, header=6, sheet_name=stSheet)
# vLookUp = vLookUp[vLookUp["CUPS"].str.contains('Domesticos') == False]

# volumeHistory.columns = ['CUPS', 'CLIENTE']+vLookUp.columns[1:].tolist()
# volumeHistory.drop_duplicates(
#     subset=[c for c in volumeHistory.columns if c != 'CLIENTE'],
#     inplace=True)

# cups = [c for c in vLookUp.CUPS if c in list(volumeHistory.CUPS)]
# realMeasure = vLookUp.set_index('CUPS').copy()
# realMeasure.update(
#     volumeHistory.set_index('CUPS').loc[cups][vLookUp.columns[1:]])
# realMeasure = realMeasure/1000

# au = ['01', '02', '03', '04', '05', '06', '07', '08', '09',
#     '10', '11', '12']

# contractVolDF = dict()
# for cups in contractVol.keys():
#     cvAux = contractVol[cups].copy()
#     cvAux = cvAux.loc[((cvAux.replace(0.0,np.nan)).dropna(how='all',subset=au,axis=0, inplace = False)).index]
#     cvAux.drop_duplicates(inplace=True)
#     if len(cvAux[['INDICE','YEAR']].drop_duplicates()) != len(cvAux[['INDICE','YEAR']]):
#         dupes = cvAux[cvAux[['INDICE','YEAR']].duplicated()][['INDICE','YEAR']].drop_duplicates()

#         for ind in dupes.index:
#             dvol = (cvAux[(cvAux.INDICE == dupes.loc[ind].INDICE)&(cvAux.YEAR == dupes.loc[ind].YEAR)][au].sum()).astype(float)
#             nind = (cvAux[(cvAux.INDICE == dupes.loc[ind].INDICE)&(cvAux.YEAR == dupes.loc[ind].YEAR)].index).to_list()

#             if len(nind) > 1:
#                 cvAux.drop([j for j in nind[1:]],inplace=True)
#                 nind = nind[0]

#             for a in au:
#                 cvAux.at[nind,a] = int(dvol.loc[a])

#     cvAux = cvAux.loc[((cvAux.replace(0.0,np.nan)).dropna(how='all',subset=au,axis=0)).index]
#     if len(set(cvAux.INDICE)) == 1:
#         y = set([int(i) for i in cvAux.YEAR])
#         cv = pd.DataFrame(pd.concat([j for i,j in (cvAux[au].T).items()],
#                                     ignore_index=True))
#         cv.columns = [cups]
#         cv.index = (pd.concat([pd.Series(pd.date_range('1/1/'+str(q),'12/1/'+str(q),freq='MS',inclusive='both')) for q in y])).sort_values(ascending=True)
#         cv = cv.loc[[i for i in cv.index if ((cv.replace({np.nan: None})).loc[i][cups]) != None or (((cv.loc[i:][cups]).notnull()).any() and ((cv.loc[:i][cups]).notnull()).any())]] #
#         contractVolDF[cups] = cv

#     elif len(cvAux) == 0:
#         continue

#     else:
#         aux = []

#         for ind in set(cvAux.INDICE):
#             y = [int(i) for i in cvAux[cvAux.INDICE == ind].YEAR]
#             cv = pd.DataFrame(pd.concat([j for i,j in (cvAux[cvAux.INDICE == ind][au].T).items()],
#                                         ignore_index=True))
#             cv.columns = [ind]
#             cv.index = (pd.concat([pd.Series(pd.date_range('1/1/'+str(q),'12/1/'+str(q),freq='MS',inclusive='both')) for q in y])).sort_values(ascending=True)
#             aux.append(cv)
#         cv = pd.concat(aux,axis=1)
#         cv = pd.DataFrame((cv.replace({np.nan: None})).apply(lambda x: sum([float(x[i]) for i in cv.columns if x[i] != None]), axis=1)) #(cv.fillna(0.0))
#         cv.columns = [cups]
#         cv = cv.loc[[i for i in cv.index if ((cv.replace({np.nan: None})).loc[i][cups]) != None  or (((cv.loc[i:][cups]).notnull()).any() and ((cv.loc[:i][cups]).notnull()).any())]] #
#         contractVolDF[cups] = cv



# realMeasure['START_DATE'] = \
#     realMeasure.apply(
#         lambda x:
#         min(
#             macroDF[macroDF['CUPS'] == x.name]['START_DATE']
#         ),
#         axis=1
# )

# realMeasure['LAST_DATE'] = \
#     realMeasure.apply(
#         lambda x:
#         min(
#             max(
#                 macroDF[macroDF['CUPS'] == x.name]['END_DATE']),
#             min(
#                 [x for x in
#                  macroDF[macroDF['CUPS'] == x.name]['FECBAJ']
#                  if x is not None]
#                 +
#                 [(dateToday + pd.DateOffset(years=100)).date()]
#             )
#         ),
#         axis=1
# )
# auxDF = pd.DataFrame(np.nan, columns=[
#                       'START_DATE', 'END_DATE']+(realMeasure.columns).to_list(), index=[0])

# [c for c in realMeasure.columns if c != currentMonthStart]
# auxDF = pd.DataFrame(index = (realMeasure.columns).to_list())
# deefe['START_DATE'] = min(
        #     macroDF[macroDF['CUPS'] == cups]['START_DATE'])
        # deefe['END_DATE'] = edate
# if cups in contractVol.keys():
        #     y = contractVol[cups].YEAR
        #     au = ['01', '02', '03', '04', '05', '06', '07', '08', '09',
        #     '10', '11', '12']
        #     cv = contractVol[cups][au].T
# ext = pd.Series(ext[cups])
        # deefe = deefe.loc[[j for j in deefe.index if j in ext.index]]
    # deefe.index = pd.to_datetime(deefe.index)
        # ext.index = pd.to_datetime(ext.index)
# deefe.index = (deefe.index).dt.date
        # if len(deefe) > 2 and len(ext[ext.index>max(deefe.index)]) > 0:




# elif len(ext[ext.index>max(deefe.index)]) == 0 and len(deefe) > 2:
        #     historical[cups] = deefe
        #     """n3: logged data, ext in model -> same rersults as n1, BUT no white noise"""
        #     model = auto_arima(np.log(deefe), trace=True, error_action="ignore", suppress_warnings=True,method='bfgs') #,m=12,stationary=True
        #     model.fit(np.log(deefe))
        #     f = model.predict(n_periods= min(forecastNum,len(deefe)))
        #     f = np.exp(f)
        #     forecast[cups] = pd.concat([deefe,f],axis=0)

        # else:
        #     historical[cups] = deefe


        # elif len(deefe.dropna()) <= 2 and len(deefe.dropna()) != 0:

# if len(ext[ext.index>max(deefe.index)]) < forecastNum:
            #     model = auto_arima(np.log(deefe),X=ext.loc[[j for j in ext.index if j in deefe.index]], trace=True, error_action="ignore", suppress_warnings=True,method='bfgs') #,m=12,stationary=True
            #     model.fit(np.log(deefe), X=ext.loc[[j for j in ext.index if j in deefe.index]])
            #     f = model.predict(n_periods=len(ext[ext.index>max(deefe.index)]), X=ext[ext.index>max(deefe.index)])
            #     f = np.exp(f)
            #     #forecast[cups] = f
            #     deefe = pd.concat([deefe,f],axis=0)

            #     model = auto_arima(np.log(deefe), trace=True, error_action="ignore", suppress_warnings=True,method='bfgs') #,m=12,stationary=True
            #     model.fit(np.log(deefe))
            #     f = model.predict(n_periods=forecastNum-len(ext[ext.index>max(deefe.index)]))
            #     f = np.exp(f)
            #     forecast[cups] = pd.concat([deefe,f],axis=0)
            # else:
            #     model = auto_arima(np.log(deefe),X=ext.loc[[j for j in ext.index if j in deefe.index]], trace=True, error_action="ignore", suppress_warnings=True,method='bfgs') #,m=12,stationary=True
            #     model.fit(np.log(deefe), X=ext.loc[[j for j in ext.index if j in deefe.index]])
            #     f = model.predict(n_periods=len(min(forecastNum, ext[ext.index>max(deefe.index)])), X=ext[ext.index>max(deefe.index)])
            #     f = np.exp(f)
            #     forecast[cups] = pd.concat([deefe,f],axis=0)
#chose n3

# """n1: No ext var in model, logged data -> white noise, similar forecast"""
# model = auto_arima(np.log(deefe), trace=True, error_action="ignore", suppress_warnings=True,method='bfgs') #,m=12,stationary=True
# model.fit(np.log(deefe), X=ext.loc[deefe.index])
# f = model.predict(n_periods=len(ext[ext.index>max(deefe.index)]), X=ext[ext.index>max(deefe.index)])
# f = np.exp(f)

# """n2: no log, No ext var in model -> Very high"""
# model = auto_arima(deefe, trace=True, error_action="ignore", suppress_warnings=True,method='bfgs') #,m=12,stationary=True
# model.fit(deefe, X=ext.loc[deefe.index])
# f = model.predict(n_periods=len(ext[ext.index>max(deefe.index)]), X=ext[ext.index>max(deefe.index)])

# """n3: logged data, ext in model -> same rersults as n1, BUT no white noise"""
# model = auto_arima(np.log(deefe),X=ext.loc[deefe.index], trace=True, error_action="ignore", suppress_warnings=True,method='bfgs') #,m=12,stationary=True
# model.fit(np.log(deefe), X=ext.loc[deefe.index])
# f = model.predict(n_periods=len(ext[ext.index>max(deefe.index)]), X=ext[ext.index>max(deefe.index)])
# f = np.exp(f)

# """n4: Logging all data -> white noise, higher results (similar trend)"""
# model = auto_arima(np.log(deefe),X=np.log(ext).loc[deefe.index], trace=True, error_action="ignore", suppress_warnings=True,method='bfgs') #,m=12,stationary=True
# model.fit(np.log(deefe), X=np.log(ext).loc[deefe.index])
# f = model.predict(n_periods=len(ext[ext.index>max(deefe.index)]), X=np.log(ext)[ext.index>max(deefe.index)])
# f = np.exp(f)

# """n5: logged, No ext var at all -> white noise, constant results"""
# model = auto_arima(np.log(deefe), trace=True, error_action="ignore", suppress_warnings=True,method='bfgs') #,m=12,stationary=True
# model.fit(np.log(deefe))
# f = model.predict(n_periods=len(ext[ext.index>max(deefe.index)]))
# f = np.exp(f)

# """n6: vainilla time series -> linear poisitive forecast"""
# model = auto_arima(deefe, trace=True, error_action="ignore", suppress_warnings=True,method='bfgs') #,m=12,stationary=True
# model.fit(deefe)
# f = model.predict(n_periods=len(ext[ext.index>max(deefe.index)]))

# bfgs

# """n31: newton -> fails"""
# model = auto_arima(np.log(deefe),X=ext.loc[deefe.index], trace=True, error_action="ignore", suppress_warnings=True,method='newton') #,m=12,stationary=True
# model.fit(np.log(deefe), X=ext.loc[deefe.index])
# f = model.predict(n_periods=len(ext[ext.index>max(deefe.index)]), X=ext[ext.index>max(deefe.index)])
# f = np.exp(f)

# """n32: nm -> white noise, similar forecast"""
# model = auto_arima(np.log(deefe),X=ext.loc[deefe.index], trace=True, error_action="ignore", suppress_warnings=True,method='nm') #,m=12,stationary=True
# model.fit(np.log(deefe), X=ext.loc[deefe.index])
# f = model.predict(n_periods=len(ext[ext.index>max(deefe.index)]), X=ext[ext.index>max(deefe.index)])
# f = np.exp(f)

# """n33: powell -> smoother forecast"""
# model = auto_arima(np.log(deefe),X=ext.loc[deefe.index], trace=True, error_action="ignore", suppress_warnings=True,method='powell') #,m=12,stationary=True
# model.fit(np.log(deefe), X=ext.loc[deefe.index])
# f = model.predict(n_periods=len(ext[ext.index>max(deefe.index)]), X=ext[ext.index>max(deefe.index)])
# f = np.exp(f)

# """n34: cg -> white noise, similar forecast"""
# model = auto_arima(np.log(deefe),X=ext.loc[deefe.index], trace=True, error_action="ignore", suppress_warnings=True,method='cg') #,m=12,stationary=True
# model.fit(np.log(deefe), X=ext.loc[deefe.index])
# f = model.predict(n_periods=len(ext[ext.index>max(deefe.index)]), X=ext[ext.index>max(deefe.index)])
# f = np.exp(f)

# """n35: ncg -> fails"""
# model = auto_arima(np.log(deefe),X=ext.loc[deefe.index], trace=True, error_action="ignore", suppress_warnings=True,method='ncg') #,m=12,stationary=True
# model.fit(np.log(deefe), X=ext.loc[deefe.index])
# f = model.predict(n_periods=len(ext[ext.index>max(deefe.index)]), X=ext[ext.index>max(deefe.index)])
# f = np.exp(f)

# """n36: basinhopping -> takes a long time, similar forecast"""
# model = auto_arima(np.log(deefe),X=ext.loc[deefe.index], trace=True, error_action="ignore", suppress_warnings=True,method='basinhopping') #,m=12,stationary=True
# model.fit(np.log(deefe), X=ext.loc[deefe.index])
# f = model.predict(n_periods=len(ext[ext.index>max(deefe.index)]), X=ext[ext.index>max(deefe.index)])
# f = np.exp(f)





# forecast = pd.DataFrame(macroDF['CUPS'].drop_duplicates().copy())

# forecast['START_DATE'] = \
#     forecast.apply(
#         lambda x:
#         min(
#             macroDF[macroDF['CUPS'] == x['CUPS']]['START_DATE']
#         ),
#         axis=1
# )

# forecast['LAST_DATE'] = \
#     forecast.apply(
#         lambda x:
#         min(
#             max(
#                 macroDF[macroDF['CUPS'] == x['CUPS']]['END_DATE']),
#             min(
#                 [x for x in
#                  macroDF[macroDF['CUPS'] == x['CUPS']]['FECBAJ']
#                  if x is not None]
#                 +
#                 [(dateToday + pd.DateOffset(years=100)).date()]
#             )
#         ),
#         axis=1
# )

# forecast[['PEAJE', 'TM', 'COGENERACION']] = ""

# forecast.set_index(
#     'CUPS',
#     inplace=True
# )

# forecast.update(
#     (
#         macroDF[['CUPS', 'PEAJE', 'TM', 'COGENERACION']].drop_duplicates(
#             subset='CUPS'
#         )
#     ).set_index(
#         'CUPS'
#     )
# )

# forecast = forecast[forecast.LAST_DATE >= metriskOffsetDate.date()]
