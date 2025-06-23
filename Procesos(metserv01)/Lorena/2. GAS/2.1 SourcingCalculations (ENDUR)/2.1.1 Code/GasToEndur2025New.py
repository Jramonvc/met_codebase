# -*- coding: utf-8 -*-
"""
Created on Thu Mar 21 15:44:35 2024

@author: lorena.sanz
"""
from json import loads, dumps
import pandas as pd
from datetime import date
import keyring
import requests
import copy
from sqlalchemy import create_engine

# dr = '\MET EE' #lor
dr = '' #meterisk

#%% AUTHENTICATION

sql = keyring.get_credential("SQL", None)
sqluser = sql.username
sqlpassword = sql.password

sql_url = f'mssql+pyodbc://{sqluser}:{sqlpassword}@met-esp-prod.database.windows.net/Risk_MGMT_Spain?driver=ODBC Driver 17 for SQL Server'
table = "METDB.TMP_CURVFORW"
engine = create_engine(sql_url, fast_executemany=True)

#%% CONTROL
dateFrom = pd.to_datetime('1/1/2025')
datePF = pd.to_datetime('2/29/2028')
tipo = 'prod' # prod | dev
v  = 'v2' # v1 | v2
volType = 'Planned'
unit = 'MWh 0°C'
mermasPrice = {}

trades = {7211540:'TTF DA',
          7216334:'TTF MA',
          7216337:'MIBGAS WD',
          7216343:'PF',
          14753595:'OMIE',
          8794150:'BRENT',
          11214794:'MIBGAS DA',
          18362278:'MIBGAS AUC DA'}#,
          # 12016225:'REGAS 2024',
          # 8793726:'LNG Storage',
          # 7344920:'AVB Position'}

mibgasTrades = {18220390:['MIBGAS Buy','C,E:F'],
               18220398:['MIBGAS Sell','C,G:H'],
               18220406:['TVB Buy','C,CS:CT'],
               18220407:['TVB Sell','C,CU:CV'],
               18220433:['AVB Buy','C,EC:ED'],
               18220438:['AVB Sell','C,EE:EF'],
               18220445:['PVB for UGS Buy','C,BV:BW'],
               18220465:['PVB for UGS Sell','C,BX:BY']}

mibgasData = {'path':r"M:"+dr+r"\Portfolio Management\Demanda diaria.xlsm",
              'sheet':'MIBGAS',
              'header':6,
              'dateColName': 'Date'}

losses = {'main':{'v':{'leg_num':1,
                       'volume_type':volType,
                       'volume_unit':unit},
                  'p':{'fin_leg_num':2}},
          'mermas':{'deal_num':18220469,
                    'volume_granularity':'Daily'},
          'wagesBuy':{'deal_num':18220528,
                      'volume_granularity':'Daily'},
          'wagesSell':{'deal_num':18220536,
                      'volume_granularity':'Daily'},
          'imbalBuy':{'deal_num':18220549,
                      'volume_granularity':'Daily'},
          'imbalSell':{'deal_num':18220555,
                      'volume_granularity':'Daily'}}

lossesTrades = {}

#12016225,8793726,7344920,7211540,7216334,7216337,7216343,8059376,8794150,11214794

#13967835,13967837,13967831,13967832,13967819,13967820,13967963,13967968



dailyTrades = {'REGAS 2024':{'columns':{'Regas for Endur':'regas'},
                             'legs':{'regas':1,'regas2':3},
                             'dealNum':12016225},

               'LNG Storage':{'columns':{'Regas for Endur':'regas',
                                         'Positive adj.':'+adj',
                                         'Negative adj.':'-adj',
                                         'Withdr. (trucks)':'cisterns'},
                              'legs':{'regas':2,'+adj':3,
                                      '-adj':4,'cisterns':5},
                              'dealNum':8793726},

               'AVB Position':{'columns':{'Position for Endur':'Position for Endur',
                                          'Positive adj.':'+adj',
                                          'Negative adj.':'-adj'},
                               'legs':{'inject':1,'withdraw':3,'+adj':5,'-adj':7},
                               'dealNum':7344920}}

minoristaPath = "M:"+dr+"\Portfolio Management\Extracción Minoristas.xlsm"
outputPath = "M:"+dr+"\Portfolio Management\Gas forecast (to Endur).xlsx"
demandaPath = "M:"+dr+"\Portfolio Management\Demanda diaria.xlsm"

cargo = {#"request_type": "GasVolumeAndPrice",
         "source_system": "CRM_ES", #"Excel"
         "email": "lorena.sanz@met.com",
         'requests': []
         }
macroCargo = {}

dateToday = pd.to_datetime(date.today())
# dateStart = pd.to_datetime('1/1/'+str(dateToday.year))#dateToday + pd.Timedelta(days=2)
dateStart = pd.to_datetime('1/1/2025')
maxRange = pd.to_datetime('12/31/'+str(dateToday.year+3))
lastYear = pd.to_datetime('1/1/'+str(maxRange.year))

# %% FUNCTIONS

statusCode = {500: 'Internal Server Error', 400: 'Bad Request-Client Error',
              200: 'Request Successful',
              401: 'Request Unauthorized', 404: 'Request (URL) Not Found',
              403: 'Request Forbidden', 405: 'Method Not Allowed (Request Type)',
              408: 'Request Timeout'}

def getToken(tipo):

    if tipo == 'prod':
        client = keyring.get_credential("endur_client_prod", None)
    elif tipo == 'dev':
        client = keyring.get_credential("endur_client", None)

    client_id = client.username
    client_secret = client.password
    url = 'https://'+tipo+'-identity-server-svc.azurewebsites.net/connect/token'
    # else:
        # return print('Tipo can only be "Prod" or "Test"')
    payload = {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
        'scopes': 'edi_endur_update_api.client'
    }
    r = requests.post(url, data=payload)
    token = loads(r.content.decode('utf-8'))['access_token']

    return token


def checkStatus(trackID, tipo):

    token = getToken(tipo)
    url = "https://"+tipo+"-edi-endur-update-api.azurewebsites.net/api/response"
    if trackID[0] != '"' and trackID[-1] != '"':
        trackID = '"'+trackID+'"'
    r = requests.post(url, headers={
                      "Content-Type": "application/json", 'Authorization': f'Bearer {token}'}, data=trackID)

    return r.text


def postDeals(jsonData, loadType, tipo, v):

    code = {'PowerVolumeAndPrice': '/power/volume-and-price',
            'Gas': '/gas/volume-and-price'}
    token = getToken(tipo)

    url = "https://"+tipo+"-edi-endur-update-api.azurewebsites.net/api/"+v+"/request/"
    if v == 'v1':
        url += code[loadType]

    r = requests.post(url, headers={
                      "Content-Type": "application/json", 'Authorization': f'Bearer {token}'}, data=jsonData)
    num = r.__dict__["status_code"]
    if num == 200:
        trackID = loads(r.content)[0]['tracking_id']
        return f'Status Code: {num}, ID: {trackID}'
    else:
        if num not in statusCode.keys():
            word = ''
        else:
            word = ' -- '+statusCode[r.__dict__["status_code"]]
        return f'Status Code: {num}{word}', word

if v == 'v1':
    volscols = ['leg_num', 'volume', 'volume_type',
            'from_date', 'to_date']
elif v == 'v2':
    volscols = ['leg_num', 'volume', 'volume_type', 'volume_unit',
          'volume_granularity','from_date', 'to_date']

# %% GET DATA

# dateFrom = (pd.to_datetime(date.today()).floor('D') + pd.DateOffset(months=1)).replace(day=1)

# cols = ['Date','TTF DA','TTF MA','BRENT','PF','MIBGAS WD','MIBGAS DA',
#         'MIBGAS AUC DA','MIBGAS AUC WD','OMIE']
#7211540,7216334,7216337,7216343,8059376,8794150,11214794

mult = pd.read_excel(
    minoristaPath,
    sheet_name='Balance',
    usecols='BR:CC', #usecols='BM:BW', #usecols='BL:BV',
    header=4)

spread = pd.read_excel(
    minoristaPath,
    sheet_name='Balance',
    usecols='BE:BP',#usecols='BA:BK', #usecols='AZ:BJ',
    header=4)

balance = pd.read_excel(
    minoristaPath,
    sheet_name='Balance',
    usecols='AR:BC', #usecols='AO:AY',
    header=4)

cols = [c[:-2] for c in mult.columns] #

mult.columns = cols
mult.dropna(how='all', axis=0, inplace=True)
outx = [n for (n, d) in enumerate(mult['Date']) if type(d) == str]
mult = mult.iloc[:outx[0]]
mult['Date'] = pd.to_datetime(mult['Date'])
mult = mult[mult['Date'] >= dateFrom]
mult['DateTo'] = mult['Date'].map(
    lambda x: x + pd.DateOffset(x.days_in_month-1))
# mult['Date'] = mult['Date'].dt.strftime('%d/%m/%Y %H:%M')
# mult['DateTo'] = mult['DateTo'].dt.strftime('%d/%m/%Y %H:%M')

spread.columns = cols
spread.dropna(how='all', axis=0, inplace=True)
outx = [n for (n, d) in enumerate(spread['Date']) if type(d) == str]
spread = spread.iloc[:outx[0]]
spread['Date'] = pd.to_datetime(spread['Date'])
spread = spread[spread['Date'] >= dateFrom]
spread['DateTo'] = spread['Date'].map(
    lambda x: x + pd.DateOffset(x.days_in_month-1))
# spread['Date'] = spread['Date'].dt.strftime('%d/%m/%Y %H:%M')
# spread['DateTo'] = spread['DateTo'].dt.strftime('%d/%m/%Y %H:%M')

balance.columns = cols
balance.dropna(how='all', axis=0, inplace=True)
outx = [n for (n, d) in enumerate(balance['Date']) if type(d) == str]
balance = balance.iloc[:outx[0]]
balance['Date'] = pd.to_datetime(balance['Date'])
balance = balance[balance['Date'] >= dateFrom]
balance['DateTo'] = balance['Date'].map(
    lambda x: x + pd.DateOffset(x.days_in_month-1))
# balance['Date'] = balance['Date'].dt.strftime('%d/%m/%Y %H:%M')
# balance['DateTo'] = balance['DateTo'].dt.strftime('%d/%m/%Y %H:%M')

mibgas = dict()
for n in mibgasTrades.keys():
    mb = pd.read_excel(mibgasData['path'],sheet_name=mibgasData['sheet'],header=mibgasData['header'],usecols=mibgasTrades[n][-1]).set_index(mibgasData['dateColName'])
    mb.columns = ['volume','price']
    mibgas[n] = mb[(mb.index >= dateFrom)]#abs&(mb.index < dateStart)


#%%######################## GAS BALANCE INSTRUMENTS ###########################
dayVol = pd.DataFrame(columns=volscols+['deal_num'])
dfDayVols = {}

regas2024 = pd.read_excel(demandaPath,sheet_name = "Balance TVB", usecols = 'E,BD', skiprows = 7)
regas2024 = regas2024[(regas2024.Date >= dateStart)&(regas2024.Date <= maxRange)].set_index('Date')
regas2024.columns = [dailyTrades['REGAS 2024']['columns'][i] for i in regas2024.columns]
regas2024['regas2'] = regas2024['regas']
dailyTrades['REGAS 2024']['df'] = copy.deepcopy(regas2024)

lngStorage = pd.read_excel(demandaPath,sheet_name = "Balance TVB", usecols = 'E,BD:BG', skiprows = 7)
lngStorage = lngStorage[(lngStorage.Date >= dateStart)&(lngStorage.Date <= maxRange)].set_index('Date')
lngStorage.columns = [dailyTrades['LNG Storage']['columns'][i] for i in lngStorage.columns]
dailyTrades['LNG Storage']['df'] = copy.deepcopy(lngStorage)

# for i,j in enumerate([regas2024,lngStorage,avbPosition]):
#     key = list(dailyTrades.keys())[i]
#     for q,w in enumerate(dailyTrades[key]['columns'].keys()):
#         dailyTrades[key]['columns'][j.columns[q]] = dailyTrades[key]['columns'].pop(w)

def positionator(row,typet):
    if typet == 'inject':
        if row[row.index[0]] >= 0:
            return 0
        else:
            return abs(row[row.index[0]])
    elif typet == 'withdraw':
        if row[row.index[0]] >= 0:
            return row[row.index[0]]
        else:
            return 0

avbPosition = pd.read_excel(demandaPath,sheet_name = "Balance AVB", usecols = 'I,AU:AW', skiprows = 7)
avbPosition = avbPosition[(avbPosition.Date >= dateStart)&(avbPosition.Date <= maxRange)].set_index('Date')
avbPosition.columns = [dailyTrades['AVB Position']['columns'][i] for i in avbPosition.columns]
avbPosition['inject'] = avbPosition.apply(lambda x: positionator(x,'inject'), axis=1)
avbPosition['withdraw'] = avbPosition.apply(lambda x: positionator(x,'withdraw'), axis=1)
dailyTrades['AVB Position']['df'] = copy.deepcopy(avbPosition)

for dTrade in dailyTrades.keys():
    aux = dailyTrades[dTrade]
    dfDayVols[aux['dealNum']] = {'requests':[]} #'deal_num':

    dfV = copy.deepcopy(dayVol)
    
    dfV.from_date = aux['df'].index.strftime('%d/%m/%Y %H:%M')
    dfV.to_date = aux['df'].index.strftime('%d/%m/%Y %H:%M')

    dfV.volume_type = volType
    dfV.volume_unit = unit
    dfV.volume_granularity = 'Daily'
    dfV.deal_num = aux['dealNum']

    for cl in aux['legs']:
        dfV2 = copy.deepcopy(dfV)
        dfV2.leg_num = aux['legs'][cl]

        dfV2.volume = aux['df'][cl].values
        # dfV.set_index('from_date').update(aux['df'][cl])


        dfDayVols[aux['dealNum']]['requests'].append(dfV2.to_dict(orient='records')) #[aux['legs'][cl]]

    dfDayVols[aux['dealNum']]['requests'] = [j for i in dfDayVols[aux['dealNum']]['requests'] for j in i]
    macroCargo[aux['dealNum']] = copy.deepcopy(cargo)
    macroCargo[aux['dealNum']]['request_type'] = 'GasVolume'
    macroCargo[aux['dealNum']]['requests'] = dfDayVols[aux['dealNum']]['requests']
    
#%% MERMAS AND IMBALANCE

mermas = pd.read_excel(demandaPath,sheet_name="Balance PVB",header=5,usecols="C,AT")
mermas.columns = ['from_date','volume']
mermas['from_date'] = pd.to_datetime(mermas['from_date'])
mermas = mermas[(mermas.from_date >= dateStart)&(mermas.from_date.dt.year == 2025)].set_index('from_date')
mermas = mermas.dropna()
mermas['price'] = 0
lossesTrades['mermas'] = mermas#.reset_index()

# 

wages = pd.read_excel(demandaPath,sheet_name="Balance PVB",header=5,usecols="C,AU")
wages.columns = ['from_date','volume']
wages['from_date'] = pd.to_datetime(wages['from_date'])
wages = wages[(wages['from_date'] >= dateStart)].set_index('from_date')
wages = wages.dropna()

#--

y = min(wages.index).year
fstr = f"select * from METDB.MET_FIXINGS where FXG_IDM_CODIGO = 'MIBGAS' and FXG_FECHA >= '{y-1}-10-01'"
q = pd.read_sql(fstr,engine)[['FXG_FECHA','FXG_PRICE']]
q.columns = ['date','price']

x = max(wages.index).year
fstr2 = f"SELECT * from METDB.MET_CURVFORW where CVF_FECPUBLIC = (SELECT MAX(CVF_FECPUBLIC) from METDB.MET_CURVFORW) and CVF_IDM_CODIGO = 'PVB' and (CVF_PERIOD = 'BM' or CVF_PERIOD = 'MM') and CVF_FECSTART < '{x+1}-10-01'"
u = pd.read_sql(fstr2,engine)[['CVF_FECSTART','CVF_PRICE']]
u.columns = ['date','price']

qu = pd.concat([q,u]).set_index('date')
qu = qu.sort_index()
for i in set(qu.index.year):
    if i < y :
        continue
    else:
        start = pd.to_datetime("10/01/"+str(i-1))
        end = pd.to_datetime("10/01/"+str(i))
        mermasPrice[i] = [i for i in qu.loc[start:end].mean()][0]

def wagesPrice(row):
    year = row.name.year
    if row.name < pd.to_datetime("10/01/"+str(year)):
        return mermasPrice[y]
    else:
        return mermasPrice[y+1]
#--

wagesBuy = wages[wages.volume < 0]*-1
wagesBuy = wagesBuy.reindex(labels=wages.index,fill_value=0)
wagesBuy['price'] = wagesBuy.apply(wagesPrice,axis=1)
lossesTrades['wagesBuy'] = wagesBuy#.reset_index()

wagesSell = wages[wages.volume > 0]
wagesSell = wagesSell.reindex(labels=wages.index,fill_value=0)
wagesSell['price'] = wagesSell.apply(wagesPrice,axis=1)
lossesTrades['wagesSell'] = wagesSell#.reset_index()

#

imbalance = pd.read_excel(demandaPath,sheet_name="Balance PVB",header=5,usecols="C,BH")
imbalance.columns = ['from_date','volume']
imbalance['from_date'] = pd.to_datetime(imbalance['from_date'])
imbalance = imbalance[(imbalance['from_date'] >= dateStart)].set_index('from_date')
imbalance = imbalance.dropna()

imbalBuy = imbalance[imbalance.volume < 0]*-1
imbalBuy = imbalBuy.reindex(labels=imbalance.index,fill_value=0)

imbalSell = imbalance[imbalance.volume > 0]
imbalSell = imbalSell.reindex(labels=imbalance.index,fill_value=0)

p = pd.read_sql("SELECT * FROM [METDB].[MET_FIXINGS] where (FXG_IDM_CODIGO = 'BALBUY' or FXG_IDM_CODIGO = 'BSELL') and FXG_FECHA >= '1/1/2025'", engine)
p.columns = ['from_date','price','code']
p.from_date = pd.to_datetime(p.from_date)
balbuy = p[p.code == 'BALBUY'][['from_date','price']].set_index('from_date')
bsell = p[p.code == 'BSELL'][['from_date','price']].set_index('from_date')

imbalBuy = imbalBuy.join(balbuy)
imbalSell = imbalSell.join(bsell)

lossesTrades['imbalBuy'] = imbalBuy
lossesTrades['imbalSell'] = imbalSell

for trade in lossesTrades.keys():
    tradeNum = losses[trade]['deal_num']
    macroCargo[tradeNum] = copy.deepcopy(cargo)
    macroCargo[tradeNum]['requests'] = []
    reqs = {'deal_num':tradeNum}
    
    vdf = lossesTrades[trade]['volume'].reset_index().copy()
    if losses[trade]['volume_granularity'] == 'Daily':
        vdf['to_date'] = vdf['from_date']# + pd.DateOffset(days=1)
    else:
        vdf['to_date'] = vdf['from_date'].apply(lambda x: x + pd.DateOffset(days=x.days_in_month))
    for c in losses['main']['v'].keys():
        vdf[c] = losses['main']['v'][c]
    vdf['from_date'] = vdf['from_date'].dt.strftime('%d/%m/%Y %H:%M')
    vdf['to_date'] = vdf['to_date'].dt.strftime('%d/%m/%Y %H:%M')
    
    pdf = lossesTrades[trade]['price'].reset_index().copy()
    pdf['from_date'] = pdf['from_date'].dt.strftime('%d/%m/%Y %H:%M')
    pdf['to_date'] = vdf.to_date
    for c in losses['main']['p'].keys():
        pdf[c] = losses['main']['p'][c]
    for c in [cc for cc in losses[trade].keys() if cc != 'trade_num']:
        vdf[c] = losses[trade][c]
        
    reqs['volumes'] = vdf.to_dict(orient='records')
    reqs['prices'] = pdf.to_dict(orient='records')
    macroCargo[tradeNum]['requests'].append(reqs)
    macroCargo[tradeNum]['request_type'] = 'GasVolumeAndPrice'
        
#%%############################## TEMPLATE ####################################

priscols = ['fin_leg_num','from_date', 'to_date'] #'index_percent','index_premium', 
vol = pd.DataFrame(columns=volscols)
vol['from_date'] = copy.deepcopy(balance['Date'])
vol['to_date'] = copy.deepcopy(balance['DateTo'])
vol['volume_type'] = volType
vol['leg_num'] = 1

if v == 'v2':
    vol['volume_unit'] = unit
    vol['volume_granularity'] = 'Monthly'

prx = pd.DataFrame(columns=priscols)
prx['from_date'] = copy.deepcopy(balance['Date'])
prx['to_date'] =copy.deepcopy(balance['DateTo'])
prx['fin_leg_num'] = 2


reqs = copy.deepcopy(dfDayVols)


# %% OTHER TRADES

for trade in trades.keys():

    macroCargo[trade] = copy.deepcopy(cargo)
    dfV = (copy.deepcopy(vol)).set_index('from_date')
    # dfV['deal_num'] = trade
    dfV['volume'].update(balance.set_index('Date')[trades[trade]])
    # dfV = dfV.join(balance.set_index('Date')[trades[trade]], how='inner',lsuffix='2')[[c for c in dfV.columns if c != 'volume2']]
    dfP = (copy.deepcopy(prx)).set_index('from_date')
    # dfP['deal_num'] = trade
    # dfV = (dfV[dfV['volume'] != 0].reset_index())[volscols]
    
    if trade == 7216343:
        dfV = (dfV[dfV.to_date <= datePF].reset_index())[volscols]
        dfP['price'] = ''
        dfP['price'].update(spread.set_index('Date')[trades[trade]])
        # dfP = dfP.join(spread.set_index('Date')[trades[trade]], how='inner',lsuffix='2')[[c for c in dfP.columns if c != 'price2']]
        dfP = (dfP[(dfP['price'] != 0)].reset_index())[priscols+['price']] #(dfP['price'] != '')
        price_or_premium = 'price'
    else:
        dfV = (dfV[dfV['volume'] != 0].reset_index())[volscols]
        dfP['index_percent'] = ''
        dfP['index_premium'] = ''
        dfP['index_percent'].update(mult.set_index('Date')[trades[trade]])
        # dfP = dfP.join(spread.set_index('Date')[trades[trade]], how='inner',lsuffix='2')[[c for c in dfP.columns if c != 'index_percent2']]
        dfP['index_premium'].update(spread.set_index('Date')[trades[trade]])
        # dfP = dfP.join(spread.set_index('Date')[trades[trade]], how='inner',lsuffix='2')[[c for c in dfP.columns if c != 'index_premium2']]
        dfP = (dfP[((dfP['index_percent'] != 0))|((dfP['index_premium'] != 0))].reset_index())[priscols+['index_premium','index_percent']]#&(dfP['index_percent'] != '')&(dfP['index_premium'] != '')
        """TURNS OUT INDEX_PERCENT IS ALREADY IN % FORMAT"""
        dfP['index_percent'] = dfP['index_percent']*100
        price_or_premium = 'index_premium'
    

    dfV['from_date'] = dfV['from_date'].dt.strftime('%d/%m/%Y %H:%M')
    dfV['to_date'] = dfV['to_date'].dt.strftime('%d/%m/%Y %H:%M')

    dfP['from_date'] = dfP['from_date'].dt.strftime('%d/%m/%Y %H:%M')
    dfP['to_date'] = dfP['to_date'].dt.strftime('%d/%m/%Y %H:%M')

    if v == 'v1':
        dfV['deal_num'] = trade
        dfP['deal_num'] = trade

    reqs[trade] = dict()

    dfV = dfV[dfV.volume != ''].dropna(subset='volume')
    dfP = dfP[dfP[price_or_premium] != ''].dropna(subset=price_or_premium)

    if len(dfV) != 0 and len(dfP) != 0:

        if v == 'v2':
            reqs[trade]['deal_num'] = trade
            macroCargo[trade]['request_type'] = 'GasVolumeAndPrice'

        reqs[trade]['volumes'] = (dfV).to_dict(orient='records')
        reqs[trade]['prices'] = (dfP).to_dict(orient='records')
        
    else:
        if len(dfV) != 0:

            if v == 'v2':
                reqs[trade]['deal_num'] = trade
                macroCargo[trade]['request_type'] = 'GasVolume'

            reqs[trade]['volumes'] = (dfV).to_dict(orient='records')

        elif len(dfP) != 0:

            if v == 'v2':
                reqs[trade]['deal_num'] = trade
                macroCargo[trade]['request_type'] = 'GasPrice'

            reqs[trade]['prices'] = (dfP).to_dict(orient='records')
        else:
            del macroCargo[trade]
            continue
    macroCargo[trade]['requests'].append(reqs[trade])

###############################################################################

###############################################################################

for mtrade in mibgas.keys():

    # if mtrade in [13967963,13967968]:
    #     continue

    macroCargo[mtrade] = copy.deepcopy(cargo)
    mdfV = copy.deepcopy((vol[0:0]))#.

    mdfV['from_date'] = mibgas[mtrade].index
    mdfV['to_date'] = mibgas[mtrade].index

    mdfV = mdfV.set_index('from_date')
    # mdfV['volume'].update(abs(mibgas[mtrade][[i for i in mibgas[mtrade].columns if 'Vol' in i]]))
    mdfV['volume'].update(abs(mibgas[mtrade].volume))
    # mdfV = mdfV.join(abs(mibgas[mtrade].volume), how='inner',lsuffix='2')[[c for c in mdfV.columns if c != 'volume2']]
    mdfV = mdfV.reset_index()

    mdfV['volume_type'] = volType
    mdfV['volume_unit'] = unit
    mdfV['volume_granularity'] = 'Daily'
    mdfV['leg_num'] = 1

    mdfV['from_date'] = mdfV['from_date'].dt.strftime('%d/%m/%Y %H:%M')
    mdfV['to_date'] = mdfV['to_date'].dt.strftime('%d/%m/%Y %H:%M')


    mdfP = copy.deepcopy(prx[0:0])#().set_index('from_date')mibgas[mtrade].index_premium

    mdfP['from_date'] = mibgas[mtrade].index
    mdfP['to_date'] = mibgas[mtrade].index

    mdfP = mdfP.set_index('from_date')
    mdfP['price'] = ''
    mdfP['price'].update(mibgas[mtrade].price)
    # mdfP = mdfP.join(abs(mibgas[mtrade].price), how='inner',lsuffix='2')[[c for c in mdfP.columns if c != 'price2']]
    mdfP = mdfP.reset_index()

    # mdfP['index_percent'] = 100
    # mdfP['index_premium'] = mibgas[mtrade][[i for i in mibgas[mtrade].columns if 'Price' in i]]
    mdfP['fin_leg_num'] = 2

    mdfP['from_date'] = mdfP['from_date'].dt.strftime('%d/%m/%Y %H:%M')
    mdfP['to_date'] = mdfP['to_date'].dt.strftime('%d/%m/%Y %H:%M')

    if v == 'v1':
        mdfV['deal_num'] = mtrade
        mdfP['deal_num'] = mtrade

    reqs[mtrade] = dict()

    mdfV = mdfV.dropna(subset='volume')
    mdfP = mdfP.dropna(subset='price')

    if len(dfV) != 0 and len(dfP) != 0:

        if v == 'v2':
            reqs[mtrade]['deal_num'] = mtrade
            macroCargo[mtrade]['request_type'] = 'GasVolumeAndPrice'

        reqs[mtrade]['volumes'] = (mdfV).to_dict(orient='records')
        reqs[mtrade]['prices'] = (mdfP).to_dict(orient='records')
        
    else:
        if len(mdfV) != 0:

            if v == 'v2':
                reqs[mtrade]['deal_num'] = mtrade
                macroCargo[mtrade]['request_type'] = 'GasVolume'

            reqs[mtrade]['volumes'] = (mdfV).to_dict(orient='records')

        elif len(mdfP) != 0:

            if v == 'v2':
                reqs[mtrade]['deal_num'] = mtrade
                macroCargo[mtrade]['request_type'] = 'GasPrice'

            reqs[mtrade]['prices'] = (mdfP).to_dict(orient='records')
        else:
            del macroCargo[mtrade]
            continue
    macroCargo[mtrade]['requests'].append(reqs[mtrade])


# %% JSON UPLOAD


if v == 'v2':
    aa = []
    for j in [num for num in macroCargo.keys() if len(macroCargo[num]) != 0]:
        if len(macroCargo[j]['requests']) == 0:
            continue
        # cargo["requests"] = [reqs[j]]
        f_dict = dumps(macroCargo[j], indent=1) #reqs[j]
        aa.append(f_dict)
        r = postDeals(f_dict, "Gas", tipo, v)
        print(j,r)

elif v == 'v1':
    aa = []
    for j in [num for num in macroCargo.keys() if len(macroCargo[num]) != 0]:
        if len(macroCargo[j]['requests']) == 0:
            continue
        f_dict = dumps([macroCargo[j]], indent=1) #
        aa.append(f_dict)
        r = postDeals(f_dict, "Gas", tipo, v)
        print(j,r)

else:
    print("v can only be either 'v1' or 'v2")

# with open('data.json', 'w') as f:
#     f.write(f_dict)

# for j in [num for num in reqs.keys() if len(reqs[num]) != 0]:
#     f_dict = dumps([reqs[j]], indent=1)


# %% REJECTED CODE

# juason = requests[j].to_dict(orient='records')

# mult = pd.read_excel(
#     minoristaPath,
#     sheet_name='Balance',
#     usecols='BH:BQ',
#     header=4)
# cols = [c[:-2] for c in mult.columns]

# vol['from_date'] = pd.to_datetime(balance['Date'].copy())
# vol['to_date'] = vol['from_date'].map(
#     lambda x: x + pd.DateOffset(x.days_in_month-1))

# prx['from_date'] = pd.to_datetime(balance['Date'].copy())
# prx['to_date'] = prx['from_date'].map(
#     lambda x: x + pd.DateOffset(x.days_in_month-1))

# for df in [mult, spread, balance]:
#     df.columns = cols
#     df.dropna(how='all', axis=0, inplace=True)
#     outx = [n for (n, d) in enumerate(df['Date']) if type(d) == str]
#     df = df.iloc[:outx[0]]
#     df = df[df['Date'] >= dateFrom]
#     df['Date'] = pd.to_datetime(df['Date'])
#     df['DateTo'] = df['Date'].map(
#         lambda x: x + pd.DateOffset(x.days_in_month-1))
#     df['Date'] = df['Date'].dt.strftime('%d/%m/%Y %H:%M')
#     df['DateTo'] = df['DateTo'].dt.strftime('%d/%m/%Y %H:%M')


# for y in set(wages.index.year):
#     mermasPrice[y] = {}
#     if max(wages[wages.index.year == y].index) < pd.to_datetime('30/09/'+str(y)):
#         fstr =  f"SELECT AVG(FXG_PRICE) FROM [METDB].[MET_FIXINGS] where FXG_IDM_CODIGO = 'MIBGAS' and FXG_FECHA >= '{y-1}-10-01' and FXG_FECHA <= '{y}-09-30'"
#         mermasPrice[y][y] = [i for i in pd.read_sql(fstr,engine).loc[0]][0]
#     else:
#         mermasPrice[y][y+1] = pd.concat([pd.read_sql(engine,f"SELECT FXG_PRICE FROM [METDB].[MET_FIXINGS] where FXG_IDM_CODIGO = 'MIBGAS' and FXG_FECHA >= '{y}-10-01' and FXG_FECHA <= '{y+1}-09-30'"), pd.read_sql(engine,"SELECT CVF_PRICE from METDB.MET_CURVFORW where CVF_FECPUBLIC = (SELECT MAX(CVF_FECPUBLIC) from METDB.MET_CURVFORW) and CVF_IDM_CODIGO = 'PVB' and CVF_PERIOD = 'MM'")])
