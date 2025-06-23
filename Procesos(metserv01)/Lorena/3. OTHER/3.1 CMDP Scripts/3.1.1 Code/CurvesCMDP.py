# -*- coding: utf-8 -*-
"""
Created on Thu May  9 11:57:22 2024

@author: lorena.sanz
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Dec 20 16:44:59 2023

@author: lorena.sanz
"""

#%% IMPORTS
import pandas as pd
import numpy as np
import requests
from sqlalchemy import create_engine, text
pd.set_option('display.max_columns', None)
import datetime
import keyring
import json
from urllib.request import urlopen
from datetime import date
#from curvas_220_234 import curvas
import time 
from datetime import datetime, timedelta
import pyodbc
from sqlalchemy.dialects.mysql import insert
import calendar
import holidays
import os

now = datetime.now().strftime('%d/%m/%Y %H:%M')

# dr = '\MET EE'# lor
dr = '' # meterisk

directoryPath = r"M:"+dr+r"\Portfolio Management\Lorena\3. OTHER\3.1 CMDP Scripts"
os.chdir(directoryPath)
try:
    fRes = open("C:Resultados.txt", "a+")#\Price_Curve\
    file = True
except:
    file = False
withTable = 'append' 
# withTable = 'replace'

if file:
    fRes.write("\n\nLog "+now+":\n")

#%% AUTHENTICATION

sql = keyring.get_credential("SQL", None)
sqluser = sql.username
sqlpassword = sql.password

sql_url = f'mssql+pyodbc://{sqluser}:{sqlpassword}@met-esp-prod.database.windows.net/Risk_MGMT_Spain?driver=ODBC Driver 17 for SQL Server'
table = "METDB.TMP_CURVFORW"
engine = create_engine(sql_url, fast_executemany=True)

credential = keyring.get_credential("system", None)
username = credential.username
password = credential.password

client = keyring.get_credential("cmdp_client", None)
client_id = client.username
client_secret = client.password

config = json.load(open(r"M:"+dr+r"\Portfolio Management\Lorena\3. OTHER\3.1 CMDP Scripts\3.1.1 Code\settings.json"))
config['client_id'] = client_id
config['client_secret'] = client_secret

#%% DATE
# pDate_end = pd.to_datetime('4/23/2024')
pDate_end = pd.to_datetime(datetime.today()).floor('D')
pDate_start = pDate_end - pd.DateOffset(days=5)

errors = []

# pDate_start = pd.to_datetime('1/1/2023')
# pDate_end = pd.to_datetime('1/1/2023')
# end = pDate_start

#%% TOKEN

def getToken():
    meta_data_url = config['issuer'] + '/.well-known/openid-configuration'
    
    meta_data = urlopen(meta_data_url)
    if meta_data:
        test = json.load(meta_data)
        for key in test.keys():
            config[key] = test[key]
    payload = {
    'grant_type': 'password',
    'client_id': config['client_id'],
    'client_secret': config['client_secret'],
    'username': username,
    'password': password,
    'scope': 'cmdp_price_api'
    }
    time_token = time.time()
    tk = requests.post(config['token_endpoint'], 
        headers={"Content-Type":"application/x-www-form-urlencoded"},
        data=payload)
    print('Token acquired!')
    #token = json.loads(tk.content.decode('utf-8'))['access_token']    
    return tk,time_token

#%% FUNCTION: PRICES
def getPrices(curveId, publicationDateFrom, publicationDateTo,tkn,t):
    
    if (time.time()-t) >= tkn.json()['expires_in']: 
        tkn, t = getToken()
            
    token = tkn.json()['access_token']
    url = f"{config['api_endpoint']}/prices/{curveId}/{publicationDateFrom}/{publicationDateTo}"
    r = requests.get(url, 
        headers={"Content-Type":"application/x-www-form-urlencoded", 'Authorization': f'Bearer {token}'})

    print(url)
    print(r)
    
    return json.loads(r.content)

#%% MACRO FUNCTION

# def getAllPrices(pDate_start,pDate_end,errors):
#%%
###############################################################################
#TOKEN TOKEN TOKEN TOKEN TOKEN TOKEN TOKEN TOKEN TOKEN TOKEN TOKEN TOKEN TOKEN 
###############################################################################
token,ttk = getToken()     
###############################################################################
#DATA DATA DATA DATA DATA DATA DATA DATA DATA DATA DATA DATA DATA DATA DATA
###############################################################################
cols = ['curveId','publicationDate','deliveryStartDate','deliveryEndDate','periodicity','periodicityAbsolute','periodicityRelative']
#--------------------------------------------------------------------------
df_quarter= {1:1,2:1,3:1,4:4,5:4,6:4,7:7,8:7,9:7,10:10,11:10,12:10}
df_m3 = {1:10,2:11,3:12,4:1,5:2,6:3,7:4,8:5,9:6,10:7,11:8,12:9}
df_m6 = {1:7,2:8,3:9,4:10,5:11,6:12,7:1,8:2,9:3,10:4,11:5,12:6}
###############################################################################
#DATES DATES DATES DATES DATES DATES DATES DATES DATES DATES DATES DATES DATES 
###############################################################################
start = pDate_start.floor('D')-pd.DateOffset(days=10) 
end = pd.to_datetime(pDate_end).floor('D')-pd.DateOffset(days=3)
originals = {}
#--------------------------------------------------------------------------
qstart = pd.to_datetime(str(df_quarter[pDate_start.month])+'/1/'+str(pDate_start.year)) #First day of quarter
aux = pd.to_datetime(datetime.today()).floor('D')
monthNext = aux.replace(day=calendar.monthrange(aux.year,aux.month)[1]) + pd.DateOffset(days=1)
mstart = pDate_start.replace(day=1)
ma_end = pDate_end.replace(day=1)
ma_start =(mstart-pd.DateOffset(days=1)).replace(day=1)-pd.DateOffset(days=2)
originals['qstart'] = qstart
qstart += pd.DateOffset(days=3)
#--------------------------------------------------------------------------
if start.month <= 3:
    start303 = pd.to_datetime(str(df_m3[originals['qstart'].month])+'/1/'+str(originals['qstart'].year-1))
    start603 = pd.to_datetime(str(df_m6[originals['qstart'].month])+'/1/'+str(originals['qstart'].year-1))
elif 3 < start.month <= 6:
    start303 = pd.to_datetime(str(df_m3[originals['qstart'].month])+'/1/'+str(originals['qstart'].year))
    start603 = pd.to_datetime(str(df_m6[originals['qstart'].month])+'/1/'+str(originals['qstart'].year-1))
else:
    start303 = pd.to_datetime(str(df_m3[originals['qstart'].month])+'/1/'+str(originals['qstart'].year))    
    start603 = pd.to_datetime(str(df_m6[originals['qstart'].month])+'/1/'+str(originals['qstart'].year))
originals['start303']=start303
originals['start603']=start603
#--------------------------------------------------------------------------
for d in [start,end,qstart,start303,start603]:
    if d.weekday() == 6:
        d -= pd.DateOffset(days=2)
    elif d.weekday() == 5:
        d -= -pd.DateOffset(days=1)
        
###############################################################################
#MIBGAS FIX MIBGAS FIX MIBGAS FIX MIBGAS FIX MIBGAS FIX MIBGAS FIX MIBGAS FIX F
###############################################################################
    #IMBALANCE
df_71 = pd.DataFrame(getPrices(71,start.strftime('%Y-%m-%d'),pDate_end.strftime('%Y-%m-%d'),token,ttk))
df_71.sort_values(by='periodicityAbsolute', ascending=True,inplace=True)
    #AUC
df_1154 = pd.DataFrame(getPrices(1154,start.strftime('%Y-%m-%d'),pDate_end.strftime('%Y-%m-%d'),token,ttk))
df_1154.sort_values(by='deliveryStartDate', ascending=True,inplace=True)
    #REF
df_1331 = pd.DataFrame(getPrices(1331,start.strftime('%Y-%m-%d'),pDate_end.strftime('%Y-%m-%d'),token,ttk))
df_1331.sort_values(by='deliveryStartDate', ascending=True,inplace=True)
    #LPI
df_1259 = pd.DataFrame(getPrices(1259,start.strftime('%Y-%m-%d'),pDate_end.strftime('%Y-%m-%d'),token,ttk))
df_1259.sort_values(by='deliveryStartDate', ascending=True,inplace=True)
    #API
df_1422 = pd.DataFrame(getPrices(1422,start.strftime('%Y-%m-%d'),pDate_end.strftime('%Y-%m-%d'),token,ttk))
df_1422.sort_values(by='deliveryStartDate', ascending=True,inplace=True)
#--------------------------------------------------------------------------
for df in [i for i in [df_71,df_1154,df_1259,df_1331,df_1422] if len(i) != 0]:
    df.fillna('NA',inplace=True)
    df.deliveryStartDate = df.deliveryStartDate.map(lambda x: pd.to_datetime(x[:10]))
    df.deliveryEndDate = df.deliveryEndDate.map(lambda x: pd.to_datetime(x[:10]))
    df.publicationDate = df.publicationDate.map(lambda x: pd.to_datetime(x[:10]))
    df = df[df['deliveryStartDate'] >= pDate_start]   
#CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CH
if len(df_71) != 0:
    if False in (df_71['priceIndex'] == df_71['priceVWAP']).tolist():
        df_71 = df_71.drop((df_71[df_71['priceIndex'] != df_71['priceVWAP']] == 0).index)
        if False in (df_71['priceIndex'] == df_71['priceVWAP']).tolist():
            print(df_71[df_71['priceIndex'] != df_71['priceVWAP']][['priceIndex','priceVWAP']])
            raise Exception("ERROR: priceIndex and priceVWAP for curve 71 do not match")
if len(df_1154) != 0:
    if False in (df_1154['priceClose'] == df_1154['priceSettlement']).tolist():
        df_1154 = df_1154.drop((df_1154[df_1154['priceClose'] != df_1154['priceSettlement']] == 0).index)
        if False in (df_1154['priceClose'] == df_1154['priceSettlement']).tolist():
            print(df_1154[df_1154['priceClose'] != df_1154['priceSettlement']][['priceClose','priceSettlement']])
            raise Exception("ERROR: priceClose and priceSettlement for curve 1154 do not match")
#CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CH
if len(df_1331) != 0:
    mibgas_wd = df_1331[(df_1331['priceVWAP']!='NA')&(df_1331.periodicity=='DD')&(df_1331.periodicityRelative == 0)][['deliveryStartDate','priceVWAP']].copy()
    mibgas_wd.priceVWAP = mibgas_wd.priceVWAP.astype(float)
    mibgas_wd['Index'] = 'MIBGAS'

    mibgas_da = df_1331[(df_1331['priceVWAP']!='NA')&(df_1331.periodicity=='DD')&(df_1331.periodicityRelative == 1)][['deliveryStartDate','priceVWAP']].copy()
    mibgas_da.priceVWAP = mibgas_da.priceVWAP.astype(float)
    mibgas_da['Index'] = 'MIBGDA'
else:
    errors.append([1331,['mibgas_da','mibgas_wd']])
    mibgas_wd = mibgas_da = []
#--------------------------------------------------------------------------
if len(df_1154) != 0:
    mibgas_wd_auc = df_1154[(df_1154['priceClose']!='NA')&(df_1154.periodicity == 'DD')&(df_1154.periodicityRelative == 0)][['deliveryStartDate','priceClose']].copy()
    mibgas_wd_auc['Index'] = 'MIBAWD'

    mibgas_da_auc = df_1154[(df_1154['priceClose']!='NA')&(df_1154.periodicity == 'DD')&(df_1154.periodicityRelative == 1)][['deliveryStartDate','priceClose']].copy()
    mibgas_da_auc['Index'] = 'MIBADA'
else:
    errors.append([1154,['mibgas_da_auc','mibgas_wd_auc']])
    mibgas_wd_auc = mibgas_da_auc = []
#--------------------------------------------------------------------------
if len(df_71) != 0:
    mibgas_imbalance_sell = df_71[df_71['priceBid']!='NA'][['deliveryStartDate','priceBid']].copy()
    mibgas_imbalance_sell['Index'] = 'BSELL'

    mibgas_imbalance_buy = df_71[df_71['priceAsk']!='NA'][['deliveryStartDate','priceAsk']].copy()
    mibgas_imbalance_buy['Index'] = 'BALBUY'
else:
    errors.append([71,['mibgas_imbalance_sell','mibgas_imbalance_buy']])
    mibgas_imbalance_sell = mibgas_imbalance_buy = []
#--------------------------------------------------------------------------
if len(df_1259) != 0:
    mibgas_lpi = df_1259[df_1259['priceIndex']!='NA'][['deliveryStartDate','priceIndex']].copy()
    mibgas_lpi['Index'] = 'MIBLPI'
else:
    errors.append([1259,['mibgas_lpi']])
    mibgas_lpi = []
#--------------------------------------------------------------------------
if len(df_1422) != 0:
    mibgas_api = df_1422[df_1422['priceSettlement']!='NA'][['deliveryStartDate','priceSettlement']].copy()
    mibgas_api['Index'] = 'MIBAPI'
else:
    errors.append([1422,['mibgas_api']])
    mibgas_api = []

###############################################################################
#OMIE FIX OMIE FIX OMIE FIX OMIE FIX OMIE FIX OMIE FIX OMIE FIX OMIE FIX OMIE F
###############################################################################
    #OMIE
df_242 = pd.DataFrame(getPrices(242,start.strftime('%Y-%m-%d'),pDate_end.strftime('%Y-%m-%d'),token,ttk))
if len(df_242) != 0:
    df_242.fillna('NA',inplace=True)
    df_242.sort_values(by='periodicityAbsolute', ascending=True,inplace=True)
    df_242.deliveryEndDate = df_242.deliveryEndDate.map(lambda x: pd.to_datetime(x[:10]+' '+x[11:13]))
    df_242.deliveryStartDate = df_242.deliveryStartDate.map(lambda x: pd.to_datetime(x[:10]+' '+x[11:13]))
    df_242.publicationDate = df_242.publicationDate.map(lambda x: pd.to_datetime(x[:10]))
    df_242 = df_242[df_242['deliveryStartDate'] >= pDate_start]
    #----------------------------------------------------------------------
    if len(df_242) != 0:
        #CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK 
        if False in (df_242['priceClose'] == df_242['priceSettlement']).tolist():
            df_242 =  df_242 = df_242.drop((df_242[df_242['priceClose'] != df_242['priceSettlement']] == 0).index)
            if False in (df_242['priceClose'] == df_242['priceSettlement']).tolist():
                  raise Exception("ERROR: priceClose and priceSettlement for curve 242 do not match")
        #CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK 
        omie_fix = df_242[df_242['priceSettlement']!='NA'][['deliveryStartDate','priceSettlement']].copy()
        omie_fix['Index'] = 'OMIE'
    else:
        errors.append([242,['omie_fix']])
        omie_fix = []
    #----------------------------------------------------------------------
else:
    errors.append([242,['omie_fix']])
    omie_fix = []

###############################################################################
#PVB FIX PVB FIX PVB FIX PVB FIX PVB FIX PVB FIX PVB FIX PVB FIX PVB FIX PVB FI
###############################################################################
    #PVB
df_220 = pd.DataFrame(getPrices(220,start.strftime('%Y-%m-%d'),pDate_end.strftime('%Y-%m-%d'),token,ttk))
if len(df_220) != 0:
    df_220.fillna('NA',inplace=True)
    df_220.sort_values(by='deliveryStartDate', ascending=True,inplace=True)
    df_220.deliveryEndDate = df_220.deliveryEndDate.map(lambda x: pd.to_datetime(x[:10]))
    df_220.deliveryStartDate = df_220.deliveryStartDate.map(lambda x: pd.to_datetime(x[:10]))
    df_220.publicationDate = df_220.publicationDate.map(lambda x: pd.to_datetime(x[:10]))
    #----------------------------------------------------------------------
        #PVB DA  ->  PVBDAH  
    # pvb_da_fix = df_220[(df_220['deliveryStartDate'] >= pDate_start)&((df_220['periodicity']=='DA')|((df_220['periodicity']=='WE')))][['deliveryStartDate','deliveryEndDate','periodicity','priceMid']]
    aux = df_220[((df_220['deliveryStartDate'] >= pDate_start)|((df_220['deliveryStartDate'] < pDate_start)&(df_220['deliveryEndDate'] > pDate_start)))&((df_220['periodicity']=='DA')|(df_220['periodicity']=='WE'))].copy()
    if len(aux) != 0:
        aux_we = aux[(aux['periodicity']=='WE')&(aux['deliveryStartDate']-aux['publicationDate']==pd.Timedelta(days=1))]
        try:
            pvb_da_fix = pd.concat([aux[aux['periodicity']=='DA'],pd.DataFrame([[j,aux_we.loc[i]['priceMid']] for i in aux_we.index for j in pd.date_range(aux_we.loc[i]['deliveryStartDate'],aux_we.loc[i]['deliveryEndDate'],freq='d',inclusive='left')],columns=['deliveryStartDate','priceMid'])])
            pvb_da_fix = pvb_da_fix[['deliveryStartDate','priceMid']]
            pvb_da_fix['Index'] = 'PVBDAH'
            pvb_da_fix = pvb_da_fix[pvb_da_fix['deliveryStartDate'] >= pDate_start]
        except:
            errors.append([220,['pvb_da_fix']])
            pvb_da_fix = []
    else:
        errors.append([220,['pvb_da_fix']])
        pvb_da_fix = []
    #----------------------------------------------------------------------
else:
    errors.append([220,['pvb_da_fix']])
    pvb_da_fix = []

###############################################################################
#TTF FIX TTF FIX TTF FIX TTF FIX TTF FIX TTF FIX TTF FIX TTF FIX TTF FIX TTF FI
###############################################################################
    #TTF
df_234 = pd.DataFrame(getPrices(234,ma_start.strftime('%Y-%m-%d'),pDate_end.strftime('%Y-%m-%d'),token,ttk))
if len(df_234) != 0:
    df_234.fillna('NA',inplace=True)
    df_234.sort_values(by='deliveryStartDate', ascending=True,inplace=True)
    df_234.deliveryEndDate = df_234.deliveryEndDate.map(lambda x: pd.to_datetime(x[:10]))
    df_234.deliveryStartDate = df_234.deliveryStartDate.map(lambda x: pd.to_datetime(x[:10]))
    df_234.publicationDate = df_234.publicationDate.map(lambda x: pd.to_datetime(x[:10]))
    #----------------------------------------------------------------------
        #TTF DA  ->  TTFD    
    taux = df_234[((df_234['deliveryStartDate'] >= pDate_start)|((df_234['deliveryStartDate'] < pDate_start)&(df_234['deliveryEndDate'] > pDate_start)))&((df_234['periodicity']=='DA')|(df_234['periodicity']=='WE'))].copy()
    if len(taux) != 0:
        taux_we = taux[(taux['periodicity']=='WE')&(taux['deliveryStartDate']-taux['publicationDate']==pd.Timedelta(days=1))]
        try:
            ttf_da_fix = pd.concat([taux[taux['periodicity']=='DA'],pd.DataFrame([[j,taux_we.loc[i]['priceMid']] for i in taux_we.index for j in pd.date_range(taux_we.loc[i]['deliveryStartDate'],taux_we.loc[i]['deliveryEndDate'],freq='d',inclusive='left')],columns=['deliveryStartDate','priceMid'])])
            ttf_da_fix = ttf_da_fix[['deliveryStartDate','priceMid']]
            ttf_da_fix['Index'] = 'TTFD'
            ttf_da_fix = ttf_da_fix[ttf_da_fix['deliveryStartDate'] >= pDate_start]
        except:
            errors.append([234,['ttf_da_fix']])
            ttf_da_fix = []
    else:
        errors.append([234,['ttf_da_fix']])
        ttf_da_fix = []
    #----------------------------------------------------------------------      
        #TTF MA  ->  TTFM & TTFMH
    ddate = min(pDate_start,ma_end)
    ttf_mm = df_234[(df_234['periodicity']=='MM')&(df_234['periodicityRelative']==1)&(df_234['deliveryStartDate']>=ddate)]#&(df_234['deliveryStartDate']<=monthNext)
    if len(ttf_mm) != 0:
        ttf_ma_fix = ttf_mm[['publicationDate','priceMid']].copy()
        ttf_ma_fix['Index'] = 'TTFMH'
        # aux_ttf_fix=[]
        try:
            ttf_ma_synth_fix = ttf_mm[ttf_mm.deliveryStartDate < max(ttf_mm.deliveryStartDate)].groupby('deliveryStartDate')['priceMid'].mean()#.reset_index()
            aux_ma_ttfsynth = ttf_mm[ttf_mm.deliveryStartDate == max(ttf_mm.deliveryStartDate)]
            synthDDate = max(ttf_mm.deliveryStartDate)
            synthPDate = max(aux_ma_ttfsynth.publicationDate)
        except:
            errors.append([234,['ttf_ma_fix']])
            ttf_ma_fix = ttf_ma_synth_fix = []
    else:
        errors.append([234,['ttf_ma_fix']])
        ttf_ma_fix = ttf_ma_synth_fix = []
    #----------------------------------------------------------------------
else:
    errors.append([234,['ttf_da_fix','ttf_ma_fix']])
    ttf_da_fix = ttf_ma_fix = ttf_ma_synth_fix = []

###############################################################################
#BRENT 603 BRENT 603 BRENT 603 BRENT 603 BRENT 603 BRENT 603 BRENT 603 BRENT 60
###############################################################################
    #DT BRENT
df_313 = pd.DataFrame(getPrices(313,start.strftime('%Y-%m-%d'),pDate_end.strftime('%Y-%m-%d'),token,ttk))
if len(df_313) != 0:
    df_313.fillna('NA',inplace=True)
    df_313.sort_values(by='deliveryStartDate', ascending=True,inplace=True)
    df_313.deliveryEndDate = df_313.deliveryEndDate.map(lambda x: pd.to_datetime(x[:10]))
    df_313.deliveryStartDate = df_313.deliveryStartDate.map(lambda x: pd.to_datetime(x[:10]))
    df_313.publicationDate = df_313.publicationDate.map(lambda x: pd.to_datetime(x[:10]))
    #----------------------------------------------------------------------
    brent = df_313[(df_313['priceClose']!='NA')&(df_313['deliveryStartDate']>=start)][['deliveryStartDate','priceClose']].copy()
    if len(brent) != 0:
        brent['Index'] = 'BRENT'
    else:
        errors.append([313,['brent']])
        brent = []
    #----------------------------------------------------------------------
else:
    errors.append([313,['brent']])
    brent = []

###############################################################################
#FX 303 FX 303 FX 303 FX 303 FX 303 FX 303 FX 303 FX 303 FX 303 FX 303 FX 303 F
###############################################################################
    #EUR/USD FX
df_268 = pd.DataFrame(getPrices(268,start.strftime('%Y-%m-%d'),pDate_end.strftime('%Y-%m-%d'),token,ttk))
if len(df_268) != 0:
    df_268.fillna('NA',inplace=True)
    df_268.sort_values(by='periodicityAbsolute', ascending=True,inplace=True)
    df_268.deliveryEndDate = df_268.deliveryEndDate.map(lambda x: pd.to_datetime(x[:10]))
    df_268.deliveryStartDate = df_268.deliveryStartDate.map(lambda x: pd.to_datetime(x[:10]))
    df_268.publicationDate = df_268.publicationDate.map(lambda x: pd.to_datetime(x[:10]))
    #CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHEC
    if False in (df_268['priceClose'] == df_268['priceSettlement']).tolist():
        df_268 =  df_268 = df_268.drop((df_268[df_268['priceClose'] != df_268['priceSettlement']] == 0).index)
        if False in (df_268['priceClose'] == df_268['priceSettlement']).tolist():
            raise Exception("ERROR: priceClose and priceSettlement for curve 268 do not match")
    #CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHEC
    df_268 = df_268[(df_268['priceSettlement']!='NA')&(df_268['deliveryStartDate']>=start)][cols+['priceSettlement']].copy()
    if len(df_268) != 0:
        tc = df_268[['deliveryStartDate','priceSettlement']].copy()
        tc['Index']='CAMB'
    else:
        errors.append([268,['tc']])
        tc = []
    #----------------------------------------------------------------------
else:
    errors.append([268,['tc']])
    tc = []

###############################################################################
#HENRY HUB HENRY HUB HENRY HUB HENRY HUB HENRY HUB HENRY HUB HENRY HUB HENRY HU
###############################################################################
    #HH FIX & HH FWD
df_67 =  pd.DataFrame(getPrices(67,start.strftime('%Y-%m-%d'),pDate_end.strftime('%Y-%m-%d'),token,ttk))
if len(df_67) != 0:
    df_67.sort_values(by='periodicityAbsolute', ascending=True,inplace=True)
    df_67.deliveryEndDate = df_67.deliveryEndDate.map(lambda x: pd.to_datetime(x[:10]))
    df_67.deliveryStartDate = df_67.deliveryStartDate.map(lambda x: pd.to_datetime(x[:10]))
    df_67.publicationDate = df_67.publicationDate.map(lambda x: pd.to_datetime(x[:10]))
    df_67.periodicityRelative = df_67.apply(lambda x: len(pd.date_range(x.publicationDate.replace(day=1),x.deliveryStartDate,freq='MS',inclusive='both'))-1,axis=1)
    #----------------------------------------------------------------------
    df_67 = df_67[df_67.periodicityRelative >= 1]
    if len(df_67) != 0:
        hhFix = df_67[df_67.periodicityRelative == 1][['publicationDate','priceSettlement']].copy()
        hhFix['curveId'] = 'HH'
            #HH SYNTH
        hh_synth_fix = []
        if any(i for i in set(df_67.publicationDate) if i.day == i.days_in_month):
            [hh_synth_fix.append(t[1][t[1].publicationDate == max(t[1].publicationDate)]) for t in df_67[(df_67.periodicityRelative == 1)&(df_67.deliveryStartDate<monthNext)].groupby('deliveryStartDate')]
            try:
                hh_synth_fix = pd.concat(hh_synth_fix)
                hh_synth_fix = hh_synth_fix[['deliveryStartDate','priceSettlement']].loc[hh_synth_fix[hh_synth_fix['priceSettlement'].notnull()].index]
                hh_synth_fix['curveId'] = 'HHFIX'
            except:
                errors.append([67,['hh_synth_fix','hh_fwd']])
                hh_synth_fix = hh_fwd = []
            #HH FWD
        df_67 = df_67[df_67['publicationDate'] == max(df_67['publicationDate'])]
        hh_fwd = df_67.loc[df_67[df_67['priceSettlement'].notnull()].index][cols+['priceSettlement']].copy()
        hh_fwd['curveId'] = 'HH'
    else:
        errors.append([67,['hh_synth_fix','hh_fwd']])
        hh_synth_fix = hh_fwd = []
    #----------------------------------------------------------------------
else:
    errors.append([67,['hh_synth_fix','hh_fwd']])
    hh_synth_fix = hh_fwd = []

###############################################################################
#TTF FORWARD TTF FORWARD TTF FORWARD TTF FORWARD TTF FORWARD TTF FORWARD TTF FO
###############################################################################
    #TTF MA FWD
df_1116 = pd.DataFrame(getPrices(1116,min(end,synthPDate).strftime('%Y-%m-%d'),pDate_end.strftime('%Y-%m-%d'),token,ttk))

    #TTF DA FWD
df_217 = pd.DataFrame(getPrices(217,end.strftime('%Y-%m-%d'),pDate_end.strftime('%Y-%m-%d'),token,ttk))
#--------------------------------------------------------------------------
aux_ma_ttffwd = df_1116[(df_1116.publicationDate.map(lambda x: pd.to_datetime(x[:10])) == synthPDate)&(df_1116.deliveryStartDate.map(lambda x: pd.to_datetime(x[:10])) == max(aux_ma_ttfsynth.deliveryStartDate))]
if False in (aux_ma_ttffwd['priceClose'] == aux_ma_ttffwd['priceSettlement']).tolist():
    raise Exception("ERROR: priceClose and priceSettlement for curve 1116 do not match")
#--------------------------------------------------------------------------
for df in [i for i in [df_217,df_1116] if len(i) != 0]:
    df.fillna('NA',inplace=True)
    df.sort_values(by='periodicityAbsolute', ascending=True,inplace=True)
    df.deliveryEndDate = df.deliveryEndDate.map(lambda x: pd.to_datetime(x[:10]))
    df.deliveryStartDate = df.deliveryStartDate.map(lambda x: pd.to_datetime(x[:10]))
    df.publicationDate = df.publicationDate.map(lambda x: pd.to_datetime(x[:10]))
    df = df[df['publicationDate'] == max(df['publicationDate'])]
#CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHEC
if len(df_1116) != 0:
    if False in (df_1116['priceClose'] == df_1116['priceSettlement']).tolist():
        df_1116 =  df_1116 = df_1116.drop((df_1116[df_1116['priceClose'] != df_1116['priceSettlement']] == 0).index)
        if False in (df_1116['priceClose'] == df_1116['priceSettlement']).tolist():
            raise Exception("ERROR: priceClose and priceSettlement for curve 1116 do not match")
#CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHECK CHEC
if len(df_1116) != 0:
    ttf_mm_fwd = df_1116[df_1116['priceSettlement']!='NA'][cols+['priceSettlement']].copy()
    ttf_mm_fwd['curveId'] = 'TTF'
else:
    errors.append([1116,['ttf_mm_fwd']])
    ttf_mm_fwd = []
#--------------------------------------------------------------------------
if len(df_217) != 0:
    ttf_dd_fwd = df_217[df_217['priceClose']!='NA'][cols+['priceClose']].copy()
    ttf_dd_fwd['curveId'] = 'TTF'
else:
    errors.append([217,['ttf_dd_fwd']])
    ttf_dd_fwd = []
#--------------------------------------------------------------------------
    #CHEEKY LITTLE TANGENT
try:
    if len(aux_ma_ttfsynth) != 0 and len(aux_ma_ttffwd) != 0:
        ex = aux_ma_ttffwd.priceSettlement.iloc[0]
        tmln = pd.date_range(synthPDate,synthPDate.replace(day=synthPDate.days_in_month),freq='d',inclusive='right')
        days = [i for i in tmln if i.day_of_week <= 4 and i not in holidays.GB() and i > synthPDate]
        n = len(aux_ma_ttfsynth) + len(days)
        mean = ((aux_ma_ttfsynth['priceMid'].sum())/n)+((ex*len(days))/n)
        ttf_ma_synth_fix.loc[synthDDate] = mean
        ttf_ma_synth_fix = pd.DataFrame(ttf_ma_synth_fix.reset_index())
        ttf_ma_synth_fix['curveId'] = 'TTFM'
    else:
        pass
except:
    pass
   
###############################################################################
#PVB FORWARD PVB FORWARD PVB FORWARD PVB FORWARD PVB FORWARD PVB FORWARD PVB FO
###############################################################################
    #PVB DA FWD
df_216 = pd.DataFrame(getPrices(216,end.strftime('%Y-%m-%d'),pDate_end.strftime('%Y-%m-%d'),token,ttk))
    #PVB MA FWD
df_548 = pd.DataFrame(getPrices(548,end.strftime('%Y-%m-%d'),pDate_end.strftime('%Y-%m-%d'),token,ttk))
#--------------------------------------------------------------------------
for df in [i for i in [df_216,df_548] if len(i) != 0]:
    df.fillna('NA',inplace=True)
    df.sort_values(by='deliveryStartDate', ascending=True,inplace=True)
    df.deliveryEndDate = df.deliveryEndDate.map(lambda x: pd.to_datetime(x[:10]))
    df.deliveryStartDate = df.deliveryStartDate.map(lambda x: pd.to_datetime(x[:10]))
    df.publicationDate = df.publicationDate.map(lambda x: pd.to_datetime(x[:10]))
    df = df[df['publicationDate'] == max(df['publicationDate'])]
#--------------------------------------------------------------------------   
if len(df_548) != 0:
    pvb_mm_fwd = df_548[df_548['priceSettlement']!='NA'][cols+['priceSettlement']].copy()
    pvb_mm_fwd['curveId'] = 'PVB'
else:
    errors.append([548,['pvb_mm_fwd']])
#--------------------------------------------------------------------------   
    pvb_mm_fwd = []
if len(df_216) != 0:
    pvb_dd_fwd = df_216[df_216['priceClose']!='NA'][cols+['priceClose']].copy()
    pvb_dd_fwd['curveId'] = 'PVB'
else:
    errors.append([216,['pvb_dd_fwd']])
    pvb_dd_fwd = []

###############################################################################
#FX FORWARD FX FORWARD FX FORWARD FX FORWARD FX FORWARD FX FORWARD FX FORWARD F
###############################################################################
df_644 = pd.DataFrame(getPrices(644,end.strftime('%Y-%m-%d'),pDate_end.strftime('%Y-%m-%d'),token,ttk))
if len(df_644) != 0:
    df_644.fillna('NA',inplace=True)
    df_644.sort_values(by='periodicityAbsolute', ascending=True,inplace=True)
    df_644.deliveryEndDate = df_644.deliveryEndDate.map(lambda x: pd.to_datetime(x[:10])+ pd.DateOffset(days=1))
    df_644.deliveryStartDate = df_644.deliveryStartDate.map(lambda x: pd.to_datetime(x[:10]))
    df_644.publicationDate = df_644.publicationDate.map(lambda x: pd.to_datetime(x[:10]))
    df_644.periodicityRelative = df_644.apply(lambda x: len(pd.date_range(x.publicationDate,x.deliveryStartDate,freq='d',inclusive='both'))-1,axis=1)
    df_644 = df_644[df_644['publicationDate'] == max(df_644['publicationDate'])]
    #----------------------------------------------------------------------
    fx_fwd = df_644[df_644['priceClose']!='NA'][cols+['priceClose']].copy()
    fx_fwd['curveId'] = 'CAMB'
else:
    errors.append([644,['fx_fwd']])
    fx_fwd = []

###############################################################################
#BRENT FORWARD BRENT FORWARD BRENT FORWARD BRENT FORWARD BRENT FORWARD BRENT FO
###############################################################################
df_1330 = pd.DataFrame(getPrices(1330,end.strftime('%Y-%m-%d'),pDate_end.strftime('%Y-%m-%d'),token,ttk))
if len(df_1330) != 0:
    df_1330.fillna('NA',inplace=True)
    df_1330.sort_values(by='periodicityAbsolute', ascending=True,inplace=True)
    df_1330.deliveryEndDate = df_1330.deliveryEndDate.map(lambda x: pd.to_datetime(x[:10]))
    df_1330.deliveryStartDate = df_1330.deliveryStartDate.map(lambda x: pd.to_datetime(x[:10]))
    df_1330.publicationDate = df_1330.publicationDate.map(lambda x: pd.to_datetime(x[:10]))
    df_1330.periodicityRelative = df_1330.apply(lambda x: len(pd.date_range(x.publicationDate.replace(day=1),x.deliveryStartDate,freq='MS',inclusive='both'))-1,axis=1)
    df_1330 = df_1330[df_1330['publicationDate'] == max(df_1330['publicationDate'])]
    #----------------------------------------------------------------------
    brent_fwd = df_1330[df_1330['priceClose']!='NA'][cols+['priceClose']].copy()
    brent_fwd['curveId'] = 'BRENT'
else:
    errors.append([1330,['brent_fwd']])
    brent_fwd = []

###############################################################################
#OMIE FORWARD OMIE FORWARD OMIE FORWARD OMIE FORWARD OMIE FORWARD OMIE FORWARD 
###############################################################################
df_188 = pd.DataFrame(getPrices(188,end.strftime('%Y-%m-%d'),pDate_end.strftime('%Y-%m-%d'),token,ttk))
if len(df_188) != 0:
    df_188.fillna('NA',inplace=True)
    df_188.sort_values(by='periodicityAbsolute', ascending=True,inplace=True)    
    df_188 = df_188[(df_188['priceClose']!='NA')&(df_188['publicationDate'] == max(df_188['publicationDate']))]
    df_188.deliveryEndDate = df_188.deliveryEndDate.map(lambda x: pd.to_datetime(x[:10]+' '+x[11:13]))
    df_188.deliveryStartDate = df_188.deliveryStartDate.map(lambda x: pd.to_datetime(x[:10]+' '+x[11:13]))
    df_188.publicationDate = df_188.publicationDate.map(lambda x: pd.to_datetime(x[:10]))
    #----------------------------------------------------------------------
        #HOURLY
    omie_hh_fwd = df_188[df_188['priceClose']!='NA'][cols+['priceClose']].copy()
    omie_hh_fwd.periodicityRelative = omie_hh_fwd.index +1
    omie_hh_fwd['curveId'] = 'OMIE'
    #---------------------------------------------------------------------- 
        #DAILY
    aux = df_188.groupby([df_188['deliveryStartDate'].dt.year,df_188['deliveryStartDate'].dt.month,df_188['deliveryStartDate'].dt.day])['priceClose'].mean()
    omie_dd_fwd = pd.DataFrame(aux.values, columns=['priceClose'])
    omie_dd_fwd['deliveryStartDate'] = pd.to_datetime(aux.index,format='(%Y, %m, %d)')
    omie_dd_fwd['deliveryEndDate'] = omie_dd_fwd.deliveryStartDate.map(lambda x: x + pd.DateOffset(days=1))
    omie_dd_fwd['periodicity'] = 'DD'
    #----------------------------------------------------------------------
        #MONTHLY
    aux = df_188.groupby([df_188['deliveryStartDate'].dt.year,df_188['deliveryStartDate'].dt.month])['priceClose'].mean()
    omie_mm_fwd = pd.DataFrame(aux.values, columns=['priceClose'])
    omie_mm_fwd['deliveryStartDate'] = pd.to_datetime(aux.index,format='(%Y, %m)')
    omie_mm_fwd['deliveryEndDate'] = omie_mm_fwd.deliveryStartDate.map(lambda x: x.replace(day=calendar.monthrange(x.year,x.month)[1]) + pd.DateOffset(days=1))
    omie_mm_fwd['periodicity'] = 'MM'
    #----------------------------------------------------------------------
    try:
        for i in set(df_188['publicationDate']):
            if i.day == 31 or ((i.day == 30)&(i.daysinmonth == 30)):
                continue
            else:
                ti = (omie_mm_fwd[omie_mm_fwd.deliveryStartDate == i.replace(day=1)]).index
                omie_mm_fwd.at[ti[0],'deliveryStartDate'] = i + pd.DateOffset(days=1)
                omie_mm_fwd.at[ti[0],'periodicity'] = 'BM'
    except:
        errors.append([188,['omie_hh_fwd','omie_dd_fwd','omie_mm_fwd']])
        omie_dd_fwd = omie_hh_fwd = omie_mm_fwd = []
    #----------------------------------------------------------------------
    for df in [qq for qq in [[omie_dd_fwd,'d','%Y%m%d'],[omie_mm_fwd,'m','%Y%m']] if len(qq[0]) != 0]:
        df[0]['publicationDate'] = max(df_188['publicationDate'])
        df[0]['curveId'] = 'OMIE'
        df[0]['periodicityAbsolute'] = (df[0].deliveryStartDate.dt.strftime(df[2])).astype(int)
        df[0]['periodicityRelative'] = df[0].apply(lambda x: len(pd.date_range(x.publicationDate,x.deliveryStartDate,freq=df[1],inclusive='left')),axis=1)
    #----------------------------------------------------------------------
    try:
        omie_dd_fwd = omie_dd_fwd[omie_dd_fwd['priceClose']!='NA'][cols+['priceClose']].copy()
    except:
        errors.append([188,['omie_dd_fwd']])
        omie_dd_fwd = []
    try:
        omie_mm_fwd = omie_mm_fwd[omie_mm_fwd['priceClose']!='NA'][cols+['priceClose']].copy()
    except:
        errors.append([188,['omie_mm_fwd']])
        omie_mm_fwd = []
else:
    errors.append([188,['omie_hh_fwd','omie_dd_fwd','omie_mm_fwd']])
    omie_dd_fwd = omie_hh_fwd = omie_mm_fwd = []

###############################################################################
#%% SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL SQL
###############################################################################
fix_dfs = [mibgas_da,mibgas_da_auc,mibgas_imbalance_buy,mibgas_imbalance_sell,mibgas_lpi,mibgas_api,mibgas_wd,mibgas_wd_auc,omie_fix,pvb_da_fix,ttf_da_fix,ttf_ma_fix,tc,brent,hhFix]
#15 fix
forw_dfs = [pvb_dd_fwd,pvb_mm_fwd,ttf_mm_fwd,ttf_dd_fwd,fx_fwd,omie_hh_fwd,omie_dd_fwd,omie_mm_fwd,brent_fwd,hh_fwd]
#10 fwd 
synth_dfs = [ttf_ma_synth_fix,hh_synth_fix]
#--------------------------------------------------------------------------
fixings = [i for i in fix_dfs if len(i) != 0]
forwards = [i for i in forw_dfs if len(i) != 0]
synths = [i for i in synth_dfs if len(i) != 0]
#--------------------------------------------------------------------------
sqlcols_fwd = ['CVF_IDM_CODIGO','CVF_FECPUBLIC','CVF_FECSTART','CVF_FECEND','CVF_PERIOD','CVF_PERABS','CVF_PERREL','CVF_PRICE']
sqlcols_fix = ['FXG_FECHA','FXG_PRICE','FXG_IDM_CODIGO']
sqlcols_synth = ['STF_FECHA','STF_PRICE','STF_IDM_CODIGO']
#--------------------------------------------------------------------------
for curve in fixings:
    curve.columns = sqlcols_fix
for curve in forwards:
    curve.columns = sqlcols_fwd
for curve in synths:
    curve.columns = sqlcols_synth
#--------------------------------------------------------------------------
fix = pd.concat(fixings,ignore_index=True)
fix.sort_values(by=['FXG_FECHA','FXG_IDM_CODIGO'],inplace=True)
fix['FXG_PRICE'] = (fix['FXG_PRICE'].astype(float)).round(decimals=4)
fix = fix.drop_duplicates()
#EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EX
for i in set(fix['FXG_IDM_CODIGO']):
    # date = min(fix[fix['FXG_IDM_CODIGO'] == i]['FXG_FECHA'])
    for date in fix[fix['FXG_IDM_CODIGO'] == i]['FXG_FECHA']:
        deleteQuery = f'''DELETE FROM [METDB].[MET_FIXINGS] WHERE [FXG_IDM_CODIGO] = '{i}' AND [FXG_FECHA] = '{date}' '''
        with engine.begin() as conn:
            print(f'DELETING FIX value with date {date} for {i} from db. DO NOT INTERUPT THIS PROCESS')
            conn.execute(text(deleteQuery))
            # fRes.write(f'Deleted FIX values with date later than {date} for {i} from db.')
#EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EX
print('Uploading Fixings Data to db. Do Not Interupt')
fix.to_sql('MET_FIXINGS',engine,schema='METDB',if_exists=withTable,index=False)

if file:
    fRes.write(f'Uploaded FIX values from date {min(fix[fix["FXG_IDM_CODIGO"] == i]["FXG_FECHA"])} to db.')
#--------------------------------------------------------------------------
synth = pd.concat(synths,ignore_index=True)
synth.sort_values(by=['STF_FECHA','STF_IDM_CODIGO'],inplace=True)
synth['STF_PRICE'] = (synth['STF_PRICE'].astype(float)).round(decimals=4)
synth = synth.drop_duplicates()
#EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EX
for i in set(synth['STF_IDM_CODIGO']):
    # date = min(synth[synth['STF_IDM_CODIGO'] == i]['STF_FECHA'])
    for date in synth[synth['STF_IDM_CODIGO'] == i]['STF_FECHA']:
        deleteQuery = f'''DELETE FROM [METDB].[MET_SYNTHFIX] WHERE [STF_IDM_CODIGO] = '{i}' AND [STF_FECHA] = '{date}' '''
        with engine.begin() as conn:
            print(f'DELETING SYNTHFIX value with date {date} for {i} from db. DO NOT INTERUPT THIS PROCESS')
            conn.execute(text(deleteQuery))
            # fRes.write(f'Deleted FIX values with date later than {date} for {i} from db.')
#EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EX
print('Uploading Fixings Data to db. Do Not Interupt')
synth.to_sql('MET_SYNTHFIX',engine,schema='METDB',if_exists=withTable,index=False)

if file:
    fRes.write(f'Uploaded SYNTH FIX values from date {min(synth[synth["STF_IDM_CODIGO"] == i]["STF_FECHA"])} to db')
#--------------------------------------------------------------------------
forwd = pd.concat(forwards)
forwd.sort_values(by=['CVF_IDM_CODIGO','CVF_FECPUBLIC'],inplace=True)
forwd['CVF_PRICE'] = (forwd['CVF_PRICE'].astype(float)).round(decimals=4)
# forwd = forwd.drop_duplicates()
#EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EX
for i in set(forwd['CVF_IDM_CODIGO']):
    for j in set(forwd[(forwd['CVF_IDM_CODIGO'] == i)]['CVF_PERIOD']):
        date = min(forwd[(forwd['CVF_IDM_CODIGO'] == i)&(forwd['CVF_PERIOD'] == j)]['CVF_FECPUBLIC'])
        deleteQuery = f'''DELETE FROM [METDB].[MET_CURVFORW] WHERE [CVF_IDM_CODIGO] = '{i}' AND [CVF_PERIOD] = '{j}' AND [CVF_FECPUBLIC] >= '{date}' '''
        with engine.begin() as conn:
            print(f'DELETING FORW values with date later than {date} for {i} from db. DO NOT INTERUPT THIS PROCESS')
            conn.execute(text(deleteQuery))
            # fRes.write(f'Deleted FORW values with date later than {date} for {i} from db.')
#EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EXT EX
print('Uploading Forwards Data to db. Do Not Interupt')
forwd.to_sql('MET_CURVFORW',engine,schema='METDB',if_exists=withTable,index=False)

if file:
    fRes.write(f'Uploaded FORW values from date {date} for {i} to db.')
#--------------------------------------------------------------------------
if file:
    if len(errors) != 0:
        for err in errors:
            fRes.write("\nLa curva numero "+str(err[0])+f" se ha descargado vacía o se ha quedado vacía después de filtrarla. No se pueden sacar los valores de {err[1]}")
            fRes.close()
        else:
            fRes.close()
    
#%%    
#     return fix,forwd
# try:
#     fixed_df,forward_df= getAllPrices(pDate_start, pDate_end, errors)
#     fRes.close()
# except:
#     for err in errors:
#         fRes.write("\nLa curva numero "+str(err[0])+f" se ha descargado vacía. No se pueden sacar los valores de {err[1]}")
#     fRes.write("\nHa ocurrido algún problema. El script no se ha ejecutado.")

#%% REJECTED CODE

#f_313.replace('NA',np.nan,inplace=True)
#603
# f_313 = df_313[(df_313['priceClose']!='NA')&(df_313['deliveryStartDate']>=originals['start603'])&(df_313['deliveryEndDate']<=originals['qstart'])][['deliveryStartDate','priceClose']].copy()
# b603 = (f_313.groupby(f_313['deliveryStartDate'].dt.month)['priceClose'].mean()).sum()/6
# brent_603 = pd.DataFrame(np.array([[pDate_end],[b603],['BRENT 603']]).T,columns=['Date','Price','Index'])
#303
# f_268 = df_268[(df_268['deliveryStartDate']>=originals['start303'])&(df_268['deliveryEndDate']<=originals['qstart'])]     
# f_268_303 = (f_268.groupby(f_268['deliveryStartDate'].dt.month)['priceSettlement'].mean()).sum()/3
# tc_303 = pd.DataFrame(np.array([[pDate_end],[f_268_303],['FX 303']]).T,columns=['Date','Price','Index'])

#omie_fix['deliveryStartDate'] = omie_fix['deliveryStartDate'].dt.strftime('%Y-%m-%d %H:%M')

# d = datetime.today()

 # omie_fix = df_242.groupby([df_242.deliveryStartDate.dt.year,df_242.deliveryStartDate.dt.month,df_242.deliveryStartDate.dt.day])['priceSettlement'].mean()
    # omie_fix = pd.DataFrame(omie_fix)
    # omie_fix['Date'] = omie_fix.index.map(lambda x: pd.to_datetime(str(x[1])+'/'+str(x[2])+'/'+str(x[0]))).values
    # omie_fix.index = [int(i) for i in np.linspace(0, len(omie_fix),len(omie_fix),endpoint=False)]4['periodicity']=='GY')&(df_234['periodicityRelative']==1)][cols + ['priceAsk']]

# df_207 = pd.DataFrame(getPrices(207,useday.strftime('%Y-%m-%d'),d.strftime('%Y-%m-%d'),token))
# df_207.fillna('NA',inplace=True)
# df_207.sort_values(by='periodicityAbsolute', ascending=True,inplace=True)
# df_207.deliveryEndDate = df_207.deliveryEndDate.map(lambda x: pd.to_datetime(x[:10]).strftime('%Y-%m-%d'))
# df_207.deliveryStartDate = df_207.deliveryStartDate.map(lambda x: pd.to_datetime(x[:10]).strftime('%Y-%m-%d'))
# df_207.publicationDate = df_207.publicationDate.map(lambda x: pd.to_datetime(x[:10]).strftime('%Y-%m-%d'))
# f_207 = df_207[cols+['priceClose']].copy()

# fix_code = {71:['MIBGAS Imbal Sell','MIBGAS Imbal Buy'],
#         1154:['MIBGAS AUC WD'],
#         1259:['MIBGAS LPI'],
#         268:['EUR_USD'],
#         313:['DTD BRENT'],
#         242:['OMIE'],
#         220:['PVB DA','PVB WE','PVB MA','PVB QA','PVB YA'],
#         234:['TTF DA','TTF WE','TTF NW','TTF BM','TTF MA','TTF QA','TTF SU',
#              'TTF WI','TTF YA','TTF GY'],
#         '303':['EUR_USD 303','BRENT DTD 303'],
#         '603':['BRENT DTD 603']}

# forw_code = {548:['PVB BM','PVB MA'],
#              216:['PVB DA'],
#              1116:['TTF BM','TTF MA'],
#              217:['TTF DA'],
#              644:['EUR_USD']}

# fixings = {'MIBGAS Imbal Sell': sql71_ask,
#            'MIBGAS Imbal Buy' : sql71_bid,
#            'MIBGAS AUC WD': sql1154,
#            'MIBGAS LPI': sql1259,
#            'EUR_USD': sql268,
#            'BRENT DATED': sql313,
#            'OMIE': sql242,
#            'TTF DA Buy',
#            'TTF WE Buy',
#            'TTF NW Buy',
#            'TTF BM Buy',
#            'TTF MA',
#            'TTF QA',
#            'TTF SU',
#            'TTF WI',
#            'TTF YA',
#            'TTF GY'}

# for df in fixings:
#     i = df.columns[-1]
#     df.columns = sqlcols_fix + [i]

# for df in forwards:
#     df.columns = sql_cols_fwd

# for curv in fix_code.values():
#     for prod in curv:
#         fixings[curv] = 
    
# #%% CURVES 220 & 234 : PVB & TTF FIXINGS

# if (time.time()-t) >= token.json()['expires_in']: token, t = getToken()

# df_220,df_234 = curvas()
# df_220.deliveryEndDate.map(lambda x: pd.to_datetime(x[:10]))
# df_220.deliveryStartDate = df_220.deliveryStartDate.map(lambda x: pd.to_datetime(x[:10]))
# df_220.publicationDate = df_220.publicationDate.map(lambda x: pd.to_datetime(x[:10]))
# df_234.deliveryEndDate = df_234.deliveryEndDate.map(lambda x: pd.to_datetime(x[:10]))
# df_234.deliveryStartDate = df_234.deliveryStartDate.map(lambda x: pd.to_datetime(x[:10]))
# df_234.publicationDate = df_234.publicationDate.map(lambda x: pd.to_datetime(x[:10]))

# # PVB PRODUCTS
# f220_da_bid = df_220[df_220['periodicity']=='DA'][cols + ['priceBid']]
# f220_da_ask = df_220[df_220['periodicity']=='DA'][cols + ['priceAsk']]
# f220_we_bid = df_220[df_220['periodicity']=='WE'][cols + ['priceBid']]
# f220_we_ask = df_220[df_220['periodicity']=='WE'][cols + ['priceAsk']]
# f220_ma_bid = df_220[(df_220['periodicity']=='MM')&(df_220['periodicityRelative']==1)][cols + ['priceBid']]
# f220_ma_ask = df_220[(df_220['periodicity']=='MM')&(df_220['periodicityRelative']==1)][cols + ['priceAsk']]
# f220_qa_bid = df_220[(df_220['periodicity']=='QQ')&(df_220['periodicityRelative']==1)][cols + ['priceBid']]
# f220_qa_ask = df_220[(df_220['periodicity']=='QQ')&(df_220['periodicityRelative']==1)][cols + ['priceAsk']]
# f220_ya_bid = df_220[(df_220['periodicity']=='YY')&(df_220['periodicityRelative']==1)][cols + ['priceBid']]
# f220_ya_ask = df_220[(df_220['periodicity']=='YY')&(df_220['periodicityRelative']==1)][cols + ['priceAsk']]

# # TTF PRODUCTS
# f234_da_bid = df_234[df_234['periodicity']=='DA'][cols + ['priceBid']]
# f234_da_ask = df_234[df_234['periodicity']=='DA'][cols + ['priceAsk']]
# f234_we_bid = df_234[df_234['periodicity']=='WE'][cols + ['priceBid']]
# f234_we_ask = df_234[df_234['periodicity']=='WE'][cols + ['priceAsk']]
# f234_bm_bid = df_234[df_234['periodicity']=='BM'][cols + ['priceBid']]
# f234_bm_ask = df_234[df_234['periodicity']=='BM'][cols + ['priceAsk']]
# f234_ma_bid = df_234[(df_234['periodicity']=='MM')&(df_234['periodicityRelative']==1)][cols + ['priceBid']]
# f234_ma_ask = df_234[(df_234['periodicity']=='MM')&(df_234['periodicityRelative']==1)][cols + ['priceAsk']]
# f234_qa_bid = df_234[(df_234['periodicity']=='QQ')&(df_234['periodicityRelative']==1)][cols + ['priceBid']]
# f234_qa_ask = df_234[(df_234['periodicity']=='QQ')&(df_234['periodicityRelative']==1)][cols + ['priceAsk']]
# f234_ya_bid = df_234[(df_234['periodicity']=='YY')&(df_234['periodicityRelative']==1)][cols + ['priceBid']]
# f234_ya_ask = df_234[(df_234['periodicity']=='YY')&(df_234['periodicityRelative']==1)][cols + ['priceAsk']]
# f234_nw_bid = df_234[(df_234['periodicity']=='NW')&(df_234['periodicityRelative']==1)][cols + ['priceBid']]
# f234_nw_ask = df_234[(df_234['periodicity']=='NW')&(df_234['periodicityRelative']==1)][cols + ['priceAsk']]
# f234_su_bid = df_234[(df_234['periodicity']=='SU')&(df_234['periodicityRelative']==1)][cols + ['priceBid']]
# f234_su_ask = df_234[(df_234['periodicity']=='SU')&(df_234['periodicityRelative']==1)][cols + ['priceAsk']]
# f234_wi_bid = df_234[(df_234['periodicity']=='WI')&(df_234['periodicityRelative']==1)][cols + ['priceBid']]
# f234_wi_ask = df_234[(df_234['periodicity']=='WI')&(df_234['periodicityRelative']==1)][cols + ['priceAsk']]
# f234_gy_bid = df_234[(df_234['periodicity']=='GY')&(df_234['periodicityRelative']==1)][cols + ['priceBid']]
# f234_gy_ask = df_234[(df_23