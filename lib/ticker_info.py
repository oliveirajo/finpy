import yfinance as yf
import pandas as pd
import sys
import logging
from datapackage import Package
import datetime as dt
sys.path.append('/Users/joelamaro/Desktop/finance/finpy/finpy')
import db.conn as db

def get_ticker_info(): #validated
    cnx = db.create_connection()
    # GLOBAL VARS
    # get stock table info to create table
    ticker = yf.Ticker('tsla')
    tipo = ticker.info
    # delete data
    del_query = "DELETE FROM ticker"
    db.execute_query(cnx,del_query)
    # create ticker table
    db.create_ticker_table(cnx,tipo)
    # initialize database
    query = "SELECT ticker FROM sp500"
    results = db.read_query(cnx,query)        
    for item in results:
        print(item[0])
        tkr = yf.Ticker(item[0])
        dic = tkr.info
        dic.pop('companyOfficers',None)
        dic.pop('address2',None)
        df = pd.DataFrame(data=dic, index=[0])
        cols = ",".join(df.columns).replace(" ","_")
        lista = []
        for i,row in df.iterrows():
            placeholder = ", ".join(["%s"] * len(row))
            sql = "INSERT INTO ticker({cols}) VALUES ({placeholder});".format(cols=cols, placeholder=placeholder)
            lista.append(list(row))
        db.execute_list_query(cnx, sql, lista)
        print("done")

def get_sp500():
    # db conn
    cnx = db.create_connection()
    c = cnx.cursor()
    package = Package('https://datahub.io/core/s-and-p-500-companies/datapackage.json')
    cols = {"ticker varchar(10)",
    "company_name varchar(50)",
    "sector varchar(50)"}    
    create_query = " CREATE TABLE IF NOT EXISTS SP500 (ticker varchar(10),company_name varchar(50), sector varchar(50))"
    db.execute_query(cnx,create_query)
    # delete table
    del_query = "DELETE FROM sp500"
    db.execute_query(cnx,del_query)
    # create table
    db.create_table(cnx,"sp500",cols)
    
    # print list of all resources:
    #print(package.resource_names)
    sp = list()
    # print processed tabular data (if exists any)
    for resource in package.resources:
        if resource.descriptor['datahub']['type'] == 'derived/csv':
            sp.append(resource.read())
    lista = []
    for cpm in sp[0]:
        if "." in cpm[0]:
            cpm[0] = str(cpm[0]).replace(".","-")  
        sql = "INSERT INTO sp500(ticker,company_name,sector) VALUES (%s, %s, %s)"
        val = (cpm[0], cpm[1],cpm[2])
        lista.append(val)
    db.execute_list_query(cnx, sql, lista)
    print("done")

# como no balance_sheet ha diferentes colunas, adicionamos uma chamado tipo de input, que vai levar o nome da coluna e multiplpicamos o numero de linhas pelo numero de colunas

def get_blc_sheet(): 
    cnx = db.create_connection()
    db.create_blc_sheet_table(cnx)
    del_query = "DELETE FROM blc_sheet"
    db.execute_query(cnx,del_query)
    query = "SELECT ticker FROM sp500 limit 10"
    results = db.read_query(cnx,query)
    
    for item in results:
        print(item[0])
        msft = yf.Ticker(item[0])
        blc_sheet = msft.balance_sheet
        df1 = pd.DataFrame(data=blc_sheet).fillna(0)
        df1.index = df1.index.set_names(['Operation'])
        df1.reset_index(level=0,inplace=True)
        df1 = (df1.set_index(["Operation"])
                 .stack()
                 .reset_index(name='Value'))
        df1.rename(columns={df1.columns[1]: "Date"}, inplace=True)
        df1.insert(0,'Ticker',item[0])
        now = dt.datetime.now()
        df1.insert(0,'date_inserted',now)
        df1['date_inserted'] = df1['date_inserted'].astype('str')
        df1['Date'] = df1['Date'].astype('str')
        #creating column lsit for insertion
        cols = ",".join(df1.columns).replace(" ","_")
        # Insert DataFrame records one by one.
        lista = []
        for i,row in df1.iterrows():
            placeholder = ", ".join(["%s"] * len(row))
            sql = "INSERT INTO blc_sheet({cols}) VALUES ({placeholder});".format(cols=cols, placeholder=placeholder)
            lista.append(list(row))
        db.execute_list_query(cnx, sql, lista)
        print("done")
        
def get_financials():
    cnx = db.create_connection()
    db.create_financials_table(cnx)
    del_query = "DELETE FROM financials"
    db.execute_query(cnx,del_query)
    query = "SELECT ticker FROM sp500"
    results = db.read_query(cnx,query)
    for item in results:
        print(item[0])
        msft = yf.Ticker(item[0])
        financials = msft.financials
        df1 = pd.DataFrame(data=financials).transpose().fillna(0)
        df1.index = df1.index.set_names(['Date'])
        df1.insert(0,'Ticker',item[0])
        df1.reset_index(level=0,inplace=True)
        df1['Date'] = df1['Date'].astype('str')
        now = dt.datetime.now()
        df1.insert(0,'date_inserted',now)
        df1['date_inserted'] = df1['date_inserted'].astype('str')
        #creating column list for insertion
        cols = ",".join(df1.columns).replace(" ","_")
        # Insert DataFrame records one by one.
        lista = []
        for i,row in df1.iterrows():
            placeholder = ", ".join(["%s"] * len(row))
            sql = "INSERT INTO financials({cols}) VALUES ({placeholder});".format(cols=cols, placeholder=placeholder)
            lista.append(list(row))

        db.execute_list_query(cnx, sql, lista)
        print("done")

def get_cashflow(): #validated
    cnx = db.create_connection()
    db.create_cashflow_table(cnx)
    del_query = "DELETE FROM cashflow"
    db.execute_query(cnx,del_query)
    query = "SELECT ticker FROM sp500"
    results = db.read_query(cnx,query)
    for item in results:
        print(item[0])
        msft = yf.Ticker(item[0])
        cashflow = msft.cashflow
        df1 = pd.DataFrame(data=cashflow).transpose().fillna(0).drop('Effect Of Exchange Rate',axis=1,errors='ignore')
        df1.index = df1.index.set_names(['Date'])
        df1.insert(0,'Ticker',item[0])
        df1.reset_index(level=0,inplace=True)
        df1['Date'] = df1['Date'].astype('str')
        now = dt.datetime.now()
        df1.insert(0,'date_inserted',now)
        df1['date_inserted'] = df1['date_inserted'].astype('str')
        #creating column lsit for insertion
        cols = ",".join(df1.columns).replace(" ","_")
        # Insert DataFrame records one by one.
        lista = []
        for i,row in df1.iterrows():
            placeholder = ", ".join(["%s"] * len(row))
            sql = "INSERT INTO cashflow({cols}) VALUES ({placeholder});".format(cols=cols, placeholder=placeholder)
            lista.append(list(row))
        db.execute_list_query(cnx, sql, lista)
        print("done")

def get_action(): #validated
    cnx = db.create_connection()
    db.create_actions_table(cnx)
    del_query = "DELETE FROM actions"
    db.execute_query(cnx,del_query)
    query = "SELECT ticker FROM sp500"
    results = db.read_query(cnx,query) 
    for item in results:
        print(item[0])
        msft = yf.Ticker(item[0])
        actions = msft.actions
        
        df1 = pd.DataFrame(data=actions)
        df1.insert(0,'Ticker',item[0])
        df1.reset_index(level=0,inplace=True)
        df1['Dividends'] =  df1['Dividends'].astype(float)
        df1['Stock Splits'] =  df1['Stock Splits'].astype(float)
        df1['Date'] = df1['Date'].astype('str')
        now = dt.datetime.now()
        df1.insert(0,'date_inserted',now)
        df1['date_inserted'] = df1['date_inserted'].astype('str')

        #creating column lsit for insertion
        cols = ",".join(df1.columns).replace(" ","_")
        
        # Insert DataFrame records one by one.
        lista = []
        for i,row in df1.iterrows():
            placeholder = ", ".join(["%s"] * len(row))
            sql = "INSERT INTO actions({cols}) VALUES ({placeholder});".format(cols=cols, placeholder=placeholder)
            lista.append(list(row))
        db.execute_list_query(cnx, sql, lista)
        print("done")