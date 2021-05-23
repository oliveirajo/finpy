import yfinance as yf
import pandas as pd
import sys
import logging
from datapackage import Package
import datetime as dt
sys.path.append('C:/Users/a73045/Desktop/finpy/finpy/')
import db.conn as db

#logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')

file_handler = logging.FileHandler('C:/Users/a73045/Desktop/finpy/finpy/log/ticcker_info.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)


def get_ticker_info():
    cnx = db.create_connection()
    # GLOBAL VARS
    # get stock info 
    ticker = yf.Ticker('tsla')
    tipo = ticker.info
    if cnx is not None:
        # delete data
        db.delete_table(cnx,'ticker')
        print("data deleted")
        # create ticker table
        db.create_ticker_table(cnx,tipo)

        # initialize database
        query = "SELECT ticker FROM sp500"
        c = cnx.cursor()
        c.execute(query)
        test = c.fetchall()
        print(type(test))
        for item in test:
            for idx,i in enumerate(item, start=1):
                print(idx,i)
                print(i)
                tkr = yf.Ticker(i)
                dic = tkr.info
                # cols to remove
                dic.pop('companyOfficers',None)
                dic.pop('address2',None)
                #insert data
                db.init_ticker_table(cnx,dic,'ticker')
        cnx.commit()
    else:
        logger.debug("ERROR! CANNOT CREATE DB CONNECTION!")

def get_sp500():
    # db conn
    cnx = db.create_connection()
    c = cnx.cursor()
    package = Package('https://datahub.io/core/s-and-p-500-companies/datapackage.json')
    cols = {"ticker varchar(10)",
    "company_name varchar(50)",
    "sector varchar(50)"}

    # delete table
    db.delete_table(cnx,'sp500')
    # create table
    db.create_table(cnx,"sp500",cols)
    
    # print list of all resources:
    #print(package.resource_names)
    sp = list()
    # print processed tabular data (if exists any)
    for resource in package.resources:
        if resource.descriptor['datahub']['type'] == 'derived/csv':
            sp.append(resource.read())

    final_sp = list()
    for cpm in sp:
        for number in cpm:
            final_sp.append(number)
            logger.info(number)
        
            sql = "INSERT INTO sp500(ticker,company_name,sector) VALUES (%s, %s, %s)"
            val = (number[0], number[1],number[2])
            c.execute(sql, val)
            cnx.commit()

def get_blc_sheet():
    cnx = db.create_connection()
    c = cnx.cursor()
    msft = yf.Ticker("AAPL")
    blc_sheet = msft.balance_sheet
    create_sql = "CREATE TABLE IF NOT EXISTS blc_sheet(date_inserted datetime,Date date,Ticker varchar(50),Total_Liab float,"\
                "Total_Stockholder_Equity float,Other_Current_Liab float,Total_Assets float,"\
                "Common_Stock float,Other_Current_Assets float,Retained_Earnings float,Other_Liab float,Treasury_Stock float,Other_Assets float,"\
                "Cash float,Total_Current_Liabilities float,Short_Long_Term_Debt float,Other_Stockholder_Equity float,Property_Plant_Equipment float,"\
                "Total_Current_Assets float,Long_Term_Investments float,Net_Tangible_Assets float,Short_Term_Investments float,Net_Receivables float,"\
                "Long_Term_Debt float,Inventory float,Accounts_Payable float)"
    c.execute(create_sql)
    db.delete_table(cnx,'ticker')
    query = "SELECT ticker FROM sp500"
    c.execute(query)
    test = c.fetchall()
    for item in test:
        df = pd.DataFrame(data=blc_sheet)
        df1 = df.transpose()
        df1.index = df1.index.set_names(['Date'])
        df1.insert(0,'Ticker',item[0])
        df1.reset_index(level=0,inplace=True)
        df1['Date'] = df1['Date'].astype('str')
        now = dt.datetime.now()
        df1.insert(0,'date_inserted',now)
        df1['date_inserted'] = df1['date_inserted'].astype('str')
        #creating column lsit for insertion
        cols = ",".join(df1.columns)
        cols_clean = cols.replace(" ","_")

        # Insert DataFrame records one by one.
        for i,row in df1.iterrows():
            placeholder = ", ".join(["%s"] * len(row))
            sql = "INSERT INTO blc_sheet({cols}) VALUES ({placeholder});".format(cols=cols_clean, placeholder=placeholder)
            c.execute(sql, tuple(row))
        # the connection is not autocommitted by default, so we must commit to save our # changes 
            cnx.commit()