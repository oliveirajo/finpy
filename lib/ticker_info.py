import yfinance as yf
import pandas as pd
import sys
import logging
from datapackage import Package
sys.path.append('C:/Users/a73045/Desktop/finpy/finpy/')
import db.conn as db

#logging
logging.basicConfig(filename='debug.log', encoding='utf-8', level=logging.DEBUG)

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
        query = "SELECT ticker FROM sp500 LIMIT 10"
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
        print("ERROR! CANNOT CREATE DB CONNECTION!")


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
            print(number)
        
            sql = "INSERT INTO sp500(ticker,company_name,sector) VALUES (%s, %s, %s)"
            val = (number[0], number[1],number[2])
            c.execute(sql, val)
            cnx.commit()
