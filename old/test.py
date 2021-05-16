import yfinance as yf
import pandas as pd
import db.conn as db

# GLOBAL VARS
# get stock info 
ticker = yf.Ticker('tsla')
tipo = ticker.info
tipo.pop('companyOfficers',None)

# file with tickers
f =  open("companies.txt","r")

def main():

  cnx = db.create_connection()

  # create tables
  if cnx is not None:

    # delete data
    db.delete_table(cnx,'ticker')
    print("data deleted")
    # create ticker table
    db.create_ticker_table(cnx,tipo)
    # initialize database
    for line in f:
      print(line)
      tkr = yf.Ticker(line.strip())
      dic = tkr.info
      # cols to remove
      dic.pop('companyOfficers',None)
      dic.pop('address2',None)

      db.init_ticker_table(cnx,dic,'ticker')
    
    cnx.commit()
  else:
    print('Error! Cannot create db connection.')

if __name__ == '__main__':
    main()