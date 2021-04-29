import yfinance as yf
import pandas as pd

# file with tickers
f =  open("companies.txt","r")

# Get 
msft = yf.Ticker("MSFT")

# get stock info 
info = msft.info
keys = list()
info.pop('companyOfficers',None)
for key in info:
    keys.append(key)

with open("db/table_struct/ticker_table.csv", 'w') as file:
    for item in keys:
        file.write("%s," % item)
    file.close()

