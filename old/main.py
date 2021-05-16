import yfinance as yf
import matplotlib.pyplot as plt 
import seaborn
import pandas as pd
import csv
import sqlite3

# global vars
db_file = 'db/fin.db'


# database connection

conn = None
try:
    conn = sqlite3.connect(db_file)
    print('tentei')
except sqlite3.Error as e:
    print(e)

# file with tickers
f =  open("companies.txt","r")

cur = conn.cursor()
query = "Select * from example"
rows = cur.fetchall()
for row in rows:
    print(row)

# Using for loop
count=0
df_list = list()

for line in f:
    count += 1
    print("{}".format(line.strip()))

    ticker = yf.Ticker(line.strip())

    # get stock info 
    tipo = ticker.info
    tipo.pop('companyOfficers',None)

    #print(tipo)

    cur = conn.cursor()

    columns = ', '.join(tipo.keys())
    col = columns.replace("52WeekChange","N52WeekChange")
    placeholders = ':'+', :'.join(tipo.keys())
    pla = placeholders.replace(":52WeekChange",":N52WeekChange")

    query = "INSERT INTO ticker (%s) VALUES (%s)" % (col,pla)
    #print(query)
    cur.execute(query, tipo)
    conn.commit()
    #print(type(tipo))




f.close()