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




def create_table(cnx, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = db.cursor()
        c.execute(create_table_sql)
    except mysql.connector.Error as e:
        print(e)

def create_ticker_table(cnx, company):
  query = 'CREATE TABLE IF NOT EXISTS ticker ('+' text , '.join(company.keys()) + '  text )'

  try:
    c = cnx.cursor()
    c.execute(query)
    
  except mysql.connector.Error as e:
    print(e)

def init_ticker_table(cnx,company,table):
  # insert data into table
  placeholder = ", ".join(["%s"] * len(company))
  stmt = "insert into {table} ({columns}) values ({values});".format(table=table, columns=",".join(company.keys()), values=placeholder)
  try:
    c = cnx.cursor()
    c.execute(stmt, list(company.values()))
    #test = c.statement
    #print(test)
  except mysql.connector.Error as e:
    print(e)

def delete_table(cnx,table):
  # delete data from table
  del_query =  "DELETE FROM {table};".format(table=table)
  try:
    c = cnx.cursor()
    c.execute(del_query)
  except mysql.connector.Error as e:
    print(e)

def main():

  cnx = db.create_connecton()

  # create tables
  if cnx is not None:
    # create ticker table
    create_ticker_table(cnx,tipo)

    # delete data
    delete_table(cnx,'ticker')

    # initialize database
    for line in f:
      print(line)
      tkr = yf.Ticker(line.strip())
      dic = tkr.info
      # cols to remove
      dic.pop('companyOfficers',None)
      dic.pop('address2',None)

      init_ticker_table(cnx,dic,'ticker')
    
    cnx.commit()
  else:
    print('Error! Cannot create db connection.')

if __name__ == '__main__':
    main()