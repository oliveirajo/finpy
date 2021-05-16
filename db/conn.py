from re import T
import mysql.connector

# db conn
def create_connection():
  try:
      cnx = mysql.connector.connect(user='root',
                                  password='root',
                                  host='127.0.0.1',
                                  database='fin')
  except mysql.connector.Error as err:
    if err.errno == mysql.connector.errorcode.ER_ACCESS_DENIED_ERROR:
      print("Something is wrong with your user name or password")
    elif err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
      print("Database does not exist")
    else:
      print(err)
  return cnx

def create_ticker_table(cnx, company):
  query = 'CREATE TABLE IF NOT EXISTS ticker ('+' text, '.join(company.keys())+' text)'

  try:
    c = cnx.cursor()
    c.execute(query)
    
  except mysql.connector.Error as e:
    print(e)

def create_table(cnx, table,cols_info):
  #query = "CREATE TABLE IF NOT EXISTS {table} ("+", ".join(cols_info)+" );".format(table=table)
  query = "CREATE TABLE IF NOT EXISTS {table} ({cols});".format(table=table, cols=",".join(cols_info))
  print(query)

  try:
    c = cnx.cursor()
    c.execute(query)
    
  except mysql.connector.Error as e:
    print(e)

def delete_table(cnx,table):
  # delete data from table
  del_query = "DELETE FROM {table};".format(table=table)
  try:
    c = cnx.cursor()
    c.execute(del_query)
  except mysql.connector.Error as e:
    print(e)

def init_ticker_table(cnx,company,table):
  # insert data into table
  placeholder = ", ".join(["%s"] * len(company))
  stmt = "INSERT INTO {table} ({columns}) values ({values});".format(table=table, columns=",".join(company.keys()), values=placeholder)
  try:
    c = cnx.cursor()
    c.execute(stmt, list(company.values()))
    #test = c.statement
    #print(test)
  except mysql.connector.Error as e:
    print(e)
