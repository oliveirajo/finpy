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

def create_blc_sheet_table(cnx):
  query = "CREATE TABLE IF NOT EXISTS blc_sheet(date_inserted datetime,Date date,Ticker varchar(50),Total_Liab float,"\
                "Total_Stockholder_Equity float,Other_Current_Liab float,Total_Assets float,"\
                "Common_Stock float,Other_Current_Assets float,Retained_Earnings float,Other_Liab float,Treasury_Stock float,Other_Assets float,"\
                "Cash float,Total_Current_Liabilities float,Short_Long_Term_Debt float,Other_Stockholder_Equity float,Property_Plant_Equipment float,"\
                "Total_Current_Assets float,Long_Term_Investments float,Net_Tangible_Assets float,Short_Term_Investments float,Net_Receivables float,"\
                "Long_Term_Debt float,Inventory float,Accounts_Payable float)"
  try:
    c = cnx.cursor()
    c.execute(query)
  except mysql.connector.Error as e:
    print(e)

def create_financials_table(cnx):
  query = "CREATE TABLE IF NOT EXISTS financials(date_inserted datetime,Date Date,Ticker varchar(50),Research_Development float,"\
                "Effect_Of_Accounting_Charges float,Income_Before_Tax float,Minority_Interest float,Net_Income float,"\
                "Selling_General_Administrative float,Gross_Profit float,Ebit float,Operating_Income float,"\
                "Other_Operating_Expenses float,Interest_Expense float,Extraordinary_Items float,Non_Recurring float,"\
                "Other_Items float,Income_Tax_Expense float,Total_Revenue float,Total_Operating_Expenses float,"\
                "Cost_Of_Revenue float,Total_Other_Income_Expense_Net float,Discontinued_Operations float,"\
                "Net_Income_From_Continuing_Ops float,Net_Income_Applicable_To_Common_Shares float)"
  try:
    c = cnx.cursor()
    c.execute(query)
  except mysql.connector.Error as e:
    print(e)
def create_cashflow_table(cnx):
  query = "CREATE TABLE IF NOT EXISTS cashflow(date_inserted datetime,Date date,Ticker varchar(50),Investments float,"\
                "Change_To_Liabilities float,Total_Cashflows_From_Investing_Activities float,Net_Borrowings float,"\
                "Total_Cash_From_Financing_Activities float,Change_To_Operating_Activities float,Issuance_Of_Stock float,"\
                "Net_Income float,Change_In_Cash float,Repurchase_Of_Stock float,Total_Cash_From_Operating_Activities float,"\
                "Depreciation float,Other_Cashflows_From_Investing_Activities float,Dividends_Paid float,Change_To_Inventory float,"\
                "Change_To_Account_Receivables float,Other_Cashflows_From_Financing_Activities float,Change_To_Netincome float,Capital_Expenditures float)"
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

def init_blc_sheet_table(cnx,table,df):
  cols = ",".join(df.columns)
  cols_clean = cols.replace(" ","_")  
  # Insert DataFrame records one by one.
  for i,row in df.iterrows():
    placeholder = ", ".join(["%s"] * len(row))
    sql = "INSERT INTO {table}({cols}) VALUES ({placeholder});".format(table=table, cols=cols_clean, placeholder=placeholder)
    print(sql)
    c = cnx.cursor()
    c.execute(sql, tuple(row))
    print(tuple(row))
# the connection is not autocommitted by default, so we must commit to save our # changes 
    cnx.commit()