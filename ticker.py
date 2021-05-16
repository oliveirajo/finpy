import yfinance as yf

msft = yf.Ticker("AAPL")
data = yf.download("SPY AAPL", start="2017-01-01", end="2017-04-30",
                   group_by="ticker")

#print(msft.info)
#print(msft.history(period="max"))
#print(msft.actions) #dividend + splits
#print(msft.dividends)
#print(msft.financials)
#print(msft.major_holders) # persons/institutions that hold company shares
#print(msft.balance_sheet)
#print(msft.cashflow)
#print(msft.earnings) #revenues + earnings (last 4 years)
#print(msft.sustainability) #Ecofriendly KPIS
#print(msft.recommendations)
#print(msft.calendar) # events of interes Earnings date
#print(msft.isin) # ISIN = International Securities Identification Number
#print(msft.options) # show options expirations
#print(msft.option_chain('2021-05-14')) # get option chain for specific expiration
