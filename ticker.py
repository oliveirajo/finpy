import yfinance as yf
from datapackage import Package

package = Package('https://datahub.io/core/s-and-p-500-companies/datapackage.json')

# print list of all resources:
print(package.resource_names)

sp = list()
# print processed tabular data (if exists any)
for resource in package.resources:
    if resource.descriptor['datahub']['type'] == 'derived/csv':
        sp.append(resource.read())
        #print(resource.read())
#print(sp)
for cpm in sp:
    print(cpm[0])
msft = yf.Ticker("MSFT")

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
