from numpy import rint
import lib.ticker_info as ticker
import multiprocessing as mt

#ticker.get_sp500()

def runInParallel(*fns):
  proc = []
  for fn in fns:
    p = mt.Process(target=fn)
    p.start()
    proc.append(p)
  for p in proc:
    p.join()

#runInParallel(ticker.get_ticker_info(),ticker.get_blc_sheet(),ticker.get_financials(),ticker.get_cashflow(),ticker.get_action())
#ticker.get_ticker_info()
ticker.get_blc_sheet()
ticker.get_financials()
ticker.get_cashflow()
ticker.get_action()