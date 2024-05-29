import concurrent.futures
import os
import pandas as pd
import yfinance as yf
import logging
from constants import CHART_DATA_STORAGE_LOCATION
import requests 
import concurrent
import asyncio
from datetime import datetime
import time

logger = logging.basicConfig(level=logging.INFO)



async def all_urls(urls, func_name):
    all_data = {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
        loop = asyncio.get_event_loop()
        futures = [loop.run_in_executor(executor, func_name, x) for x in urls]
        for url, resp in zip(urls, await asyncio.gather(*futures)):
            all_data[url] = resp.json()
    return all_data


class CandlePriceRetriever:

    def __init__(self, ticker_names, ticker_type="stock"):
        self.ticker_names = ticker_names
        self.ticker_type = ticker_type
        self.stock_to_fetch = []
        if not os.path.exists(CHART_DATA_STORAGE_LOCATION):
            os.mkdir(CHART_DATA_STORAGE_LOCATION)
        
    

    async def fetch_data(self,time_period, time_interval, source="upstox"):

        return_data = {}
        for x in self.ticker_names:
            file_location = CHART_DATA_STORAGE_LOCATION + f"{x}_{time_interval}.xlsx"

            if os.path.exists(file_location):
                logging.info(f"reading from local")
                return_data[x]  = pd.read_excel(file_location)
            else:
                self.stock_to_fetch.append(x)
        print(self.stock_to_fetch)
        if self.stock_to_fetch:

            data = {}
            if source == 'yahoo':
                
                data =  self.fetch_data_from_yahoo_finanance(time_period, time_interval)
                columns = data.columns.tolist()
                columns = {x:x.lower() for x in columns}
                data.rename(columns =columns, inplace=True)
                data['date'] = data['date'].apply(lambda x: pd.to_datetime(x).date())
                data.to_excel(file_location, index = False)
            elif source == 'upstox':
                to_date = datetime.today().strftime("%Y-%m-%d")
                data = await self.fetch_data_from_upstox_api(time_period, time_interval, to_date=to_date)
            
            for ticker, st_data in data.items():
                file_location = CHART_DATA_STORAGE_LOCATION + f"{ticker}_{time_interval}.xlsx"
                return_data[ticker] =  st_data
                st_data.to_excel(file_location, index = False)
        self.stock_to_fetch = []
        return return_data

    def fetch_data_from_yahoo_finanance(self,time_period, time_interval):
        logging.info(f"fetching {self.ticker_type} for {time_period} and {time_interval} interval from yahoo finance")
        ticker_to_fetch = f"{self.ticker_name}.NS"
        if self.ticker_type == 'index':
            ticker_to_fetch = self.ticker_name
        
        data = yf.Ticker(ticker_to_fetch)
        df = data.history(time_period, interval = time_interval)
        return df.reset_index()
    
    def e_handler(rself,request, exception):
        import pdb;pdb.set_trace()
        print(exception)
    
    async def fetch_data_from_upstox_api(self, time_period, time_interval, to_date ="2024-05-20", from_date="2014-05-20"):

        all_fetch_requests = []
        ctr = 0
        no_of_calls = 0
        tickers = []
        return_data  = {}        
        for x in self.stock_to_fetch:
            logging.info(f"fetching {x} for {time_period} and {time_interval} from upstox api")
            url = f"https://api.upstox.com/v2/historical-candle/{x}/{time_interval}/{to_date}/{from_date}"
            logging.info(url)
            all_fetch_requests.append(url)
            ctr = ctr + 1
            tickers.append(x)
            if ctr == 25:
                all_resp = await all_urls(all_fetch_requests, requests.get)
                #all_resp = grequests.map(all_fetch_requests, exception_handler=self.e_handlers

                for ticker, url, resp in zip(tickers, all_fetch_requests ,all_resp.values()):

                    stock_data = resp
                    df = pd.DataFrame(data = stock_data['data']['candles'], columns=['date', 'open', 'high', 'low', 'close', 'volume', 'open_interest'])
                    return_data[ticker] = df
                logging.info("Sleeping")
                time.sleep(5)
                ctr = 0
                tickers = []
                all_fetch_requests = []   
                no_of_calls = no_of_calls + 1
                print(f"total calls == {no_of_calls}")      
        print(f"total data == {len(return_data)}")        
        return return_data

if  __name__ == "__main__":
    obj = CandlePriceRetriever("^NSEI", ticker_type="index")
    obj.fetch_data("10y", "1d")