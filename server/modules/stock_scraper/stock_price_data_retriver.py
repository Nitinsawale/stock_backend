import concurrent.futures
import os
import pandas as pd
import yfinance as yf
import logging
import requests 
import concurrent
import asyncio
from datetime import datetime
import time
from server.utils.stock_db_service import db_obj
from constants import CHART_DATA_STORAGE_LOCATION
from server.modules.stock_scraper.stock_list_retriever import get_stock_list
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

    def __init__(self, ticker_type="stock"):
        self.ticker_names = get_stock_list()
        self.ticker_type = ticker_type
        self.stocks_to_update = []
        if not os.path.exists(CHART_DATA_STORAGE_LOCATION):
            os.mkdir(CHART_DATA_STORAGE_LOCATION)
        
        self.update_existing_data()
        
    


    def get_list_of_data_local_non_local(self, time_interval):
        self.stocks_to_update =  db_obj.get_list_stocks_with_candle_data()
        self.stocks_to_update = [
            {
                "index": 0,
                "upstox_id": "NSE_EQ|INE144J01027",
                "instrument_type": "EQUITY",
                "exchange": "NSE_EQ",
                "symbol": "20MICRONS",
                "name_of_company": "20 Microns Limited",
                "date_of_listing": "2008-10-06",
                "isin_number": "INE144J01027",
                "id": "5e8520a8-c95c-4924-8209-3c3169b5c9d0",
                "market_cap": 0
            },
            {
                "index": 1,
                "upstox_id": "NSE_EQ|INE253B01015",
                "instrument_type": "EQUITY",
                "exchange": "NSE_EQ",
                "symbol": "21STCENMGM",
                "name_of_company": "21st Century Management Services Limited",
                "date_of_listing": "1995-05-03",
                "isin_number": "INE253B01015",
                "id": "b9de69a1-1548-495b-a69c-58bd46294878",
                "market_cap": 0
            },
            {
                "index": 2,
                "upstox_id": "NSE_EQ|INE466L01038",
                "instrument_type": "EQUITY",
                "exchange": "NSE_EQ",
                "symbol": "360ONE",
                "name_of_company": "360 ONE WAM LIMITED",
                "date_of_listing": "2019-09-19",
                "isin_number": "INE466L01038",
                "id": "e5e47c8c-9d2d-4364-8fe8-2325e2d505c1",
                "market_cap": 0
            },
            {
                "index": 3,
                "upstox_id": "NSE_EQ|INE748C01038",
                "instrument_type": "EQUITY",
                "exchange": "NSE_EQ",
                "symbol": "3IINFOLTD",
                "name_of_company": "3i Infotech Limited",
                "date_of_listing": "2021-10-22",
                "isin_number": "INE748C01038",
                "id": "337ff565-9778-4263-8e15-f2cd11f2cca0",
                "market_cap": 0
            },
            {
                "index": 4,
                "upstox_id": "NSE_EQ|INE470A01017",
                "instrument_type": "EQUITY",
                "exchange": "NSE_EQ",
                "symbol": "3MINDIA",
                "name_of_company": "3M India Limited",
                "date_of_listing": "2004-08-13",
                "isin_number": "INE470A01017",
                "id": "23943793-e825-47f8-9311-775ae9ba26e3",
                "market_cap": 0
            }]

    def update_existing_data(self):
        loop = asyncio.new_event_loop()
        #loop.run_until_complete(self.fetch_data("10y", "day")) 
            


    async def fetch_data(self,time_period, time_interval, source="upstox"):

        self.get_list_of_data_local_non_local(time_interval=time_interval)
        return_data = {}
        if self.stocks_to_update:

            data = {}
            if source == 'yahoo':
                
                data =  self.fetch_data_from_yahoo_finanance(time_period, time_interval)
                columns = data.columns.tolist()
                columns = {x:x.lower() for x in columns}
                data.rename(columns =columns, inplace=True)
                data['date'] = data['date'].apply(lambda x: pd.to_datetime(x).date())
                data.to_excel(file_location, index = False)
            elif source == 'upstox':
                to_date = datetime.strptime("16-MAY-2024", "%d-%b-%Y")
                data = await self.fetch_data_from_upstox_api(time_period, time_interval)
            
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
        sections = [self.stocks_to_update[i:i+5] for i in range(0,len(self.stocks_to_update),5)]        
        for x in self.stocks_to_update:
            logging.info(f"fetching {x} for {time_period} and {time_interval} from upstox api")
            url = f"https://api.upstox.com/v2/historical-candle/{x['upstox_id']}/{time_interval}/{to_date}/{from_date}"
            logging.info(url)
            all_fetch_requests.append(url)
            ctr = ctr + 1
            tickers.append(x)
            if ctr == 5:
                all_resp = await all_urls(all_fetch_requests, requests.get)
                #all_resp = grequests.map(all_fetch_requests, exception_handler=self.e_handlers

                for ticker, url, resp in zip(tickers, all_fetch_requests ,all_resp.values()):

                    stock_data = resp
                    df = pd.DataFrame(data = stock_data['data']['candles'], columns=['date', 'open', 'high', 'low', 'close', 'volume', 'open_interest'])
                    return_data[ticker['symbol']] = df
                logging.info("Sleeping")
                time.sleep(10)
                ctr = 0
                tickers = []
                all_fetch_requests = []   
                no_of_calls = no_of_calls + 1
                print(f"total calls == {no_of_calls}")      
        print(f"total data == {len(return_data)}")        
        return return_data
    


candle_obj = CandlePriceRetriever()
