import pandas as pd
from server.modules.stock_scraper.stock_price_data_retriver import CandlePriceRetriever
from server.modules.stock_scraper.stock_list_retriever import get_stock_list
from constants import TICKER_LIST_FILE

def get_stock_data(ticker_name, time_period = "10y", time_interval="1d"):

    price_retriever = CandlePriceRetriever(ticker_name=ticker_name, ticker_type = "stock")
    df = price_retriever.fetch_data(time_period=time_period, time_interval=time_interval)
    return df.to_dict(orient="records")



async def update_latest_stock_list():
    stock_list =  refresh_stock_list()
    

async def get_list_of_stocks():
    return get_stock_list()
