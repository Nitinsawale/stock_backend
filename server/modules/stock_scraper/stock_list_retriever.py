import requests
import gzip
import io
import pandas as pd
from constants import TICKER_LIST_FOLDER

def refresh_stock_list():

    stock_list_url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    resp = requests.get(stock_list_url)    
    data = resp.content
    file = gzip.open(io.BytesIO(data))
    content_io = io.StringIO(file.read().decode("utf-8"))
    df = pd.read_csv(content_io)
    df = df[(df['instrument_type'] == 'EQUITY') | (df['instrument_type'] == 'INDEX')]
    df.drop(labels = ['exchange_token', 'tradingsymbol', 'last_price', 'expiry', 'strike', 'tick_size', 'lot_size'], inplace=True, errors='ignore')
    df.rename(columns={"instrument_key":"upstox_name"})
    df.to_pickle(f"./{TICKER_LIST_FOLDER}/ticker_list.pickle")


if __name__ == "__main__":

    refresh_stock_list()