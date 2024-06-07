import concurrent.futures
import pandas as pd
import os
from constants import TICKER_LIST_FILE, TICKER_LIST_FOLDER
from yfinance import Ticker
import concurrent
import json
import time
from datetime import datetime
import uuid

def get_stock_list():
    CSV_FILE_PATH = TICKER_LIST_FILE
    if os.path.exists(CSV_FILE_PATH):
        return pd.read_csv(CSV_FILE_PATH)
    else:
        nse_df = get_nse_stock_list()
        upstox_df = get_upstox_list()
        merged_df = pd.merge(upstox_df, nse_df, how="inner", left_on=["tradingsymbol"], right_on=["symbol"])
        cols_to_remove = ['exchange_token',  'expiry', 'strike', 'tick_size', 'lot_size',
                          'option_type',"series",  "paid_up_value", "market_lot", 'tradingsymbol', 'name', 'last_price',
                          'face_value']
        
        merged_df.drop(columns=cols_to_remove, axis=1, inplace=True, errors="raise")
        merged_df['date_of_listing'] = merged_df['date_of_listing'].apply(lambda x: datetime.strptime(x, "%d-%b-%Y"))
        merged_df['id'] = [str(uuid.uuid4()) for _ in range(len(merged_df.index))]
        merged_df['market_cap'] = 0
        all_symbols = merged_df['symbol'].tolist()
        #stock_info = await get_stock_info(all_symbols)
        #merged_df = pd.merge(stock_info, merged_df,how="inner", left_on=["symbol"], right_on=["symbol"])
        merged_df.to_csv(CSV_FILE_PATH, index = False)
        return merged_df

def get_nse_stock_list():
    df = pd.read_csv(f"{TICKER_LIST_FOLDER}EQUITY_L.csv")
    columns_to_replace = {x:x.strip().lower().replace(" ","_") for x in df.columns}
    df.rename(columns=columns_to_replace, inplace=True)
    return df

def get_upstox_list():
    df = pd.read_csv(f"{TICKER_LIST_FOLDER}complete.csv")
    df = df[(df['instrument_type'] == 'EQUITY') | (df['instrument_type'] == 'INDEX')]
    df.drop(labels = ['exchange_token', 'tradingsymbol', 'last_price', 'expiry', 'strike', 'tick_size', 'lot_size'], inplace=True, errors='ignore')
    df.rename(columns={"instrument_key":"upstox_id"}, inplace=True)
    return df


def get_ticker(name):
    info = Ticker(name).info
    return {
            "market_cap":info.get("marketCap",0),
            "symbol": name.split(".")[0],
            "sector":info.get("sector",""),
            "sector_key":info.get("sectorKey",""),
            "industry":info.get("industry",""),
            "industry_key":info.get("industryKey","")
        }

async def get_stock_info(symbol_lists):
    new_list = [x + ".NS" for x in symbol_lists]
    all_data = []
    print(len(new_list))
    pool = concurrent.futures.ThreadPoolExecutor()
    sections = [new_list[i:i+5] for i in range(0,len(new_list[:1000]),5)]
    for index, section  in enumerate(sections):
        futures = [pool.submit(get_ticker, x) for x in section]
        concurrent.futures.wait(futures, return_when=concurrent.futures.ALL_COMPLETED)
        temp_data = [x.result() for x in futures]
        all_data = all_data + temp_data
        print(f"{index + 1} done out of {len(sections)}")
        time.sleep(10)
    with open("/.temp.json", 'w') as fp:
        fp.write(json.dumps(all_data))
    return pd.DataFrame.from_dict(all_data)




if __name__ == "__main__":

    get_stock_list()