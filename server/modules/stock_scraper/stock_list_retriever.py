import pandas as pd
import os
from constants import TICKER_LIST_FILE, TICKER_LIST_FOLDER

def get_stock_list():
    CSV_FILE_PATH = TICKER_LIST_FILE
    if os.path.exists(CSV_FILE_PATH):
        return pd.read_csv(CSV_FILE_PATH)
    else:
        nse_df = get_nse_stock_list()
        upstox_df = get_upstox_list()
        merged_df = pd.merge(upstox_df, nse_df, how="inner", left_on=["tradingsymbol"], right_on=["symbol"])
        cols_to_remove = ['exchange_token', 'last_price', 'expiry', 'strike', 'tick_size', 'lot_size',
                          'option_type',"series",  "paid_up_value", "market_lot", 'trading_symbol', 'name']
        
        merged_df.drop(columns=cols_to_remove, axis=1, inplace=True, errors="ignore")
        merged_df.to_csv(CSV_FILE_PATH, index = False)
        return merged_df

def get_nse_stock_list():
    df = pd.read_csv(f"{TICKER_LIST_FOLDER}/EQUITY_L.csv")
    columns_to_replace = {x:x.strip().lower().replace(" ","_") for x in df.columns}
    df.rename(columns=columns_to_replace, inplace=True)
    return df

def get_upstox_list():
    df = pd.read_csv(f"{TICKER_LIST_FOLDER}/complete.csv")
    df = df[(df['instrument_type'] == 'EQUITY') | (df['instrument_type'] == 'INDEX')]
    df.drop(labels = ['exchange_token', 'tradingsymbol', 'last_price', 'expiry', 'strike', 'tick_size', 'lot_size'], inplace=True, errors='ignore')
    df.rename(columns={"instrument_key":"upstox_id"}, inplace=True)
    return df




if __name__ == "__main__":

    get_stock_list()