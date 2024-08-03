from server.modules.stock_fundamentals.screener_fundamentals import (get_stock_fundamentals_from_screener)
from datetime import datetime, timedelta
from server.utils.stock_db_service import conn
from server.modules.stock_fundamentals.stock_rating import calculate_stock_rating
import pandas as pd
import logging 
logging.basicConfig(level=logging.INFO)


def last_day_of_last_quarter():
    # Get the current date
    current_date = datetime.now()
    
    # Calculate the end dates of the previous quarters
    current_month = current_date.month
    current_year = current_date.year
    
    # Determine the quarter of the current date and find the last day of the last quarter
    if current_month <= 3:
        last_day_of_last_quarter = datetime(current_year - 1, 12, 31)
    elif current_month <= 6:
        last_day_of_last_quarter = datetime(current_year, 3, 31)
    elif current_month <= 9:
        last_day_of_last_quarter = datetime(current_year, 6, 30)
    else:
        last_day_of_last_quarter = datetime(current_year, 9, 30)
    
    return last_day_of_last_quarter


def get_fundamentals_from_db(stock_symbol):

     #read data from db
    tables = ['screener_info', 'company_ratios', 'quarterly_results', 'profit_loss', 'balance_sheet', 'cash_flows', 'other_ratios', 'shareholding']
    data  = {}
    for table in tables:
        query = f"SELECT * FROM {table} where symbol = '{stock_symbol}'"
        df = pd.read_sql_query(query, con=conn)
        if df.empty:
            logging.info(f"{table} not found ")
            raise Exception(f"No data found about {table}")
        data[table] = df
    
    return data



async def get_fundamentals_data(stock_symbol):


    #Check if data exists
    query = f"select * from screener_info where symbol = '{stock_symbol}'"
    df = pd.read_sql_query(query, conn)
    #if df.empty:
        #if not exists then fetch it from screener
    await get_stock_fundamentals_from_screener(stock_symbol)
    
    try:
        data = get_fundamentals_from_db(stock_symbol)
    except:
        logging.info("searching on web")
        await get_stock_fundamentals_from_screener(stock_symbol)
        data = get_fundamentals_from_db(stock_symbol)
    #check if data is 15 older older
    if "screener_info" in data and not data['screener_info'].empty:
        updated_date = datetime.strptime(data['screener_info'].iloc[0]['updated_date'],"%Y-%m-%d %H:%M:%S.%f")
        last_quarter_day = last_day_of_last_quarter()
        if updated_date < last_quarter_day:
            await get_stock_fundamentals_from_screener(stock_symbol)
            data = get_fundamentals_from_db(stock_symbol)

    return data

async def get_stock_fundamentals(stock_symbol):
    
    data = await get_fundamentals_data(stock_symbol)
    ratings =  calculate_stock_rating(data)
    return {"ratings":ratings,"data":{k:v.to_dict(orient='records') for k,v in data.items()}}
    