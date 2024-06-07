from server.modules.stock_fundamentals.screener_fundamentals import (get_stock_fundamentals_from_screener)
import asyncio
from server.utils.stock_db_service import conn
from server.modules.stock_fundamentals.stock_rating import calculate_stock_rating
import pandas as pd



async def get_fundamentals_from_db(stock_symbol):

    query = f"select * from screener_info where symbol = '{stock_symbol}'"
    df = pd.read_sql_query(query, conn)
    if df.empty:
        await get_stock_fundamentals_from_screener(stock_symbol)
    
    tables = ['screener_info', 'company_ratios', 'quarterly_results', 'profit_loss', 'balance_sheet', 'cash_flows', 'other_ratios', 'shareholding']
    data  = {}
    for table in tables:
        query = f"SELECT * FROM {table} where symbol = '{stock_symbol}'"
        df = pd.read_sql_query(query, con=conn)
        if not df.empty:
            data[table] = df
    return data

async def get_stock_fundamentals(stock_symbol):
    
    data = await get_fundamentals_from_db(stock_symbol)
    ratings =  calculate_stock_rating(data)
    return {"ratings":ratings,"data":{k:v.to_dict(orient='records') for k,v in data.items()}}
    