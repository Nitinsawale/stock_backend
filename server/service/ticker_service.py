import pandas as pd
from server.modules.stock_scraper.stock_price_data_retriver import CandlePriceRetriever
from server.utils.stock_db_service import conn, db_obj
from constants import TICKER_LIST_FILE
    

async def get_list_of_stocks():
    return db_obj.get_stocks()
    #return await get_stock_list()


def search_stock(query):

    query  = f"SELECT * FROM stock_list where symbol like '{query}%'"
    df = pd.read_sql_query(query, conn)
    return df.to_dict(orient='records')