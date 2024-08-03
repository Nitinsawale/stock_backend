from fastapi import APIRouter
from server.service.ticker_service import get_list_of_stocks, search_stock, get_stock_candle_data
from fastapi.responses import JSONResponse

router = APIRouter()


@router.get('/ticker-data/{ticker_symbol}/{time_interval}')
def get_ticker_data(ticker_symbol, time_interval):

    data = get_stock_candle_data(stock_symbol = ticker_symbol, time_interval=time_interval)
    return "updated"



# @router.get('/refresh-ticker-list')
# async def get_ticker_data():

#     await update_latest_stock_list()
#     return "Done"



@router.get("/")
async def get_list():

    df =  await get_list_of_stocks()  
    return df.to_dict(orient = "records")


@router.get("/search/{query}")
async def search_stock_api(query):

    return search_stock(query)