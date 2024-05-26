from fastapi import APIRouter
from server.service.ticker_service import get_stock_data, update_latest_stock_list
from fastapi.responses import JSONResponse

router = APIRouter()


@router.get('/ticker-data/{ticker_name}/{time_period}/{time_interval}')
def get_ticker_data(ticker_name, time_period, time_interval):

    data =get_stock_data(ticker_name = ticker_name, time_period= time_period, time_interval=time_interval)
    return data



@router.get('/refresh-ticker-list')
async def get_ticker_data():

    await update_latest_stock_list()
    return "Done"


