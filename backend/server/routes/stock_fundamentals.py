from fastapi import APIRouter
from server.service.stock_fundamentals_service import get_stock_fundamentals
from server.service.ticker_service import get_list_of_stocks
from fastapi.responses import JSONResponse




router = APIRouter()

@router.get("/{stock_symbol}")
async def get_ticker_fundamental(stock_symbol):

    return await get_stock_fundamentals(stock_symbol)