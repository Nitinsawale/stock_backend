from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from server.routes.ticker import router as ticker_router
from server.routes.strategy import router as stat_router

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(ticker_router, prefix="/ticker")
app.include_router(stat_router, prefix="/strategy")



# from stock_scraper.screener_scarper import ScreenerScraper
# import pandas as pd


# from stock_strategy.super_trend_ma import (SuperTrendwithMovingAverage)

# data = pd.read_excel("./stock_indicators/^NSEI_1d.xlsx")
# sp = SuperTrendwithMovingAverage(moving_average_length=80, super_trend_length=10, super_trend_multiplier=1.3, data_df = data)
# sp.calculate_strategy_data()
# # async def fetch_data():
# #     stock_scraper = ScreenerScraper()
# #     await stock_scraper.fetch_stock_data("RELIANCE")

# # if __name__  == "__main__":
# #     loop = asyncio.get_event_loop()
# #     loop.run_until_complete(fetch_data())
    
    
