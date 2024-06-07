from server.modules.stock_strategy.super_trend_ma import SuperTrendwithMovingAverage
from server.modules.stock_scraper.stock_price_data_retriver import CandlePriceRetriever
#from server.service.ticker_service import  get_stock_list
import pandas as pd
from datetime import datetime, timezone, timedelta

async def generate_strategy_data(strategy_name):
    print(strategy_name)
    if strategy_name == 'supertrend_with_ma':
        return await generate_super_trend_with_ma_data()
    
def find_latest_working_day():
    today = datetime.now(timezone.utc)
    diff = today.weekday() - 5
    if diff > 0:
        return today - timedelta(days=diff)
    return today

async def generate_super_trend_with_ma_data(): 

    data = []#await get_stock_list()
    data = data[(data['last_price'] > 850) & (data['last_price'] < 1000)]
    all_keys = data['upstox_id'].tolist()
    key_to_name = data[['upstox_id','symbol']].to_dict(orient="records")
    key_to_name = {x['upstox_id']:x['symbol'] for x in key_to_name}
    price_retriever = CandlePriceRetriever(all_keys)
    latest_working_day = find_latest_working_day()
    all_data = await price_retriever.fetch_data(time_period="10y", time_interval="day")
    all_signals = []
    for ticker , data_df in all_data.items():
        stat_object = SuperTrendwithMovingAverage(moving_average_length = 80, super_trend_length=10, super_trend_multiplier=1.3, data_df=data_df)
        data = stat_object.calculate_strategy_data()
        data['date'] = pd.to_datetime(data['date'])
        data.sort_values(by="date", ascending=False)
        if not data.empty:
            row = data.iloc[:1].to_dict(orient = 'records')[0]
            # date1 = row['date'].to_pydatetime()
            # if (latest_working_day - date1).days < 3:
            row['name'] = key_to_name[ticker]
            all_signals.append(row)
    return all_signals
    


            