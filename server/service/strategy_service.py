from server.modules.stock_strategy.super_trend_ma import SuperTrendwithMovingAverage
from server.service.ticker_service import get_stock_data
import pandas as pd


def generate_strategy_data(strategy_name, ticker_name):

    if strategy_name == 'super_trend_with_ma':
        return generate_super_trend_with_ma_data(ticker_name)

def generate_super_trend_with_ma_data(ticker_name):

    
    data = get_stock_data(ticker_name=ticker_name)
    data_df = pd.DataFrame.from_dict(data)
    stat_object = SuperTrendwithMovingAverage(moving_average_length = 80, super_trend_length=10, super_trend_multiplier=1.3, data_df=data_df)
    data = stat_object.calculate_strategy_data()    
    return data.iloc[-1:].to_dict()