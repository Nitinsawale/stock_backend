from abc import ABC
import pandas as pd
import pandas_ta as pta
from numpy import nan
import logging 
import numpy as np

logging.basicConfig(level=logging.INFO)
def decorator_timer(some_function):
    from time import time

    def wrapper(*args, **kwargs):
        t1 = time()
        result = some_function(*args, **kwargs)
        end = time()-t1
        logging.info(f"{some_function.__name__} took {end} seconds")
        return result
    return wrapper



class TickerIndicator(ABC):
    pass



class TrueRange:


    def __init__(self, data_df, timeframe):
        self.data_df = data_df
        self.timeframe = timeframe 


    def true_range_calculator(self, row, previous_row):
        return max([(row['high'] - row['low']),  abs(row['high'] - previous_row['close']), abs(row['low'] - previous_row['close'])])
    

    @decorator_timer
    def calculate_indicator_values(self):

        self.data_df['tr1'] = self.data_df['high'] - self.data_df['low']
        self.data_df['tr2'] = (self.data_df['high'] - self.data_df['close'].shift()).abs()
        self.data_df['tr3'] = (self.data_df['low'] - self.data_df['close'].shift()).abs()
        self.data_df['true_range'] = self.data_df[['tr1', 'tr2', 'tr3']].max(axis = 1).fillna(0)
        return self.data_df


class AverageTrueRange:

    def __init__(self, data_df, timeframe):
        self.data_df = data_df
        self.timeframe = timeframe 


    @decorator_timer
    def calculate_indicator_values(self, window):

        true_range_object  = TrueRange(self.data_df, self.timeframe)
        self.data_df = true_range_object.calculate_indicator_values()
        self.data_df.iloc[:1] = nan
        self.data_df['average_true_range'] =  self.data_df['true_range'].iloc[1:].ewm(alpha = 1 / window,  min_periods = window).mean()
        return self.data_df
    

class SuperTrend:

    def __init__(self,data_df, timeframe):
        self.data_df = data_df

    
    @decorator_timer
    def calculate_indicator_values(self, length, multiplier):
        df =  pta.supertrend(self.data_df['high'], self.data_df['low'], self.data_df['close'],length= length, multiplier = multiplier)
        self.data_df['super_trend'] = df['SUPERTd_80_1.3']
        buy_signal = (self.data_df['super_trend'] == 1) & (self.data_df['super_trend'].shift() == -1)
        sell_signal = (self.data_df['super_trend'] == -1) & (self.data_df['super_trend'].shift() == 1)
        self.data_df.loc[buy_signal, 'buy/sell signal'] = "BUY"
        self.data_df.loc[sell_signal, 'buy/sell signal'] = "SELL"
        return self.data_df



class MovingAverage:

    def __init__(self,data_df):
        self.data_df = data_df

    
    @decorator_timer
    def smooth_data(self, data, window_size=5):
        return data.rolling(window=window_size, min_periods=1).mean()

    @decorator_timer
    def calculate_indicator_values(self, length):
        # self.data_df['moving_average_1'] = self.data_df['close'].rolling(window = length).mean()
        self.data_df['moving_average'] = pta.sma(self.data_df['close'], length = length, offset=0)
        # self.data_df['mvs'] = pta.ssf(self.data_df['moving_average'], length = 5)
        # self.data_df['smoothed'] = self.smooth_data(self.data_df['close'])
        return self.data_df



if __name__ == "__main__":
    data = pd.read_excel("./^NSEI_1d.xlsx")
    tr = MovingAverage(data, '1d')
    df = tr.calculate_indicator_values(80)
    df.to_excel("result.xlsx")