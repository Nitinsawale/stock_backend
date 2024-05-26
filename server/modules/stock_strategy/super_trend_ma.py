from  server.modules.stock_strategy.indicator import MovingAverage, SuperTrend


class SuperTrendwithMovingAverage:


    def __init__(self, moving_average_length, super_trend_length, super_trend_multiplier, data_df):

        self.moving_average_length = moving_average_length
        self.super_trend_length = super_trend_length
        self.super_trend_multiplier = super_trend_multiplier
        self.data_df = data_df


    
    def find_moving_average(self):
        mv = MovingAverage(data_df=self.data_df)
        self.data_df = mv.calculate_indicator_values(length = self.moving_average_length)
        


    def find_super_trend(self):
        st = SuperTrend(self.data_df)
        self.data_df = st.calculate_indicator_values(length = self.super_trend_length, multiplier=self.super_trend_multiplier)

    
    def calculate_strategy_data(self):
        self.find_moving_average()
        self.find_super_trend()
        self.data_df = self.data_df.fillna({"buy/sell signal":""})
        temp_res = self.data_df[(self.data_df['close'] > self.data_df['moving_average']) & (self.data_df['buy/sell signal'] != "")]
        return temp_res

