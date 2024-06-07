import pandas as pd


def calculate_quarterly_results_rating(df):
    df['comparison']  = 1
    df.loc[df['netprofit'] < df['netprofit'].shift(), 'comparison'] = 0
    no_of_times_profit_increased_from_prev = df['comparison'].eq(1).sum() 
    are_all_profit_are_positive = len(df[df['netprofit'] < 0]) == len(df)
    
    if are_all_profit_are_positive and no_of_times_profit_increased_from_prev > (len(df) / 2):
        return "GOOD"

    if not are_all_profit_are_positive and no_of_times_profit_increased_from_prev > (len(df) / 2):        
        return "AVG"

    return "BAD"


def calculate_cash_flow_rating(df):

    if df['net_cash_flow'].sum() == df['net_cash_flow'].abs().sum():
        return "GOOD"
    
    elif df['net_cash_flow'].sum() > 0:
        return "AVG"

    else:
        return "BAD"


def calculate_shareholding_rating(df):
    df['temp'] = df['promoters'].astype(int)

    if df['temp'].nunique() == 1:
        return "GOOD"

    if df['temp'].is_monotonic_increasing:
        return "GOOD"

    if df['temp'].is_monotonic_decreasing:
        return "BAD"

    if df['temp'][0] - df['temp'][-1] > 0:
        return "BAD"
    
    return "AVG"

def calculate_cash_flow_rating(df):

    if df['net_cash_flow'].is_monotonic_increasing:
        return "GOOD"
    
    if df['net_cash_flow'].is_monotonic_decreasing:
        return "BAD"

    if df['net_cash_flow'].sum() < 0:
        return "BAD"
    
    return "AVG"



def check_borrowings(df):

    if df['borrowings'].is_monotonic_increasing:
        if df['borrrowings'][-1] < df['reserves'][-1]:
            return "AVG"
        else:
            return "BAD"

    if df['borrowings'].is_monotonic_decreasing:
        if df['borrowings'][-1] > df['reserves'][-1]:
            return "AVG"
        else:
            return "GOOD"
    
    return "AVG"
        

def check_equity_capital(df):

    if df['equity_capital'].nunique() == 1 or df['equity_capital'].astype(int).nunique() == 1:
        return "GOOD"

    if df['equity_capital'].is_monotonic_increasing:
        return "GOOD"
    
    if df['equity_capital'].is_monotonic_decreasing:
        return "BAD"
    
    return "AVG"

def check_reserves(df):

    if df['reserves'].is_monotonic_increasing:
        return "GOOD"
    
    if df['reserves'].is_monotonic_decreasing:
        return "BAD"
    
    return "AVG"



def calculate_balance_sheet_rating(df):

    ratings = [0]
    
    ratings.append(check_equity_capital(df))
    ratings.append(check_reserves(df))
    ratings.append(check_borrowings(df))

    if ratings.count("GOOD") == 3:
        return "GOOD"
    
    if ratings.count("BAD") >= 3:
        return "BAD"
    
    return "AVG"
    

def calculate_stock_rating(stock_data):
    return_data = {}
    return_data['quarterly_rating'] = calculate_quarterly_results_rating(stock_data.get("quarterly_results"))
    return_data['profit_loss_rating'] = calculate_quarterly_results_rating(stock_data.get("profit_loss"))
    return_data['shareholding_rating'] = calculate_shareholding_rating(stock_data.get("shareholding"))
    return_data['balance_sheet_rating'] = calculate_balance_sheet_rating(stock_data.get("balance_sheet"))
    return_data['cash_flow_rating'] = calculate_cash_flow_rating(stock_data.get("cash_flows"))
    return return_data