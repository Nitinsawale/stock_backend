
from server.utils.stock_db_service import cur,conn
from server.modules.stock_scraper.screener_scarper import ScreenerScraper
from datetime import datetime
import pandas as pd
import re
import sqlite3

async def get_stock_fundamentals_from_screener(stock_symbol):

    screenScraper = ScreenerScraper()

    all_company_data = await screenScraper.fetch_stock_data(stock_symbol)
    data_warehouse_id = all_company_data.get("data_warehouse_id","")
    data_company_id = all_company_data.get("data_company_id", "")
    try:
        conn.execute("BEGIN TRANSACTION")

        
        screener_info_df = get_screener_info_df(all_company_data.get("company_ratio_and_meta"), stock_symbol, data_warehouse_id,data_company_id)
        
        ratio_info_df = get_company_ratios_df(all_company_data.get("company_ratio_and_meta",{}).get("company_ratios",{}), stock_symbol)
        
        quarterly_results = all_company_data.get("quarterly_results",[])
        quarterly_df = get_company_quarterly_result_df(quarterly_results, stock_symbol)
        

        profit_loss = all_company_data.get("profit_loss",[])
        profit_loss_df = get_company_profit_loss_df(profit_loss, stock_symbol)
        

        balance_sheet = all_company_data.get("balance_sheet",[])
        balance_sheet_df = get_company_balance_sheet_df(balance_sheet, stock_symbol)
        

        cash_flows = all_company_data.get("cash_flow",[])
        cash_flows_df = get_company_cash_flow_df(cash_flows, stock_symbol)
        

        other_ratios = all_company_data.get("ratios",[])
        other_ratios_df = get_company_other_ratio_df(other_ratios, stock_symbol)
        

        shareholding = all_company_data.get("shareholding",[])
        shareholding_df = get_company_shareholding_df(shareholding.get("quarterly_shareholding",[]), stock_symbol)
        
        screener_info_df.to_sql("screener_info", conn, if_exists="append", index=False)
        ratio_info_df.to_sql("company_ratios", conn, if_exists="append", index=False)
        quarterly_df.to_sql("quarterly_results", conn, if_exists = "append", index = False)
        profit_loss_df.to_sql("profit_loss", conn, if_exists = "append", index = False)
        balance_sheet_df.to_sql("balance_sheet", conn, if_exists = "append", index = False)
        cash_flows_df.to_sql("cash_flows", conn, if_exists = "append", index = False)
        other_ratios_df.to_sql("other_ratios", conn, if_exists = "append", index = False)
        shareholding_df.to_sql("shareholding", conn, if_exists = "append", index = False)
        conn.commit()
    except sqlite3.Error as e:
        print("Errr ->>>>>")
        print(e)

        conn.rollback()



def remove_all_existing_data(stock_symbol):

    tables = ['screener_info', 'company_ratios', 'quarterly_results', 'profit_loss', 'balance_sheet', 'cash_flows', 'other_ratios', 'shareholding']

    for table in tables:
        query = f"delete from {table} where symbol = '{stock_symbol}';"
        cur.execute(query)




def get_screener_info_df(company_metadata, stock_symbol, data_warehouse_id, data_company_id):
    data = {
            "symbol": stock_symbol,
            "updated_date": datetime.today(),
            "data_warehouse_id": data_warehouse_id,
            "data_company_id": data_company_id
        }

    company_links = company_metadata.get("company_links",[])
    for link in  company_links:
        if link.get("link_type") == 'bse':
            data['bse_info'] = link.get("link_text","")
            data['bse_link'] = link.get("link","")
        elif link.get("link_type") == 'nse':
            data['nse_info'] = link.get("link_text","")
            data['nse_link'] = link.get("link","")
        elif link.get("link_type") == 'company':
            data['company_link'] = link.get("link_text","")

    return pd.DataFrame([data])


def get_company_ratios_df(company_ratios, stock_symbol):
    regex_format = re.compile("\s+|\\|")
    
    ratios = [{"symbol":stock_symbol, "ratio_name": re.sub(regex_format,"",key).lower(), "ratio_value":val} for key, val in company_ratios.items()]

    return pd.DataFrame(ratios)



def get_company_quarterly_result_df(quarterly_results, stock_symbol):
    regex_format = re.compile("\\s+|\\|")
    df = pd.DataFrame(quarterly_results)
    df = df.rename(columns = {"":"month_year"})
    df = df.T
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    columns = df.columns.tolist()
    cols_to_replace = {x:re.sub(regex_format,"",x).replace("%","").lower() for x in columns}
    df.rename(cols_to_replace, inplace=True, axis= 1)
    df.drop(columns=['rawpdf'], inplace=True)
    df['symbol'] = stock_symbol
    if df.columns.name == 'month_year':
        df['month_year'] = df.index
    df = update_existing_data(stock_symbol, df, "quarterly_results", "month_year")
    return df


def get_company_profit_loss_df(profit_loss_data, stock_symbol):
    regex_format = re.compile("\\s+|\\|")
    df = pd.DataFrame(profit_loss_data)
    df = df.rename(columns = {"":"month_year"})
    df = df.T
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    columns = df.columns.tolist()
    cols_to_replace = {x:re.sub(regex_format,"",x).replace("%","").lower() for x in columns}
    df.rename(cols_to_replace, inplace=True, axis= 1)
    df['symbol'] = stock_symbol
    if df.columns.name == 'month_year':
        df['month_year'] = df.index
    df = update_existing_data(stock_symbol, df, "profit_loss", "month_year")
    return df


def get_company_balance_sheet_df(balance_sheet_data, stock_symbol):
    regex_format = re.compile("\\s+|\\|")
    df = pd.DataFrame(balance_sheet_data)
    df = df.rename(columns = {"":"month_year"})
    df = df.T
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    columns = df.columns.tolist()
    cols_to_replace = {x:re.sub(regex_format,"_",x.strip()).replace("%","").lower() for x in columns}
    df.rename(cols_to_replace, inplace=True, axis=1)
    df['symbol'] = stock_symbol
    if df.columns.name == 'month_year':
        df['month_year'] = df.index
    df = update_existing_data(stock_symbol, df, "balance_sheet", "month_year")
    return df


def get_company_cash_flow_df(cash_flow_data, stock_symbol):
    regex_format = re.compile("\\s+|\\|")
    df = pd.DataFrame(cash_flow_data)
    df = df.rename(columns =    {"":"month_year"})
    df = df.T
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    columns = df.columns.tolist()
    cols_to_replace = {x:re.sub(regex_format,"_",x.strip()).replace("%","").lower() for x in columns}
    df.rename(cols_to_replace, inplace=True, axis= 1)
    df['symbol'] = stock_symbol
    
    col_to_replace = {
        "cash_from_operating_activity":"operating_activity",
        "cash_from_investing_activity":"investing_activity",
        "cash_from_financing_activity":"financing_activity"
    }
    df.rename(columns  = col_to_replace, inplace=True)
    if df.columns.name == 'month_year':
        df['month_year'] = df.index
    df = update_existing_data(stock_symbol, df, "cash_flows", "month_year")
    return df


def get_company_other_ratio_df(other_ratio_data, stock_symbol):
    regex_format = re.compile("\\s+|\\|")
    df = pd.DataFrame(other_ratio_data)
    df = df.rename(columns = {"":"month_year"})
    df = df.T
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    columns = df.columns.tolist()
    cols_to_replace = {x:x.replace("%","").strip().replace(" ","_").lower() for x in columns}
    df.rename(cols_to_replace, inplace=True, axis= 1)
    df['symbol'] = stock_symbol
    if df.columns.name == 'month_year':
        df['month_year'] = df.index
    df = update_existing_data(stock_symbol, df, "other_ratios", "month_year")
    return df


def get_company_shareholding_df(shareholding_data, stock_symbol, type="quarterly"):
    cols_to_keep = ['promoters', 'symbol', 'type', 'fiis', 'diis','public',]
    regex_format = re.compile("\\s+|\\|\.")
    df = pd.DataFrame(shareholding_data)
    df = df.rename(columns = {"":"month_year"})
    df = df.T
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    columns = df.columns.tolist()
    cols_to_replace = {x:re.sub(regex_format,"_",x.strip()).replace(".","").replace("%","").lower() for x in columns}
    df.rename(cols_to_replace, inplace=True, axis= 1)

    df['symbol'] = stock_symbol
    df['type'] = type
    if df.columns.name == 'month_year':
        df['month_year'] = df.index
    df = update_existing_data(stock_symbol, df, "shareholding", "month_year")
    return df


def get_table_data_from_db(stock_symbol, table):

    query = f"select * from {table} where symbol = '{stock_symbol}'"
    df = pd.read_sql_query(query, conn)
    return df



def update_existing_data(stock_symbol, new_df, table, column_to_check):

    old_table = get_table_data_from_db(stock_symbol, table)
    updated_df = pd.concat([old_table, new_df]).drop_duplicates(keep = False)
    return updated_df

