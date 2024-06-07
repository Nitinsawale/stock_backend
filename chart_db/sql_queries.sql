-- create table for stock list
CREATE TABLE  IF NOT EXISTS stock_list(
        id TEXT PRIMARY KEY, 
        upstox_id TEXT UNIQUE NOT NULL,
        symbol TEXT UNIQUE NOT NULL,
        instrument_type TEXT,
        exchange TEXT,
        name_of_company TEXT,
        date_of_listing datetime,
        isin_number TEXT,
        market_cap INTEGER
        );


--create table for candle price
CREATE TABLE IF NOT EXISTS candle_price(
        symbol TEXT NOT NULL,
        date datetime,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume INTEGER,
        time_interval TEXT,
            FOREIGN KEY(symbol) REFERENCES stock_list(symbol));


-- create table for screener database
CREATE TABLE IF NOT EXISTS screener_info(
        symbol TEXT NOT NULL,
        updated_date datetime,
        data_warehouse_id TEXT,
        data_company_id TEXT,
        company_link TEXT,
        nse_info TEXT,
        nse_link TEXT,
        bse_info TEXT,
        bse_link TEXT,
        FOREIGN KEY(symbol) REFERENCES stock_list(symbol));


-- create table for company ratios
CREATE TABLE IF NOT EXISTS company_ratios(
        symbol TEXT NOT NULL,
        ratio_name TEXT,
        ratio_value REAL,
            FOREIGN KEY(symbol) REFERENCES stock_list(symbol));

-- create table for company other ratios
CREATE TABLE IF NOT EXISTS other_ratios(
        symbol TEXT NOT NULL,
        month_year TEXT,
        debtor_days REAL,
        inventory_days REAL,
        days_payable REAL,
        cash_conversion_cycle REAL,
        working_capital_days REAL,
        roce REAL,
            FOREIGN KEY(symbol) REFERENCES stock_list(symbol));

--create table for shareholding
CREATE TABLE IF NOT EXISTS shareholding(
        symbol TEXT NOT NULL,
        type TEXT,
        month_year TEXT,
        promoters REAL,
        fiis REAL,
        diis REAL,
        public REAL,
        government REAL,
        others REAL,
        no_of_shareholders REAL,
            FOREIGN KEY(symbol) REFERENCES stock_list(symbol));

--create table for peers
CREATE TABLE IF NOT EXISTS cash_flows(
        symbol TEXT NOT NULL,
        month_year TEXT,
        operating_activity REAL,
        investing_activity REAL,
        financing_activity REAL,
        net_cash_flow REAL,
            FOREIGN KEY(symbol) REFERENCES stock_list(symbol));

--create table for quarterly results
CREATE TABLE IF NOT EXISTS quarterly_results(
        symbol TEXT NOT NULL,
        month_year TEXT,
        sales REAL,
        expenses REAL,
        operatingprofit REAL,
        opm REAL,
        otherincome REAL,
        interest REAL,
        depreciation REAL,
        profitbeforetax REAL,
        tax REAL,
        netprofit REAL,
        epsinrs REAL,
            FOREIGN KEY(symbol) REFERENCES stock_list(symbol));

--create table for profit loss
CREATE TABLE IF NOT EXISTS profit_loss(
        symbol TEXT NOT NULL,
        month_year TEXT,
        sales REAL,
        expenses REAL,
        operatingprofit REAL,
        opm REAL,
        otherincome REAL,
        interest REAL,
        depreciation REAL,
        profitbeforetax REAL,
        tax REAL,
        netprofit REAL,
        epsinrs REAL,
        dividendpayout REAL,
            FOREIGN KEY(symbol) REFERENCES stock_list(symbol));

--create table for balance sheet
CREATE TABLE IF NOT EXISTS balance_sheet(
        symbol TEXT NOT NULL,
        month_year TEXT,
        equity_capital REAL,
        reserves REAL,
        borrowings REAL,
        other_liabilities REAL,
        total_liabilities REAL,
        fixed_assets REAL,
        cwip REAL,
        investments REAL,
        other_assets REAL,
        total_assets REAL,
            FOREIGN KEY(symbol) REFERENCES stock_list(symbol));


