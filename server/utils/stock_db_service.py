import sqlite3
import os
from server.modules.stock_scraper.stock_list_retriever import get_stock_list
import pandas as pd

db_folder = "./chart_db/"
db_name = "data.sql"
conn = sqlite3.connect(db_folder + db_name)
cur = conn.cursor()

class CandleDB:

    def __init__(self, db_folder = "./chart_db/", db_name = "data.sql"):

        if not os.path.exists(db_folder):
            os.mkdir(db_folder)
        
        self.conn = conn
        self.cur = cur
        self.init_db()
    

    def init_db(self):
        
        try:
            with open("./chart_db/sql_queries.sql", 'r') as fp:
                sql_queries = fp.read()
            for query in sql_queries.split(";"):
                if query.strip():
                    self.cur.execute(query.strip())
            self.add_stock_list()
        except Exception as e:
            print(e)
            raise Exception("Failed to initialize db")
    

    def add_stock_list(self):
        
        if not self.get_stocks().empty:
            return 
        
        stock_list = get_stock_list()
        stock_list.to_sql('stock_list', self.conn, if_exists='replace')

    def get_candle_data(self, stock_id):

        print(stock_id)

    
    def insert_stock_data(self, stock_id, data = []):

        print(stock_id)


    def get_list_stocks_with_candle_data(self):
        pass
    
    def get_stocks(self):

        self.cur.execute("SELECT * from stock_list limit 5")
        data = self.cur.fetchall()
        columns = [description[0] for description in self.cur.description]
        df = pd.DataFrame(data = data, columns=columns)
        return df



db_obj = CandleDB()
