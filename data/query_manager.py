from connect import SQLAlchemyConnector
from data_preprocessor import DataPreprocessor
import pandas as pd

class QueryManager(SQLAlchemyConnector, DataPreprocessor):
	def __init__(self):
		super().__init__()



	def create_raw_price_info_table(self):
		query = """
			CREATE TABLE IF NOT EXISTS raw_price_info (
				Date DATE,
				Code VARCHAR(20),
				Name VARCHAR(40),
				Market VARCHAR(20),
				MarketId VARCHAR(20),
				Open BIGINT(20),
				High BIGINT(20),
				Low BIGINT(20),
				Close BIGINT(20),
				ChangeCode BIGINT(20),
				Changes BIGINT(20), 
				ChagesRatio FLOAT(20),
				Volume FLOAT(20),
				Amount BIGINT(20),
				Marcap BIGINT(20),
				Stocks BIGINT(20),
				Ranks BIGINT(20),
				PRIMARY KEY (Date, Code, Market))
			"""
		result_proxy = self.connection.execute(query)
		result_proxy.close()



	def replace_raw_price_info_table(self, r, at, total):
		query = f"REPLACE INTO raw_price_info VALUES ('{self.to_date(r.Index)}', "\
				f"'{r.Code}', '{r.Name}', '{r.Market}', '{r.MarketId}', {r.Open}, {r.High}, "\
				f"{r.Low}, {r.Close}, {r.ChangeCode}, {r.Changes}, {r.ChagesRatio}, {r.Volume}, "\
				f"{r.Amount}, {r.Marcap}, {r.Stocks}, {r.Rank})"
		result_proxy = self.connection.execute(query)
		result_proxy.close()
		self.print_replace_status('raw_price_info', at, total, r.Index, r.Code)
			


	def select_raw_price_info_table(self):
		query = f"SELECT Date,Code FROM raw_price_info ORDER BY Date"
		df = pd.read_sql(query, con = self.engine)
		return (df)


	def create_cur_comp_info_table(self):
		query = """
			CREATE TABLE IF NOT EXISTS cur_comp_info ( 
				Symbol VARCHAR(20),
				Market VARCHAR(20),
				Name VARCHAR(200),
				Sector VARCHAR(200),
				Industry VARCHAR(200),
				ListingDate VARCHAR(20),
				HomePage VARCHAR(200),
				PRIMARY KEY (Symbol, Market))
			"""
		result_proxy = self.connection.execute(query)
		result_proxy.close()
		self.print_create_status('cur_comp_info')



	def replace_cur_comp_info_table(self, r, at, total):
		code = self.util_zfill(r.종목코드)
		market_id = self.util_symbol(code)
		industry = self.str_exception_out(str(r.주요제품))
		query = f"REPLACE INTO cur_comp_info VALUES('{code}', '{market_id}', \
					'{r.회사명}', '{r.업종}', '{industry}', '{r.상장일}', \
					'{r.홈페이지}')"
		result_proxy = self.connection.execute(query)
		result_proxy.close()
		self.print_replace_status('cur_comp_info', at, total, code)


	def create_market_open_info_table(self):
		query = """
			CREATE TABLE IF NOT EXISTS market_open_info ( 
				Date DATE,
				Code VARCHAR(20),
				PRIMARY KEY (Date, Code))
			"""
		result_proxy = self.connection.execute(query)
		result_proxy.close()
		self.print_create_status('market_open_info')


	def replace_market_open_info_table(self, r, at, total):
		query_2 = f"REPLACE INTO market_open_info VALUES ('{r.Date}', '{r.Code}')"
		result_proxy = self.connection.execute(query_2)
		result_proxy.close()
		self.print_replace_status('market_open_info', at, total, r.Date)



	def create_price_info_table(self):
		query_1 = """
			CREATE TABLE IF NOT EXISTS price_info ( 
				Code VARCHAR(20),
				Date DATE,
				Open VARCHAR(20),
				High VARCHAR(20),
				Low VARCHAR(20),
				AdjClose VARCHAR(20),
				Volume VARCHAR(20),
				Changes VARCHAR(200),
				PRIMARY KEY (Code, Date))
			"""
		result_proxy = self.connection.execute(query_1)
		result_proxy.close()

	def replace_price_info_table(self, code, r, at, total, at_code, total_code):
		query = f"REPLACE INTO price_info VALUES ('{code}', '{self.to_date(r.Index)}', \
				'{r.Open}', '{r.High}', '{r.Low}', '{r.Close}', '{r.Volume}', '{r.Change}')"
		result_proxy = self.connection.execute(query)
		result_proxy.close()
		self.print_replace_status('price_info', at, total, at_code, total_code)