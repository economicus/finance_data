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


	def create_company_table(self):
		query = """
			CREATE TABLE IF NOT EXISTS company ( 
				ID	INT,
				Symbol VARCHAR(20),
				Market VARCHAR(20),
				Name VARCHAR(200),
				MainSector VARCHAR(200),
				Sector VARCHAR(200),
				Industry VARCHAR(200),
				ListingDate DATE,
				HomePage VARCHAR(200),
				PRIMARY KEY (ID, Symbol))
			"""
		result_proxy = self.connection.execute(query)
		result_proxy.close()
		self.print_create_status('company')



	def replace_company_table(self, r, at, total, market):
		code = str(r.종목코드).zfill(6)
		market_id = self.util_symbol(code, market)
		industry = self.str_exception_out(str(r.주요제품))
		query = f"REPLACE INTO company VALUES({at}, '{code}', '{market_id}', \
					'{r.회사명}', 'NULL', '{r.업종}', '{industry}', '{r.상장일}', \
					'{r.홈페이지}')"
		result_proxy = self.connection.execute(query)
		result_proxy.close()
		self.print_replace_status('company', at, total, code)


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



	def create_price_table(self):
		query_1 = """
			CREATE TABLE IF NOT EXISTS price (
				ID INT,
				Code VARCHAR(20),
				Date DATE,
				Open BIGINT(20),
				High BIGINT(20),
				Low BIGINT(20),
				AdjClose BIGINT(20),
				Volume BIGINT(20),
				PVolume BIGINT(20),
				Changes FLOAT(20),
				Marcap BIGINT(20),
				Stocks BIGINT(20),
				Ranks INT,
				PRIMARY KEY (ID, Code),
				FOREIGN KEY (ID) REFERENCES company (ID))
			"""
		result_proxy = self.connection.execute(query_1)
		result_proxy.close()
		self.print_create_status('price')


	def replace_price_table(self, code, r, at, total, at_code, total_code, data):
		addtional_data = self.get_additional_data(data, code, r.Index)
		query = f"REPLACE INTO price VALUES ({int(code.ID)}, '{code.Symbol}', '{self.to_date(r.Index)}', \
				{int(r.Open)}, {int(r.High)}, {int(r.Low)}, {int(r.Close)}, {int(r.Volume)}, \
				{int(addtional_data[2])}, {float(self.str_nan_out(r.Change))}, {int(addtional_data[3])}, \
				{int(addtional_data[4])}, {int(addtional_data[5])})"
		result_proxy = self.connection.execute(query)
		result_proxy.close()
		self.print_replace_status('price', at, total, at_code, total_code)

