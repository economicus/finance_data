from connect import SQLAlchemyConnector
from data_preprocessor import DataPreprocessor
import pandas as pd
from pymysql import NULL

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
				FOREIGN KEY (ID) REFERENCES company (ID))
			"""
		result_proxy = self.connection.execute(query_1)
		result_proxy.close()
		self.print_create_status('price')


	def replace_price_table(self, code, x, y, at, total, at_code, total_code):
		query = f"INSERT INTO price VALUES ({int(code.ID)}, '{self.to_date(y[0])}', \
				{int(x[1])}, {int(x[2])}, {int(x[3])}, {int(x[4])}, {int(x[5])}, \
				{int(y[2])}, {float(self.str_nan_out(x[6]))}, {int(y[3])}, \
				{int(y[4])}, {int(y[5])})"
		result_proxy = self.connection.execute(query)
		result_proxy.close()
		self.print_replace_status('price', at, total, at_code, total_code)


	def create_finance_table(self):
		query_1 = """
			CREATE TABLE IF NOT EXISTS finance (
				ID INT,
				Code VARCHAR(20),
				Quarter DATE,
				NetRevenue BIGINT(20),
				NetProfitMargin FLOAT(20),
				DERatio FLOAT(20),
				PER FLOAT(20),
				PSR FLOAT(20),
				PBR FLOAT(20),
				OperationActivities BIGINT(20),
				InvestingActivities BIGINT(20),
				FinancingActivities BIGINT(20),
				DividendYield FLOAT(20),
				DividendPayoutRatio FLOAT(20),
				ROA FLOAT(20),
				REO FLOAT(20),
				FOREIGN KEY (ID) REFERENCES company (ID))
			"""
		result_proxy = self.connection.execute(query_1)
		result_proxy.close()
		self.print_create_status('finance')


	def replace_finance_table(self, code, at, total, path, path1):
		try:
			f_list = self.bring_finance_data(code.Symbol, path, path1)
			for i in range(len(f_list)):
				query = f"INSERT INTO finance VALUES ({int(code.ID)}, '{code.Symbol}', \
						'{f_list[i][12]}', {f_list[i][0]}, {f_list[i][1]}, {f_list[i][2]}, \
						{f_list[i][3]}, {NULL}, {f_list[i][4]}, {f_list[i][5]}, {f_list[i][6]}, \
						{f_list[i][7]}, {f_list[i][8]}, {f_list[i][9]}, {f_list[i][10]}, \
						{f_list[i][11]})"
				result_proxy = self.connection.execute(query)
				result_proxy.close()
			self.print_replace_status('finance', at, total, code.Symbol)
		except Exception as e:
			print(e)
			self.print_replace_status('finance', at, total, 'no_files')


	def update_company_table(self, code, at, total, path):
		main_sector = self.bring_main_sector_data(code.Symbol, path)
		query = f"UPDATE company SET MainSector = '{main_sector}' "\
				f"WHERE Symbol = '{code.Symbol}'"
		result_proxy = self.connection.execute(query)
		result_proxy.close()
		self.print_update_status('company', at, total, code.Symbol)
