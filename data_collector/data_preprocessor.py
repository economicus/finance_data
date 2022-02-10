import FinanceDataReader as fdr
from datetime import datetime
import pandas as pd

class DataPreprocessor:
	
	def __init__(self):
		pass


	def print_replace_status(self, table, at, total, status_1=None, status_2=None):

		print('[{}] : ({}, {}) #{:04d} / {} rows > REPLACE INTO {} ({} %)[OK]'.\
				format(datetime.now().strftime('%Y-%m-%d %H:%M'), status_1, \
				status_2, at+1, total, table, ((at+1) / total * 100)))
		

	def print_create_status(self, table):

		print('[{}] : CREATE TABLE IF NOT EXISTS {} [OK]'.\
				format(datetime.now().strftime('%Y-%m-%d %H:%M'), table))


	def str_exception_out(self, string):
		string = str(string)
		string = string.replace("'", " ")
		string = string.replace("`", " ")
		string = string.replace("%", ".pct")
		return (string)


	def to_date(self, param):
		return (str(param)[:10])

	def util_zfill(self, code):
		return (str(code).zfill(6))

	def util_symbol(self, code):
		market = fdr.StockListing('KRX')
		return (market[market["Symbol"] == code]["Market"].values[0])


	def util_to_date(self, param):
		return (str(param)[:10])


	def get_cur_code(self):
		query = f"SELECT Symbol FROM cur_comp_info"
		df = pd.read_sql(query, con = self.engine)
		return (df["Symbol"].unique())


	def get_unique_code(self):
		unique_code_list = []
		query_1 = f"SELECT Code FROM market_open_info"
		df_1 = pd.read_sql(query_1, con = self.engine)
		df_1_list = df_1["Code"].unique()
		query_2 = f"SELECT Symbol FROM cur_comp_info"
		df_2 = pd.read_sql(query_2, con = self.engine)
		df_2_list = df_2["Symbol"].unique()
		unique_code_list.append(df_1_list)
		unique_code_list.append(df_2_list)
		unique_set = set(unique_code_list)
		unique_code_list = list(unique_set)
		return (unique_code_list)

	
	def year_gen(self):
		return ([str(year) for year in range(1995, 2023)])

	def get_adjclose(self, code):
		for year in self.year_gen():
			df = fdr.DataReader(f'{code}', f'{year}')
			if int(year) == df.index.values[0].astype('datetime64[Y]').astype(int) + 1970:
				break
		print(year, code)
		return (df)