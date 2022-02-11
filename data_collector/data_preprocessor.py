import FinanceDataReader as fdr
from datetime import datetime, date
import pandas as pd
from pymysql import NULL
import numpy as np
from soupsieve import select

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

	def str_nan_out(self, string):
		if np.isnan(string):
			return 0
		else:
			return string


	def to_date(self, param):
		year = param
		return (str(param)[:10])


	def util_symbol(self, code, market):
		market_np = market.to_numpy()
		return (market_np[market_np[:, 0] == code][0][1])


	def util_to_strdate(self, param):
		return (str(param)[:10])


	def get_cur_code(self):
		query = f"SELECT ID,Symbol FROM company"
		df = pd.read_sql(query, con = self.engine)
		return (df)


	def get_additional_data(self, data, code):
		"""
		Date,Code,Amount,Marcap,Stocks,Ranks
		"""
		end = datetime(2021, 12, 30)
		selected = data[(data["Code"] == code) & (data["Date"] <= end.date())]
		return (selected)



	def bring_additional_data(self):
		query = f"SELECT Date,Code,Amount,Marcap,Stocks,Ranks FROM raw_price_info"
		raw_df = pd.read_sql(query, con = self.engine)
		return (raw_df)


	def bring_finance_data(self, code):
		try:
			col = [1, 6, 24, 27, 29, 14, 15, 16, 31, 32, 22, 23]
			datas = []

			df = pd.read_csv(f"data/financial_whole/{code.Symbol}.csv")
			for i in range(1, 9):
				tmp = df[f"{i}"]
				data = [tmp[j] for j in col]
				datas.append([tmp[0].replace("\n\t\t\t\t\t\t\t\t\t\n", ""), data])
			return (datas)
				
		except Exception as e:
			print(f"{e} : {code}")


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

	
	def compare_date(self, df_adj, df_add):
		adj_start = df_adj["Date"][0].date()
		add_start = df_add["Date"][0]
		if (adj_start == add_start):
			return df_adj, df_add
		elif (adj_start > add_start):
			return (df_adj, df_add[df_add["Date"] >= adj_start])
		elif (adj_start < add_start):
			return (df_adj[df_adj["Date"] >= datetime(add_start.year, add_start.month, add_start.day)], df_add)
		else:
			print("invalid_type")


	def year_gen(self):
		return ([str(year) for year in range(1995, 2023)])

	def get_adjclose(self, code):
		"""
		Open, High, Low, Close, Volume, Change
		"""
		for year in self.year_gen():
			df = fdr.DataReader(f'{code}', f'{year}')
			if int(year) == df.index.values[0].astype('datetime64[Y]').astype(int) + 1970:
				break
		start = df.index[0].to_pydatetime()
		# end = df.index[-1].to_pydatetime()
		df = df.reset_index()
		df = df[df["Date"] <= datetime(2021, 12, 30)]
		return (df)