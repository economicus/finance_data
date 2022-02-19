import FinanceDataReader as fdr
from datetime import datetime, date
import pandas as pd
from pymysql import NULL
import numpy as np
from soupsieve import select
import math
from operator import itemgetter
from marcap import marcap_data


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


	def print_update_status(self, table, at, total, status):

		print('[{}] : ({}) #{:04d} / {} rows > UPDATE {} ({} %)[OK]'.\
				format(datetime.now().strftime('%Y-%m-%d %H:%M'), status, \
				at+1, total, table, ((at+1) / total * 100)))



	def year_gen(self):
		return ([str(year) for year in range(1995, 2023)])

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

	def compare_date(self, np_adj, np_add):
		adj_start = np_adj[:,0][0]
		add_start = np_add[:,0][0]
		if (adj_start == add_start):
			return np_adj, np_add
		elif (adj_start > add_start):
			np_add = np_add[np_add[:,0] >= adj_start]
			np_add = np_add[np_add[:, 0].argsort()]
			return (np_adj, np_add)

	def util_symbol(self, code, market):
		market_np = market.to_numpy()
		return (market_np[market_np[:, 0] == code][0][1])

	def util_to_strdate(self, param):
		return (str(param)[:10])

	def to_num(self, num):
		if isinstance(num, float):
			if math.isnan(num):
				return NULL
		if isinstance(num, np.float64):
			return NULL
		num = num.replace(",", "")
		if "." in num:
			return(float(num))
		else:
			return (int(num))

	def to_date(self, param):
		return (str(param)[:10])

	def get_date(self, string):
		if isinstance(string, np.float64):
			return date(1900, 1, 1)
		string = string.replace("\n\t\t\t\t\t\t\t\t\t\n", "")
		year = int(string[0:4])
		month = int(string[5:7])
		return (date(year, month, 1))

	def give_none(self, np_adj, np_add, code):
		# "Date", "Code", "Amount", "Marcap", "Stocks", "Rank"
		for i in range(len(np_adj)):
			if np_adj[:,0][i] < np_add[:,0][i]:
				np_add = np.insert(np_add, i, np.array((np_adj[:,0][i], code.Symbol, 0, np_add[:,3][i-1], \
					np_add[:,4][i-1], 0)), 0)

		return np_adj, np_add

	def get_cur_code(self):
		query = f"SELECT ID,Symbol FROM company"
		df = pd.read_sql(query, con = self.engine)
		return (df)

	def get_additional_data(self, data, code):
		"""
		Date,Code,Amount,Marcap,Stocks,Ranks
		"""
		selected = data[(data[:,1] == code)]
		selected = selected[selected[:, 0].argsort()]
		selected = selected[selected[:,0] <= datetime(2021, 12, 30)]
		return (selected)

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

	def bring_additional_data(self):
		raw_df = marcap_data('1995-05-02', '2021-12-31')
		raw_df = raw_df.reset_index()
		raw_df = raw_df[["Date", "Code", "Amount", "Marcap", "Stocks", "Rank"]]
		raw_np = raw_df.to_numpy()
		return (raw_np)

	def bring_finance_data(self, code, path, path1):
		try:
			col = [1, 6, 24, 27, 29, 14, 15, 16, 31, 32, 22, 23]
			datas = []

			# quarter
			df = pd.read_csv(f"{path}/{code}.csv")
			for i in range(1, 9): # 9
				tmp = df[f"{i}"]
				data = [self.to_num(tmp[j]) for j in col]
				data.append(self.get_date(tmp[0]))
				datas.append(data)
			
			# annual
			df = pd.read_csv(f"{path1}/{code}.csv")
			for i in range(1, 3): # 9
				tmp = df[f"{i}"]
				data = [self.to_num(tmp[j]) for j in col]
				data.append(self.get_date(tmp[0]))
				datas.append(data)
			
			return (datas)
				
		except Exception as e:
			print(f"{e} : {code}")
			print(df[f"{i}"])

	def bring_main_sector_data(self, code, path):
		df = pd.read_csv(f"{path}/{code}.csv")
		a = df["0"][0]
		n = a.find("(")
		m = a.find(")")
		return a[n+1:m]

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
		df_np = df.to_numpy()
		return (df_np)

	def check_monthly(self, ymd_parm):
		if int(self.ymd) != int(ymd_parm[5:7]):
			self.ymd = int(ymd_parm[5:7])
			return True
		else:
			return False