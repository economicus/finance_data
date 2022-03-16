from operator import itemgetter
from tkinter import E
import FinanceDataReader as fdr
import pandas_datareader as pdr
from marcap import marcap_data
from query_manager import QueryManager
import pandas as pd
from datetime import datetime
import numpy as np
import time

class DataCollector(QueryManager):

	def __init__(self):
		super().__init__()
		self.codes = self.get_cur_code()


	def get_company_table(self):
		self.create_company_table()
		df_krx = pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download', header=0)[0]
		market = fdr.StockListing('KRX')
		total = len(df_krx)
		at = 0
		for r in df_krx.itertuples():
			self.replace_company_table(r, at, total, market)
			at+=1


	def get_price_table(self):
		self.create_price_table()
		data = self.bring_additional_data()
		total = len(self.codes)
		at = 0
		for code in self.codes.itertuples():
			try:
				np_adjclose = self.get_adjclose(code.Symbol)
				np_adddata = self.get_additional_data(data, code.Symbol)
				np_adjclose, np_adddata = self.compare_date(np_adjclose, np_adddata)
				np_adjclose, np_adddata = self.give_none(np_adjclose, np_adddata, code)
				total_code = len(np_adjclose)
				at_code = 0
				for i, (x, y) in enumerate(zip(np_adjclose, np_adddata)):
					self.replace_price_table(code, x, y, at, total, at_code, total_code)
					at_code+=1
				at+=1

			except Exception as e:
				print(f'{e} : {code}')


	def get_finance_table(self, path1):
		self.create_finance_table()
		total = len(self.codes)
		at = 0
		cnt = 0
		for code in self.codes.itertuples():
			cnt = self.replace_finance_table(code, at, total, cnt, path1)
			at+=1


	def get_raw_price_info(self):
		self.create_raw_price_info_table()
		raw_price_info_df = marcap_data('1995-05-02', '2021-12-31')
		total = len(raw_price_info_df)
		at = 0
		for r in raw_price_info_df.itertuples():
			self.replace_raw_price_info_table(r, at, total)
			at+=1


	def add_main_sector_tocompany(self, path):
		total = len(self.codes)
		at = 0
		for code in self.codes.itertuples():
			try:
				self.update_company_table(code, at, total, path)
				at+=1
			except Exception as e:
				print(e, code)


	def get_price_average_info(self):
		self.create_price_average_table()
		total = len(self.codes)
		at = 0
		for code in self.codes.itertuples():
			np_adjclose = self.get_adjclose(code.Symbol)
			total_code = len(np_adjclose)
			at_code = 0
			for i, x in enumerate(np_adjclose):
				self.replace_price_average_table(code, x, at, total, at_code, total_code)
				at_code+=1
			at+=1


	def get_price_monthly_info(self):
		self.create_price_monthly_info_table()
		data = self.bring_additional_data()
		total = len(self.codes)
		error_list = []
		at = 0
		for code in self.codes.itertuples():
			try:
				np_adjclose = self.get_adjclose(code.Symbol)
				np_adddata = self.get_additional_data(data, code.Symbol)
				np_adjclose, np_adddata = self.compare_date(np_adjclose, np_adddata)
				np_adjclose, np_adddata = self.give_none(np_adjclose, np_adddata, code)
				total_code = len(np_adjclose)
				at_code = 0
				for i, (x, y) in enumerate(zip(np_adjclose, np_adddata)):
					self.replace_price_monthly_table(code, x, y, at, total, at_code, total_code)
					at_code+=1
				at+=1
				

			except Exception as e:
				error_list.append([e, code])
				print(f'{e} : {code}')
				pass
		
		print(error_list)

	def get_market_open_info(self):
		self.create_market_open_info_table()
		df_open = self.select_raw_price_info_table()
		total = len(df_open)
		at = 0
		for r in df_open.itertuples():
			self.replace_market_open_info_table(r, at, total)
			at += 1


	def update_new_price_info(self):
		flag = 0
		data = None
		# ck = time.time()
		for code in self.codes.itertuples():
			new = self.get_new_price_info(code, '2022-01-03', '2022-01-03')
			if new is None:
				continue
			if flag == 0:
				flag = 1
				data = new
				continue
			data = np.append(data, new, axis=0)
			if flag == 4:
				break
		types = ['int64', 'object', 'int64', 'int64', 'int64', 'int64', 'int64', 'int64', 'float64', 'int64', 'int64', 'int64']
		df = pd.DataFrame(data)
		dic = { name:value for name, value in zip(df.columns, types) }
		df = df.astype(dic)
		# print(time.time() - ck) # 706 초
		df.to_csv('/Users/choewonjun/Documents/coding/finance_data/quant/data/price_monthly.csv', index=False, header=False,mode='a')


if __name__ == "__main__":
	# finance data path
	path = "data/annual_fs"
	dc = DataCollector()
	# dc.get_company_table()
	# dc.get_price_table()
	# dc.get_finance_table(path)

	# dc.add_main_sector_tocompany(path)
	dc.update_new_price_info()
	# dc.get_price_average_info()