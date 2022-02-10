import FinanceDataReader as fdr
import pandas_datareader as pdr
from marcap import marcap_data
from query_manager import QueryManager
import pandas as pd
from datetime import datetime

class DataCollector(QueryManager):

	def __init__(self):
		super().__init__()


	def get_raw_price_info(self):
		self.create_raw_price_info_table()
		raw_price_info_df = marcap_data('1995-05-02', '2021-12-31')
		total = len(raw_price_info_df)
		at = 0
		for r in raw_price_info_df.itertuples():
			self.replace_raw_price_info_table(r, at, total)
			at+=1


	def get_cur_comp_info(self):
		self.create_cur_comp_info_table()
		df_krx = pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download', header=0)[0]
		total = len(df_krx)
		at = 0
		for r in df_krx.itertuples():
			self.replace_cur_comp_info_table(r, at, total)
			at+=1


	def get_market_open_info(self):
		self.create_market_open_info_table()
		df_open = self.select_raw_price_info_table()
		total = len(df_open)
		at = 0
		for r in df_open.itertuples():
			self.replace_market_open_info_table(r, at, total)
			at+=1


	def get_price_info(self):
		self.create_price_info_table()
		self.codes = self.get_cur_code()
		total = len(self.codes)
		at = 0
		for code in self.codes:
			df_adjclose = self.get_adjclose(code)
			total_code = len(df_adjclose)
			at_code = 0
			for r in df_adjclose.itertuples():
				self.replace_price_info_table(code, r, at, total, at_code, total_code)
				at_code+=1
			at+=1


if __name__ == "__main__":
	dc = DataCollector()
	# dc.get_raw_price_info() # 45min
	# dc.get_cur_comp_info()
	# dc.get_market_open_info()
	dc.get_price_info()