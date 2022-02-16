from re import L
from connect import SQLAlchemyConnector
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta

class FindCode(SQLAlchemyConnector):
	
	def __init__(self):
		super().__init__()
		self.com_np, self.fin_np = self.bring_table()
		self.ref_date = []


	def bring_table(self):
		print('getting datas...')
		query1 = f"SELECT ID,Market,MainSector FROM company"
		com_df = pd.read_sql(query1, con = self.engine)
		query2 = f"SELECT * FROM finance"
		fin_df = pd.read_sql(query2, con = self.engine)
		# query3 = f"SELECT * FROM price"
		# pri_df = pd.read_sql(query3, con = self.engine)

		com_np = com_df.to_numpy()
		fin_np = fin_df.to_numpy()
		# pri_np = pri_df.to_numpy()
		print('complete!')
		return (com_np, fin_np)


	def rebalacing(self, start_date, end_date, rebalance):
		if end_date == None:
			# end_date = datetime.now().strftime('%Y-%m-%d')
			end_date = datetime(2021, 12, 31)
		while(True):
			temp = end_date
			end_date -= relativedelta(months = rebalance)
			if (start_date >= end_date):
				break
			self.ref_date.append([end_date, temp - timedelta(days = 1)])
	

	def apply_conditions(self, cond):
		"""
		parameter
			fs_conditions : 재무 조건 <type : >
				- 거래소[market] =com								<STRING>
				- 기업 섹터(대분류)[main_sector] =com				 	<STRING>
				- 매출액[net_rev] =fin								<INT:LIST>
				- 매출액 증가율[net_rev_r] # (x)					<FLOAT:LIST>
				- 당기순이익[net_prf] =fin 							<INT:LIST>
				- 순이익 증가율[net_prf_r] # (x) 					<FLOAT:LIST>
				- 부채 비중[de_r] =fin  							<FLOAT:LIST>
				- PER[per] =fin 								<FLOAT:LIST>
				- PSR(주가대비 매출)[psr] # (x) 					<FLOAT:LIST>
				- PBR[pbr] =fin 								<FLOAT:LIST>
				- 현금흐름 (영업[op_act], 투자[iv_act], 재무[fn_act]) 	<INT:LIST>
				- 현금배당수익률[dv_yld] =fin 							<FLOAT:LIST>
				- 현금배당성향[dv_pay_r] =fin 						<FLOAT:LIST>
				- ROA[roa] =fin 								<FLOAT:LIST>
				- ROE[roe] =fin 								<FLOAT:LIST>

			ts_conditions : 기술적 조건 <type : >
				- 시가총액[marcap] =pri 							<INT:LIST>
				- 거래량[vol] =pri 									<INT:LIST>
				- 거래대금[pvol] =pri 								<INT:LIST>
				- 주가이동평균(5[sma5], 20[sma20], 60[sma60], 120[sma120]) =pri(deri) # (x)
				- 주가지수평균(5[ema5], 20[ema20], 60[ema60], 120[ema120]) =pri(deri) # (x)
				- 과거 수익률(1[past_prf_r], 3, 6, 12계월) =pri(deri)	<FLOAT:LIST>


			date_conditions : 날짜 조건 <type : >
				- 검색 시작 날짜[start_date] <datetime>
				- 검색 끝 날짜(default = 오늘)[end_date] <datetime>
				- 리벨런싱 기간[term] <int>
		return
			codes(type : list)
		"""
		self.rebalacing(cond['start_date'], cond['end_date'], cond['term'])
		# filter set 
		# 1. market(com), main_sector(com)
		# 2. marcap(pri), 
		searched = self.find_com_condition(cond)
		codes = self.find_fin_condition(searched, cond)
		return (codes)


	def find_com_condition(self, cond):
		searching = self.com_np
		conditions = self.check_com_conditions(searching, cond)
		searching = searching[conditions] # filtered (market, main_sector)
		return (searching[:,0])

	def find_fin_condition(self, searched, cond):
		searching = self.fin_np
		mask = np.in1d(searching[:,0], searched) # code filtering from com_condition result
		searching = searching[mask]

		
		ref_date = self.ref_date
		codes = []
		for i in range(len(ref_date)):
			search_date = self.get_correct_date(searched, searching, i, ref_date)
			if (len(searching) == 0):
				continue
			conditions = self.check_fin_conditions(search_date, cond)
			search_date = search_date[conditions]
			codes.append([search_date[:,0], ref_date[i]])

		return (codes)


	def check_com_conditions(self, searching, cond):
		cond_idx = dict(market=1, main_sector=2)
		conditions = True
		con_list = []
		for k, v in cond_idx.items():
			if cond[str(k)] == None:
				continue
			else:
				conditions = conditions & (searching[:,v] == cond[str(k)])
				
		return (conditions)
		

	# none check and combine all conditions
	def check_fin_conditions(self, searching, cond):
		cond_idx = dict(net_rev=3, net_prf=4, de_r=5, per=6, pbr=8, op_act=9, iv_act=10,
						fn_act=11, dv_yld=12, dv_pay_r=13, roa=14, roe=15)
		conditions = True
		con_list = []
		for k, v in cond_idx.items():
			cond_arr = cond[str(k)]
			if cond_arr[0] == None and cond_arr[1] == None:
				continue
			else:
				conditions = conditions & (searching[:,v] >= cond_arr[0]) & (searching[:,v] <= cond_arr[1])
				
		return (conditions)


	# 만약 리벨런싱기간이 6계월 이상이면 기준일로 부터 과거 가장 최근 재무재표를 참고함
	def get_correct_date(self, codes, searching, i, ref_date):
		
		searching = searching[(searching[:,2] < ref_date[i][1].date()) & (searching[:,2] >= ref_date[i][0].date())] # set_date
		searching = searching[searching[:, 2].argsort()] # sort by date
		searching = searching[searching[:, 0].argsort(kind='mergesort')] # sort by ID
		np_list = []
		for c in codes:
			date = searching[searching[:,0] == c]
			if (len(date) == 0):
				continue
			elif (len(date) == 1):
				np_list.append(date[0])
			else:
				np_list.append(date[-1])
		searched = np.array(np_list)
		return (searched)
