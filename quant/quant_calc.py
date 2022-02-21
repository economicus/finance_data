from struct import calcsize
from connect import SQLAlchemyConnector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import swifter
import time



class FindCode(SQLAlchemyConnector):
	
	def __init__(self):
		super().__init__()
		self.com_np, self.fin_np = self.bring_table()
		self.ref_date = []


	def bring_table(self):
		print('getting datas...(com_np, fin_np)')
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
	



	def find_com_condition(self, cond):
		searching = self.com_np
		conditions = self.check_com_conditions(searching, cond)
		# filtered (market, main_sector)
		searching = searching[conditions]
		return (searching[:,0])

	# none check and combine all conditions
	def check_com_conditions(self, searching, cond):
		cond_idx = dict(marcap=9)
		conditions = False
		for i in range(len(cond['main_sector'])):
			conditions = conditions | (searching[:,2] == cond['main_sector'][i])
		
		for k, v in cond_idx.items():
			cond_arr = cond[str(k)]
			if cond_arr[0] == None and cond_arr[1] == None:
				continue
			else:
				conditions = conditions & (searching[:,v] >= cond_arr[0]) & (searching[:,v] <= cond_arr[1])

		return (conditions)



	def find_fin_condition(self, searched, cond):
		searching = self.fin_np
		# code filtering from com_condition result
		mask = np.in1d(searching[:,0], searched)
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

	# none check and combine all conditions
	def check_fin_conditions(self, searching, cond):
		cond_idx = dict(net_rev=4, net_rev_r=5, net_prf=6, net_prf_r=7, de_r=8, \
						per=9, psr=10, pbr=11, pcr=12, op_act=13, iv_act=14,
						fn_act=15, dv_yld=16, dv_pay_r=17, roa=18, roe=19)
		conditions = True
		for k, v in cond_idx.items():
			cond_arr = cond[str(k)]
			if cond_arr[0] == None and cond_arr[1] == None:
				continue
			else:
				conditions = conditions & (searching[:,v] >= cond_arr[0]) & (searching[:,v] <= cond_arr[1])
				
		return (conditions)

	# 만약 리벨런싱기간이 6계월 이상이면 기준일로 부터 과거 가장 최근 재무재표를 참고함
	def get_correct_date(self, codes, searching, i, ref_date):
		
		searching = searching[(searching[:,3] < ref_date[i][1].date()) & (searching[:,3] >= ref_date[i][0].date())] # set_date
		searching = searching[searching[:, 3].argsort()] # sort by date
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


	def apply_conditions(self, **cond):
		"""
		:parameter
				- 기업 섹터(대분류)[main_sector] =com				 	<STRING>
				- 매출액[net_rev] =fin								<INT:LIST>
				- 매출액 증가율[net_rev_r] # (x)					<FLOAT:LIST>
				- 당기순이익[net_prf] =fin 							<INT:LIST>
				- 순이익 증가율[net_prf_r] # (x) 					<FLOAT:LIST>
				- 부채 비중[de_r] =fin  							<FLOAT:LIST>
				- PER[per] =fin 								<FLOAT:LIST>
				- PSR(주가대비 매출)[psr] # (x) 					<FLOAT:LIST>
				- PBR[pbr] =fin 								<FLOAT:LIST>
				- PCR[pcr]
				- 현금흐름 (영업[op_act], 투자[iv_act], 재무[fn_act]) 	<INT:LIST>
				- 현금배당수익률[dv_yld] =fin 							<FLOAT:LIST>
				- 현금배당성향[dv_pay_r] =fin 						<FLOAT:LIST>
				- ROA[roa] =fin 								<FLOAT:LIST>
				- ROE[roe] =fin 								<FLOAT:LIST>
				- 시가총액[marcap] =pri 							<INT:LIST>
				- 검색 시작 날짜[start_date] 						<datetime>
				- 검색 끝 날짜(default = 오늘)[end_date] 			<datetime>

		:return
			codes(type : list)
		"""

		self.rebalacing(cond['start_date'], cond['end_date'], cond['term'])
		searched = self.find_com_condition(cond)
		codes = self.find_fin_condition(searched, cond)
		return (codes)





class Calculate:

	def __init__(self):
		self.pri_np = self.bring_price_table()
		self.ref_date = 0


	def bring_price_table(self):
		print('getting datas...(pri_np)')
		pri_df = pd.read_csv("/Users/alvinlee/Git_Folder/stock/data/quant/price_monthly.csv")
		pri_df["Date"] = pri_df["Date"].swifter.apply(lambda x: datetime.strptime(x, "%Y-%m-%d"))
		pri_np = pri_df.to_numpy()
		print("complete!")
		return (pri_np)


	def get_profit(self, code):
		try:
			profits = []
			profit = self.last_profit
			pri_np = self.pri_np
			con_np = pri_np[(pri_np[:,0] == code) & (pri_np[:,1] >= self.ref_date[0]) \
							& (pri_np[:,1] < self.ref_date[1])]
			for i in range(len(con_np)-1):
				profit += (con_np[i+1,5] / con_np[i,5]) * 100 - 100
				profits.append(profit)
			return (profits)
		except Exception as e:
			print(e, code, self.ref_date)
			return (0)

	def calculate_term(self, cal_code, ret_date):
		self.ref_date = ret_date
		profit = 0
		result = [self.get_profit(code) for code in cal_code]
		result_np = np.array(result)
		profit = result_np.sum(axis=0)
		profit /= len(cal_code)
		self.last_profit = profit[-1]
		return profit, self.ref_date

	def calculate_profit(self, code_list):
		start = time.time()
		acc_profit = []
		self.last_profit = 0
		code_list = code_list[::-1]
		# calculate each term
		acc_profit = [self.calculate_term(i[0], i[1]) for i in code_list]
		# delta_t = time.time() - start
		# print("calculate Process: ",delta_t,"s")
		return (acc_profit)



if __name__ == "__main__":

	start = time.time()
	find_code = FindCode()
	code_list = find_code.apply_conditions(start_date=datetime(2018,1,1), end_date=None, \
									term=12, market="KOSPI", main_sector=["IT", "소재"], net_rev=[10000, 1000000000], \
									net_rev_r=[None, None], net_prf=[None, None], net_prf_r=[None, None], de_r=[None, None], \
									per=[0, 10], psr=[None, None], pbr=[None, None], pcr=[None, None], op_act=[None, None], iv_act=[None, None], \
									fn_act=[None, None], dv_yld=[None, None], dv_pay_r=[None, None], roa=[None, None], roe=[None, None], \
									marcap=[None, None], vol=None, pvol=None, sma5=None, sma20=None, \
									sma60=None, sma120=None, ema5=None, ema20=None, ema60=None, \
									ema120=None, past_prf_r1=None, past_prf_r3=None, \
									past_prf_r6=None, past_prf_r12=None)
	
	calculate = Calculate()
	ret = calculate.calculate_profit(code_list)

	print(ret)
	delta_t = time.time() - start
	print("total Process : ",delta_t,"s")