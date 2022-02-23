import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import swifter
import time

class DataLoad:
	def __init__(self):
		self.com_np, self.fin_np, self.pri_np = self.bring_datas()

	def bring_datas(self):
		print("getting datas...")
		com_df = pd.read_csv("quant/data/company.csv")
		fin_df = pd.read_csv("quant/data/finance.csv")
		pri_df = pd.read_csv("quant/data/price_monthly.csv")

		fin_df["Quarter"] = fin_df["Quarter"].swifter.apply(lambda x: datetime.strptime(x, "%Y-%m-%d"))
		pri_df["Date"] = pri_df["Date"].swifter.apply(lambda x: datetime.strptime(x, "%Y-%m-%d"))

		com_np = com_df.to_numpy()
		fin_np = fin_df.to_numpy()
		pri_np = pri_df.to_numpy()
		print("complete!")

		return com_np, fin_np, pri_np


class FindCode(DataLoad):
	def __init__(self):
		super().__init__()
		self.ref_date = []
	
	# list of rebalacing term
	def rebalacing(self, start, end):
		if end == None:
			end = datetime(2021, 12, 31,0,0,0)
		while(1):
			temp = end
			end -= relativedelta(years=1)
			if end < start:
				break
			self.ref_date.append([end, temp])

	def apply_company(self, main_sector):
		com_np = self.com_np
		if len(main_sector) == 0:
			conditions = True
		# if main_sector is not None filter conditions
		else:
			conditions = False
			# filter each main_sector with OR operater
			for ms in main_sector:
				conditions = conditions | (com_np[:,4] == ms)
		# filter apply
		com_np = com_np[conditions]
		# return True False list of IDs
		return com_np[:,0]

	def apply_each_finance(self, ref_date, fin_np, cond):
		# ref_date filtered
		fin_np = fin_np[(fin_np[:,3] >= ref_date[0].date()) & (fin_np[:,3] <= ref_date[1].date())]
		# numpy indexes
		cond_idx = dict(net_rev=4, net_rev_r=5, net_prf=6, net_prf_r=7, de_r=8, \
						per=9, psr=10, pbr=11, pcr=12, op_act=13, iv_act=14,
						fn_act=15, dv_yld=16, dv_pay_r=17, roa=18, roe=19)
		conditions = True
		# filter each finance conditions with AND operater
		for k, v in cond_idx.items():
			cond_arr = cond[str(k)]
			if cond_arr[0] == None and cond_arr[1] == None:
				continue
			else:
				conditions = conditions & (fin_np[:,v] >= cond_arr[0]) & (fin_np[:,v] <= cond_arr[1])
		# filter apply
		fin_np = fin_np[conditions]
		# return True False list of IDs
		return ([fin_np[:,0] , ref_date])

	def apply_finance(self, cond, conditions):
		fin_np = self.fin_np
		# apply con_np filter(conditions) to fin_np
		mask = np.in1d(fin_np[:,0], conditions)
		fin_np = fin_np[mask]
		conditions = [self.apply_each_finance(rf, fin_np, cond) for rf in self.ref_date]
		return (conditions)

	def apply_conditions(self, cond):
		self.rebalacing(cond["start_date"], cond["end_date"])
		conditions = self.apply_company(cond["main_sector"])
		conditions = self.apply_finance(cond, conditions)
		return (conditions)


class Calculate(DataLoad):
	def __init__(self):
		super().__init__()

	def calculate_profit(self, code_list):
		pass


class QuantCalc(FindCode, Calculate):
	def __init__(self):
		super().__init__()


	# execute all process
	def execute(self, **cond):
		# apply conditions to filter the stocks
		code_list = self.apply_conditions(cond)
		# put filtered stocks and calculate profits
		return_dict = self.calculate_profit(code_list)
		
		return return_dict
	




if __name__ == "__main__":
	a = QuantCalc()
	a.execute(start_date=datetime(2016,12,30,0,0,0), 
				end_date=None, 
				main_sector=['소재', '산업재'], 
				net_rev=[10000, 1000000000], 
				net_rev_r=[None, None], 
				net_prf=[None, None], 
				net_prf_r=[None, None], 
				de_r=[None, None], 
				per=[0, 10], 
				psr=[None, None], 
				pbr=[0, 10], 
				pcr=[None, None], 
				op_act=[0, 1000000], 
				iv_act=[-1000000, 0], 
				fn_act=[None, None], 
				dv_yld=[None, None], 
				dv_pay_r=[None, None], 
				roa=[None, None], 
				roe=[None, None], 
				marcap=[None, None])