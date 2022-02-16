import sqlalchemy
from find_code import FindCode
from calculate import Calculate
from datetime import datetime


class QuantCalc(FindCode, Calculate):

	def __init__(self):
		super().__init__()
		self.ref_date = []
	



	def execute(self):
		cond = dict(start_date=datetime(2018,1,1), end_date=None, \
									term=6, market="KOSPI", main_sector=None, net_rev=[None, None], \
									net_rev_r=[None, None], net_prf=[None, None], net_prf_r=[None, None], de_r=[None, None], \
									per=[0, 10], psr=[None, None], pbr=[None, None], op_act=[None, None], iv_act=[None, None], \
									fn_act=[None, None], dv_yld=[None, None], dv_pay_r=[None, None], roa=[None, None], roe=[None, None], \
									marcap=None, vol=None, pvol=None, sma5=None, sma20=None, \
									sma60=None, sma120=None, ema5=None, ema20=None, ema60=None, \
									ema120=None, past_prf_r1=None, past_prf_r3=None, \
									past_prf_r6=None, past_prf_r12=None)
		codes = self.apply_conditions(cond)
		print(codes)
		# self.calculate_profit(codes)

if __name__ == "__main__":

	# [ [start_date, end_date, term], 
	#
	#	[market, main_sector, net_rev, net_rev_r, net_prf, net_prf_r, de_r,
	#	per, psr, pbr, op_act, iv_act, fn_act, dv_yld, dv_pay_r, roa, roe],
	#
	#	[marcap, vol, pvol, sma5, sma20, sma60, sma120, ema5, ema20, ema60,
	#	ema120, past_prf_r1, past_prf_r3, past_prf_r6, past_prf_r12] ]

	qc = QuantCalc()
	qc.execute()