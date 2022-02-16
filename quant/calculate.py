from connect import SQLAlchemyConnector
import pandas as pd
import numpy as np

class Calculate(SQLAlchemyConnector):

	def __init__(self):
		super().__init__()
		self.pri_np = self.bring_table()

	def bring_table(self):
		query = f"SELECT * FROM price"
		pri_df = pd.read_sql(query, con = self.engine)
		pri_np = pri_df.to_numpy()

		return (pri_np)



	def calculate_profit(self, codes):
		for code in codes:
			cal_code = code[0]
			ref_date = code[1]
			code[0]