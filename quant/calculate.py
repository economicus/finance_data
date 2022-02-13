from connect import SQLAlchemyConnector

class Calculate(SQLAlchemyConnector):

	def __init__(self):
		super().__init__()

	def calculate_profit(self, codes):
		pass