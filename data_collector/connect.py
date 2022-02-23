import sqlalchemy as db

class SQLAlchemyConnector:
	
	def __init__(self):
		self.engine = db.create_engine('mysql+pymysql://root:jehmno316@localhost/finance_db')
		self.connection = self.engine.connect()

