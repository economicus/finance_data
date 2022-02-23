import sqlalchemy as db

class SQLAlchemyConnector:
	
	def __init__(self):
		self.engine = db.create_engine('mysql+pymysql://root:passwd@localhost/db_name')
		self.connection = self.engine.connect()

