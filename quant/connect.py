import sqlalchemy as db
from sqlalchemy.pool import NullPool

class SQLAlchemyConnector:
	
	def __init__(self):
		self.engine = db.create_engine('mysql+pymysql://root:passwd@localhost/db_name', \
			poolclass=NullPool)
		self.connection = self.engine.connect()