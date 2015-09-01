import sqlite3
from django.conf import settings

class sqlite3Db:
	dblink = ""
	connection = ""
	cursor = ""

	def __init__(self, dbPath = None):
		self.table = ""
		self.fields = "*"
		self.where = "1"
		self.order = ""
		self.limit = ""
		self.values = ""
		self.columns = ""
		self.qry = ""

		self.data = []
		self.insertArray = []
		self.updateData = ""

		if dbPath != None :
			self.dblink = dbPath
		else:
			self.dblink = settings.DATABASES['default']['NAME']

		self.connection = sqlite3.connect(self.dblink)
		self.connection.row_factory = sqlite3.Row
		self.cursor = self.connection.cursor()

	def close(self):
		self.connection.close()

	def select(self):
		self.qry = "SELECT "+ self.fields +" FROM "+ self.table +" WHERE "+ self.where +" "+ self.order +" "+ self.limit
		self.cursor.execute(self.qry)
		for row in self.cursor.fetchall():
			self.data.append(row)
		return self.data

	def insert(self):
		self.qry = "INSERT INTO "+ self.table +" ("+ self.columns +") VALUES ("+ self.values +")"
		self.cursor.executemany(self.qry, self.insertArray)
		self.connection.commit()

		self.fields = 'last_insert_rowid()'
		insertId = self.select()
		self.data = []
		return insertId[0][0]

	def delete(self):
		self.qry = "DELETE FROM "+ self.table +" WHERE "+ self.where
		self.connection.commit()
		return 1

	def update(self):
		self.qry = "UPDATE "+ self.table +" SET "+ self.columns +" WHERE "+ self.where
		self.cursor.execute(self.qry, self.updateData)
		self.connection.commit()
		return self.connection.total_changes