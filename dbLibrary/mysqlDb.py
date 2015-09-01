import mysql.connector
from django.conf import settings

class mySql:
	user = "root"
	password = "1234"
	database = "pretarget"
	host = "127.0.0.1"

	table = ""
	fields = "*"
	where = "1"
	order = ""
	limit = ""
	values = ""
	columns = ""
	qry = ""
	data = []
	insertArray = []
	updateData = ""

	connection = ""
	cursor = ""
	
	def __init__(self, dbObject = None):
		self.data = []
		self.insertArray = []
		self.updateData = ""
		
		if dbObject != None :
			self.connection = mysql.connector.connect(user=dbObject['user'], password=dbObject['password'], host=self.host, database=dbObject['database'])
		else:
			self.connection = mysql.connector.connect(user=self.user, password=self.password, host=self.host,
                              database=self.database)

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
		self.cursor.execute(self.qry, self.insertArray)
		self.connection.commit()
		return self.cursor.lastrowid

	def delete(self):
		self.qry = "DELETE FROM "+ self.table +" WHERE "+ self.where
		print self.qry
		self.cursor.execute(self.qry)
		rowsAffected = self.cursor.rowcount
		self.connection.commit()
		return rowsAffected

	def update(self):
		self.qry = "UPDATE "+ self.table +" SET "+ self.columns +" WHERE "+ self.where
		self.cursor.execute(self.qry, self.updateData)
		rowsAffected = self.cursor.rowcount
		self.connection.commit()
		return rowsAffected