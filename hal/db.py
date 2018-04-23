__author__ = 'Hunter'
import csv
import re
import json
import psycopg2

# TODO add a check so we don't upload redundant data
# TODO add functionality to make this more general and not just table specific
class db():

	def __init__(self, tablename):
		self.columns = []
		self.tablename = tablename
		self.dbconn = self.connect()

	def get_columns(self):
		return self.columns

	# Formats records so they can be inserted into the database
	def format_record(record):
		record = [re.sub('\s+', ' ', r).strip().replace("'", '') for r in record]
		return record

	# Check if table exists
	def table_exists(self):
		existence_query = "select exists(select * from information_schema.tables where table_name='%s');" % self.tablename
		cur = self.dbconn.cursor()
		cur.execute(existence_query)
		return cur.fetchone()[0]

	# Create our table in the database
	def create_table(self, header):
		cur = self.dbconn.cursor()
		self.columns = ','.join(['%s text' % c for c in header])
		queries = [
			'CREATE TABLE %s(%s);' % (self.tablename, self.columns),
			'ALTER TABLE %s ADD COLUMN id serial;' % self.tablename,
			'ALTER TABLE %s ADD PRIMARY KEY (id);' % self.tablename,
		]
		for query in queries:
			cur.execute(query)
		self.dbconn.commit()

	# Run the query to insert the record into our database
	def insert_data(self, dbconn, tablename, columns, data):
		cur = dbconn.cursor()
		column_str = ','.join(columns)
		record = tuple(self.format_record(data))
		query = 'INSERT INTO %s (%s) VALUES %s;' % (tablename, column_str, record)
		cur.execute(query)

	# Connects to the database
	def connect(self):
		print('Connecting to database...')
		db_file = open('db_info.json', 'r')
		db_json = json.load(db_file)
		db_file.close()
		conn_str = "host='%s' dbname='%s' user='%s' password='%s' port='%s'" % (db_json.get('host'), db_json.get('dbname'), db_json.get('user'), db_json.get('password'), db_json.get('port'))
		conn = psycopg2.connect(conn_str)
		return conn

	# Load the data in the file specified by the variable file_name, into the database
	def load_data(self, file_name):
		csv_file = open(file_name, 'r')
		reader = csv.reader(csv_file)
		columns = next(reader)
		if not self.table_exists():
			self.create_table(columns)
		print('Loading data to database...')
		for row in reader:
			self.insert_data(columns, row)
		print('Finished loading data. Closing file and committing changes.')
		self.dbconn.commit()
		csv_file.close()
