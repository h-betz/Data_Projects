__author__ = 'Hunter'
import csv
import re
import json
import psycopg2

# TODO add a check so we don't upload redundant data
# TODO add functionality to make this more general and not just table specific
class DB_Controller():

	def __init__(self, tablename):
		self.columns = []
		self.tablename = tablename
		self.dbconn = self.connect()

	# Connects to the database
	def connect(self):
		print('Connecting to database...')
		db_file = open('C://Users/Hunter/Desktop/Github/Kaggle/hal/db_info.json', 'r')
		db_json = json.load(db_file)
		db_file.close()
		conn_str = "host='%s' dbname='%s' user='%s' password='%s' port='%s'" % (db_json.get('host'), db_json.get('dbname'), db_json.get('user'), db_json.get('password'), db_json.get('port'))
		conn = psycopg2.connect(conn_str)
		return conn

	def set_columns(self, header):
		for col in header:
			self.columns.append('"%s"' % col)

	def format_columns(self, header, spec=None):
		if spec:
			columns = []
			for idx, col in enumerate(header):
				if idx in spec:
					c = '"%s" %s' % (col, spec[idx])
				else:
					c = '"%s" int' % col
				columns.append(c)
			return ','.join(columns)
		else:
			return ','.join(['"%s" text' % c for c in header])

	# Formats records so they can be inserted into the database
	def format_record(self, record, spec=None):
		new_record = []
		if spec:
			for idx, r in enumerate(record):
				if idx in spec:
					convert = spec[idx]
					if convert == 'decimal':
						new_record.append(float(r))
				else:
					new_record.append(int(float(r)))
		else:
			new_record = [re.sub('\s+', ' ', r).strip().replace("'", '') for r in record]
		return new_record

	# Check if table exists
	def table_exists(self):
		existence_query = "select exists(select * from information_schema.tables where table_name='%s');" % self.tablename
		cur = self.dbconn.cursor()
		cur.execute(existence_query)
		return cur.fetchone()[0]

	# Create our table in the database
	def create_table(self, columns):
		cur = self.dbconn.cursor()
		queries = [
			'DROP TABLE %s;' % self.tablename,
			'CREATE TABLE %s(%s);' % (self.tablename, columns),
			'ALTER TABLE %s ADD COLUMN id serial;' % self.tablename,
			'ALTER TABLE %s ADD PRIMARY KEY (id);' % self.tablename,
		]
		for query in queries:
			cur.execute(query)
		self.dbconn.commit()

	# Run the query to insert the record into our database
	def insert_data(self, row, spec=None):
		cur = self.dbconn.cursor()
		column_str = ','.join(self.columns)
		# column_str = self.columns
		record = tuple(self.format_record(row, spec=spec))
		query = 'INSERT INTO %s (%s) VALUES %s;' % (self.tablename, column_str, record)
		cur.execute(query)

	# Load the data in the file specified by the variable file_name, into the database
	def load_data(self, file_name, spec=None):
		csv_file = open(file_name, 'r')
		reader = csv.reader(csv_file)
		header = next(reader)
		self.set_columns(header=header)
		formatted_columns = self.format_columns(header, spec=spec)
		if not self.table_exists():
			self.create_table(formatted_columns)
		print('Loading data to database...')
		for row in reader:
			self.insert_data(row, spec=spec)
		print('Finished loading data. Closing file and committing changes.')
		self.dbconn.commit()
		csv_file.close()
