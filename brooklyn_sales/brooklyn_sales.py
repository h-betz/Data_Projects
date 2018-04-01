__author__ = 'Hunter'
import numpy as np
import re
import csv
import json
import psycopg2


# Check if table exists
def check_table_exists(dbconn, tablename):
	existence_query = "select exists(select * from information_schema.tables where table_name='%s');" % tablename
	cur = dbconn.cursor()
	cur.execute(existence_query)
	return cur.fetchone()[0]

# Create our table in the database
def create_table(dbconn, tablename, header):
	cur = dbconn.cursor()
	columns = ','.join(['%s text' % c for c in header])
	queries = [
		'CREATE TABLE %s(%s);' % (tablename, columns),
		'ALTER TABLE %s ADD COLUMN id serial;' % tablename,
		'ALTER TABLE %s ADD PRIMARY KEY (id);' % tablename,
	]
	for query in queries:
		cur.execute(query)
	dbconn.commit()

# Formats the string so it can be used in the query
def format_record(record):
	record = [re.sub('\s+', ' ', r).strip().replace("'", '') for r in record]
	return record

# Run the query to insert the record into our database
def insert_data(dbconn, tablename, columns, data):
	cur = dbconn.cursor()
	column_str = ','.join(columns)
	record = tuple(format_record(data))
	query = 'INSERT INTO %s (%s) VALUES %s;' % (tablename, column_str, record)
	cur.execute(query)

# Calculates median and inserts these values into the missing values of our data
def preprocess_data(data):
	temp_dat = []
	for dat in data:
		if dat > 0:
			temp_dat.append(dat)
	temp_dat = np.array(temp_dat)
	median = np.median(temp_dat)
	for idx, dat in enumerate(data):
		if dat == 0:
			data[idx] = median
	return data

if __name__ == "__main__":
	# Load JSON info for db connection
	print('Connecting to database')
	db_file = open('db_info.json', 'r')
	db_json = json.load(db_file)
	db_file.close()
	conn_str = "host='%s' dbname='%s' user='%s' password='%s' port='%s'" % (db_json.get('host'), db_json.get('dbname'), db_json.get('user'), db_json.get('password'), db_json.get('port'))
	conn = psycopg2.connect(conn_str)
	conn.autocommit = True
	print('Successfully connected to %s' % db_json.get('dbname'))

	csv_file = open('brooklyn_sales.csv', 'r')
	reader = csv.reader(csv_file)
	columns = next(reader)
	if not check_table_exists(conn, 'brooklyn_sales'):
		# Load data from csv file to input into database
		print('Creating table %s' % 'brooklyn_sales')
		create_table(conn, 'brooklyn_sales', columns)
		print('Table created.')
		print('Adding data to database...')
		count = 1
		for row in reader:
			insert_data(conn, 'brooklyn_sales', columns, row)
			print('%s' % count)
			count += 1
		print('Upload complete. Closing file connections.')
		csv_file.close()
	else:
		print('Data already loaded into database.')

	# Get data from db
	cur = conn.cursor()
	query = 'SELECT gross_sqft, sale_price FROM brooklyn_sales LIMIT 10000;'
	cur.execute(query)
	fetch = cur.fetchall()
	sqft = []
	price = []
	for tup in fetch:
		sqft.append(int(float(tup[0])))
		price.append(int(float(tup[1])))
	sqft = preprocess_data(np.array(sqft))
	price = preprocess_data(np.array(price))
	theta = np.zeros((1,1))