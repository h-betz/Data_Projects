__author__ = 'Hunter'
from db import check_table_exists
import numpy as np
import pandas as pd
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

def format_record(record):
	record = [re.sub('\s+', ' ', record).strip() for r in record]
	return record

# Run the query to insert the record into our database
def insert_data(dbconn, tablename, columns, data):
	cur = dbconn.cursor()
	column_str = ','.join(columns)
	record = format_record(data)
	data_str = ','.join(data)
	query = 'INSERT INTO %s (%s) VALUES (%s);' % (tablename, column_str, data_str)
	cur.execute(query)

if __name__ == "__main__":
	# TODO Fill in empty columns
	# Load JSON info for db connection
	print('Connecting to database')
	db_file = open('db_info.json', 'r')
	db_json = json.load(db_file)
	db_file.close()
	conn_str = "host='%s' dbname='%s' user='%s' password='%s' port='%s'" % (db_json.get('host'), db_json.get('dbname'), db_json.get('user'), db_json.get('password'), db_json.get('port'))
	conn = psycopg2.connect(conn_str)
	print('Successfully connected to %s' % db_json.get('dbname'))

	if not check_table_exists(conn, 'brooklyn_sales'):
		# Load data from csv file to input into database
		csv_file = open('brooklyn_sales.csv', 'r')
		reader = csv.reader(csv_file)
		columns = next(reader)

		print('Creating table %s' % 'brooklyn_sales')
		create_table(conn, 'brooklyn_sales', columns)
		print('Table created.')
		print('Adding data to database...')
		for row in reader:
			insert_data(conn, 'brooklyn_sales', columns, row)
		conn.commit()
		print('Upload complete. Closing file connections.')
		csv_file.close()
	else:
		print('Data already loaded into database.')
		# TODO start predicting!