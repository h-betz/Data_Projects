__author__ = 'Hunter'
from db import check_table_exists
import numpy as np
import pandas as pd
import csv
import json
import psycopg2

# Create our table in the database
def create_table(dbconn, tablename, header):
	cur = dbconn.cursor()
	columns = ','.join(['%s text' % c for c in header])
	queries = [
		'DROP TABLE %s;' % tablename,
		'CREATE TABLE %s(%s);' % (tablename, columns),
		'ALTER TABLE %s ADD COLUMN id serial;' % tablename,
		'ALTER TABLE %s ADD PRIMARY KEY (id);' % tablename,
	]
	for query in queries:
		cur.execute(query)

def insert_data(dbconn, tablename, columns, data):
	cur = dbconn.cursor()
	column_str = ','.join(columns)
	data_str = ','.join(data)
	query = 'INSERT INTO %s (%s) VALUES (%s)' % (tablename, column_str, data_str)
	dbconn.commit()

if __name__ == "__main__":
	# TODO Fill in empty columns
	# data = np.genfromtxt('brooklyn_sales.csv', delimiter=',')
	data = pd.read_csv('brooklyn_sales.csv')

	# Load JSON info for db connection
	db_file = open('db_info.json', 'r')
	db_json = json.load(db_file)
	conn_str = "host='%s' dbname='%s' user='%s' password='%s' port='%s'" % (db_json.get('host'), db_json.get('dbname'), db_json.get('user'), db_json.get('password'), db_json.get('port'))
	db_file.close()

	# Load data from csv file to input into database
	csv_file = open('brooklyn_sales.csv', 'r')
	reader = csv.reader(csv_file)
	columns = next(reader)

	conn = psycopg2.connect(conn_str)
	create_table(conn, 'brooklyn_sales', columns)
	for row in reader:
		insert_data(conn, 'brooklyn_sales', columns, row)

	csv_file.close()