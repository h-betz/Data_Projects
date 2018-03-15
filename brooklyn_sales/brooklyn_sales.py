__author__ = 'Hunter'
from db import check_table_exists
import csv
import json
import psycopg2

# Create our table in the database
def create_table(dbconn, tablename, header):
	cur = dbconn.cursor()
	columns = map(lambda c: '%s text' % c, header)
	queries = [
		'DROP TABLE %s;' % tablename,
		'CREATE TABLE %s(%s);' % (tablename, columns),
		'ALTER TABLE %s ADD COLUMN id serial;' % tablename,
		'ALTER TABLE %s ADD PRIMARY KEY (id);' % tablename,
	]
	for query in queries:
		cur.execute(query)


if __name__ == "__main__":
	# Load JSON info for db connection
	db_file = open('db_info.json', 'r')
	db_json = json.load(db_file)
	conn_str = 'host=%s dbname=%s user=%s password=%s port=%' % (db_json.get('host'), db_json.get('dbname'), db_json.get('user'), db_json.get('password'), db_json.get('port'))
	db_file.close()

	# Load data from csv file to input into database
	csv_file = open('brooklyn_sales_map.csv', 'r')
	reader = csv.reader(csv_file)
	columns = next(reader)[1:]

	conn = psycopg2.connect(conn_str)
	if not check_table_exists(conn, 'brooklyn_sales'):
		create_table(conn, 'brooklyn_sales')

	csv_file.close()