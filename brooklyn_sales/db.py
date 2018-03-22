__author__ = 'Hunter'
import csv
import re
import json
import psycopg2

def format_record(record):
	record = [re.sub('\s+', ' ', r).strip().replace("'", '') for r in record]
	return record

# Run the query to insert the record into our database
def insert_data(dbconn, tablename, columns, data):
	cur = dbconn.cursor()
	column_str = ','.join(columns)
	record = tuple(format_record(data))
	# data_str = ','.join(record)
	query = 'INSERT INTO %s (%s) VALUES %s;' % (tablename, column_str, record)
	try:
		cur.execute(query)
	except:
		print('a')

if __name__ == '__main__':
	db_file = open('db_info.json', 'r')
	db_json = json.load(db_file)
	db_file.close()
	conn_str = "host='%s' dbname='%s' user='%s' password='%s' port='%s'" % (db_json.get('host'), db_json.get('dbname'), db_json.get('user'), db_json.get('password'), db_json.get('port'))
	conn = psycopg2.connect(conn_str)
	csv_file = open('brooklyn_sales.csv', 'r')
	reader = csv.reader(csv_file)
	columns = next(reader)

	print('Adding data to database...')
	count = 1
	for row in reader:
		if count > 304816:
			insert_data(conn, 'brooklyn_sales', columns, row)
			print('%s' % count)
		count += 1
	conn.commit()
	csv_file.close()