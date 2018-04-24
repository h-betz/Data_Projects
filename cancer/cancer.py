__author__ = 'Hunter'
from hal.db_controller import DB_Controller
import numpy as np
import csv
from matplotlib import pyplot as plt

def freq_dist(x_axis, title, xlabel, ylabel):
	plt.hist(x_axis)
	plt.title(title)
	plt.xlabel(xlabel)
	plt.ylabel(ylabel)

def impute_data(data):
	col_mean = np.nanmean(data, axis=0)
	indices = np.where(np.isnan(data))
	data[indices] = np.take(col_mean, indices[1])
	return data

# Read the data from the file
def read_data(file_name):
	# Get the header row
	csv_file = open(file_name, 'r')
	reader = csv.reader(csv_file)
	header = next(reader)
	csv_file.close()

	# Read the data
	data = np.genfromtxt(file_name, delimiter=',', skip_header=1)
	return {
		'header': header,
		'data': data,
	}

def clean_data(data):
	f = open('C://Users/Hunter/Desktop/Github/Kaggle/data/cancer_data.csv', 'w')
	f.write(','.join(data['header']) + '\n')
	f.close()
	f = open('C://Users/Hunter/Desktop/Github/Kaggle/data/cancer_data.csv', 'a')
	np.savetxt(f, data['data'], delimiter=",", fmt='%f')
	f.close()


if __name__ == "__main__":
	file_name = 'kag_risk_factors_cervical_cancer.csv'
	tablename = 'CERVICAL_CANCER_RISK'

	# Pre-process data
	data = read_data(file_name)
	data['data'] = impute_data(data['data'])
	clean_data(data)

	# Load data into db
	file_name = 'C://Users/Hunter/Desktop/Github/Kaggle/data/cancer_data.csv'
	db = DB_Controller(tablename)
	spec = {
		5: 'decimal',
		6: 'decimal',
		8: 'decimal',
		10: 'decimal',
	}
	db.load_data(file_name, spec=spec)
	columns = db.columns

