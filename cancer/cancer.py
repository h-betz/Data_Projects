__author__ = 'Hunter'
from hal.db_controller import DB_Controller
import numpy as np
import csv
from matplotlib import pyplot as plt


# Plots histogram of the data in x
def freq_dist(x, title, xlabel, ylabel, count=1):
	fig = plt.figure(count)
	fig.add_subplot(111)
	fig.suptitle(title)
	if isinstance(x, list):
		plt.hist(x, label=xlabel)
		plt.legend(loc='upper right')
	else:
		plt.hist(x)
		fig.suptitle(title)
		plt.xlabel(xlabel)
		plt.ylabel(ylabel)
	plt.show()


def plot_bar(data, x, title, xlabels, ylabel, count=1):
	width = 0.35
	fig, ax = plt.subplots(count)
	x = np.arange(len(x))
	total = ax.bar(x - (2 * width/3), data[0], width, color='SkyBlue', label='Total')
	affect = ax.bar(x - width/3, data[1], width, color='Red', label='Affected')
	unaff = ax.bar(x + width/3, data[2], width, color='Yellow', label='Unaffected')

	# Adding labels and title
	ax.set_ylabel(ylabel)
	ax.set_title(title)
	ax.set_xticks(x)
	ax.set_xticklabels(xlabels)
	ax.legend()

	def autolabel(rects, xpos='center'):
		"""
		Attach a text label above each bar in *rects*, displaying its height.

		*xpos* indicates which side to place the text w.r.t. the center of
		the bar. It can be one of the following {'center', 'right', 'left'}.
		"""

		xpos = xpos.lower()  # normalize the case of the parameter
		ha = {'center': 'center', 'right': 'left', 'left': 'right'}
		offset = {'center': 0.5, 'right': 0.57, 'left': 0.43}  # x_txt = x + w*off

		for rect in rects:
			height = rect.get_height()
			ax.text(rect.get_x() + rect.get_width()*offset[xpos], 1.01*height,
					'{}'.format(height), ha=ha[xpos], va='bottom')

	autolabel(total, 'left')
	autolabel(affect, 'center')
	autolabel(unaff, 'right')
	plt.show()


def simple_plot(x, y, title, xlabel, ylabel, fig=plt.figure()):
	ax = fig.add_subplot(111)
	ax.plot(x, y, 'ro')
	ax.title(title)
	ax.xlabel(xlabel)
	ax.ylabel(ylabel)
	plt.show()


# Returns the frequency of each value in x
def get_frequency(x):
	counts = np.bincount(np.array(x))
	ii = np.nonzero(counts)[0]
	return list(zip(ii, counts[ii]))


# Impute the missing values of our data
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


# Write the cleaned, imputed data to a csv file
def clean_data(data):
	f = open('C://Users/Hunter/Desktop/Github/Kaggle/data/cancer_data.csv', 'w')
	f.write(','.join(data['header']) + '\n')
	f.close()
	f = open('C://Users/Hunter/Desktop/Github/Kaggle/data/cancer_data.csv', 'a')
	np.savetxt(f, data['data'], delimiter=",", fmt='%f')
	f.close()


if __name__ == "__main__":
	file_name = 'kag_risk_factors_cervical_cancer.csv'
	tablename = 'cervical_cancer_risk'
	db = DB_Controller(tablename)
	if not db.table_exists():
		# Pre-process data
		data = read_data(file_name)
		data['data'] = impute_data(data['data'])
		clean_data(data)
		file_name = 'C://Users/Hunter/Desktop/Github/Kaggle/data/cancer_data.csv'

		# Load data into db
		spec = {
			5: 'decimal',
			6: 'decimal',
			8: 'decimal',
			10: 'decimal',
		}
		db.load_data(file_name, spec=spec)
		columns = db.columns
	else:
		count = 1
		# Plot the biopsy results in relation to age
		title = 'Age'
		label = ['Patients with Diagnosis', 'Patients not Diagnosed', 'Patient Ages']
		ylabel = 'Number of Patients'
		query = 'SELECT "Age", "Biopsy" FROM %s;' % tablename
		fetch = db.execute_query(query)
		age = [tup[0] for tup in fetch]
		age_affected = [tup[0] for tup in fetch if tup[1]]
		age_unaffected = [tup[0] for tup in fetch if not tup[1]]
		freq_dist([age_affected, age_unaffected, age], title, label, ylabel, count)

		# Get the average age
		query = 'SELECT AVG("Age") FROM %s;' % tablename
		fetch = db.execute_query(query)
		avg_age = fetch[0][0]

		# Get the most common age
		query = 'SELECT mode() WITHIN GROUP (ORDER BY "Age") FROM %s;' % tablename
		fetch = db.execute_query(query)
		mode_age = fetch[0][0]

		"""
			Analysis: The data suggests that younger/younger-middle aged women are at higher risk of
			developing cervical cancer, however further analysis is needed. First, we see that the
			majority of patients in this dataset are less than 40 years old, yet this is the same
			group of women that have a higher incidence rate of cervical cancer. It is probably best
			to analyze this based on a percentage of their respective age group (i.e. 15% of women
			aged 20 - 40 had a positive biopsy whereas 8% of women over the age of 40 had a positive
			biopsy)
		"""


		# Plot the biopsy results relative to the number of sexual partners
		# Based on the data provided, it appears that patients with 1 - 3 sexual partners are at higher
		# risk than those with more. However, this could be misleading given the limited dataset
		count += 1
		query = 'SELECT "Number of sexual partners", "Biopsy" FROM %s;' % tablename
		fetch = db.execute_query(query)
		num_partners_freq = get_frequency([tup[0] for tup in fetch])
		num_partners, patients = [], []
		for tup in num_partners_freq:
			if tup[0] > 5:
				# Combine the group with more than 5 sexual partners
				patients[len(patients) - 1] += tup[1]
			else:
				num_partners.append(tup[0])
				patients.append(tup[1])
		num_affected_freq = get_frequency([tup[0] for tup in fetch if tup[1]])
		num_affected_partners, num_affected = [], []
		for tup in num_affected_freq:
			if tup[0] > 5:
				# Combine the group with more than 5 sexual partners
				num_affected[len(num_affected) - 1] += tup[1]
			else:
				num_affected_partners.append(tup[0])
				num_affected.append(tup[1])
		num_unaffected_freq = get_frequency([tup[0] for tup in fetch if not tup[1]])
		num_unaffected_partners, num_unaffected = [], []
		for tup in num_unaffected_freq:
			if tup[0] > 5:
				# Combine the group with more than 5 sexual partners
				num_unaffected[len(num_unaffected) - 1] += tup[1]
			else:
				num_unaffected_partners.append(tup[0])
				num_unaffected.append(tup[1])
		# label[2] = 'Number of sexual partners'
		ylabel = 'Number of patients'
		title = 'Biopsy Results by Number of Sexual Partners'
		xlabels = ['1', '2', '3', '4', '5 or more']
		plot_bar([patients, num_affected, num_unaffected], num_partners, title, xlabels, count)

		# Get the average number of sexual partners
		query = 'SELECT AVG("Number of sexual partners") FROM %s;' % tablename
		fetch = db.execute_query(query)
		avg_part = fetch[0][0]

		# Get the most common number of sexual partners
		query = 'SELECT mode() WITHIN GROUP (ORDER BY "Number of sexual partners") FROM %s;' % tablename
		fetch = db.execute_query(query)
		mode_part = fetch[0][0]

		"""
			Analysis: Similar to the age results above, the vast majority of patients having a positive
			diagnosis had a similar number of sexual partners as the vast majority of patients in the
			dataset. This doesn't support any concrete correlation that the number of sexual partners
			has a noticeably significant effect on biopsy results
		"""

		# TODO, clean up the following and perform more in depth queries to see if there is a trend
		# among a variety of life factors
		# There doesn't appear to be any noticeable trend in number of pregnancies and diagnosis
		count += 1
		query = 'SELECT "Num of pregnancies", "Biopsy" FROM %s;' % tablename
		fetch = db.execute_query(query)
		num_preg_freq = get_frequency([tup[0] for tup in fetch])
		num_pregnancies, patients = [], []
		for tup in num_preg_freq:
			num_pregnancies.append(tup[0])
			patients.append(tup[1])
		preg_affected = get_frequency([tup[0] for tup in fetch if tup[1]])
		preg_unaffected = get_frequency([tup[0] for tup in fetch if not tup[1]])
		label[2] = 'Number of pregnancies'
		# freq_dist([preg_affected, preg_unaffected, num_preg], 'Number of Pregnancies', label, ylabel, count)

		"""
			Analysis:
		"""
		pass




