__author__ = 'Hunter'
from hal import db
import numpy as np
from matplotlib import pyplot as plt

def freq_dist(x_axis, title, xlabel, ylabel):
	plt.hist(x_axis)
	plt.title(title)
	plt.xlabel(xlabel)
	plt.ylabel(ylabel)


if __name__ == "__main__":
	file_name = 'kag_risk_factors_cervical_cancer.csv'
	tablename = 'CERVICAL_CANCER_RISK'
	db.load_data(file_name, tablename)
