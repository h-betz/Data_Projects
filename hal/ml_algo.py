__author__ = 'Hunter'
import numpy as np

def sigmoid(x):
	return 1 / (1 + np.exp(-x))

# Compute the cost for multiple variables
def compute_cost_mult(x, y, theta):
	m = y.size
	h = x.dot(theta)
	sq_errors = np.square(h - y)
	cost = (1 / (2 * m)) * np.sum(sq_errors)
	return cost

# Performs multiple linear regression
def multi_linear_regression(x, y, theta, alpha=0.01, iterations=300):
	m = y.size
	for j in range(iterations):
		h = x.dot(theta)
		theta = theta - (alpha / m) * np.sum(np.multiply(h - y), x)
	pass

def logistic_regression(x, y, theta):
	h = sigmoid(x.dot(theta))

	pass