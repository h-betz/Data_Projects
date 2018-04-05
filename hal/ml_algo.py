__author__ = 'Hunter'
import numpy as np

def sigmoid(x):
	return 1 / (1 + np.exp(-x))

# Compute the cost for multiple variables
def compute_cost(x, y, theta):
	m = y.size
	h = x.dot(theta)
	sq_errors = np.square(h - y)
	cost = (1 / (2 * m)) * np.sum(sq_errors)
	return cost

def normal_equation(x, y):
	return np.linalg.inv(x.T.dot(x)).dot(x.T).dot(y)

# Performs multiple linear regression
def multi_linear_regression(x, y, theta, alpha=0.01, iterations=300):
	m = len(x)
	cost = np.zeros(iterations)
	for i in range(iterations):
		h = x.dot(theta)
		theta = theta - ((alpha * (1 / m)) * np.sum(np.multiply(h - y, x)))
		cost[i] = compute_cost(x, y, theta)
	return theta, cost

def multi_logistic_regression(x, y, theta, alpha):
	pass

def logistic_regression(x, y, theta):
	h = sigmoid(x.dot(theta))

	pass