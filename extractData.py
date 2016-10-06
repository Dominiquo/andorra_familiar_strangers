import csv
from datetime import datetime
import matplotlib.pyplot as plt

toy_file = '../workspace/yleng/CDRVisitDay/406-AndorraTelecom-CDR.csv.csv'
toy_file_2 = '../workspace/yleng/CDRVisitDay/512-AndorraTelecom-CDR.csv.csv'
larger_file = '../data_repository/datasets/telecom/cdr/201406-AndorraTelecom-CDR.csv'
most_recent = '../data_repository/datasets/telecom/cdr/201607-AndorraTelecom-CDR.csv'
towers = '../workspace/yleng/towers1.csv'

START_TIME = 3

def read_csv(filename,count=0):
	count_limit = count
	read_rows = []
	with open(filename,'rb') as csvfile:
		CDR_data = csv.reader(csvfile,delimiter=';')
		current = 0
		for row in CDR_data:
			read_rows.append(row)
			current += 1
			if current > count_limit:
				break
	csvfile.close()
	return read_rows


def initialize_towers(filename,limit=0):
	count_limit = limit
	towers_dict = {}
	with open(filename,'rb') as csvfile:
		CDR_data = csv.reader(csvfile,delimiter=';')
		current = 0
		for row in CDR_data:
			tower = row[6]
			if tower not in towers_dict:
				towers_dict[tower] = 1
			else:
				towers_dict[tower] += 1
			current += 1
			if current > count_limit:
				break
	csvfile.close()
	return towers_dict

def filter_data(data,indices):
	return map(lambda d: [d[i] for i in indices],data[1:])

def convert_to_datetime(data,index=0):
	datetime_format_string = '%Y.%m.%d %H:%M:%S'
	for i,val in enumerate(data):
		datetime_val = val[index]
		data[i] = datetime.strptime(datetime_val,datetime_format_string)
	return None

def plot_vs_time(x_vals=[],y_vals=[],title="",xaxis="",yaxis=""):
	plt.plot(x_vals,y_vals)
	plt.title(title)
	plt.xlabel(xaxis)
	plt.ylabel(yaxis)
	plt.show()

def plot_histogram():
	#TODO: check documentation
	return None

