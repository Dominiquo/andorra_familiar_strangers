import csv
import os
import extractData as ex
import cPickle
import itertools
from datetime import datetime
import time

START_TIME_INDEX = 3
TOWER_INDEX = 6
CALLER_INDEX = 0
RECEIVER_INDEX = 16
COMM_TYPE_INDEX = 10

class RawCDRCSV(object):
	"""Represents class for raw CDR data given in CSV form and operations related to it"""
	def __init__(self, filepath):
		self.filename = filepath

	def partition_by_tower_id(self,destination_dir):
		# TODO: move code 
		return None

	def filter_rows(self, destination_dir, filter_func):
		# TODO: 
		return None

	def filter_and_partition(self,destination_dir, filter_func):
		# TODO:
		return None

	def rows_generator(self):
		with open(filepath) as infile:
			data_csv = csv.reader(csvfile,delimiter=';')
			for row in data_csv:
				yield row



def remove_data_comms(row):
	if row[COMM_TYPE_INDEX] != 'S-CDR':
		return True
	else:
		return False


