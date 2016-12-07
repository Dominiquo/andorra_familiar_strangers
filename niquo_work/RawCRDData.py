import csv
import os
import getMaps as Maps
import file_constants as constants
import cPickle
import itertools
from datetime import datetime
import time

START_TIME_INDEX = 3
TOWER_INDEX = 6
CALLER_INDEX = 0
RECEIVER_INDEX = 16
COMM_TYPE_INDEX = 10
DATE_INDEX = 10

class RawCDRCSV(object):
	"""Represents class for raw CDR data given in CSV form and operations related to it"""
	def __init__(self, filename):
		self.filename = filename

	def filter_and_partition(self,destination_dir, filter_func=lambda row: True, limit=float('inf')):
		tower_map = Maps.tower_map_id()
		tower_file_prefix = "cdr_tower_"
		csv_suffix = ".csv"
		current_towers = set([])
		current_count = 0
		files_count = 1
		for row in self.rows_generator():
			if (row[TOWER_INDEX]== 'ID_CELLA_INI') or (not filter_func(row)):
				continue
			pre_funnel_id = row[TOWER_INDEX]
			if pre_funnel_id not in tower_map:
				continue
			else:
				tower_id = tower_map[pre_funnel_id]

			call_time = row[START_TIME_INDEX]
			call_date = call_time[:DATE_INDEX]
			date_path = os.path.join(destination_dir,call_date)
			# check if the path for the date exists yet
			if not os.path.exists(date_path):
				os.makedirs(date_path)

			tower_file = tower_file_prefix + tower_id + csv_suffix
			tower_path = os.path.join(date_path,tower_file)
			if tower_path in current_towers:
				tower_file_obj = open(tower_path, 'a')
			else:	
				files_count += 1
				tower_file_obj = open(tower_path,'wb')

			tower_file_csv = csv.writer(tower_file_obj,delimiter=';')
			tower_file_csv.writerow(row)
			tower_file_obj.close()

			if current_count > limit:
				break
			current_count += 1
			current_towers.add(tower_path)			
		print 'created',files_count,'new files of towers.'

	def rows_generator(self):
		print 'opening file to read from as a csv...'
		with open(self.filename) as csvfile:
			data_csv = csv.reader(csvfile,delimiter=';')
			for row in data_csv:
				yield row



def is_comm_type_data(row):
	return row[COMM_TYPE_INDEX] != 'S-CDR'

def main():
	csv_file = constants.JULY_DATA_FILTERED
	destination_dir = constants.FILTERED_PARTITIONED
	csvData = RawCDRCSV(csvfile)
	csvData.filter_and_partition(destination_dir)

if __name__ == '__main__':
    main()

