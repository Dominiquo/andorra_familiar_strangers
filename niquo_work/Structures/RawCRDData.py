import csv
import os
import pandas as pd
import Misc.getMaps as Maps
import Misc.file_constants as constants
import cPickle
import itertools
from datetime import datetime
import time

# START_TIME_INDEX = 3
# TOWER_INDEX = 6
# CALLER_INDEX = 0
# RECEIVER_INDEX = 16
# COMM_TYPE_INDEX = 10
# DATE_INDEX = 10


START_TIME_INDEX = 1
TOWER_COLUMN = 'ID_CELLA_INI'
TIMESTAMP = 'DT_CDDATAINICI'
CALLER_INDEX = 0
RECEIVER_INDEX = -1
YEAR_INDEX = 4
DATE_INDEX = 5


class RawCDRCSV(object):
	"""Represents class for raw CDR data given in CSV form and operations related to it"""
	def __init__(self, filename):
		self.filename = filename

	def filter_and_partition(self, destination_dir, filter_func=lambda row: True, chunksize=10**4, limit=float('inf')):
		tower_map = Maps.tower_map_id()
		TOWER_NUMBER = 'tower_id'
		DATE = 'date'
		date_file_prefix = 'cdr_date_'
		csv_suffix = '.csv'
		current_dates = set(os.listdir(destination_dir))
		lines_count = 0

		for data_chunk in pd.read_csv(self.filename, delimiter=';', chunksize=chunksize):
			data_chunk[TOWER_NUMBER] = data_chunk[TOWER_COLUMN].apply(lambda tid: tower_map[tid] if tid in tower_map else False)
			data_chunk[DATE] = data_chunk[TIMESTAMP].apply(lambda tstamp: trans_timestamp(tstamp))
			data_chunk = data_chunk[data_chunk[TOWER_NUMBER] != False]
			data_chunk = data_chunk[data_chunk.apply(lambda row: filter_func(row), axis=1)]
			lines_count += len(data_chunk)
			if lines_count > limit:
				break
			for date_str, date_group in data_chunk.groupby(DATE):
				filename = date_file_prefix + date_str + csv_suffix
				filepath = os.path.join(destination_dir, filename)
				if filename in current_dates:
					date_group.to_csv(filepath, mode='a', index=False)
				else:
					date_group.to_csv(filepath, index=False)
		return lines_count

def trans_timestamp(timestamp):
	year_end = 4
	month_s = 5
	month_f = 7
	date_s = 8
	date_f = 10
	return timestamp[:year_end] + '_' + timestamp[month_s:month_f] + '_' + timestamp[date_s:date_f]

def is_comm_type_data(row):
	return row[COMM_TYPE_INDEX] != 'S-CDR'

def main():
	csv_file = constants.JULY_DATA_FILTERED
	destination_dir = constants.FILTERED_PARTITIONED
	csvData = RawCDRCSV(csvfile)
	csvData.filter_and_partition(destination_dir)

if __name__ == '__main__':
    main()

