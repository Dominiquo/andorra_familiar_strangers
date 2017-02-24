import csv
import os
import pandas as pd
import Misc.getMaps as Maps
import Misc.file_constants as constants
import cPickle
import itertools
from datetime import datetime
import time
import time


class RawCDRCSV(object):
	"""Represents class for raw CDR data given in CSV form and operations related to it"""
	def __init__(self, filename):
		self.filename = filename

	def filter_and_partition(self, destination_dir, filter_func=lambda row: True, chunksize=10**4, limit=float('inf')):
		tower_map = Maps.tower_map_id()
		TOWER_NUMBER = 'tower_id'
		DATE_STRING = 'date'
		DAYTIME = 'timestamp'
		date_file_prefix = 'cdr_date_'
		csv_suffix = '.csv'
		lines_count = 0

		for data_chunk in pd.read_csv(self.filename, usecols=constants.USEFUL_ROWS, delimiter=';', chunksize=chunksize):
			data_chunk[TOWER_NUMBER] = data_chunk[constants.TOWER_COLUMN].apply(lambda tid: tower_map[tid] if tid in tower_map else False)
			data_chunk[DATE_STRING] = data_chunk[constants.TIMESTAMP].apply(lambda tstamp: trans_date_string(tstamp))
			data_chunk[DAYTIME] = data_chunk[constants.TIMESTAMP].apply(lambda tstamp: int(trans_datetime(tstamp)))
			data_chunk = data_chunk[data_chunk[TOWER_NUMBER] != False]			
			data_chunk = data_chunk[data_chunk.apply(lambda row: filter_func(row), axis=1)]
			lines_count += len(data_chunk)
			if lines_count > limit:
				break
			for date_str, date_group in data_chunk.groupby(DATE_STRING):
				filename = date_file_prefix + date_str + csv_suffix
				filepath = os.path.join(destination_dir, filename)
				if filename in os.listdir(destination_dir):
					date_group.to_csv(filepath, mode='a', index=False)
				else:
					date_group.to_csv(filepath, index=False)
		return lines_count


def remove_foreigners(row):
	id_val = 21303
	CARRIER = 'ID_CDOPERADORORIGEN'
	return row[CARRIER] == id_val

def trans_date_string(timestamp):
	year_end = 4
	month_s = 5
	month_f = 7
	date_s = 8
	date_f = 10
	return timestamp[:year_end] + '_' + timestamp[month_s:month_f] + '_' + timestamp[date_s:date_f]

def trans_datetime(timestamp):
	format_string = "%Y.%m.%d %H:%M:%S"
	time_object = datetime.strptime(timestamp,format_string)
	return time.mktime(time_object.timetuple())

def is_comm_type_data(row):
	return row[COMM_TYPE_INDEX] != 'S-CDR'

def main():
	return None

if __name__ == '__main__':
    main()

