import csv
import os
import pandas as pd
import Misc.getMaps as Maps
import Misc.file_constants as constants
import Misc.utils as utils
import cPickle
import itertools
from datetime import datetime
import time


class RawCDRCSV(object):
	"""Represents class for raw CDR data given in CSV form and operations related to it"""
	def __init__(self, filename):
		self.filename = filename

	def filter_and_partition(self, destination_dir, filter_func=lambda row: True, chunksize=10**5, delimiter=',', limit=float('inf')):
		tower_map = Maps.tower_map_id()
		TOWER_NUMBER = 'tower_id'
		DATE_STRING = 'date'
		DAYTIME = 'timestamp'
		date_file_prefix = 'cdr_date_'
		csv_suffix = '.csv'
		lines_count = 0

		for data_chunk in pd.read_csv(self.filename, usecols=constants.USEFUL_ROWS, delimiter=delimiter, chunksize=chunksize):
			data_chunk[TOWER_NUMBER] = data_chunk[constants.TOWER_COLUMN].apply(lambda tid: tower_map[tid] if tid in tower_map else False)
			data_chunk[DATE_STRING] = data_chunk[constants.TIMESTAMP].apply(utils.trans_date_string)
			data_chunk[DAYTIME] = data_chunk[constants.TIMESTAMP].apply(lambda t: int(utils.trans_datetime(t)))
			data_chunk = data_chunk[data_chunk[TOWER_NUMBER] != False]			
			data_chunk = data_chunk[data_chunk.apply(lambda row: filter_func(row), axis=1)]
			lines_count += len(data_chunk) 
			if lines_count > limit:
				break
			elif len(data_chunk) == 0:
				continue
			for date_str, date_group in data_chunk.groupby(DATE_STRING):
				filename = date_file_prefix + date_str + csv_suffix
				filepath = os.path.join(destination_dir, filename)
				if filename in os.listdir(destination_dir):
					date_group.to_csv(filepath, mode='a', header=False, index=False)
				else:
					date_group.to_csv(filepath, index=False)
		return lines_count


def main():
	return None

if __name__ == '__main__':
    main()

