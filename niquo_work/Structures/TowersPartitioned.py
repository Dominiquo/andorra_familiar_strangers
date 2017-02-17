import csv
import os
import cPickle
import itertools
from datetime import datetime
import pandas as pd
import Misc.file_constants as constants
import time


class TowersPartitioned(object):
	"""Represents Operations on a directory of csv for each tower"""
	def __init__(self, towers_dir):
		self.directory = towers_dir


	def pair_users_from_towers(self,destination_path, enc_window=1):
		for date_file in self.generate_dates():
			date_path = os.path.join(self.directory, date_file)
			date_data = pd.read_csv(date_path)
			# TODO: switch back to assigned tower number
			for tower_id in date_data[constants.TOWER_COLUMN].unique():
				# TODO: Switch back here as well
				single_tower_data = date_data[date_data[constants.TOWER_COLUMN] == tower_id]
				single_tower_data = single_tower_data.sort_values([constants.DAYTIME])
				self.pair_users_single_file(destination_path, single_tower_data, enc_window, tower_id)


	def pair_users_single_file(self, destination_path, single_tower_data, enc_window, tower_id):
		window_secs = 60*60*enc_window
		all_data = []
		for index, row in single_tower_data.iterrows():
			current_timestamp = int(row[constants.DAYTIME])
			encountered_group = single_tower_data[(single_tower_data[constants.DAYTIME] >= current_timestamp)&
												(single_tower_data[constants.DAYTIME] <= current_timestamp + window_secs)]
			encountered_group[constants.ENC_ROOT] = current_timestamp
			all_data.append(encountered_group)
		return self.store_encounters(pd.concat(all_data), destination_path, tower_id)


	def store_encounters(self, encountered_data, destination_path, tower_id):
		tower_file_prefix = 'cdr_tower_'
		csv_suffix = '.csv'
		tower_filename = tower_file_prefix + str(tower_id) + csv_suffix
		tower_path = os.path.join(destination_path, tower_filename)
		if tower_filename in os.listdir(destination_path):
			encountered_data.to_csv(tower_path, mode='a', index=False)
		else:
			encountered_data.to_csv(tower_path, index=False)
		return True


	def generate_dates(self):
		all_dates_dirs = sorted(set(os.listdir(self.directory)))
		print "total count of available date files:",len(all_dates_dirs)
		for date_dir in all_dates_dirs:
			print "checking towers from", date_dir
			yield date_dir


# **********************************************
# HELPER FUNCTIONS FOR COMPARING RAW FILES
# **********************************************


def average_call_times(time_stamp_1,time_stamp_2):
	hour_s = 0
	hour_f = 2
	min_s = 3
	min_f = 5
	sec_s = 6
	sec_f = 8
	head = time_stamp_1[:DATE_INDEX]
	time1 = time_stamp_1[DATE_INDEX+1:]
	time2 = time_stamp_2[DATE_INDEX+1:]
	avgh = (int(time1[hour_s:hour_f]) + int(time2[hour_s:hour_f]))/2
	avgm = (int(time1[min_s:min_f]) + int(time2[min_s:min_f]))/2
	avgs = (int(time1[sec_s:sec_f]) + int(time2[sec_s:sec_f]))/2
	zero_pad = lambda v: '0' + str(v) if v < 10 else str(v)
	return head + ' ' + zero_pad(avgh) + ':' + zero_pad(avgm) + ':' + str(avgs)


def main():
	return None

if __name__ == '__main__':
    main()