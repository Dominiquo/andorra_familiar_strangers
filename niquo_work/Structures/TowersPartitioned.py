import csv
import os
import cPickle
import itertools
import networkx as nx
from datetime import datetime
import pandas as pd
import Misc.file_constants as constants
import GraphLite as gl
import networkx as nx
import time
from joblib import Parallel, delayed


DATE_INDEX = 10


class TowersPartitioned(object):
	"""Represents Operations on a directory of csv for each tower"""
	def __init__(self, towers_dir):
		self.directory = towers_dir
		self.all_dates = sorted(os.listdir(self.directory))

	def pair_users_from_towers(self,destination_path, enc_window=1):
		print 'beginning pairing users...'
		for date_file in self.all_dates:
			print 'loading data from:', date_file
			date_path = os.path.join(self.directory, date_file)
			date_data = pd.read_csv(date_path)
			date_dir = create_date_dir(destination_path, date_file)
			total = len(date_data[constants.TOWER_COLUMN].unique())
			# for tower_id in date_data[constants.TOWER_COLUMN].unique():
			Parallel(n_jobs=4)(delayed(process_date_date)(date_data[date_data[constants.TOWER_COLUMN] == tower_id], date_dir, tower_id, enc_window) for tower_id in date_data[constants.TOWER_COLUMN].unique())

	def pair_towers_multiple_days(self, destination_path, towers=['471'], days_count=3, enc_window=1):
		for tower_id in towers:
			current_date = 1
			tower_day_dfs = []
			# TODO: REMOVE THIS BLOCK
			for date_file in self.all_dates[:days_count]:
				date_path = os.path.join(self.directory, date_file)
				print 'loading data from ', date_path
				date_data = pd.read_csv(date_path)[:100000]
				print 'length of data:', len(date_data)
				tower_day_dfs.append(date_data[date_data[constants.TOWER_COLUMN] == tower_id])
				if (current_date % days_count) == 0:
					dest = create_date_dir(destination_path, date_file + '_date_range_' + str(current_date))
					print 'combining data from dates...'
					combined_data = pd.concat(tower_day_dfs)
					process_date_date(combined_data, dest, tower_id, enc_window)
				tower_day_dfs = []
				current_date += 1



# PARALLEL FUNCTIONS THAT CAN'T BE A PART OF THE CLASS

def process_date_date(single_tower_data, destination_path, tower_id, enc_window):
	print 'started process for tower id:', tower_id
	single_tower_data = single_tower_data.sort_values([constants.DAYTIME])
	single_tower_data = single_tower_data.reset_index(drop=True)
	pair_users_single_file(destination_path, single_tower_data, enc_window, tower_id)


def create_date_dir(destination_path, date_csv):
	date = date_csv.split('.')[0]
	date_path = os.path.join(destination_path, date)
	if not os.path.isdir(date_path):
		os.makedirs(date_path)
	return date_path

def pair_users_single_file(destination_path, single_tower_data, enc_window, tower_id):
	window_secs = 60*60*enc_window
	all_data = []
	encs_graph = nx.Graph()
	total_values = len(single_tower_data)
	for index, row in single_tower_data.iterrows():
		if index % 500 == 0:
			print 'single tower progress', index, '/', total_values
		if index == len(single_tower_data):
			break
		domain_values = single_tower_data[index + 1:]
		current_timestamp = row[constants.DAYTIME]
		encountered_group = domain_values[(domain_values[constants.DAYTIME] <= add_strings(current_timestamp, window_secs))]
		# print 'adding', len(encountered_group), 'edges'
		add_edges_network(encs_graph, row, encountered_group)
	return store_encounters(encs_graph, destination_path, tower_id)


def add_edges_network(encs_graph, source_row, encountered_df):
	source_user = source_row[constants.SOURCE]
	enc_tower = source_row[constants.TOWER_COLUMN]
	first_time = source_row[constants.TIMESTAMP]
	for index, dest_row in encountered_df.iterrows():
		dest_user = dest_row[constants.SOURCE]
		if source_user == dest_user:
			continue
		second_time = source_row[constants.TIMESTAMP]
		avg_time = average_call_times(first_time, second_time)
		attr_dict = {'time':avg_time}
		encs_graph.add_edge(source_user, dest_user, attr_dict=attr_dict)


def store_encounters(encs_graph, destination_path, tower_id):
	tower_file_prefix = 'cdr_tower_'
	p_suffix = '.p'
	tower_filename = tower_file_prefix + str(tower_id) + p_suffix
	tower_path = os.path.join(destination_path, tower_filename)
	print 'storing data in:', tower_path
	cPickle.dump(encs_graph, open(tower_path, 'wb'))


# **********************************************
# HELPER FUNCTIONS
# **********************************************

def add_strings(timestr1, timestr2):
	return str(int(timestr1) + int(timestr2))

def only_day_second(tstamp):
	return tstamp[8:10] + ' ' + str(60*int(tstamp[11:13])*int(tstamp[14:16]))

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
	new_time = head + ' ' + zero_pad(avgh) + ':' + zero_pad(avgm) + ':' + str(avgs)
	return only_day_second(new_time)


def main():
	return None

if __name__ == '__main__':
    main()