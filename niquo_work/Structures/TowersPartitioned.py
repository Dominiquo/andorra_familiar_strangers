import csv, os, sys
import cPickle
import networkx as nx
from datetime import datetime
import pandas as pd
import Misc.file_constants as constants
import GraphLite as gl
import networkx as nx
import time
import numpy as np
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
			total = len(date_data[constants.TOWER_NUMBER].unique())
			Parallel(n_jobs=4)(delayed(process_date_data)(date_data[date_data[constants.TOWER_NUMBER] == tower_id], date_dir, tower_id, enc_window) for tower_id in date_data[constants.TOWER_NUMBER].unique())
			# for tower_id in date_data[constants.TOWER_NUMBER].unique():
			# 	single_tower_data = date_data[date_data[constants.TOWER_NUMBER] == tower_id]
			# 	process_date_data(single_tower_data, date_dir, tower_id, enc_window)



# PARALLEL FUNCTIONS THAT CAN'T BE A PART OF THE CLASS
def process_date_data(single_tower_data, destination_path, tower_id, enc_window):
	print 'started process for tower id:', tower_id, 'to be stored', destination_path
	single_tower_data = single_tower_data.sort_values([constants.DAYTIME])
	pair_users_single_file(destination_path, single_tower_data, enc_window, tower_id)


def create_date_dir(destination_path, date_csv):
	date = date_csv.split('.')[0]
	date_path = os.path.join(destination_path, date)
	if not os.path.isdir(date_path):
		os.makedirs(date_path)
	return date_path

def pair_users_single_file(destination_path, single_tower_data, enc_window, tower_id):
	window_secs = 60*60*enc_window
	total_values = len(single_tower_data)
	encs_obj = gl.GraphLite()
	current_index = 0
	single_tower_data = single_tower_data.reset_index(drop=True)
	start = time.time()
	prev = start
	for index, row in single_tower_data.iterrows():
		if index % 1000 == 0:
			now = time.time()
			print 'single tower progress for ', tower_id,':' , index, '/', total_values
			print 'completed last 1000 in:', now-prev,'seconds'
			prev = now
			sys.stdout.flush()
		if index == len(single_tower_data):
			break
		domain_values = single_tower_data[index + 1:]
		current_timestamp = row[constants.DAYTIME]
		encountered_group = domain_values[(domain_values[constants.DAYTIME] <= (current_timestamp + window_secs))]
		add_edges_network(encs_obj, row, encountered_group)
	print 'finished ', total_values, 'in: ', time.time()-start
	return store_encounters(encs_obj, destination_path, tower_id)


def add_edges_network(encs_obj, source_row, encountered_df):
	source_user = source_row[constants.SOURCE]
	enc_tower = source_row[constants.TOWER_NUMBER]
	first_time = source_row[constants.TIMESTAMP]
	for index, dest_row in encountered_df.iterrows():
		dest_user = dest_row[constants.SOURCE]
		if source_user == dest_user:
			continue
		second_time = source_row[constants.TIMESTAMP]
		avg_time = average_call_times(first_time, second_time)
		encs_obj.add_edge(source_user, dest_user, {'t':avg_time})



def store_encounters(encs_obj, destination_path, tower_id):
	tower_file_prefix = 'cdr_tower_'
	p_suffix = '.p'
	tower_filename = tower_file_prefix + str(tower_id) + p_suffix
	tower_path = os.path.join(destination_path, tower_filename)
	print 'storing data in:', tower_path
	cPickle.dump(encs_obj, open(tower_path, 'wb'))


# **********************************************
# HELPER FUNCTIONS
# **********************************************

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