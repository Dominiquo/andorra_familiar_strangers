import csv, os, sys
import cPickle
import networkx as nx
from datetime import datetime
import pandas as pd
import Misc.file_constants as constants
import Misc.utils as utils
import GraphLite as gl
import networkx as nx
import time
import numpy as np
import itertools
import operator
from joblib import Parallel, delayed



class TowersPartitioned(object):
	"""Represents Operations on a directory of csv for each tower"""
	def __init__(self, towers_dir, destination_path):
		self.directory = towers_dir
		self.destination_path = destination_path
		self.all_dates = sorted(np.array(os.listdir(self.directory)))

	def pair_users_from_towers(self, lower=0, upper=0, enc_window=30, threshold=16000, thresh_compare=operator.lt):
		if upper == 0: upper = len(self.all_dates)
		print 'beginning pairing users...'
		for date_file in self.all_dates[lower:upper]:
			print date_file, 'in :', self.directory
			date_path = os.path.join(self.directory, date_file)
			date_dir = create_date_dir(self.destination_path, date_file)
			print 'loading data from:', date_file
			date_data = pd.read_csv(date_path).sort_values([constants.MIN_TIME])
			data_grouped = date_data.groupby(constants.TOWER_NUMBER)
			towers_sorted = data_grouped.size().sort_values()
			for tower_id, size in towers_sorted.iteritems():
				if thresh_compare(size,threshold):
					print 'current tower_id:', tower_id
					print 'current tower_size:', size
					tower_df = data_grouped.get_group(tower_id)
					print 'beginning pairing for tower:', tower_id
					pair_users_single_file(date_dir, tower_df, tower_id, enc_window)
			del date_data

	def pair_users_specific_tower(self, tower_id, lower=0, upper=0, enc_window=30):
		if upper == 0: upper = len(self.all_dates)
		print 'beginning pairing users...'
		for date_file in self.all_dates[lower:upper]:
			print date_file, 'in :', self.directory
			date_path = os.path.join(self.directory, date_file)
			date_dir = create_date_dir(self.destination_path, date_file)
			print 'loading data from:', date_file
			date_data = pd.read_csv(date_path).sort_values([constants.MIN_TIME])
			print 'beginning pairing for tower:', tower_id
			tower_df = date_data[date_data[constants.TOWER_NUMBER] == tower_id]
			pair_users_single_file(date_dir, tower_df, tower_id, enc_window)
			del date_data



# PARALLEL FUNCTIONS THAT CAN'T BE A PART OF THE CLASS
def pair_users_single_file(destination_path, single_tower_data, tower_id, enc_window):
	window_secs = enc_window*60
	enc_delta = round((enc_window/float(60)), 5)
	total_values = len(single_tower_data)
	encs_obj = nx.MultiGraph()
	all_time_chunks = single_tower_data[constants.TIME_BLOCK].unique()
	usable_blocks = set(all_time_chunks)
	i = 1

	start = time.time()
	prev = start
	gb_prev = start
	
	add_adj_prev = start

	for time_block, current_block_data in single_tower_data.groupby(constants.TIME_BLOCK):
		gb_end = time.time()

		add_curr_prev = time.time()
		add_current_time_chunk_network(encs_obj, current_block_data)
		add_curr_time = time.time() - add_curr_prev

		add_adj_start = time.time()
		next_block = time_block + enc_window
		if next_block in usable_blocks:
			next_block_data = single_tower_data[(single_tower_data[constants.TIME_BLOCK]==next_block)]
			add_adjacent_block_encounters(encs_obj, window_secs, current_block_data, next_block_data)
		add_adj_time = time.time() - add_adj_start

		# TIMING #

		now = time.time()
		gb_time = gb_end - gb_prev
		gb_prev = time.time()
		# print '*************************'
		# print 'current time block:', time_block
		# print 'grouped in:', gb_time
		# print 'current block added in:', add_curr_time
		# print 'adjecent blocks added in:', add_adj_time
		print 'progress:', i ,'/', len(all_time_chunks)
		print 'completed last block in:', now-prev,'seconds'
		prev = now
		# TIMING #

		i += 1

	print 'finished tower of ', total_values, 'rows in: ', time.time()-start
	store_encounters(encs_obj, destination_path, tower_id)
	return True


def add_adjacent_block_encounters(encs_obj,  window_secs, current_block_data, next_block_data):
	user_index = 0
	time_index = 1

	for row in current_block_data[[constants.SOURCE, constants.MAX_TIME]].values:
		user = row[user_index]
		lower_time = row[time_index]
		upper_bound = lower_time + window_secs
		next_block_intersect = next_block_data[next_block_data[constants.MIN_TIME] <= upper_bound]
		for other_row in next_block_intersect[[constants.SOURCE, constants.MIN_TIME]].values:
			other_user = other_row[user_index]
			other_time = other_row[time_index]
			avg_time = np.average([lower_time, other_time])
			encs_obj.add_edge(user, other_user, attr_dict={'t': avg_time})


def add_current_time_chunk_network(encs_obj, encountered_df):
	user_index = 0
	time_index = 1
	subset = encountered_df[[constants.SOURCE, constants.MIN_TIME]].values
	for first, second in itertools.combinations(subset, 2):
		source_user = first[user_index]
		other_user = second[user_index]
		first_time = first[time_index]
		second_time = second[time_index]
		avg_time = np.average([first_time, second_time])
		encs_obj.add_edge(source_user, other_user, attr_dict={'t':avg_time})



def store_encounters(encs_obj, destination_path, tower_id):
	tower_file_prefix = 'cdr_tower_'
	p_suffix = '.p'
	tower_filename = tower_file_prefix + str(tower_id) + p_suffix
	tower_path = os.path.join(destination_path, tower_filename)
	print 'storing data in:', tower_path
	# encs_obj.store_object(tower_path)
	with open(tower_path, 'wb') as outfile:
		cPickle.dump(encs_obj, outfile)
		del encs_obj
		print 'deleted object for tower_id:', tower_id
	return True


# **********************************************
# HELPER FUNCTIONS
# **********************************************

def only_day_second(tstamp):
	return tstamp[8:10] + ' ' + str(60*int(tstamp[11:13])*int(tstamp[14:16]))


def create_date_dir(destination_path, date_csv):
	date = date_csv.split('.')[0]
	date_path = os.path.join(destination_path, date)
	if not os.path.isdir(date_path):
		os.makedirs(date_path)
	return date_path


def produce_larger_graphs(root_path, condensed_data_path, lower, upper):
	destination_path = utils.create_dir(root_path, 'tower_encounters')
	tpart = TowersPartitioned(condensed_data_path, destination_path)
	tpart.pair_users_from_towers(lower, upper, thresh_compare=operator.ge)
	return None

def main(root_path, condensed_data_path, lower, upper):
	destination_path = utils.create_dir(root_path, 'tower_encounters')
	tpart = TowersPartitioned(condensed_data_path, destination_path)
	tpart.pair_users_from_towers(lower, upper)

	return None
