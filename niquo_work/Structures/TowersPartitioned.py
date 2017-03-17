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
from joblib import Parallel, delayed


DATE_INDEX = 10


class TowersPartitioned(object):
	"""Represents Operations on a directory of csv for each tower"""
	def __init__(self, towers_dir, destination_path):
		self.directory = towers_dir
		self.destination_path = destination_path
		self.all_dates = sorted(np.array(os.listdir(self.directory)))

	def pair_users_from_towers(self, lower=0, upper=0, enc_window=1):
		if upper == 0: upper = len(self.all_dates)
		print 'beginning pairing users...'
		for date_file in self.all_dates[lower:upper]:
			print date_file, 'in :', self.directory
			date_path = os.path.join(self.directory, date_file)
			date_dir = create_date_dir(self.destination_path, date_file)
			print 'loading data from:', date_file
			date_data = pd.read_csv(date_path).sort_values([constants.MIN_TIME])
			for tower_id, tower_df in date_data.groupby(constants.TOWER_NUMBER):
				print 'beginning pairing for tower:', tower_id
				pair_users_single_file(date_dir, tower_df, tower_id, enc_window)
			del date_data

	def pair_users_specific_tower(self, tower_id, lower=0, upper=0, enc_window=1):
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
	window_secs = 60*60*enc_window
	total_values = len(single_tower_data)
	encs_obj = nx.Graph()
	start = time.time()
	prev = start
	all_hours = single_tower_data[constants.HOUR].unique()
	usable_hours = set(all_hours)
	i = 1
	for hour, current_hour_data in single_tower_data.groupby(constants.HOUR):
		add_current_hour_network(encs_obj, current_hour_data)
		next_h = hour + 1
		if next_h in usable_hours:
			next_hour_data = single_tower_data[(single_tower_data[constants.HOUR]==next_h)]
			add_adjacent_hour_encounters(encs_obj, window_secs, current_hour_data, next_hour_data)
		

		# TIMING #
		now = time.time()
		print 'current hour:', hour
		print 'progress:', i ,'/', len(all_hours)
		print 'completed last hour in:', now-prev,'seconds'
		prev = now
		# TIMING #

		i += 1

	print 'finished tower of ', total_values, 'rows in: ', time.time()-start
	store_encounters(encs_obj, destination_path, tower_id)
	return True


def add_adjacent_hour_encounters(encs_obj,  window_secs, current_hour_data, next_hour_data):
	user_index = 0
	time_index = 1

	for row in current_hour_data[[constants.SOURCE, constants.MAX_TIME]].values:
		user = row[user_index]
		lower_time = row[time_index]
		upper_bound = lower_time + window_secs
		next_hour_intersect = next_hour_data[next_hour_data[constants.MIN_TIME] <= upper_bound]
		for other_row in next_hour_intersect[[constants.SOURCE, constants.MIN_TIME]].values:
			other_user = other_row[user_index]
			other_time = other_row[time_index]
			avg_time = np.average([lower_time, other_time])
			encs_obj.add_edge(user, other_user, attr_dict={'t': avg_time})


def add_current_hour_network(encs_obj, encountered_df):
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


def main(root_path, condensed_data_path, lower, upper):
	destination_path = utils.create_dir(root_path, 'tower_encounters')
	tpart = TowersPartitioned(condensed_data_path, destination_path)
	tpart.pair_users_from_towers(lower, upper)
	return None
