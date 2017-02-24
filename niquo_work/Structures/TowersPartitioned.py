import csv
import os
import cPickle
import itertools
import networkx as nx
from datetime import datetime
import pandas as pd
import Misc.file_constants as constants
import GraphLite as gl
import time


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
			date_dir = self.create_date_dir(destination_path, date_file)
			total = len(date_data[constants.TOWER_NUMBER].unique())
			count = 0
			for tower_id in date_data[constants.TOWER_NUMBER].unique():
				count += 1
				print 'current tower id:', tower_id, count, '/', total
				single_tower_data = date_data[date_data[constants.TOWER_NUMBER] == tower_id]
				single_tower_data = single_tower_data.sort_values([constants.DAYTIME])
				single_tower_data = single_tower_data.reset_index(drop=True)
				self.pair_users_single_file(date_dir, single_tower_data, enc_window, tower_id)

	def create_date_dir(self, destination_path, date_csv):
		date = date_csv.split('.')[0]
		date_path = os.path.join(destination_path, date)
		if not os.path.isdir(date_path):
			os.makedirs(date_path)
		return date_path

	def pair_users_single_file(self, destination_path, single_tower_data, enc_window, tower_id):
		window_secs = 60*60*enc_window
		all_data = []
		encs_graph = gl.GraphLite()
		for index, row in single_tower_data.iterrows():
			if index == len(single_tower_data):
				break
			domain_values = single_tower_data[index + 1:]
			current_timestamp = row[constants.DAYTIME]
			encountered_group = domain_values[(domain_values[constants.DAYTIME] <= add_strings(current_timestamp, window_secs))]
			self.add_edges_network(encs_graph, row, encountered_group)
		return self.store_encounters(encs_graph, destination_path, tower_id)


	def add_edges_network(self, encs_graph, source_row, encountered_df):
		source_user = source_row[constants.SOURCE]
		enc_tower = source_row[constants.TOWER_COLUMN]
		comm_type = source_row[constants.COMM_TYPE]
		first_time = source_row[constants.TIMESTAMP]
		for index, dest_row in encountered_df.iterrows():
			dest_user = dest_row[constants.SOURCE]
			if source_user == dest_user:
				continue
			comm_type_dest = dest_row[constants.COMM_TYPE]
			second_time = source_row[constants.TIMESTAMP]
			avg_time = average_call_times(first_time, second_time)
			attr_dict = {'time':avg_time, 'tower': enc_tower, 'source_comm_type': comm_type, 'dest_comm_type': comm_type_dest}
			encs_graph.add_edge(source_user, dest_user, attr_dict=attr_dict)


	def store_encounters(self, encs_graph, destination_path, tower_id):
		tower_file_prefix = 'cdr_tower_'
		p_suffix = '.p'
		tower_filename = tower_file_prefix + str(tower_id) + p_suffix
		tower_path = os.path.join(destination_path, tower_filename)
		print 'storing data in:', tower_path
		encs_graph.store_object(destination_path)


# **********************************************
# HELPER FUNCTIONS FOR COMPARING RAW FILES
# **********************************************

def add_strings(timestr1, timestr2):
	return str(int(timestr1) + int(timestr2))


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