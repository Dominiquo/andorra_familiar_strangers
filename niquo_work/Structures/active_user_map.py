import csv
import os
import pandas as pd
import Misc.getMaps as Maps
import Misc.file_constants as constants
import Misc.utils as utils
import reduce_density as RD
import cPickle
import itertools
from datetime import datetime
import time


def get_active_users(data_path, lower_range=0, upper_range=float('inf'), date_lower=1, date_upper=31):
	print 'loading coompressed data...'
	df = pd.read_csv(data_path)
	df = df[df[constants.DAY].between(date_lower, date_upper)]
	print 'grouping by SOURCE'
	grouped = df.groupby(constants.SOURCE).size().sort_values()
	print 'getting valid values'
	valid_values = grouped[grouped.between(lower_range, upper_range, inclusive=True)]
	return set(valid_values.index)


def reduce_size(graph_path, active_users):
	with open(graph_path, 'rb') as infile:
		print 'loading graph object'
		G = cPickle.load(infile)
	print 'Found', len(active_users), 'active users. '
	print 'removing unactive nodes...'
	for node in G.nodes():
		if node not in active_users:
			G.remove_node(node)
	return G


def make_smaller_graphs(data_path, graphs_dir, dest_dir, lower_range, upper_range, date_lower, date_upper):
	active_users = get_active_users(data_path, lower_range, upper_range, date_lower, date_upper)
	for graph_file in os.listdir(graphs_dir):
		dest_path = os.path.join(dest_dir, graph_file)
		graph_path = os.path.join(graphs_dir, graph_file)
		smaller_g = reduce_size(graph_path, active_users)
		with open(graph_path, 'wb') as outfile:
			cPickle.dump(smaller_g, outfile)
	return True


def quick_script_generate():
	start_dir = 'cdr_date_2016_07_24'
	dates_dir = '../niquo_data/small_range/tower_encounters_OLD'
	data_path = '../niquo_data/small_range/condensed_data/cdr_data_1_31_time_10.csv'
	dest_dir = '../niquo_data/small_range/tower_encounters_REDUCED'
	range_set = [(5,10),(11,20),(21,50)]
	for lower, upper in range_set:
		for d_dir in os.listdir(dates_dir):
			if d_dir > start_dir:
				graphs_dir = os.path.join(dates_dir, d_dir)
				range_dest_dir = os.path.join(dest_dir, d_dir + '_' + str(lower) + '_' + str(upper))
				make_smaller_graphs(data_path, graphs_dir, range_dest_dir, lower, upper, 1, 31)
	return True



