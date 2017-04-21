import csv
import os
import pandas as pd
import Misc.getMaps as Maps
import networkx as nx
import Misc.file_constants as constants
import Misc.utils as utils
import reduce_density as RD
import cPickle
import itertools
from datetime import datetime
import Misc.utils as utils


def get_active_users(data_path, lower_range=0, upper_range=float('inf'), date_lower=1, date_upper=31):
	print 'loading coompressed data...'
	df = pd.read_csv(data_path)
	df = df[df[constants.DAY].between(date_lower, date_upper)]
	print 'grouping by SOURCE'
	grouped = df.groupby(constants.SOURCE).size().sort_values()
	print 'getting valid values'
	valid_values = grouped[grouped.between(lower_range, upper_range, inclusive=True)]
	#################
	with open('/home/niquo/niquo_data/small_range/user_hash_map.p', 'rb') as infile:
		user_key_map = cPickle.load(infile)
	reverse_map = {v:k for k,v in user_key_map.iteritems()}
	val_keys = valid_values.index
	return set([reverse_map[v] for v in val_keys])

	#################
	return set(valid_values.index)


def change_labels_dir(dir_name):
	with open('/home/niquo/niquo_data/small_range/user_hash_map.p', 'rb') as infile:
		user_key_map = cPickle.load(infile)
	reverse_map = {v:k for k,v in user_key_map.iteritems()}
	for map_file in os.listdir(dir_name):
		map_path = os.path.join(dir_name, map_file)
		transform_map(map_path, reverse_map)
	return True

def transform_map(map_path, reverse_map):
	with open(map_path, 'rb') as infile:
		G = cPickle.load(infile)
	new_G = nx.relabel_nodes(G, reverse_map)
	with open(map_path, 'wb') as outfile:
		cPickle.dump(new_G, outfile)
	return True


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
		with open(dest_path, 'wb') as outfile:
			cPickle.dump(smaller_g, outfile)
	return True


def quick_script_generate():
	dates_use = ['cdr_date_2016_07_25', 'cdr_date_2016_07_26', 'cdr_date_2016_07_27', 'cdr_date_2016_07_29', 'cdr_date_2016_07_30', 'cdr_date_2016_07_31']
	dates_dir = '../niquo_data/small_range/tower_encounters'
	data_path = '../niquo_data/small_range/condensed_data/cdr_data_1_31_time_10.csv'
	dest_dir = '../niquo_data/small_range/tower_encounters_REDUCED_V2'
	range_set = [(5,10),(11,20),(21,50)]
	for lower, upper in range_set:
		print 'current range', lower, upper
		range_dir = utils.create_dir(dest_dir, 'counts_' + str(lower) + '_' + str(upper))
		for d_dir in dates_use:
			graphs_dir = os.path.join(dates_dir, d_dir)
			print 'graphs dir:', graphs_dir
			final_dest_dir = utils.create_dir(range_dir,  d_dir)
			
			make_smaller_graphs(data_path, graphs_dir, final_dest_dir, lower, upper, 24, 31)
	return True


