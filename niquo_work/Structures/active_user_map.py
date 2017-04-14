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


def get_active_users(data_path, lower_range=0, upper_range=float('inf')):
	df = pd.read_csv(data_path)
	grouped = df.groupby(constants.SOURCE).size().sort_values()
	valid_values = grouped[grouped.between(lower_range, upper_range, inclusive=True)]
	return set(valid_values.index)


def reduce_size(graph_path, data_path, lower_range, upper_range, destination_path):
	with open(graph_path, 'rb') as infile:
		G = cPickle.load(infile)
	active_users = get_active_users(data_path, lower_range, upper_range)
	for node in G.nodes():
		if node not in active_users:
			G.remove_node(node)
