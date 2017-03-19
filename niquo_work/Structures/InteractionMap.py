import csv
import os, sys
import cPickle
import itertools
from datetime import datetime
import Misc.file_constants as constants
import networkx as nx
import pandas as pd
import time


####ADD IN THE TOWER FOR 


class InteractionMap(object):
	"""docstring for InteractionMaps"""
	def __init__(self, root_path):
		self.directory = root_path
		self.master_graph = nx.Graph()
		self.master_filename = 'MASTER_GRAPH.p'

	def combine_by_day(self, day_path):
		for tower_file in self.get_tower_directory(day_path):
			print 'loading file:', tower_file
			with open(tower_file, 'rb') as infile:
				tower_number = parse_tower_name(tower_file)
				tower_graph = cPickle.load(infile)
				add_tower_number(tower_graph)
			self.combine_maps(tower_graph)
		master_path = os.path.join(day_path, self.master_filename)
		self.store_data(master_path)
		self.delete_old_maps(day_path)


	def get_day_directories(self, tower_encs_root):
		all_dates = []
		for date_file in os.listdir(tower_encs_root):
			all_dates.append(os.path.join(tower_encs_root, date_file))
		return all_dates

	def get_tower_directory(self, date_path):
		all_towers = []
		for tower_file in os.listdir(date_path):
			all_towers.append(os.path.join(date_path, tower_file))
		return all_towers

	def store_data(self, destination_path):
		with open(destination_path, 'wb') as outfile:
			cPickle.dump(self.master_graph, outfile)
		return True

	def delete_old_maps(self, day_path):
		master_path = os.path.join(day_path, self.master_filename)
		for tower_file in self.get_tower_directory(day_path):
			if tower_file != master_path:
				os.remove(tower_file)
		return True

	def combine_maps(self, other_graph):
		self.master_graph = nx.compose(self.master_graph, other_graph)

def parse_tower_name(tower_file_string):
	no_extension = os.path.splitext(tower_file_string)[0]
	just_file = no_extension.split('/')[-1]
	tower_number_string  = just_file.split('_')[-1]
	try:
		return int(tower_number_string)
	except Exception as e:
		print 'Messed up parsing filename'
		print e
		sys.exit(0)



def main():
	return True
	# imap = InteractionMaps()

if __name__ == '__main__':
    main()
