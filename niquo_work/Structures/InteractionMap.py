import csv
import os
import cPickle
import itertools
from datetime import datetime
import Misc.file_constants as constants
import networkx as nx
import pandas as pd
import time

class InteractionMap(object):
	"""docstring for InteractionMaps"""
	def __init__(self, root_path):
		self.directory = directory_path

	def get_day_directories(self, tower_encs_root):
		for date_file in os.listdir(tower_encs_root):
			date_path = os.path.join(tower_encs_root, date_file)
			yield date_path

	def get_tower_directory(self, date_path):
		for tower_file in os.listdir(date_path):
			yield os.path.join(date_path, tower_file)

	def store_data(self, destination_path):
		with open(destination_path, 'wb') as outfile:
			cPickle.dump(outfile)
		return True

	def combine_maps(self, map):
		return True

def main():
	return True
	# imap = InteractionMaps()

if __name__ == '__main__':
    main()
