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
	def __init__(self, directory_path):
		self.directory = directory_path
		self.encounters_graph = nx.Graph()

	def create_interaction_network():
		for tower_file in os.listdir(self.directory):
			tower_path = os.path.join(self.directory, tower_file)
			tower_data = pd.read_csv(tower_path)
			for encs_data in tower_data.groupby(constants.ENC_ROOT):
				self.process_df(encs_data)
		return True

	def process_df(self, encounter_group):
		return True

	def store_data(self, destination_path):
		with open(destination_path, 'wb') as outfile:
			cPickle.dump(outfile)
		return True


def main():
	return True
	# imap = InteractionMaps()

if __name__ == '__main__':
    main()
