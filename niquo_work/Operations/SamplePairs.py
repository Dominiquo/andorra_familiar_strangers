import networkx as nx
import Misc.file_constants as constants
import Misc.utils as utils
import cPickle
import pandas as pd
import random
import os

class SamplePairs(object):
	"""Represents Operations on a directory of csv for each tower"""
	def __init__(self, data_path):
		self.data_path = data_path
		self.users = []
		self.pairs = []

	def get_unique_users(self, chunksize=10**6, filter_func=lambda x: True):
		for data_chunk in pd.read_csv(self.data_path, usecols=constants.USEFUL_ROWS, chunksize=chunksize):
			data_chunk = data_chunk.mask(filter_func).dropna()
			self.users += set(data_chunk[constants.SOURCE].unique())
		self.users = list(set(self.users))

	def generate_random_pairs(self, num_pairs, random_seed=42):
		random.seed(random_seed)
		for i in range(num_pairs):
			user1, user2 = random.sample(self.pairs)
			u_pair = user1 + '_' + u2
			self.pairs.append(u_pair)
		print 'created list of', num_pairs, 'pairs of randomly sampled users'

	def store_pairs_df(self, store_path):
		pairs_row = 'USER_PAIRS'
		pairs_dict = {pairs_row: self.pairs}
		df = pd.DataFrame(pairs_dict)
		df.to_csv(df, index=False)
		print 'stored pairs at', store_path, 'with column name', pairs_row
		return True