import csv
import os
# import extractData as ex
import cPickle
import itertools
from datetime import datetime
import time



class TowersPartitioned(object):
	"""Represents Operations on a directory of csv for each tower"""
	def __init__(self, towers_dir):
		self.directory = towers_dir
	
