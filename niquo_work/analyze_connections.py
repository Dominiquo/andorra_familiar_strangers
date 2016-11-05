import cPickle
import csv
import os
import crossing_paths as cp

def get_all_counts(enc_map):
	all_counts = []
	for caller, receiver_dict in enc_map.iteritems():
		for receiver, occurances in receiver_dict.iteritems():
			all_counts.append(len(occurances))
	return all_counts

