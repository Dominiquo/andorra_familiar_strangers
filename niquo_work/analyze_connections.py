import cPickle
import csv
import os
import numpy as np
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
import crossing_paths as cp

def get_encounters_count(enc_map):
	all_counts = []
	for caller, receiver_dict in enc_map.iteritems():
		for receiver, occurances in receiver_dict.iteritems():
			all_counts.append(len(occurances))
	return all_counts

def get_entire_distribution(enc_maps_path):
	all_encounters_count = []
	all_dates = os.listdir(enc_maps_path)
	for date in all_dates:
		date_dir = enc_maps_path + date
		all_towers = os.listdir(date_dir)
		for tower_map in all_towers:
			tower_path = date_dir + '/' + tower_map
			enc_map = cPickle.load(open(tower_path,'rb'))
			all_encounters_count.append(get_encounters_count(enc_map))
	return np.array(all_encounters_count)

def create_dist_histogram(x_vals,save_file):
	plt.hist(x_vals)
	plt.savefig(save_file)
	return True


