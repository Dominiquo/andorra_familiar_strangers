import cPickle
import csv
import os
import sys
import numpy as np
import matplotlib.pyplot as plt
import crossing_paths as cp
import extractData as ex

def get_encounters_count(enc_map):
	all_counts = []
	for caller, receiver_dict in enc_map.iteritems():
		for receiver, occurances in receiver_dict.iteritems():
			all_counts.append(len(occurances))
	print 'found', len(all_counts),'encounters.'
	return all_counts

def get_entire_distribution(enc_maps_path):
	all_encounters_count = []
	all_dates = os.listdir(enc_maps_path)
	print 'checking', len(all_dates),'date files'
	for date in all_dates:
		date_dir = enc_maps_path + date
		all_towers = os.listdir(date_dir)
		print 'checking', len(all_towers),'for date', date
		for tower_map in all_towers:
			tower_path = date_dir + '/' + tower_map
			enc_map = cPickle.load(open(tower_path,'rb'))
			print 'finding all counts for', tower_path
			all_encounters_count.append(get_encounters_count(enc_map))
	return np.array(all_encounters_count)

def filter_xvals(file_path,save_path,filter_func=lambda x: True):
	days_row = 2
	all_rows = ex.read_csv(file_path,float('inf'))
	x_vals = [convert_row(row) for row in all_rows if filter_func(row)]

	date_filename =  file_path.split('/')[-1]
	filename = save_path + date_filename[:-3] + 'png'
	return x_vals,filename

def create_dist_histogram(x_vals,bins,bin_range,save_file):
	plt.hist(x_vals,bins,range=bin_range)
	plt.savefig(save_file)
	return True

def first_encountered_after_hour():
	return True

def encountered_on_weekends():
	return True

def days_seconds_to_hours(days,seconds):
	total_secs = seconds + days*24*60*60
	total_hours = total_secs/(60*60)
	return total_hours

def convert_row(row):
	days_row = 2
	seconds_row = 3
	days = int(row[days_row])
	seconds = int(row[seconds_row])
	return days_seconds_to_hours(days,seconds)

def main(args):
	file_path = args[0]
	dest_path = args[1]
	print 'FILENAME*****',file_path
	print 'DESTINATION****', dest_path
	bins = 150
	bin_range = [0,180]
	print 'retreiving values...'
	x_vals,filename = filter_xvals(file_path,dest_path)
	create_dist_histogram(x_vals,bins,bin_range,filename)
	print 'complete'

if __name__ == '__main__':
    main(sys.argv)