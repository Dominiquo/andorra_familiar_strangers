import cPickle
import csv
import os
import sys
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import crossing_paths as cp
import extractData as ex
import itertools

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

def create_friends_file(file,destination,limit=0):
	return None


def filter_xvals(file_path,filter_func=lambda x: True):
	days_row = 2
	all_rows = ex.read_csv(file_path,float('inf'))
	x_vals = [convert_row(row) for row in all_rows if filter_func(row)]
	return x_vals

def create_dist_histogram(x_vals,bins,bin_range,save_file):
	plt.hist(x_vals,bins,range=bin_range)
	plt.savefig(save_file)
	plt.clf()
	return True

def isMorning(time):
	return time_in_range(time,9,17)

def isHome(time):
	return time_in_range(time,20,24)

def isNight(time):
	return time_in_range(time,0,4)

def time_in_range(time,lower,upper):
	time_obj = get_time_obj(time)
	if (lower <= time_obj.hour < upper):
		return True
	return False

def time_obj_to_string(time_obj):
	format_string = "%Y.%m.%d %H:%M:%S"
	return time_obj.strftime(format_string)


def get_time_obj(time_string):
	format_string = "%Y.%m.%d %H:%M:%S"
	return datetime.strptime(time_string,format_string)

def create_time_string_from_delta(time,delta_d,delta_s):
	time_obj = get_time_obj(time)
	delta_obj = timedelta(int(delta_d),int(delta_s))
	new_obj = time_obj + delta_obj
	new_time = time_obj_to_string(new_obj)
	return new_time

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

def encounter_time_conditional(ecnounters_csv,first_cond,second_cond):
	# row = [caller,caller_enc,delta_days,delta_seconds,tower,next_tower,last_time]
	last_time = -1
	delt_d = 2
	delt_s = 3
	filter_func = lambda row: first_cond(row[last_time]) and second_cond(create_time_string_from_delta(row[last_time],row[delt_d],row[delt_s]))
	return filter_xvals(ecnounters_csv,filter_func)

def create_combo_histograms(conditions,encounters_csv,destination_dir):
	base = os.path.basename(encounters_csv)[:-4]
	for first,second in  itertools.combinations(conditions, 2):
		all_encs = encounter_time_conditional(encounters_csv,first,second)
		bins = 150
		bin_range = [0,180]
		filename = destination_dir + '/' + base + '_' + first.func_name + '_' + second.func_name + '.png'
		create_dist_histogram(all_encs,bins,bin_range,filename)

		print 'creating flipped version of', first.func_name, second.func_name

		all_encs = encounter_time_conditional(encounters_csv,second,first)
		filename_flipped = destination_dir + '/' + base + '_' + second.func_name + '_' + first.func_name + '.png'
		create_dist_histogram(all_encs,bins,bin_range,filename_flipped)
		print 'completed'

	return True

def create_graphs_for_time_conditions(encounters_dir,images_dir,conditions):
	for encounters_file in os.listdir(encounters_dir):
		encounters_csv = os.path.join(encounters_dir,encounters_file)
		print 'creating combinations for ', encounters_csv
		create_combo_histograms(conditions,encounters_csv,images_dir)
		print 'completed making plots for ', encounters_csv

def encounters_tower_conditional(encounters_csv,first,second,towers_map):
	# row = [caller,caller_enc,delta_days,delta_seconds,tower,next_tower,last_time]
	first_tower = 4
	next_tower = 5
	filter_func = lambda row: get_tower_code(row,first_tower, towers_map) == first and get_tower_code(row,next_tower,towers_map) == second
	return filter_xvals(encounters_csv,filter_func)

def get_tower_code(row,index,towers_map):
	# TODO: MAKE LESS HACKY
	# very hacky but only need it for this one number.
	return towers_map[row[index][10:-2]]['code']

def get_tower_types(towers_map):
	tower_codes = set([])
	for key,value in towers_map.iteritems():
		tower_codes.add(value['code'])
	return list(tower_codes)

def encounters_on_tower(encounters_csv,images_dir,towers_map):
	tower_types = get_tower_types(towers_map)
	for first,second in  itertools.combinations(tower_types, 2):
		all_encs = encounters_tower_conditional(encounters_csv,first,second,towers_map)
		bins = 150
		bin_range = [0,180]
		filename = destination_dir + '/' + base + '_type_' + str(first) + '_type_' + str(second) + '.png'
		create_dist_histogram(all_encs,bins,bin_range,filename)

		print 'creating flipped version of', first, second

		all_encs = encounters_tower_conditional(encounters_csv,second,first,towers_map)
		filename_flipped = destination_dir + '/' + base + '_type_' + str(second) + '_type_' + str(first) + '.png'
		create_dist_histogram(all_encs,bins,bin_range,filename_flipped)

		print 'completed'
	return True

def time_of_days():
	encounters_dir = '../niquo_data/filtered_data/encounters_CSVs/'
	images_dir = '../niquo_data/filtered_data/plot_images/time_comparisons/'
	# encounters_dir = '../niquo_data/encounters_CSVs/'
	# images_dir = '../niquo_data/plot_images/'
	conditions = [isMorning,isNight,isHome]
	create_graphs_for_time_conditions(encounters_dir,images_dir,conditions)
	print 'done plotting images to be put in ', images_dir


def tower_types():
	encounters_dir = '../niquo_data/filtered_data/encounters_CSVs/'
	images_dir = '../niquo_data/filtered_data/plot_images/tower_types/'
	towers_map = cp.create_tower_mapping()
	for encounters_file in os.listdir(encounters_dir):
		print 'creating tower type histograms for', encounters_file
		encounters_csv = os.path.join(encounters_dir,encounters_file)
		encounters_on_tower(encounters_csv,images_dir,towers_map)
		print 'completed making plots for',encounters_csv


def main(args):
	# time_of_days()
	tower_types()

if __name__ == '__main__':
    main(sys.argv)