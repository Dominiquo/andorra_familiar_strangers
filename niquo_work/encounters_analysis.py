import cPickle
import csv
import os
import sys
import numpy as np
import json
import getMaps as maps
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import itertools


def read_json_file_generator(json_filename,limit=float('inf')):
	with open(json_filename) as infile:
		for line in infile:
			yield json.loads(line)

def filter_xvals(json_filename,filter_func=lambda row:True):
	return [convert_row(row) for row in read_json_file_generator(json_filename) if filter_func(row)]


def create_graphs_on_tower_type(encounters_json, destination_path, n, bins=150, bin_range=[0,180]):
	tower_map = maps.tower_to_activity()
	tower_types = get_tower_types(tower_map)
	axis_ranges = [50, 200, 500, 1000, 2000, 50000]
	for first, second in itertools.permutations(tower_types, 2):
		filter_func = create_loc_filter_func(first, second, n)
		x_vals = filter_xvals(encounters_json, filter_func)
		y_axis = get_axis_range_for_max(max(x_vals), axis_ranges)
		save_file = create_file_name(encounters_json, str(first), str(second), n, False)
		create_dist_histogram(x_vals, bins, bin_range, y_axis,  save_file)
	return True

def create_graphs_on_times(encounters_json, destination_path, n, bins=150, bin_range=[0,180]):
	all_conditions = [isMorning,isHome,isNight]
	axis_ranges = [50, 200, 500, 1000, 2000, 50000]
	for first, second in itertools.permutations(all_conditions, 2):
		filter_func = create_times_filter_func(first,second, n)
		x_vals = filter_xvals(encounters_json, filter_func)
		y_axis = get_axis_range_for_max(max(x_vals), axis_ranges)
		save_file = create_file_name(encounters_json, first.func_name, second.func_name, n, False)
		create_dist_histogram(x_vals, bins, bin_range, y_axis,  save_file)
	return True

def create_loc_filter_func(first, second, towers_map):
	first_times = 'first_times'
	first_tower = 'first_tower'
	next_tower = 'next_tower'
	return lambda row: (first in get_tower_code(row,first_tower, towers_map)) and (second in get_tower_code(row,next_tower,towers_map)) and (len(row[first_times]) >= n)

def create_times_filter_func(first_cond,second_cond, n, use_majority=True):
	first_times = 'first_times'
	next_time = 'next_time'
	last_element = -1
	if use_majority:
		return lambda row: majority_check(row[first_times],first_cond) and second_cond(row[next_time]) and (len(row[first_times]) >= n)
	else:
		return lambda row: first_cond(row[first_times][last_element]) and second_cond(row[next_time]) and (len(row[first_times]) >= n)

def create_encounters_count_filter(n):
	return lambda row: ((row['first_times'] > n) and (row['distance'] > 0))

def create_friend_dist_graph(encounters_json, destination_path, n):
	filter_func = create_encounters_count_filter(n)
	x_vals = filter_xvals(encounters_json, filter_func)
	y_axis = get_axis_range_for_max(max(x_vals), axis_ranges)
	save_file = create_file_name(encounters_json, None, None, n, True)
	create_dist_histogram(x_vals, bins, bin_range, y_axis,  save_file)
	return True

def create_box_plot():
	return True



# *************HELPER FUNCTIONS*************

def create_file_name(json_filename, first, second, n, is_graph):
	dir_name = os.path.dir_name(json_filename)
	suffix = '.png'
	if is_graph:
		filename = os.path.basename(json_filename)[:21] + '_' + 'DISTANCES' + 'N_' + str(n)
	else: 
		filename = os.path.basename(json_filename)[:21] + '_' + first + '_' + second + '_N_' + str(n)
	return os.path.join(dir_name, filename  + suffix)


def graph_filter_vals(encounters_json, filter_func):
	return [row['distance'] for row in read_json_file_generator(json_filename) if filter_func(row)]	

def convert_row(row):
	days = int(row['delta_days'])
	seconds = int(row['delta_seconds'])
	return days_seconds_to_hours(days,seconds)

def get_tower_code(row,index,towers_map):
	# TODO: MAKE LESS HACKY
	# very hacky but only need it for this one number.
	try:
		return towers_map[row[index][10:-2]]
	except:
		return set([])

def get_axis_range_for_max(max_val, axis_ranges):
	largest = float('-inf')
	for val in axis_ranges:
		if val > largest and val < max_val:
			largest = val
	return largest


def get_tower_types(towers_map):
	tower_codes = set([])
	for key,value in towers_map.iteritems():
		tower_codes = tower_codes.union(value)

	# manual clenaing
	if '' in tower_codes:
		tower_codes.remove('')
	if 'Category code' in tower_codes:
		tower_codes.remove('Category code')

	return list(tower_codes)

def majority_check(all_times,filter_func):
	n = len(all_times)
	positive_vals = sum([1 for time_str in all_times if filter_func(time_str)])
	if (n % 2) == 0:
		return (positive_vals >= (n/2))
	else:
		return (positive_vals > (n/2))

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

def days_seconds_to_hours(days,seconds):
	total_secs = seconds + days*24*60*60
	total_hours = total_secs/(60*60)
	return total_hours

def create_dist_histogram(x_vals,bins, bin_range, y_axis, save_file):
	plt.hist(x_vals,bins,range=bin_range)
	plt.ylim([0, y_axis])
	plt.savefig(save_file)
	plt.clf()
	return True

def Main():
	encounters_json = '../niquo_data/v2_data_root/encounters_files/2016.07.01_2016.07.07_encounter.json'
	destination_path = '../niquo_data/v2_data_root/plots'

	for n in range(2,20,4):
		print 'creating graphs for n =', n
		# create_graphs_on_tower_type(encounters_json, destination_path, n)
		create_graphs_on_times(encounters_json, destination_path, n)
		create_friend_dist_graph(encounters_json, destination_path, n)

	return True


if __name__ == '__main__':
    Main()