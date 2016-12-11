import cPickle
import csv
import os
import sys
import numpy as np
import getMaps as maps
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import itertools


def read_json_file_generator(json_filename,limit=float('inf')):
	with open(filename) as infile:
	for line in infile:
		yield json.loads(line)

def filter_xvals(json_filename,filter_func=lambda row:True):
	return [convert_row(row) for row in read_json_file_generator(json_filename) if filter_func(row)]


def create_graphs_on_tower_type(encounters_json, destination_path):
	return True

def create_graphs_on_times(encounters_json, destination_path):
	all_conditions = [isMorning,isHome,isNight]
	

	return True

def create_friend_dist_graph(encounters_json, destination_path):

	return True

def create_box_plot():
	return True



# *************HELPER FUNCTIONS*************


def majority_check(all_times,filter_func):
	n = len(all_times)



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

def create_dist_histogram(x_vals,bins,bin_range,save_file):
	plt.hist(x_vals,bins,range=bin_range)
	plt.savefig(save_file)
	plt.clf()
	return True