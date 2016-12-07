import csv
import os
import cPickle
import itertools
import extractData as ex
from datetime import datetime
import time

START_TIME_INDEX = 3
TOWER_INDEX = 6
CALLER_INDEX = 0
RECEIVER_INDEX = 16
DATE_INDEX = 10


class TowersPartitioned(object):
	"""Represents Operations on a directory of csv for each tower"""
	def __init__(self, towers_dir):
		self.directory = towers_dir

	def pair_users_from_towers(self,destination_path,limit=10000):
		for date_dir in self.generate_dates():
			date_path = os.path.join(self.directory,date_dir)
			tower_files = set(os.listdir(date_path))
			dest_date_dir = os.path.join(destination_path,date_dir)
			if not os.path.exists(dest_date_dir):
				os.makedirs(dest_date_dir)
				print 'made directory',dest_date_dir
		tower_count = 1
		for tower_name in tower_files:
			total_towers = len(tower_files)
			print 'creating pair map object', tower_count, '/', total_towers,'for day',date_dir
			tower_count += 1
			tower_path = os.path.join(date_path,tower_name)
			dest_pickle_file = dest_date_dir + '/' + tower_name.split('.')[0] + '.p'
			if os.path.isfile(dest_pickle_file):
				continue
			self.pair_users_single_file(tower_path,dest_pickle_file,limit)
		return True

	def pair_users_single_file(self,tower_path,dest_pickle_file,limit):
		all_callers = ex.read_csv(tower_path,float('inf'))
		if len(all_callers) > limit:
					return False
		print 'sorting rows...'
		all_callers.sort(key=lambda val:val[START_TIME_INDEX])
		print 'rows sorted.'
		print 'total row count', len(all_callers)
		print 'finding collision pairs...'
		pairs = find_collisions_from_tower(all_callers)
		print 'found', len(pairs), 'pairs of collisions.'
		pair_map = {}
		print 'building map for pairs to be stored at', dest_pickle_file
		for first,second in pairs:
			first_number = first[CALLER_INDEX]
			first_call_time = first[START_TIME_INDEX]
			second_number = second[CALLER_INDEX]
			second_call_time = second[START_TIME_INDEX]
			avg_call_time = average_call_times(first_call_time,second_call_time)
			tower_id = first[TOWER_INDEX]
			if first_number in pair_map:
				first_num_dict = pair_map[first_number]
				if second_number in first_num_dict:
					first_num_dict[second_number].append(avg_call_time)
				else:
					first_num_dict[second_number] = [avg_call_time]
			else:
				pair_map[first_number] = {second_number: [avg_call_time]}
		print 'dumping pickle file...'
		cPickle.dump(pair_map,open(dest_pickle_file,'wb'))
		print 'created file for', dest_pickle_file
		return True

	def generate_dates(self):
		all_dates_dirs = sorted(set(os.listdir(self.directory)))
		print "total count of available date files:",len(all_dates_dirs)
		for date_dir in all_dates_dirs:
			print "checking towers from", date_dir
			yield date_dir


# **********************************************
# HELPER FUNCTIONS FOR COMPARING RAW FILES
# **********************************************


def find_collisions_from_tower(tower_rows,time_range=1):
	collision_pairs = set([])
	total_size = len(tower_rows)
	lower_edge = 0
	higher_edge = 0
	for lower_index in range(len(tower_rows)):
		for upper_index in range(lower_index+1,len(tower_rows)):
			lower_row = tower_rows[lower_index]
			upper_row = tower_rows[upper_index]
			if users_met(lower_row,upper_row,time_range):
				collision_pairs.add((tuple(lower_row),tuple(upper_row)))
			else:
				break
	return collision_pairs



def users_met(cdr_user_1,cdr_user_2,time_range=1):
	"""ASSUMES SECOND NUMBER IS ALWAYS LATER THAN FIRST"""
	time_1 = cdr_user_1[START_TIME_INDEX]
	time_2 = cdr_user_2[START_TIME_INDEX]
	year_cutoff_index = 10
	hour_start_index = 11
	hour_end_index = 13
	min_start = 14
	min_finish = 16

	# TODO: refine for corner case of near midnight
	# overlaps of calls
	if time_1[:year_cutoff_index] != time_2[:year_cutoff_index]:
			return False
	
	t1_hour = int(time_1[hour_start_index:hour_end_index])
	t2_hour = int(time_2[hour_start_index:hour_end_index])

	t1_min = int(time_1[min_start:min_finish])
	t2_min = int(time_2[min_start:min_finish])

	if ((t2_hour - t1_hour) < time_range):
		return True
	elif ((t2_hour - t1_hour) == time_range) and (t2_min <= t1_min):
		return True
	else:
		return False 


def average_call_times(time_stamp_1,time_stamp_2):
	hour_s = 0
	hour_f = 2
	min_s = 3
	min_f = 5
	sec_s = 6
	sec_f = 8
	head = time_stamp_1[:DATE_INDEX]
	time1 = time_stamp_1[DATE_INDEX+1:]
	time2 = time_stamp_2[DATE_INDEX+1:]
	avgh = (int(time1[hour_s:hour_f]) + int(time2[hour_s:hour_f]))/2
	avgm = (int(time1[min_s:min_f]) + int(time2[min_s:min_f]))/2
	avgs = (int(time1[sec_s:sec_f]) + int(time2[sec_s:sec_f]))/2
	zero_pad = lambda v: '0' + str(v) if v < 10 else str(v)
	return head + ' ' + zero_pad(avgh) + ':' + zero_pad(avgm) + ':' + str(avgs)


def main():
	towers_dir = 'some_dir/'
	dest_path = '/pickle.p'
	print 'creating user maps for ', towers_dir, 'to be stored at', dest_path
	partitioned = TowersPartitioned(towers_dir)
	partitioned.pair_users_from_towers(dest_path)
	print 'completed pairing users.'

if __name__ == '__main__':
    main()