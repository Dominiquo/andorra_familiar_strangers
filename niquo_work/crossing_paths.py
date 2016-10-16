import csv
import os
import extractData as ex
import pickle
import itertools

START_TIME_INDEX = 3
TOWER_INDEX = 6
CALLER_INDEX = 0
RECEIVER_INDEX = 16

def partition_users_by_tower(filename,limit=float('inf')):
	data_dir = "../niquo_data/"
	towers_dir = "partitioned_towers/"
	tower_file_prefix = "cdr_tower_"
	tower_path_prefix = data_dir + towers_dir
	csv_suffix = ".csv"
	current_towers = set(os.listdir(tower_path_prefix))
	day_1 = "2016.07.01"

	with open(filename,'rb') as csvfile:
		print 'opening file to read from as a csv...'
		data_csv = csv.reader(csvfile,delimiter=';')
		current_row = 0
		print 'will now read', limit, 'rows'
		for row in data_csv:
			tower_id = row[TOWER_INDEX]
			call_time = row[START_TIME_INDEX]
			# TODO: remove when using all data
			# only partition calls from the first of the month
			if not call_time.startswith(day_1):
				break
			# Skip the first row or csv
			if tower_id == 'ID_CELLA_INI':
				continue
			tower_file = tower_file_prefix + tower_id + csv_suffix
			tower_path = tower_path_prefix + tower_file
			if tower_file in current_towers:
				tower_file_obj = open(tower_path, 'a')
			else:
				tower_file_obj = open(tower_path,'wb')

			tower_file_csv = csv.writer(tower_file_obj,delimiter=';')
			tower_file_csv.writerow(row)
			tower_file_obj.close()

			if current_row > limit:
				break
			current_row += 1
			current_towers.add(tower_file)
	print 'created', len(os.listdir(tower_path_prefix)),'new files of towers.'

def create_pair_users_obj(towers_directory,destination_path,file_limit=float('inf')):
	pairs_dictionary = pair_users_from_towers(towers_directory,file_limit)
	pickle.dump(pairs_dictionary,open(destination_path,'wb'))
	return True

def pair_users_from_towers(towers_directory,limit = float('inf')):
	all_tower_files = set(os.listdir(towers_directory))
	print "total count of available tower files:",len(all_tower_files)
	inf = float('inf')
	pair_map = {}
	current = 0
	for tower_name in all_tower_files:
		if current > limit:
			break
		tower_path = towers_directory + tower_name
		all_callers = ex.read_csv(tower_path,inf)
		all_callers.sort(key=lambda val:val[START_TIME_INDEX])
		pairs = find_collisions_from_tower(all_callers)
		for first,second in pairs:
			first_number = first[CALLER_INDEX]
			second_number = second[CALLER_INDEX]
			if first_number in pair_map:
				pair_map[first_number].add(second)
			else:
				pair_map[first_number] = set([second])
			if second_number in pair_map:
				pair_map[second_number].add(first)
			else:
				pair_map[second_number] = set([first])
		current += 1
	return pair_map


def find_collisions_from_tower(tower_rows):
	collision_pairs = set([])
	total_size = len(tower_rows)
	lower_edge = 0
	higher_edge = 0
	for lower_index in range(len(tower_rows)):
		for upper_index in range(lower_index+1,len(tower_rows)):
			lower_row = tower_rows[lower_index]
			upper_row = tower_rows[upper_index]
			if users_met(lower_row,upper_index):
				collision_pairs.add((lower_row,upper_row))
			else:
				break
	return collision_pairs


def users_met(cdr_user_1,cdr_user_2,time_range=1):
	time_1 = cdr_user_1[START_TIME_INDEX]
	time_2 = cdr_user_2[START_TIME_INDEX]
	year_cutoff_index = 10
	hour_start_index = 11
	hour_end_index = 13

	# TODO: refine for corner case of near midnight
	# overlaps of calls
	if time_1[:year_cutoff_index] != time_2[:year_cutoff_index]:
			return False
	
	t1_hour = int(time_1[hour_start_index:hour_end_index])
	t2_hour = int(time_2[hour_start_index:hour_end_index])

	# TODO: add minute cut-off for exactly two hour_end_index
	# range of overlap

	if abs(t1_hour - t2_hour) <= time_range:
		return True
	else:
		return False

def 


def main():
	# data_filename = ex.most_recent
	# print "retrieving data from",data_filename
	# print "partitioning data by tower name..."
	# partition_users_by_tower(data_filename)
	# print "partitioning complete"
	towers_directory = '../niquo_data/partitioned_towers/'
	destination_path = '../niquo_data/paired_callers/paired_dict.p'
	create_pair_users_obj(towers_directory,destination_path)

main()
