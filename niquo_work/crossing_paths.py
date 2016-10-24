import csv
import os
import extractData as ex
import pickle
import cPickle
import itertools
from datetime import datetime

START_TIME_INDEX = 3
TOWER_INDEX = 6
CALLER_INDEX = 0
RECEIVER_INDEX = 16
DATE_INDEX = 10

def create_tower_mapping(filepath=ex.towers,pickle_path=None):
	geo_map = {}
	tower_map = {}
	lat = 2
	lon = 3
	with open(filepath) as tower_file:
		towers_data = [row for row in csv.reader(tower_file.read().splitlines())]

	for i,tower in enumerate(towers_data):
		if i == 0:
			continue
		t_lat = tower[lat]
		t_lon = tower[lon]
		tower_id = tower[1]
		lat_lon = (t_lat,t_lon)
   		if lat_lon in geo_map:
   			tower_map[tower_id] = geo_map[lat_lon]
		else:
			geo_map[lat_lon] = tower_id
			tower_map[tower_id] = tower_id
	if pickle_path:
		pickle.dump(tower_map,open(pickle_path,'wb'))

	return tower_map

def partition_users_by_tower(filename,limit=float('inf')):
	data_dir = "../niquo_data/"
	towers_dir = "partitioned_towers/"
	tower_file_prefix = "cdr_tower_"
	tower_map = create_tower_mapping()
	tower_path_prefix = data_dir + towers_dir
	csv_suffix = ".csv"
	current_towers = set([])
	# JULY_MONTH = 7
	# days = get_dates_for_month(JULY_MONTH)
	files_count = 1

	with open(filename,'rb') as csvfile:
		print 'opening file to read from as a csv...'
		data_csv = csv.reader(csvfile,delimiter=';')
		current_row = 0
		print 'will now read', limit, 'rows'
		for row in data_csv:
			#skip the first row
			if row[TOWER_INDEX]== 'ID_CELLA_INI':
				continue
			pre_funnel_id = row[TOWER_INDEX]
			if pre_funnel_id not in tower_map:
				tower_id = pre_funnel_id
			else:
				tower_id = tower_map[pre_funnel_id]
			call_time = row[START_TIME_INDEX]
			call_date = call_time[:DATE_INDEX]
			date_path = data_dir + towers_dir + call_date + '/'
			# check if the path for the date exists yet
			if not os.path.exists(date_path):
				os.makedirs(date_path)

			tower_file = tower_file_prefix + tower_id + csv_suffix
			tower_path = date_path + tower_file
			if tower_path in current_towers:
				tower_file_obj = open(tower_path, 'a')
			else:	
				files_count += 1
				tower_file_obj = open(tower_path,'wb')

			tower_file_csv = csv.writer(tower_file_obj,delimiter=';')
			tower_file_csv.writerow(row)
			tower_file_obj.close()

			if current_row > limit:
				break
			current_row += 1
			current_towers.add(tower_path)
	print 'created',files_count,'new files of towers.'

def pair_users_from_towers(towers_directory,destination_path,limit = float('inf')):
	all_dates_dirs = sorted(set(os.listdir(towers_directory)))
	print "total count of available date files:",len(all_dates_dirs)
	inf = float('inf')
	for date_dir in all_dates_dirs:
		print "checking towers from", date_dir
		date_path = towers_directory + date_dir + '/'
		tower_files = set(os.listdir(date_path))
		dest_date_dir = destination_path + date_dir + '/'
		if not os.path.exists(dest_date_dir):
			print 'made directory',dest_date_dir
			os.makedirs(dest_date_dir)	
		tower_count = 1
		for tower_name in tower_files:
			total_towers = len(tower_files)
			print 'creating pair map object', tower_count, '/', total_towers,'for day',date_dir
			tower_count += 1
			tower_path = date_path + tower_name
			dest_pickle_file = dest_date_dir + tower_name.split('.')[0] + '.p'
			if os.path.isfile(dest_pickle_file):
				continue
			all_callers = ex.read_csv(tower_path,inf)
			if len(all_callers) > 8000:
				continue
			all_callers.sort(key=lambda val:val[START_TIME_INDEX])
			pairs = find_collisions_from_tower(all_callers)
			pair_map = {}
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

			pickle.dump(pair_map,open(dest_pickle_file,'wb'))

	return pair_map


def find_next_meeting(meetings_path, destination_path, num_encounters=2, limit=float('inf')):
	all_dates = sorted(os.listdir(meetings_path))
	destination_file = open(destination_path,'wb')
	encounter_times_csv = csv.writer(destination_file,delimiter=';')
	for date in all_dates:
		print 'checking',len(all_dates),'dates'
		towers_path = meetings_path + "/" + date
		all_towers = sorted(os.listdir(towers_path))
		for tower in all_towers:
			print 'checking', len(all_towers),'on day:',date
			pair_map_path = towers_path + "/" + tower
			user_pair_map = pickle.load(open(pair_map_path,'rb'))
			print 'checking users from', pair_map_path
			for user,encounters_dict in user_pair_map.iteritems():
				for encountered_user,encounters_set in encounters_dict.iteritems():
					# ignore case when person 'encounters' themselves
					if encountered_user == user:
						continue
					current_encounters_count = len(encounters_set)
					last_encounter = max(encounters_set)
					delta_days,delta_seconds = search_next_encounter(meetings_path, user, encountered_user, tower,last_encounter,current_encounters_count,num_encounters)
					if not delta_days:
						continue
					row = [user,encountered_user,delta_days,delta_seconds]
					encounter_times_csv.writerow(row)
	return True


def search_next_encounter(meetings_path, user, encountered_user, tower_init,last_encounter,current_encounters_count,num_encounters):
	all_dates = sorted(os.listdir(meetings_path))
	last_encounter_date = last_encounter[:DATE_INDEX]
	for date in all_dates:
		if date < last_encounter:
			continue
		towers_dir = meetings_path + '/' + date
		all_towers = os.listdir(towers_dir)
		for tower in all_towers:
			tower_file = towers_dir + "/" + tower
			print 'checking file',tower_file,'for matches on day',date
			if (num_encounters - current_encounters_count) == 1:
				# skip this tower since we want meetings in a different tower
				if tower == tower_init:
					continue
				min_time_met = open_file_find_nearest_time(tower_file,user,encountered_user,last_encounter)
				if not min_time_met:
					continue
				delta_h,delta_s = time_difference(last_encounter,min_time_met)
			else:
				if tower != tower_init:
					continue
				min_time_met = open_file_find_nearest_time(tower_file,user, encountered_user, last_encounter)
				if not min_time_met:
					continue
				new_encounter_count = current_encounters_count + 1
				delta_h,delta_s = search_next_encounter(meetings_path,user,encountered_user,tower_init,min_time_met,new_encounter_count,num_encounters)
				return delta_h,delta_s
	return None,None


def open_file_find_nearest_time(tower_file,user,encountered_user,last_encounter):
	tower_pair_map = cPickle.load(open(tower_file,'rb'))
	users_encounters = tower_pair_map.get(user,None)
	if not users_encounters:
		return None
	times_list = users_encounters.get(encountered_user,None)
	if not times_list:
		return None
	min_time_met = times_list[0]
	if min_time_met > last_encounter:
		return min_time_met
	else:
		for min_time_met in times_list:
			if min_time_met > last_encounter:
				return min_time_met
	return None

def time_difference(first_time,second_time):
	format_string = "%Y.%m.%d %H:%M:%S"
	time_1 = datetime.strptime(first_time,format_string)
	time_2 = datetime.strptime(second_time,format_string)
	time_difference = time_2 - time_1
	return time_difference.days,time_difference.seconds


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


def find_collisions_from_tower(tower_rows,time_range=1):
	collision_pairs = set([])
	total_size = len(tower_rows)
	lower_edge = 0
	higher_edge = 0
	print 'starting the comparision on',len(tower_rows),'of data.'
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


def analyze_pairings_dict(filename,meetings):
	all_remeet_times = []
	encounters_dict = pickle.load(open(filename,'rb'))
	for call_id,encounters in encounters_dict.iteritems():
		all_remeet_times += find_times(encounters,meetings)
	return all_remeet_times


def main():
	#print "partitioning data by tower name..."
	#data_filename = '../../data_repository/datasets/telecom/cdr/201607-AndorraTelecom-CDR.csv'
	#partition_users_by_tower(data_filename)
	#print "partitioning complete"
	#towers_directory = '../niquo_data/partitioned_towers/'
	destination_path = '../niquo_data/paired_callers/'
	#pair_users_from_towers(towers_directory,destination_path)
	deltas_2enc_file = '../niquo_data/encounter_n_2.csv'
	find_next_meeting(destination_path,deltas_2enc_file,2)
	print 'completed finding encounter time difference for n=2'



main()
