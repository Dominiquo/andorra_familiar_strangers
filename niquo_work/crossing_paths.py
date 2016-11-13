import csv
import os
import extractData as ex
import cPickle
import itertools
from datetime import datetime
import time

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
		cPickle.dump(tower_map,open(pickle_path,'wb'))

	return tower_map


def partition_users_by_tower(filename,limit=float('inf')):
	data_dir = "../niquo_data/"
	towers_dir = "partitioned_towers/"
	tower_file_prefix = "cdr_tower_"
	tower_map = create_tower_mapping()
	tower_path_prefix = data_dir + towers_dir
	csv_suffix = ".csv"
	current_towers = set([])
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
	

def pair_users_from_towers(towers_directory,destination_path,limit = 10000):
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
			if len(all_callers) > limit:
				continue
			print 'sorting rows...'
			all_callers.sort(key=lambda val:val[START_TIME_INDEX])
			print 'rows sorted.'
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

	return pair_map

def combine_pair_mappings(first_map, second_map):
	for user,encounters_map in second_map.iteritems():
		if user in first_map:
			first_encounters_map = first_map[user]
			for encountered_user,encounters_list in encounters_map.iteritems():
				if encountered_user in first_encounters_map:
					first_encounters_map[encountered_user].extend(encounters_list)
				else:
					first_encounters_map[encountered_user] = encounters_list
		else:
			first_map[user] = encounters_map

	return first_map

def combine_tower_maps(dates_path, destination_path, len_combine=7):
	all_dates = sorted(os.listdir(dates_path))
	num_days = len(all_dates)
	for day_index in range(0,num_days,len_combine):
		if day_index + len_combine > num_days:
			working_set = all_dates[day_index:]
		else:
			working_set = all_dates[day_index:day_index + len_combine]
		num_in_set = len(working_set)
		first_day = working_set[0]
		first_day_path = dates_path + '/' + first_day + '/'
		first_day_towers = set(os.listdir(first_day_path))
		if num_in_set == 1:
			continue
		date_dir = destination_path + first_day + "_" + working_set[-1] 
		print 'combining tower files to be stored in ', date_dir
		if not os.path.exists(date_dir):
				os.makedirs(date_dir)
		all_dates_paths = [first_day_path] 
		for day_dir_index in range(1,num_in_set):
			day_path = dates_path + '/' + working_set[day_dir_index] + '/'
			all_dates_paths.append(day_path)
		all_towers,tower_to_paths = get_intersecting_towers_map(all_dates_paths)
		for tower in all_towers:
			map_dump_loc = date_dir + '/' + tower
			if os.path.isfile(map_dump_loc):
				continue
			days_paths = tower_to_paths[tower]
			print 'combining maps for', tower, 'for ', len(days_paths), 'days'
			combined_map = combine_days_for_tower(tower,days_paths)
			cPickle.dump(combined_map, open(map_dump_loc,'wb'))
	return True

def get_intersecting_towers_map(paths_list):
	all_towers = set([])
	paths_to_towers = {}
	tower_to_paths = {}

	for day_path in paths_list:
		days_towers = os.listdir(day_path)
		paths_to_towers[day_path] = set([])
		for tower_file in days_towers:
			paths_to_towers[day_path].add(tower_file)
			all_towers.add(tower_file)

	for tower in all_towers:
		current_intersections = []
		for path, towers in paths_to_towers.iteritems():
			if tower in towers:
				current_intersections.append(path)
		current_intersections.sort()
		tower_to_paths[tower] = current_intersections

	return all_towers,tower_to_paths

def combine_days_for_tower(tower_file,days_paths):
	"""days_paths expected to be sorted"""
	first_day_path = days_paths[0]
	first_day_tower_pairing = first_day_path + tower_file
	first_day_map = cPickle.load(open(first_day_tower_pairing,'rb'))
	if len(days_paths) == 1:
		return first_day_map
	for day_dir in days_paths[1:]:
		next_day_tower = day_dir + tower_file
		next_day_tower_map = cPickle.load(open(next_day_tower, 'rb'))
		first_day_map = combine_pair_mappings(first_day_map, next_day_tower_map)
	return first_day_map


def combine_towers_by_path(left_path,right_path,destination_path):
	left_map = cPickle.load(open(left_path,'rb'))
	right_map = cPickle.load(open(right_path,'rb'))
	combined_map = combine_pair_mappings(left_map,right_map)
	cPickle.dump(combined_map,open(destination_path,'wb'))
	return True


def create_delta_time_file(meetings_path, destination_path, num_encounters=2, limit=float('inf')):
	""" being abandoned because this method of finding encounters is too slow """
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
			user_pair_map = cPickle.load(open(pair_map_path,'rb'))
			print 'checking users from', pair_map_path
			now_time = time.time()
			for user,encounters_dict in user_pair_map.iteritems():
				prev_time = now_time
				now_time = time.time()
				print 'time since previous loop:', now_time - prev_time
				print 'checking if user', user, 'made multiple encounters'
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

# *********************************************
#FIND ENCOUNTER GIVEN THAT ALL FILES ARE OPEN

def find_mult_enc_single_week(week_path,destination_path,n=2):
	destination_file = open(destination_path,'a')
	all_towers = os.listdir(week_path)
	print 'checking', len(all_towers),'total tower files...'
	all_maps = {}
	encounter_times_csv = csv.writer(destination_file,delimiter=';')
	print 'openend', destination_file,'for writing csv data...'
	print 'loading tower files in to RAM..'
	tower_count = 1
	for tower in all_towers:
		print 'adding tower',tower_count,'/',len(all_towers)
		tower_count += 1
		tower_path = week_path + tower
		all_maps[tower] = cPickle.load(open(tower_path,'rb'))
		if tower_count > 3:
			break

	print 'loading files complete..'
	for tower, tower_enc_map in all_maps.iteritems():
		print 'checking matches for tower:',tower
		print tower,'contains',len(tower_enc_map),'encounterees'
		for caller, encounteree_map in tower_enc_map.iteritems():
			for caller_enc, times in encounteree_map.iteritems():
				if caller_enc == caller or (len(times) < n-1):
					continue
				last_time = times[-1]
				delta_days, delta_seconds, next_tower = find_next_encounter(tower,caller,caller_enc,last_time,all_maps)
				if not delta_days:
					continue
				# ROW STRUCTURE OF ENCOUNTERS
				row = [caller,caller_enc,delta_days,delta_seconds,tower,next_tower,last_time]
				print 'found encounter..'
				encounter_times_csv.writerow(row)
	return True

def find_next_encounter(tower,caller,caller_enc,last_time,all_maps):
	most_recent = []
	for t,enc_map in all_maps.iteritems():
		if t == tower:
			continue

		if (caller in enc_map) and (caller_enc in enc_map[caller]):
			last_enc = find_nearest_time(enc_map[caller][caller_enc],last_time)
			if last_time != None:
				most_recent.append((last_enc,t))

		elif (caller_enc in enc_map) and (caller in enc_map[caller_enc]):
			last_enc = find_nearest_time(enc_map[caller_enc][caller],last_time)
			if last_enc != None:
				most_recent.append((last_enc,t))

	if most_recent and min(most_recent,key=lambda x:x[0]) != None:
		closest_encounter, next_tower = min(most_recent,key=lambda x:x[0])
		delta_days, delta_seconds = time_difference(last_time,closest_encounter)
		return delta_days, delta_seconds, next_tower
	else:
		return None, None, None


def find_nearest_time(encs_list,last_encounter):
	min_time_met = encs_list[0]
	if min_time_met > last_encounter:
		print 'found time:',min_time_met
		return min_time_met
	else:
		print 'met before first encounter, finding later encounter...'
		for min_time_met in encs_list:
			if min_time_met > last_encounter:
				print 'found time after search:',min_time_met
				return min_time_met
	return None


def encounter_after_threshold(time,hour):
	return True


# ********************************************** 



def open_file_find_nearest_time(tower_file,user,encountered_user,last_encounter):
	"""Not used anymore since this method of reading is too slow"""
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
	diff = time_2 - time_1
	return diff.days,diff.seconds


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





def main():
	func_dict = {"create_tower_mapping": create_tower_mapping,
				"partition_users_by_tower": partition_users_by_tower,
				"pair_users_from_towers": pair_users_from_towers,
				"create_delta_time_file": create_delta_time_file}
	# function = func_dict[arg[0]]


	#data_filename = '../../data_repository/datasets/telecom/cdr/201607-AndorraTelecom-CDR.csv'
	#partition_users_by_tower(data_filename)


	# towers_directory = '../niquo_data/partitioned_towers/'
	# destination_path = '../niquo_data/paired_callers/'
	# pair_users_from_towers(towers_directory,destination_path)


	# deltas_2enc_file = '../niquo_data/encounter_n_2.csv'


	# create_delta_time_file(destination_path,deltas_2enc_file,2)
	# print 'completed finding encounter time difference for n=2'

	# dates_path = '../niquo_data/paired_callers/'
	# combined_dates_path = '../niquo_data/combined_callers/'
	# combine_tower_maps(dates_path, combined_dates_path)



	week_path = '../niquo_data/combined_callers/2016.07.01_2016.07.07/'
	destination_path = '../niquo_data/week_encounter_n_2.csv'
	find_mult_enc_single_week(week_path,destination_path,2)	

	# week_path = '../niquo_data/combined_callers/'
	# destination_path = '../niquo_data/week_encounter_n_2.csv'
	# all_week_paths = os.listdir(week_path)
	# for week_dir in all_week_paths:
	# 	current_week_path = week_path + week_dir
	# 	find_mult_enc_single_week(current_week_path,destination_path,2)	


	# create_delta_time_file(combined_dates_path, deltas_2enc_file,2)


if __name__ == '__main__':
    main()
