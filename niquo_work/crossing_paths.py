import csv
import os
import extractData as ex
import pickle
import itertools

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
	all_dates_dirs = set(os.listdir(towers_directory))
	print "total count of available date files:",len(all_dates_dirs)
	inf = float('inf')
	total_dates = len(all_dates_dirs)

	for date_dir in all_dates_dirs:
		print "checking towers from", date_dir
		date_path = towers_directory + date_dir + '/'
		tower_files = set(os.listdir(date_path))
		dest_date_dir = destination_path + date_dir + '/'
		if not os.path.exists(dest_date_dir):
			print 'made directory',dest_date_dir
			os.makedirs(dest_date_dir)	

		for tower_name in tower_files:
			tower_path = date_path + tower_name
			dest_pickle_file = dest_date_dir + tower_name.split('.')[0] + '.p'
			all_callers = ex.read_csv(tower_path,inf)
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
						first_num_dict[second_number].add(avg_call_time)
					else:
						first_num_dict[second_number] = set([avg_call_time])
				else:
					pair_map[first_number] = {second_number:set([avg_call_time])}

			pickle.dump(pair_map,open(dest_pickle_file,'wb'))

	return pair_map


def find_next_meeting(meetings_path,destination_path,limit=float('inf')):
	# TODO: PARALLELIZE THIS
#	dates
# 	towers
# 	destination ecounter times
#	 for every encounteree for every encounterer for each tower in each date
# 		find the nearest date/time in a different tower that those two encountered
# 			store the encounter time difference in csv file
# 	
# 
# 	
	return True



def average_call_times(time_stamp_1,time_stamp_2):
	hour_s = 11
	hour_f = 13
	min_s = 14
	min_f = 16
	sec_s = 17
	sec_f = 19
	head = time_stamp_1[:DATE_INDEX]
	time1 = time_stamp_1[hour_s:]
	time2 = time_stamp_2[hour_s:]
	avgh = (int(time1[hour_s:hour_f]) + int(time2[hour_s:hour_f]))/2
	avgm = (int(time1[min_s:min_f]) + int(time2[min_s:min_f]))/2
	avgs = (int(time1[sec_s:sec_f]) + int(time2[sec_s:sec_f]))/2
	return head + ' ' + str(avgh) + ':' + str(avgm) + ':' + str(avgs)


def find_collisions_from_tower(tower_rows,time_range=2):
	collision_pairs = set([])
	total_size = len(tower_rows)
	lower_edge = 0
	higher_edge = 0
	print 'starting the comparision'
	for lower_index in range(len(tower_rows)):
		for upper_index in range(lower_index+1,len(tower_rows)):
			lower_row = tower_rows[lower_index]
			upper_row = tower_rows[upper_index]
			if users_met(lower_row,upper_row,time_range):
				collision_pairs.add((tuple(lower_row),tuple(upper_row)))
			else:
				break
	return collision_pairs


def users_met(cdr_user_1,cdr_user_2,time_range=2):
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



def find_times(encouters_set,num_encounters):
	# TODO: update later if we want to change criterion
	# difference between first time met and last time met
	times = []
	encounter_map = {}
	for encouter in encouters_set:
		user_id = encouter[0]
		time = encouter[1]
		if user_id in encouter:
			encounter_map[user_id].append(time)
		else:
			encounter_map[user_id] = [time]
	for user,met_times in encounter_map.iteritems():
		if len(met_times) > num_encounters:
			times.append(get_time_sum(met_times))
	return times


def main():
	print "partitioning data by tower name..."
	data_filename = '../../data_repository/datasets/telecom/cdr/201607-AndorraTelecom-CDR.csv'
	partition_users_by_tower(data_filename)
	print "partitioning complete"
	#towers_directory = '../niquo_data/partitioned_towers/'
	#destination_path = '../niquo_data/paired_callers/'
	#pair_users_from_towers(towers_directory,destination_path)

main()
