import csv
import os
import extractData as ex
import pickle
import itertools

START_TIME_INDEX = 3
TOWER_INDEX = 6
CALLER_INDEX = 0
RECEIVER_INDEX = 16

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
	days = ["2016.07.0" + str(d) if d<10 else "2016.07." + str(d) for d in range(1,32)]
	date_index = 10
	unfound_towers = set([])
	unfound_count = 0
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
				unfound_count += 1
				unfound_towers.add(pre_funnel_id)
				continue
			tower_id = tower_map[pre_funnel_id]
			call_time = row[START_TIME_INDEX]
			call_date = call_time[:date_index]
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
	print 'number of entries not entered',unfound_count
	print 'could not find towers',unfound_towers
	print 'created',files_count,'new files of towers.'

def create_pair_users_obj(towers_directory,destination_path,file_limit=float('inf')):
	pairs_dictionary = pair_users_from_towers(towers_directory,file_limit)
	pickle.dump(pairs_dictionary,open(destination_path,'wb'))
	return True

def pair_users_from_towers(towers_directory,limit = float('inf')):
	all_tower_files = set(os.listdir(towers_directory))
	print "total count of available tower files:",len(all_tower_files)
	inf = float('inf')
	pair_map = {}
	current = 1
	total_towers = len(all_tower_files)
	for tower_name in all_tower_files:
		print "checking tower",current,"of",total_towers
		if current > limit:
			break
		tower_path = towers_directory + tower_name
		all_callers = ex.read_csv(tower_path,inf)
		all_callers.sort(key=lambda val:val[START_TIME_INDEX])
		pairs = find_collisions_from_tower(all_callers)
		for first,second in pairs:
			first_number = first[CALLER_INDEX]
			first_call_time = first[START_TIME_INDEX]
			second_number = second[CALLER_INDEX]
			second_call_time = second[START_TIME_INDEX]
			tower_id = first[TOWER_INDEX]
			if first_number in pair_map:
				pair_map[first_number].add((second_number,second_call_time,tower_id))
			else:
				pair_map[first_number] = set([(second_number,second_call_time,tower_id)])
			# TODO: check to make sure this is valid and stores all information
			# if second_number in pair_map:
			# 	pair_map[second_number].add((first_number,second_call_time,tower_id))
			# else:
			# 	pair_map[second_number] = set([(first_number,second_call_time,tower_id)])
		current += 1
	return pair_map

def combine_tower_mappings(filepath):

	return -1


def find_collisions_from_tower(tower_rows):
	collision_pairs = set([])
	total_size = len(tower_rows)
	lower_edge = 0
	higher_edge = 0
	for lower_index in range(len(tower_rows)):
		for upper_index in range(lower_index+1,len(tower_rows)):
			lower_row = tower_rows[lower_index]
			upper_row = tower_rows[upper_index]
			if users_met(lower_row,upper_row):
				collision_pairs.add((tuple(lower_row),tuple(upper_row)))
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


def get_time_sum(meeting_times):
	# TODO: define how we want to value this.
	return -1


def main():
	data_filename = ex.most_recent
	print "retrieving data from",data_filename
	print "partitioning data by tower name..."
	partition_users_by_tower(data_filename)
	print "partitioning complete"
	# towers_directory = '../niquo_data/partitioned_towers/'
	# destination_path = '../niquo_data/paired_callers/paired_dict.p'
	# create_pair_users_obj(towers_directory,destination_path)

main()
