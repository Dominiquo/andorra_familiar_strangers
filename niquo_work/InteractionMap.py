import csv
import os
import cPickle
import itertools
import extractData as ex
from datetime import datetime
import time

class InteractionMap(object):
	"""docstring for InteractionMaps"""
	def __init__(self, directory_path):
		self.directory = directory_path
		self.towers = sorted(os.listdir(self.directory))

	def combine_interaction_maps(self, otherMaps, destination_path, len_combine=7):
		num_days = len(otherMaps)
		for day_index in range(0,num_days,len_combine):
			if (day_index + len_combine) > num_days:
				working_set = otherMaps[day_index:]
			else:
				working_set = otherMaps[day_index:day_index + len_combine]

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

	@classmethod
	def createInteractionMapsSet(cls,days_directory):
		return [InteractionMaps(path) for path in os.listdir(days_directory)]



# ***********************HELPER FUNCTIONS********************


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


def find_mult_enc_single_week(week_path,destination_path,n=2):
	destination_file = open(destination_path,'wb')
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

	print 'loading files complete..'
	for tower, tower_enc_map in all_maps.iteritems():
		print 'checking matches for tower:',tower
		print tower,'contains',len(tower_enc_map),'encounterees'
		for caller, encounteree_map in tower_enc_map.iteritems():
			for caller_enc, times in encounteree_map.iteritems():
				# if len(times) != n:
					# continue
				if caller_enc == caller or (len(times) < n-1):
					continue
				last_time = times[-1]
				delta_days, delta_seconds, next_tower = find_next_encounter(tower,caller,caller_enc,last_time,all_maps)
				if not delta_days:
					continue
				# ROW STRUCTURE OF ENCOUNTERS
				row = [caller,caller_enc,delta_days,delta_seconds,tower,next_tower,last_time]
				encounter_times_csv.writerow(row)
	return True

def find_next_encounter(tower,caller,caller_enc,last_time,all_maps):
	most_recent = []
	for t,enc_map in all_maps.iteritems():
		if t == tower:
			continue

		if (caller in enc_map) and (caller_enc in enc_map[caller]):
			last_enc = find_nearest_time(enc_map[caller][caller_enc],last_time)
			if last_enc != None:
				most_recent.append((last_enc,t))

		elif (caller_enc in enc_map) and (caller in enc_map[caller_enc]):
			last_enc = find_nearest_time(enc_map[caller_enc][caller],last_time)
			if last_enc !=  None:
				most_recent.append((last_enc,t))

	if most_recent and min(most_recent,key=lambda x:x[0]) != (None,None):
		closest_encounter, next_tower = min(most_recent,key=lambda x:x[0])
		delta_days, delta_seconds = time_difference(last_time,closest_encounter)
		return delta_days, delta_seconds, next_tower
	else:
		return None, None, None


def find_nearest_time(encs_list,last_encounter):
	min_time_met = encs_list[0]
	if min_time_met > last_encounter:
		# print 'found time:',min_time_met
		return min_time_met
	else:
		# print 'met before first encounter, finding later encounter...'
		for min_time_met in encs_list:
			if min_time_met > last_encounter:
				# print 'found time after search:',min_time_met
				return min_time_met
	return None



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


def main():
	return True
	# imap = InteractionMaps()

if __name__ == '__main__':
    main()
