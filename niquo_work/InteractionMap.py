import csv
import os
import cPickle
import itertools
from datetime import datetime
import time

class InteractionMap(object):
	"""docstring for InteractionMaps"""
	def __init__(self, directory_path):
		self.directory = directory_path
		self.base = os.path.basename(self.directory)
		self.towers = [os.path.join(os.getcwd(),path) for path in  sorted(os.listdir(self.directory))]

	def get_towers_paths(self):
		return self.towers

	@staticmethod
	def combine_interaction_maps(combMaps, destination_path, len_combine=7):
		num_days = len(combMaps)
		for day_index in range(0,num_days,len_combine):
			if (day_index + len_combine) > num_days:
				working_set = combMaps[day_index:]
			else:
				working_set = combMaps[day_index:day_index + len_combine]
			first_day = working_set[0]
			num_in_set = len(working_set)
			if num_in_set == 1:
				continue
			date_dir = os.path.join(destination_path, first_day.base + "_" + working_set[-1].base)
			print 'combining tower files to be stored in ', date_dir
			if not os.path.exists(date_dir):
					os.makedirs(date_dir)
			all_towers,tower_to_paths = get_intersecting_towers_map([imap.directory for imap in combMaps])
			for tower in all_towers:
				map_dump_loc = os.path.join(date_dir, tower)
				if os.path.isfile(map_dump_loc):
					continue
				days_paths = tower_to_paths[tower]
				print 'combining maps for', tower, 'for ', len(days_paths), 'days'
				combined_map = combine_days_for_tower(tower,days_paths)
				cPickle.dump(combined_map, open(map_dump_loc,'wb'))
		return True

	@classmethod
	def createInteractionMapsSet(cls,days_directory):
		return [InteractionMap(os.path.join(days_directory, path)) for path in sorted(os.listdir(days_directory))]



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
	first_day_tower_pairing = os.path.join(first_day_path, tower_file)
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
