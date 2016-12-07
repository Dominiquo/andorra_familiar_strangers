import csv
import os
import cPickle
import itertools
from datetime import datetime
import time
import file_constants as constants


def id_to_lat_long(filepath=constants.TOWERS_ID):
	geo_map = {}
	lat_index = 2
	lon_index = 3
	t_index = 1
	with open(filepath) as tower_file:
		towers_data = [row for row in csv.reader(tower_file.read().splitlines())]

	for i,tower in enumerate(towers_data):
		if i == 0:
			continue
		t_lat = tower[lat_index]
		t_lon = tower[lon_index]
		tower_id = tower[t_index]
		lat_lon = (t_lat,t_lon)
		try:
   			geo_map[tower_id] = lat_lon
   		except Exception as e:
   			pass

	return geo_map


def tower_map_id():
	geo_map = id_to_lat_long()
	loc_id = {}
	tower_map = {}
	for tower_id,lat_lon in geo_map.iteritems():
		if lat_lon not in loc_id:
			loc_id[lat_lon] = tower_id
			tower_map[tower_id] = tower_id
		else:
			tower_map[tower_id] = loc_id[lat_lon]
	return tower_map


def tower_to_activity():
	activity_map = ()
	id_index = 0
	cat_index = 9
	with open(constants.TOWERS_TYPE,'rb') as csvfile:
		activity_data = [row for row in csv.reader(csvfile.read().splitlines())]

	for row in activity_data:
		category_vals = row[cat_index]
		tower_id = row[id_index]
		vals_set = set([cat for cat in category_vals.split(',')])
		activity_map[tower_id] = vals_set

	return activity_map


