import csv
import os
import extractData as ex
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
		t_lat = tower[lat]
		t_lon = tower[lon]
		tower_id = tower[t_index]
		lat_lon = (t_lat,t_lon)
		try:
   			geo_map[int(tower_id)] = lat_lon
   		except Exception e:
   			pass

	return geo_map


def tower_map_id():
	geo_map = id_to_lat_long()
	loc_id = {}
	tower_map = {}
	for tower_id,lat_lon in geo_map.iteritems():
		if tower_id not in loc_id:
			loc_id[lat_lon] = tower_id
			tower_map[tower_id] = tower_id
		else:
			tower_map[tower_id] = loc_id[lat_lon]
	return tower_map


def tower_to_activity():
	tower_map = tower_map_id()
	# TODO: MAKE THIS HAPPEN
	return True 