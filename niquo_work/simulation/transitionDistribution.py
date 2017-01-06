import pandas as pd
import numpy as np
import cPickle
import os


raw_data = '../../niquo_data/filtered_data/06_2017_no_data.csv'
id_latlon_path = '../../niquo_data/filtered_data/id_to_latlon.p'
latlon_act_path = '../../niquo_data/filtered_data/latlon_to_activities.p'
usecols = ['DS_CDNUMORIGEN','DT_CDDATAINICI','ID_CELLA_INI','ID_CDTIPUSCOM','ID_CDOPERADORORIGEN','DS_CDNUMDESTI','TAC_IMEI']
id_latlon = cPickle.load(open(id_latlon_path))
latlon_activity = cPickle.load(open(latlon_act_path))

def get_data(data_path,limit=None):
	if limit:
		data = pd.read_csv(data_path, sep=';', index_col=False, usecols=usecols,nrows=limit)
	else:
		data = pd.read_csv(data_path, sep=';', index_col=False, usecols=usecols)
	# add latlon row to dataframe
	data['latlon'] = data.apply(get_latlon,axis=1)
	return data

def get_latlon(row):
	val = str(row['ID_CELLA_INI'])
	if val in id_latlon:
		return id_latlon[val]
	else:
		return None

def get_basic_tower_dist(data):
	count = 0
	latlon_count = {}
	data['latlon'] = data.apply(get_latlon,axis=1)
	for name,group in data.groupby('latlon'):
		latlon_count[name] = len(group)
		count += len(group)
	return {key:float(val)/count for key,val in latlon_count.iteritems()}

def get_minute_tower_dist(data):
	# add row for just the minute of a start time
	data['minute_start'] = data.apply(lambda row: row['DT_CDDATAINICI'][11:16], axis=1)
	time_tower_dist = {}
	for name, group in data.groupby('minute_start'):
		time_tower_dist[name] = get_basic_tower_dist(group)
	return time_tower_dist


def location_type_dist(latlon_dist):
	loc_type_dist = {}
	total = 0
	for latlon,dist in latlon_dist.iteritems():
		locations = latlon_activity[latlon] 
		for loc in locations:
			total += dist
			if loc in loc_type_dist:
				loc_type_dist[loc] += dist
			else:
				loc_type_dist[loc] = dist
	return {key: float(val)/total for key,val in loc_type_dist.iteritems()}


def get_minute_location_dist(data):
	# add row for just the minute of a start time
	data['minute_start'] = data.apply(lambda row: row['DT_CDDATAINICI'][11:16], axis=1)
	time_loc_dist = {}
	for name, group in data.groupby('minute_start'):
		tower_dist = get_basic_tower_dist(group)
		time_loc_dist[name] = location_type_dist(tower_dist)
	return time_loc_dist


def get_transition_probability_dist(data):
	transition_dist = {}
	for caller, caller_dataframe in data.groupby('DS_CDNUMORIGEN'):
		visited_towers_set = set([])
		for index, row in caller_dataframe.sort_values(['DT_CDDATAINICI']).iterrows():
			latlon = row['latlon']
			if latlon != None:
				for source in visited_towers_set:
					udpate_trans_dist(transition_dist,source,latlon)
				visited_towers_set.add(latlon)
	return normalize_trans_dist(transition_dist)


def udpate_trans_dist(transition_dist,source,dest):
	if (source in transition_dist) and (dest in transition_dist[source]):
		transition_dist[source][dest] += 1
	elif (source in transition_dist) and (dest not in transition_dist[source]):
		transition_dist[source][dest] = 1
	elif (source not in transition_dist):
		transition_dist[source] = {dest: 1}


def normalize_trans_dist(transition_dist):
	normalized_dict = {}
	for source, source_dict in transition_dist.iteritems():
		factor = 1.0/sum(source_dict.itervalues())
		for dest,prev_val in source_dict.iteritems():
			normalized_dict[source][dest] = prev_val*factor
	return normalized_dict


def Main():
	dest_root = '../../niquo_data/filtered_data'
	data = get_data(raw_data) 
	basic_path = os.path.join(dest_root,'basic_tower_dist.p')
	cPickle.dump(get_basic_tower_dist(data),open(basic_path,'wb'))

	minute_path = os.path.join(dest_root,'minute_tower_dist.p')
	cPickle.dump(get_minute_tower_dist(data),open(minute_path,'wb'))

	basic_loc_path = os.path.join(dest_root,'basic_location_dist.p')
	cPickle.dump(location_type_dist(get_basic_tower_dist(data)), open(basic_loc_path,'wb'))

	minute_loc_path = os.path.join(dest_root,'minute_location_dist.p')
	cPickle.dump(get_minute_location_dist(data),open(minute_loc_path,'wb'))

	print "DONE."


if __name__ == '__main__':
    Main()