import pandas as pd
import numpy as np
import cPickle


raw_data = '../../niquo_data/filtered_data/06_2017_no_data.csv'
id_latlon_path = '../../niquo_data/filtered_data/id_to_latlon.p'
latlon_act_path = '../../niquo_data/filtered_data/latlon_to_activities.p'
usecols = ['DS_CDNUMORIGEN','DT_CDDATAINICI','ID_CELLA_INI','ID_CDTIPUSCOM','ID_CDOPERADORORIGEN','DS_CDNUMDESTI','TAC_IMEI']
id_latlon = cPickle.load(open(id_latlon_path))
latlon_activity = cPickle.load(open(latlon_act_path))

def get_data(limit=None):
	if limit:
		data = pd.read_csv(raw_data, sep=';', index_col=False, usecols=usecols,nrows=limit)
	else:
		data = pd.read_csv(raw_data, sep=';', index_col=False, usecols=usecols)
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
	return {key:float(val)/count for key,val in latlon_count.iteritems}

def get_minute_tower_dist(data):
	# add row for just the minute of a start tiem
	data['minute_start'] = data.apply(lambda row: row['DT_CDDATAINICI'][11:16])
	time_tower_dist = {}
	for name, group in data.groupby('minute_start'):
		time_tower_dist[name] = get_basic_tower_dist(group)
	return time_tower_dist

