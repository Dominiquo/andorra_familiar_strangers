import csv
import os
import extractData as ex
import cPickle
import itertools
from datetime import datetime
import time



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
		tower_path = os.path.join(week_path, tower)
		all_maps[tower] = cPickle.load(open(tower_path,'rb'))
		if tower_count > 5:
			break

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

def time_difference(first_time,second_time):
	format_string = "%Y.%m.%d %H:%M:%S"
	time_1 = datetime.strptime(first_time,format_string)
	time_2 = datetime.strptime(second_time,format_string)
	diff = time_2 - time_1
	return diff.days,diff.seconds

	