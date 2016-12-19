import json
import cPickle
import Network as net

def add_element_json(old_json, new_json):
	first_times = 'first_times'
	count = 0
	with open(old_json) as infile:
		with open(new_json, 'wb') as outfile:
			for line in infile:
				old_val = json.loads(line)
				times = old_val[first_times]
				encs_count = get_new_enc_count(times)
				old_val['encs_count'] = encs_count
				json.dump(old_val, outfile)
				outfile.write('\n')
				count += 1
	print 'wrote', count, 'new values to ', new_json


def get_new_enc_count(times_list):
	hour_index_cutoff = 13
	hours_set = set([t[:13] for t in times_list])
	return len(hours_set)

def get_new_hours_encs(times_list):
	hour_index_cutoff = 13
	hours_set = set([t[:13] for t in times_list])
	return [(t + ':00:00') for t in hours_set]

def update_distance_val_json(old_json,new_json,friendship_graph):
	print 'loading graph...'
	friend_graph = cPickle.load(open(friend_graph))
	distance_key = 'distance'
	caller_key = 'caller'
	caller_enc_key = 'caller_enc'
	print 'reading json file...'
	with open(old_json) as infile:
		with open(new_json, 'wb') as outfile:
			for line in infile:
				old_val = json.loads(line)
				prev_distance = old_val[distance_key]
				caller = old_val[caller_key]
				caller_enc = old_val[caller_enc_key]
				new_dis = net.get_graph_distance(caller, caller_enc, friendship_graph)
				old_val[distance_key] = new_dis
				json.dump(old_val, outfile)
				outfile.write('\n')
				count += 1
	print 'wrote', count, 'new values to ', new_json


def Main():
	new_graph = '../niquo_data/filtered_data/network_object_100_removed_voicemail_UPDATED.p'
	encounters_json = '../niquo_data/v3_data_root/encounters_files/2016.07.01_2016.07.07_encounter.json'
	new_encounters_json = '../niquo_data/v3_data_root/encounters_files/2016.07.01_2016.07.07_encounter_dist_UPDATED.json'
	update_distance_val_json()

if __name__ == '__main__':
    Main()