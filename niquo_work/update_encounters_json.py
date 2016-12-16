import json

def add_element_json(old_json, new_json):
	first_times = 'first_times'
	count = 0
	with open(old_json) as infile:
		with open(new_json, 'wb') as outfile:
			for line in infile:
				old_val = json.loads(line)
				times = old_val[first_times]
				encs_count = get_mew_enc_count(times)
				old_val['encs_count'] = encs_count
				json.dump(old_val, outfile)
				outfile.write('\n')
				count += 1
	print 'wrote', count, 'new values to ', new_json


def get_mew_enc_count(times_list):
	hour_index_cutoff = 13
	hours_set = set([t[:13] for t in times_list])
	return len(hours_set)
