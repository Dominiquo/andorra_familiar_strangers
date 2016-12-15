import cPickle
import networkx
import json
import csv
import itertools
import RawCRDData as raw


filtered_data = '../niquo_data/filtered_data/06_2017_no_data.csv'
outgoing_only = '../niquo_data/filtered_data/outgoingonly_calls_june.csv'
voicemail_map = '../niquo_data/filtered_data/voicemail_map.p'
sorted_rows = '../niquo_data/filtered_data/sorted_rows.p'
network_raw_obj = '../niquo_data/filtered_data/network_object.p'
comm_type_index = 10
caller_index = 0
receiver_index = 16
start_index = 3
end_index = 4

def walk_through_pairs(csv_data=filtered_data):
	all_values = []
	count = 0
	i = 0
	raw_data = raw.RawCDRCSV(csv_data)
	for row in raw_data.rows_generator():
		all_values.append(row)
	print 'sorting data..'
	all_values.sort(key=lambda row: row[3])
	print 'all values count:', len(all_values)
	print 'data sorted.'
	print 'cycling through values...'
	outfile = open(outgoing_only, 'wb')
	while (i+1) < len(all_values):
		first = all_values[i]
		second = all_values[i+1]
		i += 1
		if (i % 1000) == 0:
			print "currently on ", float(i)/len(all_values)*100,'%'
		f_start = first[start_index]
		f_end = first[end_index]
		s_start = second[start_index]
		s_end = second[end_index]
		if (f_start == s_start) and (f_end == s_end):
			f_comm = first[comm_type_index]
			s_comm = second[comm_type_index]
			if f_comm == 'MTC':
				f_caller = first[receiver_index]
				f_receiver = first[caller_index]
			elif f_comm == 'MOC':
				f_caller = first[caller_index]
				f_receiver = first[receiver_index]
			else:
				continue

			if s_comm == 'MTC':
				s_caller = second[receiver_index]
				s_reciever = second[caller_index]	
			elif s_comm == 'MOC':
				s_caller = second[caller_index]
				s_reciever = second[receiver_index]
			else:
				continue

			if (f_receiver == s_caller) or (s_reciever == f_caller):
				count += 1
				obj1 = {'caller': f_caller, 'receiver': f_receiver, 's_time': f_start, 'e_time': f_end}
				obj2 = {'caller': s_caller, 'receiver': s_reciever, 's_time': s_start, 'e_time': s_end}
				json.dump(obj1,outfile)
				outfile.write('\n')
				json.dump(obj2,outfile)
				outfile.write('\n')
	print 'wrote ', count, 'objects to file'



def create_voicemail_dict(json_file):
	user_hash_dict = {}
	possiblilites = [val for val in json_generator(json_file)]
	times_dict = {}
	overwritten_keys = set([])
	for val in possiblilites:
		time = (val['s_time'],val['e_time'])
		if time in times_dict:
			times_dict[time].append(val)
		else:
			times_dict[time] = [val]

	for time,values in times_dict.iteritems():
		if len(values) < 2:
			continue
		for first, second in itertools.combinations(values, 2):
			if first['receiver'] == second['caller']:
				if (first['receiver'] in user_hash_dict) and user_hash_dict[first['receiver']] != second['receiver']:
					overwritten_keys.add(first['receiver'])
				user_hash_dict[first['receiver']] = second['receiver']
			elif second['receiver'] == first['caller']:
				if (second['receiver'] in user_hash_dict) and user_hash_dict[second['receiver']] != first['receiver']:
					overwritten_keys.add(second['receiver'])
				user_hash_dict[second['receiver']] = first['receiver']
	
	for rep_key in overwritten_keys:
		user_hash_dict.pop(rep_key,None)

	return user_hash_dict


def create_new_graph(csv_file, user_map, cleaned_path, threshold=100):
	csvData = raw.RawCDRCSV(csv_file)
	# friend_graph = nx.Graph()
	caller_index = 0
	receiver_index = 16
	remapped_rec = 0
	diff_rec = 0
	same_rec = 0
	normal = 0

	for row in csvData.rows_generator():
		caller = row[caller_index]
		receiver = row[receiver_index]
		if receiver in user_map:
			remapped_rec += 1
			# friend_graph.add_edge(caller,user_map[receiver])
		elif caller in user_map and (user_map[caller] != receiver ):
			diff_rec += 1
		elif caller in user_map and (user_map[caller] == receiver):
			same_rec += 1
		else:
			normal += 1

	print 'receiver in map', remapped_rec
	print 'caller in map but not same receiver', diff_rec
	print 'call in map and same receiver ', same_rec
	print 'normal entries ', normal
	# cPickle.dump(friend_graph, open(store_path,'wb'))
	return True



def json_generator(json_filename):
	with open(json_filename) as infile:
		for line in infile:
			yield json.loads(line)


def main():
	user_hash_dict = create_voicemail_dict(outgoing_only)
	new_network = '../niquo_data/filtered_data/network_remapped.p'
	create_new_graph(filtered_data, user_hash_dict, new_network)

if __name__ == '__main__':
    main()