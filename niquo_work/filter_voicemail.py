import cPickle
import networkx
import json
import csv
import itertools
import RawCRDData as raw


filtered_data = '../niquo_data/filtered_data/06_2017_no_data.csv'
outgoing_only = '../niquo_data/filtered_data/outgoingonly_calls_june.csv'
intersecting_callers = ''
voicemail_map = '../niquo_data/filtered_data/voicemail_map.p'
sorted_rows = '../niquo_data/filtered_data/sorted_rows.p'
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
	print 'data sorted.'
	print 'cycling through values...'
	with open(outgoing_only, 'wb') as outfile:
		while (i+1) < len(all_values):
			first = all_values[i]
			second = all_values[i+1]
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
			i += 1
			print "currently on ", i ,'/',len(all_values)
	print 'wrote ', count, 'objects to file'





def filter_calls_voicemail(csv_data=filtered_data):
	raw_data = raw.RawCDRCSV(csv_data)
	count = 0
	with open(outgoing_only, 'wb') as outfile:
		for row in raw_data.rows_generator():
			start_time = row[start_index]
			end_time = row[end_index]
			caller = row[caller_index]
			receiver = row[receiver_index]
			comm_type = row[comm_type_index]

			if (start_time == end_time) and (comm_type == 'MOC'):
				obj = {'caller': caller, 'receiver': receiver, 'time': start_time}
				json.dump(obj, outfile)
				outfile.write('\n')
				count += 1

			elif (start_time == end_time) and (comm_type == 'MTC'):
				obj = {'caller': receiver, 'receiver': caller, 'time': start_time}
				json.dump(obj, outfile)
				outfile.write('\n')
				count += 1
		print 'wrote', count,'new lines of possible voicemail calls'

def create_voicemail_dict(json_file):
	user_hash_dict = {}
	possiblilites = [val for val in json_generator(json_file)]
	times_dict = {}
	overwritten_keys = set([])
	for val in possiblilites:
		time = val['time']
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
					print 'overwriting data for key: ',first['receiver']
					overwritten_keys.add(first['receiver'])
				user_hash_dict[first['receiver']] = second['receiver']
			elif second['receiver'] == first['caller']:
				if (second['receiver'] in user_hash_dict) and user_hash_dict[second['receiver']] != first['receiver']:
					print 'overwriting data for key: ',first['receiver']
					overwritten_keys.add(second['receiver'])
				user_hash_dict[second['receiver']] = first['receiver']
	return user_hash_dict,overwritten_keys




def json_generator(json_filename):
	with open(json_filename) as infile:
		for line in infile:
			yield json.loads(line)


def main():
	filter_calls_voicemail()

if __name__ == '__main__':
    main()